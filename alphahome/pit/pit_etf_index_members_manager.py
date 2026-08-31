"""Manager for ``pit.pit_etf_index_members_monthly``."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Sequence

import pandas as pd
from psycopg2.extras import execute_values

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .calculators.etf_index_members_calculator import ETFIndexMembersCalculator


class PITETFIndexMembersMonthlyManager(PITMonthlySnapshotManager):
    """Build PIT constituent snapshots for ETF-tracked indices."""

    DEFAULT_FULL_START = date(2014, 1, 31)
    DEFAULT_INCREMENTAL_MONTHS = 8
    DEFAULT_BACKFILL_BATCH_MONTHS = 6
    HOLDING_QUERY_LOOKBACK_DAYS = 730

    def __init__(self) -> None:
        super().__init__("pit_etf_index_members_monthly")
        self.calculator = ETFIndexMembersCalculator()

    def _ensure_table_exists(self) -> None:
        super()._ensure_table_exists()
        self._apply_idempotent_table_ddl()
        self._ensure_updated_at_triggers()

    def incremental_update(
        self,
        months: int = DEFAULT_INCREMENTAL_MONTHS,
        batch_size: int | None = None,
        index_codes: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        requested = max(int(months or self.DEFAULT_INCREMENTAL_MONTHS), 1)
        latest = self.latest_complete_month()
        target_months = self.incremental_months(requested, end_date=latest)
        return self._run_months(
            target_months,
            batch_size=batch_size,
            index_codes=index_codes,
            result_key="updated_records",
        )

    def full_backfill(
        self,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        batch_size: int = DEFAULT_BACKFILL_BATCH_MONTHS,
        index_codes: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        start = self.as_month_end(start_date or self.DEFAULT_FULL_START)
        end = self.as_month_end(end_date or self.latest_complete_month())
        target_months = self.month_ends(start, end)
        return self._run_months(
            target_months,
            batch_size=batch_size,
            index_codes=index_codes,
            result_key="backfilled_records",
        )

    def _run_months(
        self,
        target_months: list[date],
        *,
        batch_size: int | None,
        index_codes: Sequence[str] | None,
        result_key: str,
    ) -> dict[str, Any]:
        codes = self._resolve_index_codes(index_codes)
        if not target_months or not codes:
            return {
                result_key: 0,
                "processed_months": [],
                "processed_index_count": len(codes),
                "message": "无可处理的月份或ETF跟踪指数",
            }

        self._ensure_table_exists()
        month_batch = max(
            int(batch_size or self.DEFAULT_BACKFILL_BATCH_MONTHS), 1
        )
        total_rows = 0
        processed_months: list[str] = []
        batch_audits: list[dict[str, Any]] = []
        total_source_pairs = {
            "official_index_weight": 0,
            "etf_disclosed_holding": 0,
            "unavailable": 0,
        }

        for offset in range(0, len(target_months), month_batch):
            months = target_months[offset : offset + month_batch]
            sources = self._load_sources(months, codes)
            calculated = self.calculator.calculate(
                sources["official_weights"],
                sources["fund_holdings"],
                months,
                codes,
            )
            self._validate_output(calculated, months, codes)
            inserted = self._atomic_replace_scope(calculated, months, codes)
            total_rows += inserted
            processed_months.extend(value.isoformat() for value in months)
            audit = {
                "months": [value.isoformat() for value in months],
                **self.calculator.last_audit,
            }
            batch_audits.append(audit)
            for key in total_source_pairs:
                total_source_pairs[key] += int(
                    audit.get("source_pair_counts", {}).get(key, 0)
                )
            self.logger.info(
                "ETF指数成分月批次完成: %s ~ %s, indices=%s, rows=%s",
                min(months),
                max(months),
                len(codes),
                inserted,
            )

        self.stats["processed_records"] += total_rows
        self.stats["success_records"] += total_rows
        return {
            result_key: total_rows,
            "processed_months": processed_months,
            "processed_index_count": len(codes),
            "method_version": ETFIndexMembersCalculator.METHOD_VERSION,
            "source_pair_counts": total_source_pairs,
            "dependency_freshness": self._dependency_freshness(codes),
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "pit_semantics": (
                "official index weights use the latest complete snapshot no later "
                "than obs_date; ETF holdings use only rows with ann_date <= obs_date "
                "and combine visible disclosures for the selected report period"
            ),
            "fallback_semantics": (
                "ETF holdings are a labelled lower-tier proxy and never overwrite "
                "a valid official index-weight snapshot"
            ),
        }

    def _resolve_index_codes(
        self, index_codes: Sequence[str] | None
    ) -> list[str]:
        if index_codes is not None:
            return sorted(
                {
                    str(value).strip()
                    for value in index_codes
                    if str(value).strip()
                }
            )
        frame = self.context.query_dataframe(
            """
            SELECT DISTINCT index_code
            FROM rawdata.fund_etf_basic
            WHERE status = 'L'
              AND etf_type = '纯境内'
              AND index_code IS NOT NULL
              AND btrim(index_code) <> ''
            ORDER BY index_code
            """
        )
        if frame is None or frame.empty:
            return []
        return sorted(frame["index_code"].astype(str).str.strip().unique().tolist())

    def _load_sources(
        self, obs_dates: list[date], index_codes: list[str]
    ) -> dict[str, pd.DataFrame]:
        min_obs = min(obs_dates)
        max_obs = max(obs_dates)
        names = self._load_index_names(index_codes)
        official = self.context.query_dataframe(
            """
            SELECT index_code, trade_date AS weight_trade_date,
                   con_code AS ts_code, weight AS raw_weight
            FROM rawdata.index_weight
            WHERE index_code = ANY(%s)
              AND trade_date BETWEEN %s AND %s
            ORDER BY index_code, trade_date, con_code
            """,
            (
                index_codes,
                min_obs
                - timedelta(
                    days=self.calculator.threshold.official_max_staleness_days
                ),
                max_obs,
            ),
        )
        holdings = self.context.query_dataframe(
            """
            SELECT f.index_code, f.ts_code AS etf_code,
                   p.ann_date, p.end_date, p.symbol AS ts_code,
                   p.stk_mkv_ratio AS raw_weight
            FROM rawdata.fund_etf_basic f
            JOIN rawdata.fund_portfolio p ON p.ts_code = f.ts_code
            WHERE f.status = 'L'
              AND f.index_code = ANY(%s)
              AND p.ann_date <= %s
              AND p.end_date <= %s
              AND p.end_date >= %s
              AND p.stk_mkv_ratio IS NOT NULL
              AND p.stk_mkv_ratio > 0
            ORDER BY f.index_code, f.ts_code, p.end_date, p.ann_date, p.symbol
            """,
            (
                index_codes,
                max_obs,
                max_obs,
                min_obs - timedelta(days=self.HOLDING_QUERY_LOOKBACK_DAYS),
            ),
        )
        if official is None or official.empty:
            official = pd.DataFrame(
                columns=[
                    "index_code",
                    "weight_trade_date",
                    "ts_code",
                    "raw_weight",
                ]
            )
        if holdings is None or holdings.empty:
            holdings = pd.DataFrame(
                columns=[
                    "index_code",
                    "etf_code",
                    "ann_date",
                    "end_date",
                    "ts_code",
                    "raw_weight",
                ]
            )
        for frame in (official, holdings):
            frame["index_name"] = frame["index_code"].map(names).fillna(
                frame["index_code"]
            )
        return {
            "official_weights": official,
            "fund_holdings": holdings,
        }

    def _load_index_names(self, index_codes: list[str]) -> dict[str, str]:
        frame = self.context.query_dataframe(
            """
            WITH requested(index_code) AS (
                SELECT unnest(%s::text[])
            ),
            etf_names AS (
                SELECT index_code, MIN(benchmark) AS benchmark
                FROM rawdata.fund_etf_basic
                WHERE index_code = ANY(%s)
                GROUP BY index_code
            )
            SELECT r.index_code,
                   COALESCE(MAX(i.index_name), MAX(i.indx_csname),
                            MAX(e.benchmark), r.index_code) AS index_name
            FROM requested r
            LEFT JOIN rawdata.fund_etf_index i ON i.ts_code = r.index_code
            LEFT JOIN etf_names e ON e.index_code = r.index_code
            GROUP BY r.index_code
            ORDER BY r.index_code
            """,
            (index_codes, index_codes),
        )
        if frame is None or frame.empty:
            return {code: code for code in index_codes}
        return {
            str(row.index_code): str(row.index_name or row.index_code)
            for row in frame.itertuples(index=False)
        }

    @staticmethod
    def _validate_output(
        output: pd.DataFrame,
        target_months: Iterable[date],
        index_codes: Iterable[str],
    ) -> None:
        if output.empty:
            return
        expected_dates = {pd.Timestamp(value).normalize() for value in target_months}
        expected_codes = {str(value) for value in index_codes}
        actual_dates = set(pd.to_datetime(output["obs_date"]).dt.normalize())
        actual_codes = set(output["index_code"].astype(str))
        if actual_dates - expected_dates:
            raise ValueError("ETF指数成分结果含目标范围外月份")
        if actual_codes - expected_codes:
            raise ValueError("ETF指数成分结果含目标范围外指数")
        keys = ["obs_date", "index_code", "ts_code", "method_version"]
        duplicates = int(output.duplicated(keys, keep=False).sum())
        if duplicates:
            raise ValueError(f"ETF指数成分结果主键重复: {duplicates}")
        sums = output.groupby(["obs_date", "index_code"], sort=False)["weight"].sum()
        if not sums.between(0.999999, 1.000001, inclusive="both").all():
            raise ValueError("ETF指数成分归一化权重不等于1")
        if (pd.to_datetime(output["source_available_date"]) > pd.to_datetime(output["obs_date"])).any():
            raise ValueError("ETF指数成分使用了观察日之后才可见的来源")

    def _atomic_replace_scope(
        self,
        frame: pd.DataFrame,
        obs_dates: Sequence[date],
        index_codes: Sequence[str],
    ) -> int:
        dates = sorted({pd.Timestamp(value).date() for value in obs_dates})
        codes = sorted({str(value) for value in index_codes})
        data = frame.reindex(columns=ETFIndexMembersCalculator.OUTPUT_COLUMNS).copy()
        if not data.empty:
            data["obs_date"] = pd.to_datetime(data["obs_date"]).dt.date
            for column in ("source_effective_date", "source_available_date"):
                data[column] = pd.to_datetime(data[column]).dt.date

        relation = '"pit"."pit_etf_index_members_monthly"'
        staging = f"staging_pit_etf_index_members_{uuid.uuid4().hex}"
        quoted_staging = f'"{staging}"'
        quoted_columns = ", ".join(
            f'"{column}"' for column in ETFIndexMembersCalculator.OUTPUT_COLUMNS
        )
        connection = self.context.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TEMP TABLE {quoted_staging} "
                    f"(LIKE {relation} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP"
                )
                if not data.empty:
                    records = [
                        tuple(self._postgres_value(value) for value in row)
                        for row in data.itertuples(index=False, name=None)
                    ]
                    execute_values(
                        cursor,
                        f"INSERT INTO {quoted_staging} ({quoted_columns}) VALUES %s",
                        records,
                        page_size=max(int(self.batch_size or 1000), 1),
                    )
                cursor.execute(
                    f"DELETE FROM {relation} "
                    "WHERE obs_date = ANY(%s) "
                    "  AND index_code = ANY(%s) "
                    "  AND method_version = %s",
                    (dates, codes, ETFIndexMembersCalculator.METHOD_VERSION),
                )
                cursor.execute(f"SELECT COUNT(*) FROM {quoted_staging}")
                staged_count = int(cursor.fetchone()[0])
                if staged_count:
                    cursor.execute(
                        f"INSERT INTO {relation} ({quoted_columns}) "
                        f"SELECT {quoted_columns} FROM {quoted_staging}"
                    )
            connection.commit()
            return staged_count
        except Exception:
            connection.rollback()
            raise

    def _dependency_freshness(self, index_codes: Sequence[str]) -> dict[str, Any]:
        frame = self.context.query_dataframe(
            """
            SELECT
                (SELECT MAX(trade_date)::date FROM rawdata.index_weight
                 WHERE index_code = ANY(%s)) AS index_weight_max_trade_date,
                (SELECT MAX(ann_date)::date
                 FROM rawdata.fund_portfolio p
                 JOIN rawdata.fund_etf_basic f ON f.ts_code = p.ts_code
                 WHERE f.index_code = ANY(%s)) AS fund_portfolio_max_ann_date,
                (SELECT MAX(end_date)::date
                 FROM rawdata.fund_portfolio p
                 JOIN rawdata.fund_etf_basic f ON f.ts_code = p.ts_code
                 WHERE f.index_code = ANY(%s)) AS fund_portfolio_max_end_date
            """,
            (list(index_codes), list(index_codes), list(index_codes)),
        )
        return {} if frame is None or frame.empty else frame.iloc[0].to_dict()


__all__ = ["PITETFIndexMembersMonthlyManager"]
