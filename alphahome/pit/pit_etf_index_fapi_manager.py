"""Manager for ``pit.pit_etf_index_fapi_monthly``."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Iterable, Sequence

import pandas as pd
from psycopg2.extras import execute_values

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .calculators.etf_index_fapi_calculator import ETFIndexFAPICalculator
from .calculators.etf_index_members_calculator import ETFIndexMembersCalculator


class PITETFIndexFAPIMonthlyManager(PITMonthlySnapshotManager):
    """Refresh ETF-index FAPI and expected-ROE month-end facts."""

    DEFAULT_FULL_START = date(2014, 2, 28)
    DEFAULT_INCREMENTAL_MONTHS = 8
    DEFAULT_BACKFILL_BATCH_MONTHS = 3
    MEMBER_METHOD_VERSION = ETFIndexMembersCalculator.METHOD_VERSION
    PIT_SEMANTICS = (
        "exact ETF-index PIT members at t; adjacent-month common "
        "stock-broker FTTM rows; current-month book equity; relative to CSI800"
    )
    WEIGHT_LIMIT = (
        "FAPI uses the exact constituent set and economic aggregation; "
        "index-weighted alternatives remain research-layer comparisons"
    )

    def __init__(self) -> None:
        super().__init__("pit_etf_index_fapi_monthly")
        self.calculator = ETFIndexFAPICalculator()

    def _ensure_table_exists(self) -> None:
        super()._ensure_table_exists()
        self._apply_idempotent_table_ddl()
        self._ensure_updated_at_triggers()

    def incremental_update(
        self,
        months: int = DEFAULT_INCREMENTAL_MONTHS,
        batch_size: int | None = None,
        index_codes: Sequence[str] | None = None,
        cutoff_date: date | str | pd.Timestamp | None = None,
    ) -> dict[str, Any]:
        latest = self._latest_available_month(cutoff_date=cutoff_date)
        if latest is None:
            raise RuntimeError("ETF指数FAPI上游没有共同可用的完整月份")
        requested = max(
            int(months or self.DEFAULT_INCREMENTAL_MONTHS),
            self.DEFAULT_INCREMENTAL_MONTHS,
        )
        target_months = self.incremental_months(requested, end_date=latest)
        target_months = [
            value for value in target_months if value >= self.DEFAULT_FULL_START
        ]
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
        latest = self._latest_available_month()
        if latest is None:
            raise RuntimeError("ETF指数FAPI上游没有共同可用的完整月份")
        start = max(
            self.as_month_end(start_date or self.DEFAULT_FULL_START),
            self.DEFAULT_FULL_START,
        )
        end = min(self.as_month_end(end_date or latest), latest)
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
        for offset in range(0, len(target_months), month_batch):
            months = target_months[offset : offset + month_batch]
            anchor = self.previous_month_end(min(months))
            stock_months = [anchor, *months]
            sources = self._load_sources(months, stock_months, codes)
            self._validate_dependencies(sources, months, stock_months)
            if sources["members"].empty:
                calculated = pd.DataFrame(
                    columns=self.calculator.INDEX_OUTPUT_COLUMNS
                )
                self.calculator.last_audit = {
                    "requested_month_count": len(months),
                    "requested_index_count": len(codes),
                    "requested_pair_count": len(months) * len(codes),
                    "missing_member_pair_count": len(months) * len(codes),
                }
            else:
                calculated = self.calculator.calculate(
                    sources["members"],
                    sources["equity"],
                    sources["benchmark_members"],
                    sources["stock_fttm"],
                    obs_dates=months,
                )
            self._validate_output(calculated, months, codes)
            inserted = self._atomic_replace_scope(calculated, months, codes)
            total_rows += inserted
            processed_months.extend(value.isoformat() for value in months)
            batch_audits.append(
                {
                    "months": [value.isoformat() for value in months],
                    "anchor_month": anchor.isoformat(),
                    **self.calculator.last_audit,
                    "output_row_count": int(len(calculated)),
                    "eligible_row_count": int(calculated["is_eligible"].sum())
                    if not calculated.empty
                    else 0,
                }
            )
            self.logger.info(
                "ETF指数FAPI月批次完成: %s ~ %s, indices=%s, rows=%s",
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
            "method_version": self.calculator.METHOD_VERSION,
            "member_method_version": self.MEMBER_METHOD_VERSION,
            "dependency_freshness": self._dependency_freshness(),
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "pit_semantics": self.PIT_SEMANTICS,
            "weight_limit": self.WEIGHT_LIMIT,
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
            FROM pit.pit_etf_index_members_monthly
            WHERE method_version = %s
            ORDER BY index_code
            """,
            (self.MEMBER_METHOD_VERSION,),
        )
        if frame is None or frame.empty:
            return []
        return sorted(frame["index_code"].astype(str).str.strip().unique().tolist())

    def _load_sources(
        self,
        target_months: list[date],
        stock_months: list[date],
        index_codes: list[str],
    ) -> dict[str, pd.DataFrame]:
        target_values = ", ".join(["(%s::date)"] * len(target_months))
        stock_values = ", ".join(["(%s::date)"] * len(stock_months))
        target_parameters = tuple(target_months)
        stock_parameters = tuple(stock_months)
        members = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {target_values})
            SELECT m.obs_date, m.index_code, m.index_name, m.ts_code,
                   m.weight_basis, m.weight_source, m.source_code,
                   m.source_effective_date, m.source_available_date,
                   m.source_staleness_days, m.source_coverage_rate,
                   m.source_quality, m.is_fallback, m.is_eligible,
                   m.quality_reasons, m.constituent_scope, m.is_proxy,
                   m.scope_weight_rate
            FROM pit.pit_etf_index_members_monthly m
            JOIN requested r USING (obs_date)
            WHERE m.index_code = ANY(%s)
              AND m.method_version = %s
            ORDER BY m.obs_date, m.index_code, m.ts_code
            """,
            target_parameters
            + (index_codes, self.MEMBER_METHOD_VERSION),
        )
        stock_fttm = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {stock_values})
            SELECT s.obs_date, s.ts_code, s.org_name, s.fttm_np,
                   s.formula_version, s.selected_report_date
            FROM pit.pit_stock_fttm_monthly s
            JOIN requested r USING (obs_date)
            """,
            stock_parameters,
        )
        equity = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {target_values}),
            equity_dates AS (
                SELECT r.obs_date,
                       MAX(d.trade_date)::date AS equity_trade_date
                FROM requested r
                LEFT JOIN rawdata.stock_dailybasic d
                  ON d.trade_date <= r.obs_date
                 AND d.trade_date >= r.obs_date - INTERVAL '31 days'
                GROUP BY r.obs_date
            )
            SELECT e.obs_date, d.ts_code, e.equity_trade_date,
                   d.total_mv, d.pb
            FROM equity_dates e
            LEFT JOIN rawdata.stock_dailybasic d
              ON d.trade_date = e.equity_trade_date
            """,
            target_parameters,
        )
        benchmark_members = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {target_values}),
            weight_dates AS (
                SELECT r.obs_date,
                       MAX(w.trade_date)::date AS benchmark_weight_trade_date
                FROM requested r
                LEFT JOIN rawdata.index_weight w
                  ON w.index_code = '000906.SH'
                 AND w.trade_date <= r.obs_date
                 AND w.trade_date >= r.obs_date - INTERVAL '65 days'
                GROUP BY r.obs_date
            )
            SELECT d.obs_date,
                   '000906.SH'::text AS benchmark_code,
                   '中证800'::text AS benchmark_name,
                   d.benchmark_weight_trade_date,
                   w.con_code AS ts_code,
                   w.weight
            FROM weight_dates d
            LEFT JOIN rawdata.index_weight w
              ON w.index_code = '000906.SH'
             AND w.trade_date = d.benchmark_weight_trade_date
            """,
            target_parameters,
        )
        return {
            "members": members if members is not None else pd.DataFrame(),
            "stock_fttm": stock_fttm
            if stock_fttm is not None
            else pd.DataFrame(),
            "equity": equity if equity is not None else pd.DataFrame(),
            "benchmark_members": benchmark_members
            if benchmark_members is not None
            else pd.DataFrame(),
        }

    @staticmethod
    def _date_set(frame: pd.DataFrame, column: str) -> set[pd.Timestamp]:
        if column not in frame:
            return set()
        return set(
            pd.to_datetime(frame[column], errors="coerce").dropna().dt.normalize()
        )

    @classmethod
    def _validate_dependencies(
        cls,
        sources: dict[str, pd.DataFrame],
        target_months: Iterable[date],
        stock_months: Iterable[date],
    ) -> None:
        target = {pd.Timestamp(value).normalize() for value in target_months}
        stock_target = {pd.Timestamp(value).normalize() for value in stock_months}
        stock_dates = cls._date_set(sources["stock_fttm"], "obs_date")
        equity_dates = cls._date_set(
            sources["equity"].loc[
                sources["equity"]["equity_trade_date"].notna()
            ],
            "obs_date",
        )
        benchmark_dates = cls._date_set(
            sources["benchmark_members"].loc[
                sources["benchmark_members"]["benchmark_weight_trade_date"].notna()
                & sources["benchmark_members"]["ts_code"].notna()
            ],
            "obs_date",
        )
        problems: list[str] = []
        if stock_target - stock_dates:
            problems.append(
                "个股FTTM=" + cls._format_dates(stock_target - stock_dates)
            )
        if target - equity_dates:
            problems.append("权益代理=" + cls._format_dates(target - equity_dates))
        if target - benchmark_dates:
            problems.append(
                "中证800成分=" + cls._format_dates(target - benchmark_dates)
            )
        if problems:
            raise RuntimeError("ETF指数FAPI依赖缺失: " + "; ".join(problems))

    @staticmethod
    def _format_dates(values: Iterable[pd.Timestamp]) -> str:
        return ",".join(sorted(value.date().isoformat() for value in values))

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
        if set(output["obs_date"]) - expected_dates:
            raise ValueError("ETF指数FAPI结果含目标范围外月份")
        if set(output["index_code"].astype(str)) - expected_codes:
            raise ValueError("ETF指数FAPI结果含目标范围外指数")
        keys = ["obs_date", "index_code", "benchmark_code", "method_version"]
        duplicates = int(output.duplicated(keys, keep=False).sum())
        if duplicates:
            raise ValueError(f"ETF指数FAPI结果主键重复: {duplicates}")
        future = (
            pd.to_datetime(output["member_source_available_date"])
            > pd.to_datetime(output["obs_date"])
        ) | (
            pd.to_datetime(output["source_max_report_date"])
            > pd.to_datetime(output["obs_date"])
        )
        if bool(future.fillna(False).any()):
            raise ValueError("ETF指数FAPI结果使用了观察日之后的信息")

    def _atomic_replace_scope(
        self,
        frame: pd.DataFrame,
        obs_dates: Sequence[date],
        index_codes: Sequence[str],
    ) -> int:
        columns = self.calculator.INDEX_OUTPUT_COLUMNS
        dates = sorted({pd.Timestamp(value).date() for value in obs_dates})
        codes = sorted({str(value) for value in index_codes})
        data = frame.reindex(columns=columns).copy()
        if not data.empty:
            for column in (
                "obs_date",
                "member_source_effective_date",
                "member_source_available_date",
                "equity_trade_date",
                "benchmark_weight_trade_date",
                "source_max_report_date",
                "previous_source_max_report_date",
            ):
                data[column] = pd.to_datetime(data[column], errors="coerce").dt.date

        relation = '"pit"."pit_etf_index_fapi_monthly"'
        staging = f"staging_pit_etf_index_fapi_{uuid.uuid4().hex}"
        quoted_staging = f'"{staging}"'
        quoted_columns = ", ".join(f'"{column}"' for column in columns)
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
                    (dates, codes, self.calculator.METHOD_VERSION),
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

    def _latest_available_month(
        self, cutoff_date: date | str | pd.Timestamp | None = None
    ) -> date | None:
        latest_complete = self.complete_month_cutoff(cutoff_date)
        frame = self.context.query_dataframe(
            """
            SELECT
                (SELECT MAX(obs_date)::date
                 FROM pit.pit_etf_index_members_monthly
                 WHERE method_version = %s
                   AND obs_date <= %s) AS member_max_obs_date,
                (SELECT MAX(obs_date)::date
                 FROM pit.pit_stock_fttm_monthly
                 WHERE obs_date <= %s) AS stock_fttm_max_obs_date
            """,
            (
                self.MEMBER_METHOD_VERSION,
                latest_complete,
                latest_complete,
            ),
        )
        if frame is None or frame.empty:
            return None
        values = [
            frame.iloc[0]["member_max_obs_date"],
            frame.iloc[0]["stock_fttm_max_obs_date"],
            latest_complete,
        ]
        if any(pd.isna(value) for value in values):
            return None
        return min(pd.Timestamp(value).date() for value in values)

    def _dependency_freshness(self) -> dict[str, Any]:
        frame = self.context.query_dataframe(
            """
            SELECT
                (SELECT MAX(obs_date)::date
                 FROM pit.pit_etf_index_members_monthly) AS member_max_obs_date,
                (SELECT MAX(obs_date)::date
                 FROM pit.pit_stock_fttm_monthly) AS stock_fttm_max_obs_date,
                (SELECT MAX(trade_date)::date
                 FROM rawdata.stock_dailybasic) AS stock_dailybasic_max_trade_date,
                (SELECT MAX(trade_date)::date
                 FROM rawdata.index_weight
                 WHERE index_code = '000906.SH') AS csi800_weight_max_trade_date,
                (SELECT MAX(report_date)::date
                 FROM rawdata.stock_report_rc) AS stock_report_rc_max_report_date
            """
        )
        return {} if frame is None or frame.empty else frame.iloc[0].to_dict()


__all__ = ["PITETFIndexFAPIMonthlyManager"]
