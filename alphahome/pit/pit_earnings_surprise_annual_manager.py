"""Manager for ``pit.pit_earnings_surprise_annual``."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Dict, Sequence

import pandas as pd
from psycopg2.extras import execute_values

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .base.pit_config import PITConfig
from .base.pit_table_manager import PITTableManager
from .calculators.annual_earnings_surprise_calculator import (
    AnnualEarningsSurpriseCalculator,
)


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


class PITEarningsSurpriseAnnualManager(PITTableManager):
    DEFAULT_FULL_START = date(2010, 1, 1)
    DEFAULT_INCREMENTAL_DAYS = 550
    DEFAULT_BATCH_YEARS = 2
    OUTPUT_MIGRATION_COLUMNS = {
        "actual_source_row_count": "integer",
        "actual_source_value_conflict": "boolean",
        "actual_source_update_time": "timestamp without time zone",
        "actual_source_selection_basis": "varchar(64)",
    }

    def __init__(self) -> None:
        super().__init__("pit_earnings_surprise_annual")
        self.calculator = AnnualEarningsSurpriseCalculator()

    def _ensure_table_exists(self) -> None:
        super()._ensure_table_exists()
        alter_parts = [
            f"ADD COLUMN IF NOT EXISTS {column} {column_type}"
            for column, column_type in self.OUTPUT_MIGRATION_COLUMNS.items()
        ]
        self.context.db_manager.execute_sync(
            f"ALTER TABLE {PITConfig.PIT_SCHEMA}.{self.table_name} "
            f"{', '.join(alter_parts)}"
        )

    def incremental_update(
        self,
        days: int = DEFAULT_INCREMENTAL_DAYS,
        batch_size: int | None = None,
    ) -> Dict[str, Any]:
        requested = max(int(days or self.DEFAULT_INCREMENTAL_DAYS), 1)
        start_date, end_date = self.resolve_incremental_date_range(
            requested,
            (
                (
                    f"{PITConfig.PIT_SCHEMA}.pit_income_quarterly",
                    ("ann_date",),
                    "updated_at",
                ),
                (
                    f"{PITConfig.PIT_SCHEMA}.pit_stock_consensus_fy_monthly",
                    ("obs_date",),
                    "updated_at",
                ),
            ),
        )
        return self._run_range(
            date.fromisoformat(start_date),
            date.fromisoformat(end_date),
            batch_size=batch_size,
            result_key="updated_records",
        )

    def full_backfill(
        self,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        batch_size: int = DEFAULT_BATCH_YEARS,
    ) -> Dict[str, Any]:
        start = pd.Timestamp(start_date or self.DEFAULT_FULL_START).date()
        end = pd.Timestamp(end_date or datetime.now().date()).date()
        return self._run_range(
            start,
            end,
            batch_size=batch_size,
            result_key="backfilled_records",
        )

    def _run_range(
        self,
        start_date: date,
        end_date: date,
        batch_size: int | None,
        result_key: str,
    ) -> Dict[str, Any]:
        if start_date > end_date:
            return {result_key: 0, "processed_ranges": [], "message": "日期范围为空"}
        self._ensure_table_exists()
        self._validate_source_schema()
        batch_years = max(int(batch_size or self.DEFAULT_BATCH_YEARS), 1)

        total_rows = 0
        processed_ranges: list[list[str]] = []
        batch_audits: list[dict[str, Any]] = []
        cursor = pd.Timestamp(start_date)
        final = pd.Timestamp(end_date)
        while cursor <= final:
            batch_end = min(
                cursor + pd.DateOffset(years=batch_years) - pd.Timedelta(days=1), final
            )
            batch_start_date = cursor.date()
            batch_end_date = batch_end.date()
            actuals = self._load_actuals(batch_start_date, batch_end_date)
            if actuals.empty:
                self.calculator.last_audit = {
                    "source_actual_row_count": 0,
                    "actual_event_count": 0,
                    "matched_consensus_count": 0,
                    "eligible_count": 0,
                }
                calculated = pd.DataFrame(
                    columns=AnnualEarningsSurpriseCalculator.OUTPUT_COLUMNS
                )
            else:
                consensus = self._load_consensus(actuals)
                calculated = self.calculator.calculate(actuals, consensus)

            inserted = self._atomic_replace_ann_range(
                calculated,
                batch_start_date,
                batch_end_date,
                columns=AnnualEarningsSurpriseCalculator.OUTPUT_COLUMNS,
                primary_keys=("ts_code", "end_date", "ann_date"),
            )
            total_rows += inserted
            processed_ranges.append(
                [batch_start_date.isoformat(), batch_end_date.isoformat()]
            )
            batch_audits.append(
                {
                    "start_date": batch_start_date.isoformat(),
                    "end_date": batch_end_date.isoformat(),
                    **self.calculator.last_audit,
                }
            )
            cursor = batch_end + pd.Timedelta(days=1)

        self.stats["processed_records"] += total_rows
        self.stats["success_records"] += total_rows
        return {
            result_key: total_rows,
            "processed_ranges": processed_ranges,
            "formula_version": AnnualEarningsSurpriseCalculator.FORMULA_VERSION,
            "source_freshness": self._source_freshness(),
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "pit_rule": "first formal annual report matched to latest month-end consensus strictly before ann_date",
        }

    def _validate_source_schema(self) -> None:
        income_columns = set(
            self._get_table_columns(PITConfig.PIT_SCHEMA, "pit_income_quarterly")
        )
        required_income = {
            "n_income_attr_p_ytd",
            "basic_eps_ytd",
            "diluted_eps_ytd",
            "report_source_row_count",
            "report_source_value_conflict",
            "report_source_update_time",
            "report_source_selection_basis",
        }
        missing_income = sorted(required_income - income_columns)
        if missing_income:
            raise RuntimeError(
                "pit_income_quarterly 缺少年度实际值字段，请先执行利润表任务迁移: "
                f"{missing_income}"
            )
        consensus_columns = set(
            self._get_table_columns(
                PITConfig.PIT_SCHEMA, "pit_stock_consensus_fy_monthly"
            )
        )
        required_consensus = {
            "obs_date",
            "ts_code",
            "target_year",
            "np_consensus_median",
            "eps_consensus_median",
            "org_count",
            "np_org_count",
            "np_dispersion_rate",
            "is_eligible",
            "availability_basis",
            "source_max_report_date",
        }
        missing_consensus = sorted(required_consensus - consensus_columns)
        if missing_consensus:
            raise RuntimeError(
                f"pit_stock_consensus_fy_monthly 缺少字段: {missing_consensus}"
            )

    def _load_actuals(self, start_date: date, end_date: date) -> pd.DataFrame:
        return self.context.query_dataframe(
            f"""
            WITH ranked AS (
                SELECT ts_code,
                       end_date,
                       ann_date,
                       n_income_attr_p_ytd AS actual_np_yuan,
                       basic_eps_ytd AS actual_basic_eps,
                       diluted_eps_ytd AS actual_diluted_eps,
                       report_source_row_count AS actual_source_row_count,
                       report_source_value_conflict AS actual_source_value_conflict,
                       report_source_update_time AS actual_source_update_time,
                       report_source_selection_basis AS actual_source_selection_basis,
                       updated_at AS source_income_updated_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY ts_code, end_date
                           ORDER BY ann_date, updated_at
                       ) AS event_rank
                FROM {PITConfig.PIT_SCHEMA}.pit_income_quarterly
                WHERE data_source = 'report'
                  AND EXTRACT(MONTH FROM end_date) = 12
                  AND EXTRACT(DAY FROM end_date) = 31
            )
            SELECT ts_code,
                   end_date,
                   ann_date,
                   actual_np_yuan,
                   actual_basic_eps,
                   actual_diluted_eps,
                   actual_source_row_count,
                   actual_source_value_conflict,
                   actual_source_update_time,
                   actual_source_selection_basis,
                   source_income_updated_at
            FROM ranked
            WHERE event_rank = 1
              AND ann_date >= %s
              AND ann_date <= %s
            ORDER BY ann_date, ts_code, end_date
            """,
            (start_date, end_date),
        )

    def _load_consensus(self, actuals: pd.DataFrame) -> pd.DataFrame:
        events = actuals[["ts_code", "end_date", "ann_date"]].copy()
        events["target_year"] = pd.to_datetime(events["end_date"]).dt.year.astype(int)
        ts_codes = events["ts_code"].astype(str).tolist()
        target_years = events["target_year"].astype(int).tolist()
        ann_dates = pd.to_datetime(events["ann_date"]).dt.date.tolist()
        return self.context.query_dataframe(
            f"""
            WITH events AS (
                SELECT *
                FROM UNNEST(%s::text[], %s::integer[], %s::date[])
                    AS event(ts_code, target_year, ann_date)
            )
            SELECT matched.obs_date,
                   event.ts_code,
                   event.target_year,
                   matched.np_consensus_median,
                   matched.eps_consensus_median,
                   matched.org_count,
                   matched.np_org_count,
                   matched.np_dispersion_rate,
                   matched.is_eligible,
                   matched.availability_basis,
                   matched.source_max_report_date
            FROM events event
            JOIN LATERAL (
                SELECT consensus.*
                FROM {PITConfig.PIT_SCHEMA}.pit_stock_consensus_fy_monthly consensus
                WHERE consensus.ts_code = event.ts_code
                  AND consensus.target_year = event.target_year
                  AND consensus.obs_date < event.ann_date
                ORDER BY consensus.obs_date DESC
                LIMIT 1
            ) matched ON TRUE
            ORDER BY event.ts_code, event.target_year
            """,
            (ts_codes, target_years, ann_dates),
        )

    def _atomic_replace_ann_range(
        self,
        frame: pd.DataFrame,
        start_date: date,
        end_date: date,
        columns: Sequence[str],
        primary_keys: Sequence[str],
    ) -> int:
        data = frame.reindex(columns=columns).copy()
        if not data.empty:
            data["ann_date"] = pd.to_datetime(data["ann_date"], errors="coerce").dt.date
            data["end_date"] = pd.to_datetime(data["end_date"], errors="coerce").dt.date
            outside = ~data["ann_date"].between(start_date, end_date)
            if outside.any():
                raise ValueError("staging 含目标公告范围外数据")
            if data[list(primary_keys)].isna().any(axis=None):
                raise ValueError("staging 主键存在空值")
            duplicates = int(data.duplicated(list(primary_keys), keep=False).sum())
            if duplicates:
                raise ValueError(f"staging 主键重复行: {duplicates}")

        schema = PITConfig.PIT_SCHEMA
        table = self.table_name
        relation = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
        staging = _quote_identifier(f"staging_{table}_{uuid.uuid4().hex}")
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        key_list = ", ".join(_quote_identifier(column) for column in primary_keys)
        connection = self.context.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TEMP TABLE {staging} "
                    f"(LIKE {relation} INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP"
                )
                if not data.empty:
                    records = [
                        tuple(
                            PITMonthlySnapshotManager._postgres_value(value)
                            for value in row
                        )
                        for row in data.itertuples(index=False, name=None)
                    ]
                    execute_values(
                        cursor,
                        f"INSERT INTO {staging} ({quoted_columns}) VALUES %s",
                        records,
                        page_size=max(int(self.batch_size or 1000), 1),
                    )
                cursor.execute(f"SELECT COUNT(*) FROM {staging}")
                staged_count = int(cursor.fetchone()[0])
                if staged_count != len(data):
                    raise RuntimeError(
                        f"staging 行数不一致: expected={len(data)}, actual={staged_count}"
                    )
                cursor.execute(
                    f"SELECT COUNT(*) FROM (SELECT {key_list}, COUNT(*) "
                    f"FROM {staging} GROUP BY {key_list} HAVING COUNT(*) > 1) d"
                )
                if int(cursor.fetchone()[0]):
                    raise RuntimeError("staging 主键重复")
                cursor.execute(
                    f"DELETE FROM {relation} WHERE ann_date BETWEEN %s AND %s",
                    (start_date, end_date),
                )
                if staged_count:
                    cursor.execute(
                        f"INSERT INTO {relation} ({quoted_columns}) "
                        f"SELECT {quoted_columns} FROM {staging}"
                    )
            connection.commit()
            return staged_count
        except Exception:
            connection.rollback()
            raise

    def _source_freshness(self) -> Dict[str, Any]:
        frame = self.context.query_dataframe(
            f"""
            SELECT
                (SELECT MAX(updated_at) FROM {PITConfig.PIT_SCHEMA}.pit_income_quarterly)
                    AS income_max_updated_at,
                (SELECT MAX(obs_date) FROM {PITConfig.PIT_SCHEMA}.pit_stock_consensus_fy_monthly)
                    AS consensus_max_obs_date,
                (SELECT MAX(updated_at) FROM {PITConfig.PIT_SCHEMA}.pit_stock_consensus_fy_monthly)
                    AS consensus_max_updated_at
            """
        )
        return {} if frame.empty else frame.iloc[0].to_dict()


__all__ = ["PITEarningsSurpriseAnnualManager"]
