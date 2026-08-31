"""Manager for ``pit.pit_stock_consensus_fy_monthly``."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict

import pandas as pd

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .calculators.stock_consensus_fy_calculator import StockConsensusFYCalculator


class PITStockConsensusFYMonthlyManager(PITMonthlySnapshotManager):
    DEFAULT_FULL_START = date(2010, 1, 31)
    DEFAULT_INCREMENTAL_MONTHS = 8
    DEFAULT_BACKFILL_BATCH_MONTHS = 12

    def __init__(self) -> None:
        super().__init__("pit_stock_consensus_fy_monthly")
        self.calculator = StockConsensusFYCalculator()

    def incremental_update(
        self,
        months: int = DEFAULT_INCREMENTAL_MONTHS,
        batch_size: int | None = None,
    ) -> Dict[str, Any]:
        requested = max(
            int(months or self.DEFAULT_INCREMENTAL_MONTHS),
            self.DEFAULT_INCREMENTAL_MONTHS,
        )
        target_months = self.incremental_months(requested)
        return self._run_months(
            target_months,
            batch_size=batch_size,
            result_key="updated_records",
        )

    def full_backfill(
        self,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        batch_size: int = DEFAULT_BACKFILL_BATCH_MONTHS,
    ) -> Dict[str, Any]:
        start = self.as_month_end(start_date or self.DEFAULT_FULL_START)
        latest = self.latest_complete_month()
        requested_end = self.as_month_end(end_date or latest)
        end = min(requested_end, latest)
        target_months = self.month_ends(start, end)
        return self._run_months(
            target_months,
            batch_size=batch_size,
            result_key="backfilled_records",
        )

    def _run_months(
        self,
        target_months: list[date],
        batch_size: int | None,
        result_key: str,
    ) -> Dict[str, Any]:
        if not target_months:
            return {
                result_key: 0,
                "processed_months": [],
                "message": "无可处理的完整月份",
            }
        self._ensure_table_exists()
        batch_months = max(int(batch_size or self.DEFAULT_BACKFILL_BATCH_MONTHS), 1)

        total_rows = 0
        processed_months: list[str] = []
        batch_audits: list[dict[str, Any]] = []
        for offset in range(0, len(target_months), batch_months):
            months = target_months[offset : offset + batch_months]
            calculation_months = self._calculation_months(months)
            source_start = (
                pd.Timestamp(min(calculation_months))
                - pd.DateOffset(months=StockConsensusFYCalculator.VISIBILITY_MONTHS)
            ).date()
            source_end = max(months)
            forecasts = self._load_forecasts(source_start, source_end)
            calculated = self.calculator.calculate(forecasts, calculation_months)
            month_values = {pd.Timestamp(value) for value in months}
            snapshots = calculated.loc[calculated["obs_date"].isin(month_values)].copy()
            inserted = self._atomic_replace_months(
                snapshots,
                months,
                columns=StockConsensusFYCalculator.OUTPUT_COLUMNS,
                primary_keys=("obs_date", "ts_code", "target_year"),
            )
            total_rows += inserted
            processed_months.extend(value.isoformat() for value in months)
            batch_audits.append(
                {
                    "months": [value.isoformat() for value in months],
                    "calculation_months": [
                        value.isoformat() for value in calculation_months
                    ],
                    **self.calculator.last_audit,
                }
            )
            self.logger.info(
                "固定财年一致预期月批次完成: %s ~ %s, rows=%s",
                min(months),
                max(months),
                inserted,
            )

        self.stats["processed_records"] += total_rows
        self.stats["success_records"] += total_rows
        source_freshness = self._source_freshness()
        return {
            result_key: total_rows,
            "processed_months": processed_months,
            "formula_version": StockConsensusFYCalculator.FORMULA_VERSION,
            "availability_basis": StockConsensusFYCalculator.AVAILABILITY_BASIS,
            "source_freshness": source_freshness,
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "historical_pit_limit": (
                "historical availability is reconstructed from report_date; "
                "stock_report_rc create_time is a backfill timestamp and is not used"
            ),
        }

    @staticmethod
    def _calculation_months(target_months: list[date]) -> list[date]:
        values = {pd.Timestamp(value) for value in target_months}
        for value in target_months:
            stamp = pd.Timestamp(value)
            values.add(stamp - pd.offsets.MonthEnd(1))
            values.add(stamp - pd.offsets.MonthEnd(3))
        return [value.date() for value in sorted(values)]

    def _load_forecasts(self, start_date: date, end_date: date) -> pd.DataFrame:
        return self.context.query_dataframe(
            """
            SELECT ts_code,
                   org_name,
                   author_name,
                   report_date,
                   quarter,
                   np,
                   eps,
                   report_title,
                   report_type,
                   rating,
                   create_time,
                   update_time
            FROM rawdata.stock_report_rc
            WHERE report_date >= %s
              AND report_date <= %s
              AND quarter ~ '^[0-9]{4}Q4$'
              AND (np IS NOT NULL OR eps IS NOT NULL)
            """,
            (start_date, end_date),
        )

    def _source_freshness(self) -> Dict[str, Any]:
        frame = self.context.query_dataframe(
            """
            SELECT MAX(report_date)::date AS stock_report_rc_max_report_date,
                   MAX(update_time) AS stock_report_rc_max_update_time
            FROM rawdata.stock_report_rc
            """
        )
        return {} if frame.empty else frame.iloc[0].to_dict()


__all__ = ["PITStockConsensusFYMonthlyManager"]
