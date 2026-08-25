"""Manager for ``pit.pit_stock_fttm_monthly``."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict

import pandas as pd

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .calculators.stock_fttm_calculator import StockFTTMCalculator


def load_stock_fttm_forecast_sources(
    context: Any, start_date: date, end_date: date
) -> pd.DataFrame:
    """Load report events with exact-report-date shares for FTTM calculation."""
    return context.query_dataframe(
        """
        SELECT
            r.ts_code,
            r.org_name,
            r.author_name,
            r.report_date,
            r.quarter,
            r.np,
            r.eps,
            d.total_share,
            r.ann_date,
            r.end_date,
            r.report_title,
            r.report_type,
            r.classify,
            r.create_time,
            r.update_time
        FROM rawdata.stock_report_rc r
        LEFT JOIN rawdata.stock_dailybasic d
          ON d.ts_code = r.ts_code
         AND d.trade_date = r.report_date
        WHERE r.report_date >= %s
          AND r.report_date <= %s
        """,
        (start_date, end_date),
    )


class PITStockFTTMManager(PITMonthlySnapshotManager):
    DEFAULT_INCREMENTAL_MONTHS = 8
    DEFAULT_BACKFILL_BATCH_MONTHS = 12

    def __init__(self) -> None:
        super().__init__("pit_stock_fttm_monthly")
        self.calculator = StockFTTMCalculator()

    def incremental_update(
        self, months: int = DEFAULT_INCREMENTAL_MONTHS, batch_size: int | None = None
    ) -> Dict[str, Any]:
        requested = max(int(months or self.DEFAULT_INCREMENTAL_MONTHS), self.DEFAULT_INCREMENTAL_MONTHS)
        target_months = self.incremental_months(requested)
        return self._run_months(target_months, batch_size=batch_size, result_key="updated_records")

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
            return {result_key: 0, "processed_months": [], "message": "无可处理的完整月份"}
        batch_months = max(int(batch_size or self.DEFAULT_BACKFILL_BATCH_MONTHS), 1)
        self._ensure_table_exists()

        total_rows = 0
        batch_audits: list[dict[str, Any]] = []
        processed_months: list[str] = []
        for offset in range(0, len(target_months), batch_months):
            months = target_months[offset : offset + batch_months]
            source_start = (pd.Timestamp(min(months)) - pd.DateOffset(months=6)).date()
            source_end = max(months)
            forecasts = self._load_forecasts(source_start, source_end)
            snapshots = self.calculator.calculate(forecasts, months)
            inserted = self._atomic_replace_months(
                snapshots,
                months,
                columns=StockFTTMCalculator.OUTPUT_COLUMNS,
                primary_keys=("ts_code", "org_name", "obs_date"),
            )
            total_rows += inserted
            processed_months.extend(value.isoformat() for value in months)
            batch_audits.append(
                {
                    "months": [value.isoformat() for value in months],
                    **self.calculator.last_audit,
                }
            )
            self.logger.info(
                "FTTM个股月批次完成: %s ~ %s, rows=%s",
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
            "formula_version": StockFTTMCalculator.FORMULA_VERSION,
            "source_max_report_date": source_freshness.get("stock_report_rc_max_report_date"),
            "source_freshness": source_freshness,
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "revision_limit": (
                "rolling incremental covers at least eight complete months; "
                "older source corrections require manual_range/full audit because no verified row change log is used"
            ),
        }

    def _load_forecasts(self, start_date: date, end_date: date) -> pd.DataFrame:
        return load_stock_fttm_forecast_sources(self.context, start_date, end_date)

    def _source_freshness(self) -> Dict[str, Any]:
        frame = self.context.query_dataframe(
            """
            SELECT
                (SELECT MAX(report_date)::date FROM rawdata.stock_report_rc)
                    AS stock_report_rc_max_report_date,
                (SELECT MAX(trade_date)::date FROM rawdata.stock_dailybasic)
                    AS stock_dailybasic_max_trade_date
            """
        )
        return {} if frame.empty else frame.iloc[0].to_dict()


__all__ = ["PITStockFTTMManager", "load_stock_fttm_forecast_sources"]
