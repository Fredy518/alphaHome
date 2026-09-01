"""Manager for ``pit.pit_industry_fapi_monthly``."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterable

import pandas as pd

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .calculators.industry_fapi_calculator import IndustryFAPICalculator


class PITIndustryFAPIManager(PITMonthlySnapshotManager):
    """Refresh source-adapted SW industry FAPI month-end facts."""

    DEFAULT_FULL_START = date(2014, 2, 28)
    DEFAULT_INCREMENTAL_MONTHS = 8
    DEFAULT_BACKFILL_BATCH_MONTHS = 12

    def __init__(self) -> None:
        super().__init__("pit_industry_fapi_monthly")
        self.calculator = IndustryFAPICalculator()

    def _ensure_table_exists(self) -> None:
        super()._ensure_table_exists()
        self._apply_idempotent_table_ddl()

    def incremental_update(
        self,
        months: int = DEFAULT_INCREMENTAL_MONTHS,
        batch_size: int | None = None,
        cutoff_date: date | str | pd.Timestamp | None = None,
    ) -> Dict[str, Any]:
        latest = self._latest_available_month(cutoff_date=cutoff_date)
        if latest is None:
            raise RuntimeError("行业FAPI上游没有共同可用的完整月份")
        requested = max(
            int(months or self.DEFAULT_INCREMENTAL_MONTHS),
            self.DEFAULT_INCREMENTAL_MONTHS,
        )
        target_months = self.incremental_months(requested, end_date=latest)
        target_months = [
            value for value in target_months if value >= self.DEFAULT_FULL_START
        ]
        return self._run_months(
            target_months, batch_size=batch_size, result_key="updated_records"
        )

    def full_backfill(
        self,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        batch_size: int = DEFAULT_BACKFILL_BATCH_MONTHS,
    ) -> Dict[str, Any]:
        latest = self._latest_available_month()
        if latest is None:
            raise RuntimeError("行业FAPI上游没有共同可用的完整月份")
        requested_start = self.as_month_end(start_date or self.DEFAULT_FULL_START)
        start = max(requested_start, self.DEFAULT_FULL_START)
        requested_end = self.as_month_end(end_date or latest)
        end = min(requested_end, latest)
        target_months = self.month_ends(start, end)

        # FAPI(t+1) uses the forecast snapshot at t as its fixed prior sample.
        # A bounded replay therefore closes one additional complete month.
        if target_months and end_date is not None:
            propagated = self.next_month_end(target_months[-1])
            if propagated <= latest and propagated not in target_months:
                target_months.append(propagated)
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
        source_max_report_date: Any = None
        equity_trade_date_max: Any = None
        benchmark_weight_trade_date_max: Any = None

        for offset in range(0, len(target_months), batch_months):
            months = target_months[offset : offset + batch_months]
            anchor = self.previous_month_end(min(months))
            stock_months = [anchor, *months]
            sources = self._load_sources(months, stock_months)
            self._validate_dependencies(sources, months, stock_months)
            calculated = self.calculator.calculate(
                sources["classifications"],
                sources["equity"],
                sources["benchmark_members"],
                sources["stock_fttm"],
                obs_dates=months,
            )
            persist = calculated.loc[
                calculated["obs_date"].isin(pd.to_datetime(months))
            ].copy()
            if not persist.empty:
                for column, current in (
                    ("source_max_report_date", source_max_report_date),
                    ("equity_trade_date", equity_trade_date_max),
                    (
                        "benchmark_weight_trade_date",
                        benchmark_weight_trade_date_max,
                    ),
                ):
                    value = persist[column].dropna().max()
                    if pd.notna(value):
                        selected = value if current is None else max(current, value)
                        if column == "source_max_report_date":
                            source_max_report_date = selected
                        elif column == "equity_trade_date":
                            equity_trade_date_max = selected
                        else:
                            benchmark_weight_trade_date_max = selected

            inserted = self._atomic_replace_months(
                persist,
                months,
                columns=IndustryFAPICalculator.OUTPUT_COLUMNS,
                primary_keys=(
                    "obs_date",
                    "classification_source",
                    "industry_level",
                    "industry_code",
                    "benchmark_code",
                    "method_version",
                ),
            )
            total_rows += inserted
            processed_months.extend(value.isoformat() for value in months)
            batch_audits.append(
                {
                    "months": [value.isoformat() for value in months],
                    "anchor_month": anchor.isoformat(),
                    **self.calculator.last_audit,
                }
            )
            self.logger.info(
                "行业FAPI月批次完成: %s ~ %s, rows=%s",
                min(months),
                max(months),
                inserted,
            )

        self.stats["processed_records"] += total_rows
        self.stats["success_records"] += total_rows
        return {
            result_key: total_rows,
            "processed_months": processed_months,
            "method_version": IndustryFAPICalculator.METHOD_VERSION,
            "org_weight_version": IndustryFAPICalculator.ORG_WEIGHT_VERSION,
            "quality_rule_version": IndustryFAPICalculator.QUALITY_RULE_VERSION,
            "source_max_report_date": source_max_report_date,
            "equity_trade_date_max": equity_trade_date_max,
            "benchmark_weight_trade_date_max": benchmark_weight_trade_date_max,
            "dependency_freshness": self._dependency_freshness(),
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "pit_semantics": (
                "t and t-1 use common stock-broker rows, current-month SW membership, "
                "current-month total_mv/pb book equity, and current CSI800 membership"
            ),
            "weight_limit": (
                "coverage-recency weight is an available-data adaptation; the source "
                "forecast-accuracy weight is unavailable and is not imputed"
            ),
        }

    def _load_sources(
        self, target_months: list[date], stock_months: list[date]
    ) -> dict[str, pd.DataFrame]:
        target_placeholders = ", ".join(["(%s::date)"] * len(target_months))
        stock_placeholders = ", ".join(["(%s::date)"] * len(stock_months))
        target_parameters = tuple(target_months)
        stock_parameters = tuple(stock_months)

        classifications = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {target_placeholders})
            SELECT c.ts_code, c.obs_date, c.data_source,
                   c.industry_code1, c.industry_level1,
                   c.industry_code2, c.industry_level2
            FROM pit.pit_industry_classification c
            JOIN requested r USING (obs_date)
            WHERE c.data_source = 'sw'
            """,
            target_parameters,
        )
        stock_fttm = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {stock_placeholders})
            SELECT s.obs_date, s.ts_code, s.org_name, s.fttm_np,
                   s.formula_version, s.selected_report_date
            FROM pit.pit_stock_fttm_monthly s
            JOIN requested r USING (obs_date)
            """,
            stock_parameters,
        )
        equity = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {target_placeholders}),
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
            WITH requested(obs_date) AS (VALUES {target_placeholders}),
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
            "classifications": classifications,
            "stock_fttm": stock_fttm,
            "equity": equity,
            "benchmark_members": benchmark_members,
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
        classifications = sources["classifications"]
        class_dates = cls._date_set(classifications, "obs_date")
        l1_dates = cls._date_set(
            classifications.loc[classifications["industry_code1"].notna()],
            "obs_date",
        )
        l2_dates = cls._date_set(
            classifications.loc[classifications["industry_code2"].notna()],
            "obs_date",
        )
        stock_dates = cls._date_set(sources["stock_fttm"], "obs_date")
        equity_obs_dates = cls._date_set(
            sources["equity"].loc[sources["equity"]["equity_trade_date"].notna()],
            "obs_date",
        )
        benchmark_obs_dates = cls._date_set(
            sources["benchmark_members"].loc[
                sources["benchmark_members"]["benchmark_weight_trade_date"].notna()
                & sources["benchmark_members"]["ts_code"].notna()
            ],
            "obs_date",
        )
        problems: list[str] = []
        if target - class_dates:
            problems.append("申万分类=" + cls._format_dates(target - class_dates))
        if target - l1_dates:
            problems.append("申万L1=" + cls._format_dates(target - l1_dates))
        if target - l2_dates:
            problems.append("申万L2=" + cls._format_dates(target - l2_dates))
        if stock_target - stock_dates:
            problems.append("个股FTTM=" + cls._format_dates(stock_target - stock_dates))
        if target - equity_obs_dates:
            problems.append("权益代理=" + cls._format_dates(target - equity_obs_dates))
        if target - benchmark_obs_dates:
            problems.append(
                "中证800成分=" + cls._format_dates(target - benchmark_obs_dates)
            )
        if problems:
            raise RuntimeError("行业FAPI依赖缺失: " + "; ".join(problems))

    @staticmethod
    def _format_dates(values: Iterable[pd.Timestamp]) -> str:
        return ",".join(sorted(value.date().isoformat() for value in values))

    def _latest_available_month(
        self, cutoff_date: date | str | pd.Timestamp | None = None
    ) -> date | None:
        latest_complete = self.complete_month_cutoff(cutoff_date)
        frame = self.context.query_dataframe(
            """
            SELECT
                (SELECT MAX(obs_date)::date
                 FROM pit.pit_industry_classification
                 WHERE data_source = 'sw'
                   AND industry_code1 IS NOT NULL
                   AND industry_code2 IS NOT NULL
                   AND obs_date <= %s) AS classification_max_obs_date,
                (SELECT MAX(obs_date)::date
                 FROM pit.pit_stock_fttm_monthly
                 WHERE obs_date <= %s) AS stock_fttm_max_obs_date
            """,
            (latest_complete, latest_complete),
        )
        if frame.empty:
            return None
        values = [
            frame.iloc[0]["classification_max_obs_date"],
            frame.iloc[0]["stock_fttm_max_obs_date"],
            latest_complete,
        ]
        if any(pd.isna(value) for value in values):
            return None
        return min(pd.Timestamp(value).date() for value in values)

    def _dependency_freshness(self) -> Dict[str, Any]:
        frame = self.context.query_dataframe(
            """
            SELECT
                (SELECT MAX(obs_date)::date FROM pit.pit_stock_fttm_monthly)
                    AS pit_stock_fttm_max_obs_date,
                (SELECT MAX(obs_date)::date
                 FROM pit.pit_industry_classification
                 WHERE data_source = 'sw')
                    AS pit_industry_classification_max_obs_date,
                (SELECT MAX(trade_date)::date FROM rawdata.stock_dailybasic)
                    AS stock_dailybasic_max_trade_date,
                (SELECT MAX(trade_date)::date
                 FROM rawdata.index_weight
                 WHERE index_code = '000906.SH')
                    AS csi800_weight_max_trade_date,
                (SELECT MAX(report_date)::date FROM rawdata.stock_report_rc)
                    AS stock_report_rc_max_report_date
            """
        )
        return {} if frame.empty else frame.iloc[0].to_dict()


__all__ = ["PITIndustryFAPIManager"]
