"""Manager for ``pit.pit_industry_fttm_monthly``."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterable

import pandas as pd

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .calculators.industry_fttm_calculator import IndustryFTTMCalculator
from .calculators.stock_fttm_calculator import StockFTTMCalculator
from .pit_stock_fttm_manager import load_stock_fttm_forecast_sources


class PITIndustryFTTMManager(PITMonthlySnapshotManager):
    DEFAULT_INCREMENTAL_MONTHS = 8
    DEFAULT_BACKFILL_BATCH_MONTHS = 12

    def __init__(self) -> None:
        super().__init__("pit_industry_fttm_monthly")
        self.calculator = IndustryFTTMCalculator()

    def _ensure_table_exists(self) -> None:
        super()._ensure_table_exists()
        # Early handoff drafts used varchar(32), while the fixed V1 aggregation
        # identifier itself is 34 characters. Upgrade only affected deployments.
        column = self.context.query_dataframe(
            """
            SELECT character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'pit'
              AND table_name = 'pit_industry_fttm_monthly'
              AND column_name = 'aggregation_version'
            """
        )
        if (
            not column.empty
            and column.iloc[0]["character_maximum_length"] is not None
            and int(column.iloc[0]["character_maximum_length"]) < 64
        ):
            self.context.db_manager.execute_sync(
                """
                ALTER TABLE pit.pit_industry_fttm_monthly
                ALTER COLUMN aggregation_version TYPE varchar(64)
                """
            )

    def incremental_update(
        self, months: int = DEFAULT_INCREMENTAL_MONTHS, batch_size: int | None = None
    ) -> Dict[str, Any]:
        latest = self._latest_available_month()
        if latest is None:
            raise RuntimeError("pit_industry_classification 没有可用的申万月末快照")
        requested = max(int(months or self.DEFAULT_INCREMENTAL_MONTHS), self.DEFAULT_INCREMENTAL_MONTHS)
        target_months = self.incremental_months(requested, end_date=latest)
        return self._run_months(target_months, batch_size=batch_size, result_key="updated_records")

    def full_backfill(
        self,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        batch_size: int = DEFAULT_BACKFILL_BATCH_MONTHS,
    ) -> Dict[str, Any]:
        latest = self._latest_available_month()
        if latest is None:
            raise RuntimeError("pit_industry_classification 没有可用的申万月末快照")
        start = self.as_month_end(start_date or self.DEFAULT_FULL_START)
        requested_end = self.as_month_end(end_date or latest)
        end = min(requested_end, latest)
        target_months = self.month_ends(start, end)

        # A bounded historical replay can alter t+1 momentum/diffusion.  Extend
        # that closure when the next complete dependency month exists.
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
            return {result_key: 0, "processed_months": [], "message": "无可处理的完整月份"}
        self._ensure_table_exists()
        batch_months = max(int(batch_size or self.DEFAULT_BACKFILL_BATCH_MONTHS), 1)
        total_rows = 0
        selected_source_max: Any = None
        selected_weight_date_max: Any = None
        processed_months: list[str] = []
        batch_audits: list[dict[str, Any]] = []

        for offset in range(0, len(target_months), batch_months):
            months = target_months[offset : offset + batch_months]
            anchor = self.previous_month_end(min(months))
            calculation_months = [anchor, *months]
            sources = self._load_sources(calculation_months)
            sources["stock_fttm"], anchor_stock_source = self._ensure_stock_anchor(
                sources["stock_fttm"], anchor
            )
            self._validate_dependencies(sources["classifications"], sources["stock_fttm"], months)
            calculated = self.calculator.calculate(
                sources["classifications"],
                sources["stock_basic"],
                sources["weights"],
                sources["stock_fttm"],
                obs_dates=calculation_months,
            )
            persist = calculated.loc[
                calculated["obs_date"].isin(pd.to_datetime(months))
            ].copy()
            if not persist.empty:
                batch_source_max = persist["source_max_report_date"].dropna().max()
                batch_weight_max = persist["weight_trade_date"].dropna().max()
                if pd.notna(batch_source_max):
                    selected_source_max = (
                        batch_source_max
                        if selected_source_max is None
                        else max(selected_source_max, batch_source_max)
                    )
                if pd.notna(batch_weight_max):
                    selected_weight_date_max = (
                        batch_weight_max
                        if selected_weight_date_max is None
                        else max(selected_weight_date_max, batch_weight_max)
                    )
            inserted = self._atomic_replace_months(
                persist,
                months,
                columns=IndustryFTTMCalculator.OUTPUT_COLUMNS,
                primary_keys=(
                    "obs_date",
                    "classification_source",
                    "industry_level",
                    "industry_code",
                    "weight_basis",
                ),
            )
            total_rows += inserted
            processed_months.extend(value.isoformat() for value in months)
            batch_audits.append(
                {
                    "months": [value.isoformat() for value in months],
                    "anchor_month": anchor.isoformat(),
                    "anchor_stock_source": anchor_stock_source,
                    **self.calculator.last_audit,
                }
            )
            self.logger.info(
                "FTTM行业月批次完成: %s ~ %s, rows=%s",
                min(months),
                max(months),
                inserted,
            )

        self.stats["processed_records"] += total_rows
        self.stats["success_records"] += total_rows
        return {
            result_key: total_rows,
            "processed_months": processed_months,
            "aggregation_version": IndustryFTTMCalculator.AGGREGATION_VERSION,
            "quality_rule_version": IndustryFTTMCalculator.QUALITY_RULE_VERSION,
            "source_max_report_date": selected_source_max,
            "weight_trade_date_max": selected_weight_date_max,
            "dependency_freshness": self._dependency_freshness(),
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "revision_limit": (
                "rolling incremental covers at least eight complete months and t+1 within the planned closure; "
                "older source corrections require manual_range/full audit because no verified row change log is used"
            ),
        }

    def _ensure_stock_anchor(
        self, stock_fttm: pd.DataFrame, anchor: date
    ) -> tuple[pd.DataFrame, str]:
        anchor_stamp = pd.Timestamp(anchor).normalize()
        existing_dates = (
            set(pd.to_datetime(stock_fttm["obs_date"], errors="coerce").dropna())
            if "obs_date" in stock_fttm
            else set()
        )
        if anchor_stamp in existing_dates:
            return stock_fttm, "persisted_pit_stock_fttm"

        transient = self._calculate_transient_stock_month(anchor)
        if transient.empty:
            return stock_fttm, "unavailable"
        required_columns = [
            "obs_date",
            "ts_code",
            "org_name",
            "fttm_np",
            "formula_version",
            "selected_report_date",
        ]
        combined = pd.concat(
            [stock_fttm.reindex(columns=required_columns), transient[required_columns]],
            ignore_index=True,
        )
        combined["obs_date"] = pd.to_datetime(
            combined["obs_date"], errors="coerce"
        ).dt.normalize()
        combined = combined.sort_values(
            ["obs_date", "ts_code", "org_name"], kind="mergesort"
        ).drop_duplicates(["obs_date", "ts_code", "org_name"], keep="last")
        return combined.reset_index(drop=True), "recomputed_from_raw_read_only"

    def _calculate_transient_stock_month(self, anchor: date) -> pd.DataFrame:
        source_start = (pd.Timestamp(anchor) - pd.DateOffset(months=6)).date()
        forecasts = load_stock_fttm_forecast_sources(
            self.context, source_start, anchor
        )
        return StockFTTMCalculator().calculate(forecasts, [anchor])

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
                (SELECT MAX(report_date)::date FROM rawdata.stock_report_rc)
                    AS stock_report_rc_max_report_date
            """
        )
        return {} if frame.empty else frame.iloc[0].to_dict()

    def _latest_available_month(self) -> date | None:
        latest_complete = self.latest_complete_month()
        frame = self.context.query_dataframe(
            """
            SELECT MAX(obs_date)::date AS max_date
            FROM pit.pit_industry_classification
            WHERE data_source = 'sw'
              AND industry_code1 IS NOT NULL
              AND industry_code2 IS NOT NULL
              AND obs_date <= %s
            """,
            (latest_complete,),
        )
        if frame.empty or pd.isna(frame.iloc[0]["max_date"]):
            return None
        return pd.Timestamp(frame.iloc[0]["max_date"]).date()

    def _load_sources(self, obs_dates: list[date]) -> dict[str, pd.DataFrame]:
        placeholders = ", ".join(["(%s::date)"] * len(obs_dates))
        parameters = tuple(obs_dates)
        classifications = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {placeholders})
            SELECT c.ts_code, c.obs_date, c.data_source,
                   c.industry_code1, c.industry_level1,
                   c.industry_code2, c.industry_level2
            FROM pit.pit_industry_classification c
            JOIN requested r USING (obs_date)
            WHERE c.data_source = 'sw'
            """,
            parameters,
        )
        stock_fttm = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {placeholders})
            SELECT s.obs_date, s.ts_code, s.org_name, s.fttm_np,
                   s.formula_version, s.selected_report_date
            FROM pit.pit_stock_fttm_monthly s
            JOIN requested r USING (obs_date)
            """,
            parameters,
        )
        weights = self.context.query_dataframe(
            f"""
            WITH requested(obs_date) AS (VALUES {placeholders}),
            weight_dates AS (
                SELECT r.obs_date, MAX(d.trade_date)::date AS weight_trade_date
                FROM requested r
                LEFT JOIN rawdata.stock_dailybasic d
                  ON d.trade_date <= r.obs_date
                 AND d.trade_date >= r.obs_date - INTERVAL '31 days'
                GROUP BY r.obs_date
            )
            SELECT w.obs_date, d.ts_code, w.weight_trade_date, d.total_mv
            FROM weight_dates w
            LEFT JOIN rawdata.stock_dailybasic d
              ON d.trade_date = w.weight_trade_date
            """,
            parameters,
        )
        stock_basic = self.context.query_dataframe(
            """
            SELECT ts_code, list_date, delist_date, exchange, curr_type
            FROM rawdata.stock_basic
            """
        )
        return {
            "classifications": classifications,
            "stock_fttm": stock_fttm,
            "weights": weights,
            "stock_basic": stock_basic,
        }

    @staticmethod
    def _validate_dependencies(
        classifications: pd.DataFrame,
        stock_fttm: pd.DataFrame,
        target_months: Iterable[date],
    ) -> None:
        target = {pd.Timestamp(value).normalize() for value in target_months}
        class_dates = (
            set(pd.to_datetime(classifications["obs_date"], errors="coerce").dropna())
            if "obs_date" in classifications
            else set()
        )
        l1_dates = (
            set(
                pd.to_datetime(
                    classifications.loc[
                        classifications["industry_code1"].notna(), "obs_date"
                    ],
                    errors="coerce",
                ).dropna()
            )
            if {"obs_date", "industry_code1"} <= set(classifications.columns)
            else set()
        )
        l2_dates = (
            set(
                pd.to_datetime(
                    classifications.loc[
                        classifications["industry_code2"].notna(), "obs_date"
                    ],
                    errors="coerce",
                ).dropna()
            )
            if {"obs_date", "industry_code2"} <= set(classifications.columns)
            else set()
        )
        stock_dates = (
            set(pd.to_datetime(stock_fttm["obs_date"], errors="coerce").dropna())
            if "obs_date" in stock_fttm
            else set()
        )
        missing_class = sorted(target - class_dates)
        missing_l1 = sorted(target - l1_dates)
        missing_l2 = sorted(target - l2_dates)
        missing_stock = sorted(target - stock_dates)
        if missing_class:
            raise RuntimeError(
                "行业FTTM依赖缺失: pit_industry_classification sw 月份 "
                + ", ".join(value.date().isoformat() for value in missing_class)
            )
        if missing_l1 or missing_l2:
            details = []
            if missing_l1:
                details.append(
                    "L1=" + ",".join(value.date().isoformat() for value in missing_l1)
                )
            if missing_l2:
                details.append(
                    "L2=" + ",".join(value.date().isoformat() for value in missing_l2)
                )
            raise RuntimeError(
                "行业FTTM依赖缺失: pit_industry_classification sw 层级 "
                + "; ".join(details)
            )
        if missing_stock:
            raise RuntimeError(
                "行业FTTM依赖缺失: pit_stock_fttm_monthly 月份 "
                + ", ".join(value.date().isoformat() for value in missing_stock)
            )


__all__ = ["PITIndustryFTTMManager"]
