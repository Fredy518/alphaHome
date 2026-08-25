"""Manager for ``pit.pit_index_fttm_monthly``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable

import pandas as pd

from .base.monthly_snapshot_manager import PITMonthlySnapshotManager
from .calculators.index_fttm_calculator import IndexFTTMCalculator
from .calculators.stock_fttm_calculator import StockFTTMCalculator
from .pit_stock_fttm_manager import load_stock_fttm_forecast_sources


@dataclass(frozen=True)
class IndexUniverseSpec:
    code: str
    name: str
    first_valid_weight_date: date


IMPORTANT_INDEX_SPECS: tuple[IndexUniverseSpec, ...] = (
    IndexUniverseSpec("000001.SH", "上证指数", date(2011, 9, 30)),
    IndexUniverseSpec("000016.SH", "上证50", date(2009, 4, 1)),
    IndexUniverseSpec("000300.SH", "沪深300", date(2016, 1, 29)),
    IndexUniverseSpec("000906.SH", "中证800", date(2008, 12, 23)),
    IndexUniverseSpec("000905.SH", "中证500", date(2008, 1, 31)),
    IndexUniverseSpec("000852.SH", "中证1000", date(2015, 5, 29)),
    IndexUniverseSpec("932000.CSI", "中证2000", date(2023, 8, 31)),
    IndexUniverseSpec("399303.SZ", "国证2000", date(2014, 3, 31)),
    IndexUniverseSpec("399006.SZ", "创业板指", date(2010, 6, 30)),
    IndexUniverseSpec("000688.SH", "科创50", date(2020, 7, 31)),
    IndexUniverseSpec("000510.SH", "中证A500", date(2024, 10, 31)),
    IndexUniverseSpec("000922.CSI", "中证红利", date(2017, 1, 26)),
    IndexUniverseSpec("930955.CSI", "红利低波100", date(2017, 5, 31)),
)

ALL_A_CODE = "ALL_A"
ALL_A_NAME = "全A"
ALL_A_START = date(2014, 1, 31)


def configured_universe_count(obs_date: date | str | pd.Timestamp) -> int:
    """Return the number of configured universes expected at one month-end."""
    stamp = pd.Timestamp(obs_date).date()
    return 1 + sum(
        spec.first_valid_weight_date <= stamp for spec in IMPORTANT_INDEX_SPECS
    )


class PITIndexFTTMManager(PITMonthlySnapshotManager):
    DEFAULT_INCREMENTAL_MONTHS = 8
    DEFAULT_BACKFILL_BATCH_MONTHS = 12

    def __init__(self) -> None:
        super().__init__("pit_index_fttm_monthly")
        self.calculator = IndexFTTMCalculator()

    def incremental_update(
        self, months: int = DEFAULT_INCREMENTAL_MONTHS, batch_size: int | None = None
    ) -> Dict[str, Any]:
        latest = self._latest_available_month()
        if latest is None:
            raise RuntimeError("指数FTTM上游没有共同可用的完整月份")
        requested = max(
            int(months or self.DEFAULT_INCREMENTAL_MONTHS),
            self.DEFAULT_INCREMENTAL_MONTHS,
        )
        target_months = self.incremental_months(requested, end_date=latest)
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
            raise RuntimeError("指数FTTM上游没有共同可用的完整月份")
        start = self.as_month_end(start_date or self.DEFAULT_FULL_START)
        requested_end = self.as_month_end(end_date or latest)
        end = min(requested_end, latest)
        target_months = self.month_ends(start, end)
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
        batch_months = max(
            int(batch_size or self.DEFAULT_BACKFILL_BATCH_MONTHS), 1
        )
        total_rows = 0
        processed_months: list[str] = []
        batch_audits: list[dict[str, Any]] = []
        selected_source_max: Any = None
        selected_weight_date_max: Any = None

        for offset in range(0, len(target_months), batch_months):
            months = target_months[offset : offset + batch_months]
            anchor = self.previous_month_end(min(months))
            calculation_months = [anchor, *months]
            sources = self._load_sources(calculation_months)
            sources["stock_fttm"], anchor_stock_source = self._ensure_stock_anchor(
                sources["stock_fttm"], anchor
            )
            self._validate_dependencies(
                sources["members"], sources["stock_fttm"], months
            )
            calculated = self.calculator.calculate(
                sources["members"],
                sources["stock_basic"],
                sources["stock_fttm"],
                obs_dates=calculation_months,
            )
            persist = calculated.loc[
                calculated["obs_date"].isin(pd.to_datetime(months))
            ].copy()
            self._validate_output_coverage(persist, months)
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
                columns=IndexFTTMCalculator.OUTPUT_COLUMNS,
                primary_keys=(
                    "obs_date",
                    "universe_type",
                    "universe_code",
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
                "FTTM指数/全A月批次完成: %s ~ %s, rows=%s",
                min(months),
                max(months),
                inserted,
            )

        self.stats["processed_records"] += total_rows
        self.stats["success_records"] += total_rows
        return {
            result_key: total_rows,
            "processed_months": processed_months,
            "configured_index_count": len(IMPORTANT_INDEX_SPECS),
            "includes_all_a": True,
            "aggregation_version": IndexFTTMCalculator.AGGREGATION_VERSION,
            "quality_rule_version": IndexFTTMCalculator.QUALITY_RULE_VERSION,
            "source_max_report_date": selected_source_max,
            "weight_trade_date_max": selected_weight_date_max,
            "dependency_freshness": self._dependency_freshness(),
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "revision_limit": (
                "rolling incremental covers at least eight complete months and t+1; "
                "older corrections require manual_range/full audit"
            ),
        }

    def _load_sources(self, obs_dates: list[date]) -> dict[str, pd.DataFrame]:
        placeholders = ", ".join(["(%s::date)"] * len(obs_dates))
        parameters = tuple(obs_dates)
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
        stock_basic = self.context.query_dataframe(
            """
            SELECT ts_code, list_date, delist_date, exchange, curr_type
            FROM rawdata.stock_basic
            """
        )
        return {
            "members": self._load_members(obs_dates),
            "stock_fttm": stock_fttm,
            "stock_basic": stock_basic,
        }

    def _load_members(self, obs_dates: list[date]) -> pd.DataFrame:
        configured_values = ", ".join(
            ["(%s::text, %s::text, %s::date)"] * len(IMPORTANT_INDEX_SPECS)
        )
        requested_values = ", ".join(["(%s::date)"] * len(obs_dates))
        configured_parameters: list[Any] = []
        for spec in IMPORTANT_INDEX_SPECS:
            configured_parameters.extend(
                [spec.code, spec.name, spec.first_valid_weight_date]
            )
        parameters = tuple(configured_parameters) + tuple(obs_dates) + (ALL_A_START,)
        return self.context.query_dataframe(
            f"""
            WITH configured(index_code, index_name, first_valid_weight_date) AS (
                VALUES {configured_values}
            ),
            requested(obs_date) AS (VALUES {requested_values}),
            bounds AS (
                SELECT MIN(obs_date)::date AS min_obs_date,
                       MAX(obs_date)::date AS max_obs_date
                FROM requested
            ),
            snapshot_stats AS (
                SELECT w.index_code,
                       w.trade_date::date AS weight_trade_date,
                       COUNT(*)::bigint AS member_count,
                       SUM(w.weight) AS weight_sum
                FROM rawdata.index_weight w
                JOIN configured c ON c.index_code = w.index_code
                CROSS JOIN bounds b
                WHERE w.trade_date >= b.min_obs_date - INTERVAL '65 days'
                  AND w.trade_date <= b.max_obs_date
                GROUP BY w.index_code, w.trade_date
                HAVING SUM(w.weight) BETWEEN 99 AND 101
            ),
            chosen AS (
                SELECT r.obs_date,
                       c.index_code,
                       c.index_name,
                       MAX(s.weight_trade_date)::date AS weight_trade_date
                FROM requested r
                CROSS JOIN configured c
                LEFT JOIN snapshot_stats s
                  ON s.index_code = c.index_code
                 AND s.weight_trade_date <= r.obs_date
                 AND s.weight_trade_date >= r.obs_date - INTERVAL '65 days'
                WHERE r.obs_date >= c.first_valid_weight_date
                GROUP BY r.obs_date, c.index_code, c.index_name
            ),
            index_members AS (
                SELECT c.obs_date,
                       'index'::text AS universe_type,
                       c.index_code::text AS universe_code,
                       c.index_name::text AS universe_name,
                       'official_weight'::text AS weight_basis,
                       'rawdata.index_weight'::text AS weight_source,
                       c.weight_trade_date,
                       w.con_code::text AS ts_code,
                       w.weight::numeric AS raw_weight
                FROM chosen c
                JOIN rawdata.index_weight w
                  ON w.index_code = c.index_code
                 AND w.trade_date = c.weight_trade_date
            ),
            all_a_weight_dates AS (
                SELECT r.obs_date,
                       MAX(d.trade_date)::date AS weight_trade_date
                FROM requested r
                LEFT JOIN rawdata.stock_dailybasic d
                  ON d.trade_date <= r.obs_date
                 AND d.trade_date >= r.obs_date - INTERVAL '31 days'
                WHERE r.obs_date >= %s::date
                GROUP BY r.obs_date
            ),
            all_a_members AS (
                SELECT r.obs_date,
                       'all_a'::text AS universe_type,
                       'ALL_A'::text AS universe_code,
                       '全A'::text AS universe_name,
                       'total_mv'::text AS weight_basis,
                       'rawdata.stock_dailybasic'::text AS weight_source,
                       wd.weight_trade_date,
                       b.ts_code::text AS ts_code,
                       d.total_mv::numeric AS raw_weight
                FROM requested r
                JOIN rawdata.stock_basic b
                  ON b.list_date <= r.obs_date
                 AND (b.delist_date IS NULL OR b.delist_date > r.obs_date)
                 AND b.exchange IN ('SSE', 'SZSE', 'BSE')
                 AND b.curr_type = 'CNY'
                LEFT JOIN all_a_weight_dates wd ON wd.obs_date = r.obs_date
                LEFT JOIN rawdata.stock_dailybasic d
                  ON d.trade_date = wd.weight_trade_date
                 AND d.ts_code = b.ts_code
                WHERE r.obs_date >= %s::date
            )
            SELECT * FROM index_members
            UNION ALL
            SELECT * FROM all_a_members
            """,
            parameters + (ALL_A_START,),
        )

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
        return combined.sort_values(
            ["obs_date", "ts_code", "org_name"], kind="mergesort"
        ).drop_duplicates(["obs_date", "ts_code", "org_name"], keep="last").reset_index(
            drop=True
        ), "recomputed_from_raw_read_only"

    def _calculate_transient_stock_month(self, anchor: date) -> pd.DataFrame:
        source_start = (pd.Timestamp(anchor) - pd.DateOffset(months=6)).date()
        forecasts = load_stock_fttm_forecast_sources(
            self.context, source_start, anchor
        )
        return StockFTTMCalculator().calculate(forecasts, [anchor])

    def _latest_available_month(self) -> date | None:
        latest_complete = self.latest_complete_month()
        codes = [spec.code for spec in IMPORTANT_INDEX_SPECS]
        frame = self.context.query_dataframe(
            """
            WITH recent_snapshots AS (
                SELECT index_code, trade_date::date AS trade_date
                FROM rawdata.index_weight
                WHERE index_code = ANY(%s)
                  AND trade_date >= %s::date - INTERVAL '120 days'
                GROUP BY index_code, trade_date
                HAVING SUM(weight) BETWEEN 99 AND 101
            ),
            latest_weights AS (
                SELECT index_code, MAX(trade_date)::date AS max_weight_date
                FROM recent_snapshots
                GROUP BY index_code
            )
            SELECT
                (SELECT MAX(obs_date)::date FROM pit.pit_stock_fttm_monthly)
                    AS stock_fttm_max_date,
                (SELECT MAX(trade_date)::date FROM rawdata.stock_dailybasic)
                    AS stock_dailybasic_max_date,
                (SELECT MIN(max_weight_date + 65)::date FROM latest_weights)
                    AS index_weight_valid_through,
                (SELECT COUNT(*)::int FROM latest_weights)
                    AS current_index_count
            """,
            (codes, latest_complete),
        )
        if frame.empty:
            return None
        row = frame.iloc[0]
        if int(row.get("current_index_count") or 0) != len(IMPORTANT_INDEX_SPECS):
            return None
        candidates = [
            latest_complete,
            row.get("stock_fttm_max_date"),
            row.get("stock_dailybasic_max_date"),
            row.get("index_weight_valid_through"),
        ]
        if any(pd.isna(value) for value in candidates):
            return None

        def month_end_on_or_before(value: Any) -> date:
            stamp = pd.Timestamp(value).normalize()
            forward = stamp + pd.offsets.MonthEnd(0)
            if stamp == forward:
                return stamp.date()
            return (stamp - pd.offsets.MonthEnd(1)).date()

        return min(month_end_on_or_before(value) for value in candidates)

    @staticmethod
    def _validate_dependencies(
        members: pd.DataFrame,
        stock_fttm: pd.DataFrame,
        target_months: Iterable[date],
    ) -> None:
        target = {pd.Timestamp(value).normalize() for value in target_months}
        stock_dates = (
            set(pd.to_datetime(stock_fttm["obs_date"], errors="coerce").dropna())
            if "obs_date" in stock_fttm
            else set()
        )
        missing_stock = sorted(target - stock_dates)
        if missing_stock:
            raise RuntimeError(
                "指数FTTM依赖缺失: pit_stock_fttm_monthly 月份 "
                + ", ".join(value.date().isoformat() for value in missing_stock)
            )

        if members.empty:
            raise RuntimeError("指数FTTM依赖缺失: 指数/全A成分权重为空")
        prepared = members.copy()
        prepared["obs_date"] = pd.to_datetime(
            prepared["obs_date"], errors="coerce"
        ).dt.normalize()
        actual = set(
            prepared.loc[
                prepared["obs_date"].isin(target),
                ["obs_date", "universe_type", "universe_code"],
            ].itertuples(index=False, name=None)
        )
        expected: set[tuple[pd.Timestamp, str, str]] = set()
        for month in target:
            expected.add((month, "all_a", ALL_A_CODE))
            expected.update(
                (month, "index", spec.code)
                for spec in IMPORTANT_INDEX_SPECS
                if spec.first_valid_weight_date <= month.date()
            )
        missing = sorted(expected - actual)
        if missing:
            details = ", ".join(
                f"{month.date().isoformat()}:{code}" for month, _, code in missing
            )
            raise RuntimeError("指数FTTM成分权重缺失或超过陈旧阈值: " + details)

    @staticmethod
    def _validate_output_coverage(
        output: pd.DataFrame, target_months: Iterable[date]
    ) -> None:
        actual = set(
            output[["obs_date", "universe_type", "universe_code"]].itertuples(
                index=False, name=None
            )
        ) if not output.empty else set()
        expected: set[tuple[pd.Timestamp, str, str]] = set()
        for value in target_months:
            month = pd.Timestamp(value).normalize()
            expected.add((month, "all_a", ALL_A_CODE))
            expected.update(
                (month, "index", spec.code)
                for spec in IMPORTANT_INDEX_SPECS
                if spec.first_valid_weight_date <= value
            )
        missing = sorted(expected - actual)
        if missing:
            raise RuntimeError(
                "指数FTTM计算结果缺少配置对象: "
                + ", ".join(f"{month.date()}:{code}" for month, _, code in missing)
            )

    def _dependency_freshness(self) -> Dict[str, Any]:
        frame = self.context.query_dataframe(
            """
            SELECT
                (SELECT MAX(obs_date)::date FROM pit.pit_stock_fttm_monthly)
                    AS pit_stock_fttm_max_obs_date,
                (SELECT MAX(trade_date)::date FROM rawdata.index_weight)
                    AS index_weight_max_trade_date,
                (SELECT MAX(trade_date)::date FROM rawdata.stock_dailybasic)
                    AS stock_dailybasic_max_trade_date,
                (SELECT MAX(report_date)::date FROM rawdata.stock_report_rc)
                    AS stock_report_rc_max_report_date
            """
        )
        return {} if frame.empty else frame.iloc[0].to_dict()


__all__ = [
    "ALL_A_CODE",
    "ALL_A_NAME",
    "ALL_A_START",
    "IMPORTANT_INDEX_SPECS",
    "IndexUniverseSpec",
    "PITIndexFTTMManager",
    "configured_universe_count",
]
