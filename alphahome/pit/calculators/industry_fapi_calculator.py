"""Point-in-time industry analyst prosperity (FAPI) calculator.

The implementation is intentionally limited to month-end facts.  Smoothing,
cross-sectional ranking, trend confirmation, crowding filters and forward
returns belong to the research layer rather than this PIT table.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FAPIQualityThreshold:
    min_industry_stocks_per_org: int = 2
    min_benchmark_stocks_per_org: int = 20
    min_matched_orgs: int = 5


class IndustryFAPICalculator:
    """Build source-adapted SW industry FAPI against CSI 800.

    For every broker that covers a common stock sample at ``t`` and ``t-1``,
    current-month book equity is used for both snapshots.  This holds the
    denominator and membership fixed so that the direction flag isolates the
    change in analyst forecasts instead of mixing it with valuation movement.
    """

    CLASSIFICATION_SOURCE = "sw"
    BENCHMARK_CODE = "000906.SH"
    BENCHMARK_NAME = "中证800"
    EQUITY_BASIS = "total_mv_div_pb"
    METHOD_VERSION = "source_adapted_sw_csi800_v1"
    ORG_WEIGHT_VERSION = "coverage_recency_half_life_183d_v1"
    QUALITY_RULE_VERSION = "min_5_matched_orgs_v1"
    REPORT_AGE_HALF_LIFE_DAYS = 183.0
    MAX_EQUITY_STALENESS_DAYS = 31
    MAX_BENCHMARK_STALENESS_DAYS = 65
    RATIO_EPSILON = 1e-12

    OUTPUT_COLUMNS = [
        "obs_date",
        "classification_source",
        "industry_level",
        "industry_code",
        "industry_name",
        "benchmark_code",
        "benchmark_name",
        "equity_basis",
        "equity_trade_date",
        "equity_staleness_days",
        "benchmark_weight_trade_date",
        "benchmark_weight_staleness_days",
        "structural_member_count",
        "equity_available_member_count",
        "matched_stock_count",
        "matched_org_count",
        "ratio_matched_org_count",
        "median_common_stock_count",
        "p25_common_stock_count",
        "average_report_age_days",
        "benchmark_structural_member_count",
        "benchmark_matched_stock_count",
        "benchmark_eligible_org_count",
        "benchmark_median_common_stock_count",
        "spread_up_org_count",
        "spread_down_or_flat_org_count",
        "ratio_up_org_count",
        "ratio_down_or_flat_org_count",
        "spread_up_org_weight",
        "spread_total_org_weight",
        "ratio_up_org_weight",
        "ratio_total_org_weight",
        "fapi_spread_equal",
        "fapi_spread_weighted",
        "fapi_ratio_equal",
        "fapi_ratio_weighted",
        "expected_roe_equal",
        "expected_roe_weighted",
        "previous_expected_roe_equal",
        "previous_expected_roe_weighted",
        "benchmark_expected_roe_equal",
        "benchmark_expected_roe_weighted",
        "previous_benchmark_expected_roe_equal",
        "previous_benchmark_expected_roe_weighted",
        "source_max_report_date",
        "previous_source_max_report_date",
        "stock_formula_versions",
        "is_eligible",
        "is_ratio_eligible",
        "quality_reasons",
        "method_version",
        "org_weight_version",
        "quality_rule_version",
    ]

    _IDENTITY_KEYS = [
        "obs_date",
        "classification_source",
        "industry_level",
        "industry_code",
        "industry_name",
    ]

    def __init__(self, threshold: FAPIQualityThreshold | None = None) -> None:
        self.threshold = threshold or FAPIQualityThreshold()
        self.last_audit: dict[str, Any] = {}
        self.last_relative_org: pd.DataFrame = pd.DataFrame()

    def calculate(
        self,
        classifications: pd.DataFrame,
        equity: pd.DataFrame,
        benchmark_members: pd.DataFrame,
        stock_fttm: pd.DataFrame,
        obs_dates: Sequence[date | str | pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        members = self._prepare_members(classifications)
        if obs_dates is not None:
            wanted = {pd.Timestamp(value).normalize() for value in obs_dates}
            members = members.loc[members["obs_date"].isin(wanted)].copy()
        else:
            wanted = set(members["obs_date"].dropna())
        if members.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        equity_frame, equity_meta = self._prepare_equity(equity)
        benchmark_frame, benchmark_meta = self._prepare_benchmark_members(
            benchmark_members
        )
        fttm = self._prepare_stock_fttm(stock_fttm)
        matched = self._build_adjacent_matched_fttm(fttm, wanted)

        structural = self._build_structural(members, equity_frame)
        industry_stock = members.merge(
            matched, on=["obs_date", "ts_code"], how="inner", sort=False
        ).merge(equity_frame, on=["obs_date", "ts_code"], how="inner", sort=False)
        matched_coverage = (
            industry_stock.groupby(self._IDENTITY_KEYS, sort=False, dropna=False)
            .agg(matched_stock_count=("ts_code", "nunique"))
            .reset_index()
            if not industry_stock.empty
            else pd.DataFrame(columns=self._IDENTITY_KEYS + ["matched_stock_count"])
        )
        structural = structural.merge(
            matched_coverage,
            on=self._IDENTITY_KEYS,
            how="left",
            sort=False,
        )
        industry_org = self._build_industry_org(industry_stock)

        benchmark_stock = matched.merge(
            benchmark_frame,
            on=["obs_date", "ts_code"],
            how="inner",
            sort=False,
        ).merge(equity_frame, on=["obs_date", "ts_code"], how="inner", sort=False)
        benchmark_org = self._build_benchmark_org(benchmark_stock)
        benchmark_coverage = self._build_benchmark_coverage(
            benchmark_frame, benchmark_stock, benchmark_org
        )

        relative = industry_org.merge(
            benchmark_org,
            on=["obs_date", "org_name"],
            how="inner",
            sort=False,
            suffixes=("", "_benchmark"),
        )
        relative = self._build_relative_measures(relative)
        self.last_relative_org = relative.copy()
        aggregate = self._aggregate_relative_org(relative)

        result = structural.merge(
            aggregate,
            on=self._IDENTITY_KEYS,
            how="left",
            sort=False,
        )
        result = result.merge(equity_meta, on="obs_date", how="left", sort=False)
        result = result.merge(benchmark_meta, on="obs_date", how="left", sort=False)
        result = result.merge(benchmark_coverage, on="obs_date", how="left", sort=False)
        result = self._finish_result(result)
        return result

    def _prepare_members(self, frame: pd.DataFrame) -> pd.DataFrame:
        required = {
            "ts_code",
            "obs_date",
            "data_source",
            "industry_code1",
            "industry_level1",
            "industry_code2",
            "industry_level2",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"pit_industry_classification 缺少字段: {missing}")

        source = frame.copy()
        source["obs_date"] = pd.to_datetime(
            source["obs_date"], errors="coerce"
        ).dt.normalize()
        source["ts_code"] = source["ts_code"].astype("string").str.strip()
        source["data_source"] = (
            source["data_source"].astype("string").str.strip().str.lower()
        )
        source = source.loc[
            source["obs_date"].notna()
            & source["ts_code"].ne("")
            & source["data_source"].eq(self.CLASSIFICATION_SOURCE)
        ].copy()

        pieces: list[pd.DataFrame] = []
        missing_code_count = 0
        missing_name_count = 0
        for level, code_column, name_column in (
            ("L1", "industry_code1", "industry_level1"),
            ("L2", "industry_code2", "industry_level2"),
        ):
            piece = source[
                ["obs_date", "ts_code", "data_source", code_column, name_column]
            ].rename(
                columns={
                    "data_source": "classification_source",
                    code_column: "industry_code",
                    name_column: "industry_name",
                }
            )
            piece["industry_level"] = level
            piece["industry_code"] = (
                piece["industry_code"].astype("string").fillna("").str.strip()
            )
            piece["industry_name"] = (
                piece["industry_name"].astype("string").fillna("").str.strip()
            )
            missing_code_count += int(piece["industry_code"].eq("").sum())
            missing_name_count += int(piece["industry_name"].eq("").sum())
            pieces.append(
                piece.loc[
                    piece["industry_code"].ne("") & piece["industry_name"].ne("")
                ].copy()
            )

        members = pd.concat(pieces, ignore_index=True)
        identity_without_name = self._IDENTITY_KEYS[:-1]
        name_counts = members.groupby(identity_without_name, dropna=False)[
            "industry_name"
        ].nunique()
        name_conflict_count = int((name_counts > 1).sum())
        names = members.sort_values(
            identity_without_name + ["industry_name"], kind="mergesort"
        ).drop_duplicates(identity_without_name, keep="first")
        members = members.drop(columns="industry_name").merge(
            names[identity_without_name + ["industry_name"]],
            on=identity_without_name,
            how="left",
            sort=False,
        )
        members = members.sort_values(
            identity_without_name + ["ts_code"], kind="mergesort"
        ).drop_duplicates(identity_without_name + ["ts_code"], keep="first")
        self.last_audit = {
            "classification_source_row_count": int(len(source)),
            "missing_industry_code_count": missing_code_count,
            "missing_industry_name_count": missing_name_count,
            "industry_name_conflict_count": name_conflict_count,
        }
        return members.reset_index(drop=True)

    def _prepare_equity(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        required = {
            "obs_date",
            "ts_code",
            "equity_trade_date",
            "total_mv",
            "pb",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"stock_dailybasic 权益代理缺少字段: {missing}")
        source = frame[list(required)].copy()
        source["obs_date"] = pd.to_datetime(
            source["obs_date"], errors="coerce"
        ).dt.normalize()
        source["equity_trade_date"] = pd.to_datetime(
            source["equity_trade_date"], errors="coerce"
        ).dt.normalize()
        source["ts_code"] = source["ts_code"].astype("string").fillna("").str.strip()
        source["total_mv"] = pd.to_numeric(source["total_mv"], errors="coerce")
        source["pb"] = pd.to_numeric(source["pb"], errors="coerce")
        source["equity_staleness_days"] = (
            source["obs_date"] - source["equity_trade_date"]
        ).dt.days
        meta = (
            source.groupby("obs_date", sort=False, dropna=False)
            .agg(
                equity_trade_date=("equity_trade_date", "max"),
                equity_staleness_days=("equity_staleness_days", "max"),
            )
            .reset_index()
        )
        valid = (
            source["ts_code"].ne("")
            & source["total_mv"].gt(0)
            & source["pb"].gt(0)
            & source["equity_staleness_days"].between(
                0, self.MAX_EQUITY_STALENESS_DAYS, inclusive="both"
            )
        )
        source["book_equity"] = np.where(
            valid, source["total_mv"] / source["pb"], np.nan
        )
        source = source.loc[source["book_equity"].notna()].copy()
        source = source.sort_values(
            ["obs_date", "ts_code", "equity_trade_date"],
            ascending=[True, True, False],
            kind="mergesort",
        ).drop_duplicates(["obs_date", "ts_code"], keep="first")
        return source[["obs_date", "ts_code", "book_equity"]], meta

    def _prepare_benchmark_members(
        self, frame: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        required = {
            "obs_date",
            "benchmark_code",
            "benchmark_name",
            "benchmark_weight_trade_date",
            "ts_code",
            "weight",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"index_weight 中证800成分缺少字段: {missing}")
        source = frame[list(required)].copy()
        source["obs_date"] = pd.to_datetime(
            source["obs_date"], errors="coerce"
        ).dt.normalize()
        source["benchmark_weight_trade_date"] = pd.to_datetime(
            source["benchmark_weight_trade_date"], errors="coerce"
        ).dt.normalize()
        source["benchmark_code"] = (
            source["benchmark_code"].astype("string").fillna(self.BENCHMARK_CODE)
        )
        source["benchmark_name"] = (
            source["benchmark_name"].astype("string").fillna(self.BENCHMARK_NAME)
        )
        source["ts_code"] = source["ts_code"].astype("string").fillna("").str.strip()
        source["weight"] = pd.to_numeric(source["weight"], errors="coerce")
        source["benchmark_weight_staleness_days"] = (
            source["obs_date"] - source["benchmark_weight_trade_date"]
        ).dt.days
        meta = (
            source.groupby("obs_date", sort=False, dropna=False)
            .agg(
                benchmark_weight_trade_date=("benchmark_weight_trade_date", "max"),
                benchmark_weight_staleness_days=(
                    "benchmark_weight_staleness_days",
                    "max",
                ),
            )
            .reset_index()
        )
        valid = (
            source["benchmark_code"].eq(self.BENCHMARK_CODE)
            & source["ts_code"].ne("")
            & source["weight"].gt(0)
            & source["benchmark_weight_staleness_days"].between(
                0, self.MAX_BENCHMARK_STALENESS_DAYS, inclusive="both"
            )
        )
        source = source.loc[valid].copy()
        source = source.sort_values(
            ["obs_date", "ts_code", "benchmark_weight_trade_date"],
            ascending=[True, True, False],
            kind="mergesort",
        ).drop_duplicates(["obs_date", "ts_code"], keep="first")
        return source[["obs_date", "ts_code"]], meta

    @staticmethod
    def _prepare_stock_fttm(frame: pd.DataFrame) -> pd.DataFrame:
        required = {
            "obs_date",
            "ts_code",
            "org_name",
            "fttm_np",
            "selected_report_date",
            "formula_version",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"pit_stock_fttm_monthly 缺少字段: {missing}")
        source = frame[list(required)].copy()
        source["obs_date"] = pd.to_datetime(
            source["obs_date"], errors="coerce"
        ).dt.normalize()
        source["selected_report_date"] = pd.to_datetime(
            source["selected_report_date"], errors="coerce"
        ).dt.normalize()
        source["ts_code"] = source["ts_code"].astype("string").str.strip()
        source["org_name"] = source["org_name"].astype("string").fillna("").str.strip()
        source["formula_version"] = (
            source["formula_version"].astype("string").fillna("").str.strip()
        )
        source["fttm_np"] = pd.to_numeric(source["fttm_np"], errors="coerce")
        source = source.loc[
            source["obs_date"].notna()
            & source["ts_code"].ne("")
            & source["org_name"].ne("")
            & source["fttm_np"].notna()
            & np.isfinite(source["fttm_np"])
        ].copy()
        return source.sort_values(
            ["obs_date", "ts_code", "org_name", "selected_report_date"],
            ascending=[True, True, True, False],
            kind="mergesort",
            na_position="last",
        ).drop_duplicates(["obs_date", "ts_code", "org_name"], keep="first")

    @staticmethod
    def _build_adjacent_matched_fttm(
        fttm: pd.DataFrame, wanted: set[pd.Timestamp]
    ) -> pd.DataFrame:
        current = fttm.loc[fttm["obs_date"].isin(wanted)].copy()
        previous = fttm.copy()
        previous["obs_date"] = previous["obs_date"] + pd.offsets.MonthEnd(1)
        previous = previous.rename(
            columns={
                "fttm_np": "previous_fttm_np",
                "selected_report_date": "previous_selected_report_date",
                "formula_version": "previous_formula_version",
            }
        )
        matched = current.merge(
            previous[
                [
                    "obs_date",
                    "ts_code",
                    "org_name",
                    "previous_fttm_np",
                    "previous_selected_report_date",
                    "previous_formula_version",
                ]
            ],
            on=["obs_date", "ts_code", "org_name"],
            how="inner",
            sort=False,
        ).rename(
            columns={
                "fttm_np": "current_fttm_np",
                "selected_report_date": "current_selected_report_date",
                "formula_version": "current_formula_version",
            }
        )
        matched["current_report_age_days"] = (
            matched["obs_date"] - matched["current_selected_report_date"]
        ).dt.days.clip(lower=0)
        matched["current_report_age_days"] = matched["current_report_age_days"].fillna(
            0.0
        )
        return matched

    def _build_structural(
        self, members: pd.DataFrame, equity: pd.DataFrame
    ) -> pd.DataFrame:
        structural = (
            members.groupby(self._IDENTITY_KEYS, sort=False, dropna=False)
            .agg(structural_member_count=("ts_code", "nunique"))
            .reset_index()
        )
        equity_members = members.merge(
            equity[["obs_date", "ts_code"]],
            on=["obs_date", "ts_code"],
            how="inner",
            sort=False,
        )
        available = (
            equity_members.groupby(self._IDENTITY_KEYS, sort=False, dropna=False)
            .agg(equity_available_member_count=("ts_code", "nunique"))
            .reset_index()
        )
        return structural.merge(
            available, on=self._IDENTITY_KEYS, how="left", sort=False
        )

    def _build_industry_org(self, stock: pd.DataFrame) -> pd.DataFrame:
        if stock.empty:
            return pd.DataFrame(columns=self._IDENTITY_KEYS + ["org_name"])
        group_keys = self._IDENTITY_KEYS + ["org_name"]
        rows = (
            stock.groupby(group_keys, sort=False, dropna=False)
            .agg(
                common_stock_count=("ts_code", "nunique"),
                common_book_equity=("book_equity", "sum"),
                current_fttm_sum=("current_fttm_np", "sum"),
                previous_fttm_sum=("previous_fttm_np", "sum"),
                average_report_age_days=("current_report_age_days", "mean"),
                source_max_report_date=("current_selected_report_date", "max"),
                previous_source_max_report_date=(
                    "previous_selected_report_date",
                    "max",
                ),
            )
            .reset_index()
        )
        rows = rows.loc[
            rows["common_stock_count"].ge(self.threshold.min_industry_stocks_per_org)
            & rows["common_book_equity"].gt(0)
        ].copy()
        rows["current_industry_expected_roe"] = (
            rows["current_fttm_sum"] / rows["common_book_equity"]
        )
        rows["previous_industry_expected_roe"] = (
            rows["previous_fttm_sum"] / rows["common_book_equity"]
        )

        versions = (
            stock.groupby(group_keys, sort=False, dropna=False)
            .apply(
                lambda group: sorted(
                    {
                        str(value)
                        for value in pd.concat(
                            [
                                group["current_formula_version"],
                                group["previous_formula_version"],
                            ],
                            ignore_index=True,
                        ).dropna()
                        if str(value)
                    }
                ),
                include_groups=False,
            )
            .rename("stock_formula_versions")
            .reset_index()
        )
        return rows.merge(versions, on=group_keys, how="left", sort=False)

    def _build_benchmark_org(self, stock: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "obs_date",
            "org_name",
            "benchmark_common_stock_count",
            "current_benchmark_expected_roe",
            "previous_benchmark_expected_roe",
            "benchmark_source_max_report_date",
            "benchmark_previous_source_max_report_date",
        ]
        if stock.empty:
            return pd.DataFrame(columns=columns)
        rows = (
            stock.groupby(["obs_date", "org_name"], sort=False, dropna=False)
            .agg(
                benchmark_common_stock_count=("ts_code", "nunique"),
                benchmark_common_book_equity=("book_equity", "sum"),
                current_benchmark_fttm_sum=("current_fttm_np", "sum"),
                previous_benchmark_fttm_sum=("previous_fttm_np", "sum"),
                benchmark_source_max_report_date=(
                    "current_selected_report_date",
                    "max",
                ),
                benchmark_previous_source_max_report_date=(
                    "previous_selected_report_date",
                    "max",
                ),
            )
            .reset_index()
        )
        rows = rows.loc[
            rows["benchmark_common_stock_count"].ge(
                self.threshold.min_benchmark_stocks_per_org
            )
            & rows["benchmark_common_book_equity"].gt(0)
        ].copy()
        rows["current_benchmark_expected_roe"] = (
            rows["current_benchmark_fttm_sum"] / rows["benchmark_common_book_equity"]
        )
        rows["previous_benchmark_expected_roe"] = (
            rows["previous_benchmark_fttm_sum"] / rows["benchmark_common_book_equity"]
        )
        return rows[columns]

    @staticmethod
    def _build_benchmark_coverage(
        members: pd.DataFrame,
        matched_stock: pd.DataFrame,
        benchmark_org: pd.DataFrame,
    ) -> pd.DataFrame:
        dates = pd.DataFrame({"obs_date": sorted(set(members["obs_date"].dropna()))})
        if dates.empty:
            return pd.DataFrame(
                columns=[
                    "obs_date",
                    "benchmark_structural_member_count",
                    "benchmark_matched_stock_count",
                    "benchmark_eligible_org_count",
                    "benchmark_median_common_stock_count",
                ]
            )
        structural = (
            members.groupby("obs_date", sort=False)["ts_code"]
            .nunique()
            .rename("benchmark_structural_member_count")
            .reset_index()
        )
        matched = (
            matched_stock.groupby("obs_date", sort=False)["ts_code"]
            .nunique()
            .rename("benchmark_matched_stock_count")
            .reset_index()
            if not matched_stock.empty
            else pd.DataFrame(columns=["obs_date", "benchmark_matched_stock_count"])
        )
        orgs = (
            benchmark_org.groupby("obs_date", sort=False)
            .agg(
                benchmark_eligible_org_count=("org_name", "nunique"),
                benchmark_median_common_stock_count=(
                    "benchmark_common_stock_count",
                    "median",
                ),
            )
            .reset_index()
            if not benchmark_org.empty
            else pd.DataFrame(
                columns=[
                    "obs_date",
                    "benchmark_eligible_org_count",
                    "benchmark_median_common_stock_count",
                ]
            )
        )
        return (
            dates.merge(structural, on="obs_date", how="left")
            .merge(matched, on="obs_date", how="left")
            .merge(orgs, on="obs_date", how="left")
        )

    def _build_relative_measures(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        result = frame.copy()
        result["current_relative_spread"] = (
            result["current_industry_expected_roe"]
            - result["current_benchmark_expected_roe"]
        )
        result["previous_relative_spread"] = (
            result["previous_industry_expected_roe"]
            - result["previous_benchmark_expected_roe"]
        )
        current_ratio_valid = (
            result["current_benchmark_expected_roe"].abs().gt(self.RATIO_EPSILON)
        )
        previous_ratio_valid = (
            result["previous_benchmark_expected_roe"].abs().gt(self.RATIO_EPSILON)
        )
        result["current_relative_ratio"] = np.where(
            current_ratio_valid,
            result["current_industry_expected_roe"]
            / result["current_benchmark_expected_roe"],
            np.nan,
        )
        result["previous_relative_ratio"] = np.where(
            previous_ratio_valid,
            result["previous_industry_expected_roe"]
            / result["previous_benchmark_expected_roe"],
            np.nan,
        )
        result["effective_source_max_report_date"] = result[
            ["source_max_report_date", "benchmark_source_max_report_date"]
        ].max(axis=1)
        result["effective_previous_source_max_report_date"] = result[
            [
                "previous_source_max_report_date",
                "benchmark_previous_source_max_report_date",
            ]
        ].max(axis=1)
        result["available_org_weight"] = np.log1p(
            result["common_stock_count"]
        ) * np.power(
            0.5,
            result["average_report_age_days"] / self.REPORT_AGE_HALF_LIFE_DAYS,
        )
        return result

    @staticmethod
    def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
        valid = values.notna() & weights.notna() & weights.gt(0)
        if not valid.any():
            return np.nan
        return float(np.average(values.loc[valid], weights=weights.loc[valid]))

    def _aggregate_relative_org(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=self._IDENTITY_KEYS)

        def aggregate(group: pd.DataFrame) -> pd.Series:
            spread = group.loc[
                group["current_relative_spread"].notna()
                & group["previous_relative_spread"].notna()
            ].copy()
            ratio = group.loc[
                group["current_relative_ratio"].notna()
                & group["previous_relative_ratio"].notna()
            ].copy()
            spread_up = spread["current_relative_spread"].gt(
                spread["previous_relative_spread"]
            )
            ratio_up = ratio["current_relative_ratio"].gt(
                ratio["previous_relative_ratio"]
            )
            spread_weights = spread["available_org_weight"]
            ratio_weights = ratio["available_org_weight"]
            versions = sorted(
                {
                    str(version)
                    for values in group["stock_formula_versions"]
                    if isinstance(values, list)
                    for version in values
                    if str(version)
                }
            )
            return pd.Series(
                {
                    "matched_org_count": int(len(spread)),
                    "ratio_matched_org_count": int(len(ratio)),
                    "median_common_stock_count": group["common_stock_count"].median(),
                    "p25_common_stock_count": group["common_stock_count"].quantile(
                        0.25
                    ),
                    "average_report_age_days": group["average_report_age_days"].mean(),
                    "spread_up_org_count": int(spread_up.sum()),
                    "spread_down_or_flat_org_count": int(len(spread) - spread_up.sum()),
                    "ratio_up_org_count": int(ratio_up.sum()),
                    "ratio_down_or_flat_org_count": int(len(ratio) - ratio_up.sum()),
                    "spread_up_org_weight": float(spread_weights.loc[spread_up].sum()),
                    "spread_total_org_weight": float(spread_weights.sum()),
                    "ratio_up_org_weight": float(ratio_weights.loc[ratio_up].sum()),
                    "ratio_total_org_weight": float(ratio_weights.sum()),
                    "fapi_spread_equal": float(spread_up.mean())
                    if len(spread)
                    else np.nan,
                    "fapi_spread_weighted": self._weighted_mean(
                        spread_up.astype(float), spread_weights
                    ),
                    "fapi_ratio_equal": float(ratio_up.mean())
                    if len(ratio)
                    else np.nan,
                    "fapi_ratio_weighted": self._weighted_mean(
                        ratio_up.astype(float), ratio_weights
                    ),
                    "expected_roe_equal": group["current_industry_expected_roe"].mean(),
                    "expected_roe_weighted": self._weighted_mean(
                        group["current_industry_expected_roe"],
                        group["available_org_weight"],
                    ),
                    "previous_expected_roe_equal": group[
                        "previous_industry_expected_roe"
                    ].mean(),
                    "previous_expected_roe_weighted": self._weighted_mean(
                        group["previous_industry_expected_roe"],
                        group["available_org_weight"],
                    ),
                    "benchmark_expected_roe_equal": group[
                        "current_benchmark_expected_roe"
                    ].mean(),
                    "benchmark_expected_roe_weighted": self._weighted_mean(
                        group["current_benchmark_expected_roe"],
                        group["available_org_weight"],
                    ),
                    "previous_benchmark_expected_roe_equal": group[
                        "previous_benchmark_expected_roe"
                    ].mean(),
                    "previous_benchmark_expected_roe_weighted": self._weighted_mean(
                        group["previous_benchmark_expected_roe"],
                        group["available_org_weight"],
                    ),
                    "source_max_report_date": group[
                        "effective_source_max_report_date"
                    ].max(),
                    "previous_source_max_report_date": group[
                        "effective_previous_source_max_report_date"
                    ].max(),
                    "stock_formula_versions": versions,
                }
            )

        return (
            frame.groupby(self._IDENTITY_KEYS, sort=False, dropna=False)
            .apply(aggregate, include_groups=False)
            .reset_index()
        )

    def _finish_result(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        for column in self.OUTPUT_COLUMNS:
            if column not in result:
                result[column] = np.nan
        count_columns = [
            "structural_member_count",
            "equity_available_member_count",
            "matched_stock_count",
            "matched_org_count",
            "ratio_matched_org_count",
            "benchmark_structural_member_count",
            "benchmark_matched_stock_count",
            "benchmark_eligible_org_count",
            "spread_up_org_count",
            "spread_down_or_flat_org_count",
            "ratio_up_org_count",
            "ratio_down_or_flat_org_count",
        ]
        for column in count_columns:
            if column not in result:
                result[column] = 0
            result[column] = (
                pd.to_numeric(result[column], errors="coerce").fillna(0).astype(int)
            )
        for column in (
            "spread_up_org_weight",
            "spread_total_org_weight",
            "ratio_up_org_weight",
            "ratio_total_org_weight",
        ):
            if column not in result:
                result[column] = 0.0
            result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)
        if "stock_formula_versions" not in result:
            result["stock_formula_versions"] = [[] for _ in range(len(result))]
        result["stock_formula_versions"] = result["stock_formula_versions"].apply(
            lambda value: value if isinstance(value, list) else []
        )
        result["benchmark_code"] = self.BENCHMARK_CODE
        result["benchmark_name"] = self.BENCHMARK_NAME
        result["equity_basis"] = self.EQUITY_BASIS
        result["method_version"] = self.METHOD_VERSION
        result["org_weight_version"] = self.ORG_WEIGHT_VERSION
        result["quality_rule_version"] = self.QUALITY_RULE_VERSION

        def assess(row: pd.Series) -> pd.Series:
            base_reasons: list[str] = []
            if pd.isna(row["equity_trade_date"]):
                base_reasons.append("missing_equity_snapshot")
            elif int(row["equity_staleness_days"]) > self.MAX_EQUITY_STALENESS_DAYS:
                base_reasons.append("stale_equity_snapshot")
            if pd.isna(row["benchmark_weight_trade_date"]):
                base_reasons.append("missing_benchmark_snapshot")
            elif (
                int(row["benchmark_weight_staleness_days"])
                > self.MAX_BENCHMARK_STALENESS_DAYS
            ):
                base_reasons.append("stale_benchmark_snapshot")
            if int(row["matched_org_count"]) < self.threshold.min_matched_orgs:
                base_reasons.append("insufficient_matched_orgs")
            if pd.isna(row["fapi_spread_equal"]):
                base_reasons.append("no_spread_fapi")
            if (
                pd.notna(row["source_max_report_date"])
                and row["source_max_report_date"] > row["obs_date"]
            ):
                base_reasons.append("future_current_report_date")
            if (
                pd.notna(row["previous_source_max_report_date"])
                and row["previous_source_max_report_date"] >= row["obs_date"]
            ):
                base_reasons.append("future_previous_report_date")
            ratio_eligible = (
                not {
                    "missing_equity_snapshot",
                    "stale_equity_snapshot",
                    "missing_benchmark_snapshot",
                    "stale_benchmark_snapshot",
                    "future_current_report_date",
                    "future_previous_report_date",
                }.intersection(base_reasons)
                and int(row["ratio_matched_org_count"])
                >= self.threshold.min_matched_orgs
                and pd.notna(row["fapi_ratio_equal"])
            )
            return pd.Series(
                {
                    "quality_reasons": sorted(base_reasons),
                    "is_eligible": not bool(base_reasons),
                    "is_ratio_eligible": bool(ratio_eligible),
                }
            )

        quality = result.apply(assess, axis=1)
        for column in quality.columns:
            result[column] = quality[column]
        result = (
            result.reindex(columns=self.OUTPUT_COLUMNS)
            .sort_values(
                ["obs_date", "industry_level", "industry_code"], kind="mergesort"
            )
            .reset_index(drop=True)
        )
        self.last_audit.update(
            {
                "output_row_count": int(len(result)),
                "l1_row_count": int(result["industry_level"].eq("L1").sum()),
                "l2_row_count": int(result["industry_level"].eq("L2").sum()),
                "spread_valued_row_count": int(
                    result["fapi_spread_equal"].notna().sum()
                ),
                "eligible_row_count": int(result["is_eligible"].sum()),
                "ratio_eligible_row_count": int(result["is_ratio_eligible"].sum()),
                "spread_denominator_mismatch_count": int(
                    (
                        result["matched_org_count"]
                        != result["spread_up_org_count"]
                        + result["spread_down_or_flat_org_count"]
                    ).sum()
                ),
                "ratio_denominator_mismatch_count": int(
                    (
                        result["ratio_matched_org_count"]
                        != result["ratio_up_org_count"]
                        + result["ratio_down_or_flat_org_count"]
                    ).sum()
                ),
            }
        )
        return result


__all__ = [
    "FAPIQualityThreshold",
    "IndustryFAPICalculator",
]
