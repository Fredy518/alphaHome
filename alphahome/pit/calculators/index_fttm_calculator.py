"""Database-agnostic important-index and all-A FTTM aggregation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .matched_fttm_revision_calculator import (
    REVISION_OUTPUT_COLUMNS,
    REVISION_SOURCE_COLUMNS,
    REVISION_VERSION,
    build_matched_revision_metrics,
)
from .stock_fttm_calculator import StockFTTMCalculator


@dataclass(frozen=True)
class IndexQualityThreshold:
    """Coverage gates for one index-universe type."""

    min_covered_stocks: int
    min_covered_weight_rate: float = 0.60
    min_weight_data_coverage_rate: float = 0.98
    min_org_count: int = 5
    min_matched_org_count: int = 5


DEFAULT_QUALITY_THRESHOLDS: Mapping[str, IndexQualityThreshold] = {
    "index": IndexQualityThreshold(min_covered_stocks=10),
    "all_a": IndexQualityThreshold(min_covered_stocks=100),
}


class IndexFTTMCalculator:
    """Aggregate stock/broker FTTM snapshots into index-like universe rows."""

    AGGREGATION_VERSION = "org_weighted_then_equal_mean_v1"
    QUALITY_RULE_VERSION = "index_coverage_gate_v1"
    MAX_WEIGHT_STALENESS_DAYS = {
        "official_weight": 65,
        "total_mv": 31,
    }
    OUTPUT_COLUMNS = [
        "obs_date",
        "universe_type",
        "universe_code",
        "universe_name",
        "weight_basis",
        "weight_source",
        "weight_trade_date",
        "weight_staleness_days",
        "structural_member_count",
        "active_member_count",
        "weight_available_count",
        "covered_stock_count",
        "org_count",
        "median_org_stock_count",
        "median_org_weight_coverage",
        "p25_org_weight_coverage",
        "matched_org_count",
        "up_org_count",
        "down_or_flat_org_count",
        "structural_weight",
        "covered_weight",
        "covered_stock_rate",
        "covered_weight_rate",
        "weight_data_coverage_rate",
        "index_fttm_np",
        "index_fttm_np_median",
        "previous_index_fttm_np",
        "fttm_np_mom_abs",
        "fttm_np_mom_rate",
        "diffusion_up",
        *REVISION_OUTPUT_COLUMNS,
        "is_revision_eligible",
        "revision_quality_reasons",
        "is_eligible",
        "is_diffusion_eligible",
        "quality_reasons",
        "stock_formula_version",
        "aggregation_version",
        "quality_rule_version",
        "source_max_report_date",
    ]

    def __init__(
        self,
        thresholds: Mapping[str, IndexQualityThreshold] | None = None,
        max_weight_staleness_days: Mapping[str, int] | None = None,
    ) -> None:
        self.thresholds = dict(thresholds or DEFAULT_QUALITY_THRESHOLDS)
        self.max_weight_staleness_days = dict(
            max_weight_staleness_days or self.MAX_WEIGHT_STALENESS_DAYS
        )
        self.last_audit: dict[str, Any] = {}
        self.last_org_universe: pd.DataFrame = pd.DataFrame()

    def calculate(
        self,
        members: pd.DataFrame,
        stock_basic: pd.DataFrame,
        stock_fttm: pd.DataFrame,
        obs_dates: Sequence[date | str | pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        universe_members = self._prepare_members(members)
        if obs_dates is not None:
            wanted = {pd.Timestamp(value).normalize() for value in obs_dates}
            universe_members = universe_members.loc[
                universe_members["obs_date"].isin(wanted)
            ].copy()
        if universe_members.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        basics = self._prepare_stock_basic(stock_basic)
        fttm = self._prepare_stock_fttm(stock_fttm)
        universe_members = universe_members.merge(
            basics, on="ts_code", how="left", sort=False
        )
        universe_members["is_active"] = (
            universe_members["is_supported_a_share"].fillna(False)
            & universe_members["list_date"].notna()
            & (universe_members["list_date"] <= universe_members["obs_date"])
            & (
                universe_members["delist_date"].isna()
                | (universe_members["delist_date"] > universe_members["obs_date"])
            )
        )

        covered_stocks = fttm[["obs_date", "ts_code"]].drop_duplicates()
        covered_stocks["has_fttm"] = True
        universe_members = universe_members.merge(
            covered_stocks,
            on=["obs_date", "ts_code"],
            how="left",
            sort=False,
        )
        universe_members["has_fttm"] = (
            universe_members["has_fttm"].astype("boolean").fillna(False).astype(bool)
        )
        universe_members["has_valid_weight"] = (
            universe_members["is_active"]
            & universe_members["valid_weight"].notna()
            & (universe_members["valid_weight"] > 0)
        )

        group_keys = [
            "obs_date",
            "universe_type",
            "universe_code",
            "universe_name",
            "weight_basis",
            "weight_source",
        ]
        structural = self._build_structural_rows(universe_members, group_keys)
        org_rows = self._build_org_rows(universe_members, fttm, structural, group_keys)
        self.last_org_universe = org_rows.copy()
        consensus = self._build_consensus(org_rows, group_keys)
        result = structural.merge(consensus, on=group_keys, how="left", sort=False)

        result = self._attach_previous_consensus(result)
        diffusion = self._build_diffusion(org_rows)
        diffusion_keys = [
            "obs_date",
            "universe_type",
            "universe_code",
            "weight_basis",
        ]
        result = result.merge(diffusion, on=diffusion_keys, how="left", sort=False)
        revision_members = universe_members.loc[
            universe_members["is_active"] & universe_members["has_valid_weight"]
        ][group_keys + ["ts_code", "valid_weight"]]
        revision = build_matched_revision_metrics(
            revision_members,
            fttm,
            group_keys=group_keys,
            weight_column="valid_weight",
        )
        result = result.merge(revision, on=group_keys, how="left", sort=False)
        for column in (
            "org_count",
            "matched_org_count",
            "up_org_count",
            "down_or_flat_org_count",
            "revision_comparable_stock_count",
            "revision_comparable_org_count",
        ):
            result[column] = result[column].astype("Int64").fillna(0).astype(int)

        result["diffusion_up"] = np.where(
            result["matched_org_count"] > 0,
            result["up_org_count"] / result["matched_org_count"],
            np.nan,
        )
        result = self._apply_quality(result)
        result = self._apply_revision_quality(result)
        result["stock_formula_version"] = result["stock_formula_version"].fillna(
            StockFTTMCalculator.FORMULA_VERSION
        )
        result["aggregation_version"] = self.AGGREGATION_VERSION
        result["quality_rule_version"] = self.QUALITY_RULE_VERSION
        result["revision_version"] = result["revision_version"].fillna(REVISION_VERSION)
        result = (
            result[self.OUTPUT_COLUMNS]
            .sort_values(
                ["obs_date", "universe_type", "universe_code"], kind="mergesort"
            )
            .reset_index(drop=True)
        )
        self.last_audit.update(
            {
                "output_row_count": int(len(result)),
                "index_row_count": int((result["universe_type"] == "index").sum()),
                "all_a_row_count": int((result["universe_type"] == "all_a").sum()),
                "valued_row_count": int(result["index_fttm_np"].notna().sum()),
                "eligible_row_count": int(result["is_eligible"].sum()),
            }
        )
        return result

    def _prepare_members(self, frame: pd.DataFrame) -> pd.DataFrame:
        required = {
            "obs_date",
            "universe_type",
            "universe_code",
            "universe_name",
            "weight_basis",
            "weight_source",
            "weight_trade_date",
            "ts_code",
            "raw_weight",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"指数/全A成分权重缺少字段: {missing}")

        members = frame[list(required)].copy()
        for column in (
            "universe_type",
            "universe_code",
            "universe_name",
            "weight_basis",
            "weight_source",
            "ts_code",
        ):
            members[column] = members[column].astype("string").fillna("").str.strip()
        members["universe_type"] = members["universe_type"].str.lower()
        members["obs_date"] = pd.to_datetime(
            members["obs_date"], errors="coerce"
        ).dt.normalize()
        members["weight_trade_date"] = pd.to_datetime(
            members["weight_trade_date"], errors="coerce"
        ).dt.normalize()
        members["raw_weight"] = pd.to_numeric(members["raw_weight"], errors="coerce")
        members = members.loc[
            members["obs_date"].notna()
            & members["universe_type"].isin(self.thresholds)
            & members["universe_code"].ne("")
            & members["universe_name"].ne("")
            & members["weight_basis"].isin(self.max_weight_staleness_days)
            & members["weight_source"].ne("")
            & members["ts_code"].ne("")
        ].copy()
        members["weight_staleness_days"] = (
            members["obs_date"] - members["weight_trade_date"]
        ).dt.days
        allowed_staleness = members["weight_basis"].map(self.max_weight_staleness_days)
        valid = (
            members["raw_weight"].notna()
            & (members["raw_weight"] > 0)
            & members["weight_staleness_days"].ge(0)
            & members["weight_staleness_days"].le(allowed_staleness)
        )
        members["valid_weight"] = members["raw_weight"].where(valid)

        identity = [
            "obs_date",
            "universe_type",
            "universe_code",
            "weight_basis",
        ]
        name_conflicts = int(
            (
                members.groupby(identity, dropna=False)["universe_name"].nunique() > 1
            ).sum()
        )
        members = members.sort_values(
            identity + ["ts_code", "weight_trade_date", "raw_weight"],
            ascending=[True, True, True, True, True, False, False],
            kind="mergesort",
            na_position="last",
        ).drop_duplicates(identity + ["ts_code"], keep="first")
        self.last_audit = {
            "member_source_row_count": int(len(frame)),
            "prepared_member_row_count": int(len(members)),
            "universe_name_conflict_count": name_conflicts,
            "invalid_weight_row_count": int(members["valid_weight"].isna().sum()),
        }
        return members.reset_index(drop=True)

    @staticmethod
    def _prepare_stock_basic(frame: pd.DataFrame) -> pd.DataFrame:
        required = {"ts_code", "list_date", "delist_date", "exchange", "curr_type"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"stock_basic 缺少字段: {missing}")
        basics = frame[list(required)].copy()
        basics["ts_code"] = basics["ts_code"].astype("string").str.strip()
        basics["list_date"] = pd.to_datetime(
            basics["list_date"], errors="coerce"
        ).dt.normalize()
        basics["delist_date"] = pd.to_datetime(
            basics["delist_date"], errors="coerce"
        ).dt.normalize()
        basics["exchange"] = basics["exchange"].astype("string").str.strip().str.upper()
        basics["curr_type"] = (
            basics["curr_type"].astype("string").str.strip().str.upper()
        )
        basics["is_supported_a_share"] = basics["exchange"].isin(
            {"SSE", "SZSE", "BSE"}
        ) & basics["curr_type"].eq("CNY")
        return basics.sort_values(
            ["ts_code", "list_date", "delist_date"],
            kind="mergesort",
            na_position="last",
        ).drop_duplicates("ts_code", keep="first")

    @staticmethod
    def _prepare_stock_fttm(frame: pd.DataFrame) -> pd.DataFrame:
        required = {"obs_date", "ts_code", "org_name", "fttm_np"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"pit_stock_fttm_monthly 缺少字段: {missing}")
        columns = list(required) + [
            column
            for column in (
                "formula_version",
                "selected_report_date",
                *REVISION_SOURCE_COLUMNS,
            )
            if column in frame.columns
        ]
        columns = list(dict.fromkeys(columns))
        fttm = frame[columns].copy()
        fttm["obs_date"] = pd.to_datetime(
            fttm["obs_date"], errors="coerce"
        ).dt.normalize()
        fttm["ts_code"] = fttm["ts_code"].astype("string").str.strip()
        fttm["org_name"] = fttm["org_name"].astype("string").fillna("").str.strip()
        fttm["fttm_np"] = pd.to_numeric(fttm["fttm_np"], errors="coerce")
        if "selected_report_date" in fttm:
            fttm["selected_report_date"] = pd.to_datetime(
                fttm["selected_report_date"], errors="coerce"
            ).dt.normalize()
        else:
            fttm["selected_report_date"] = pd.NaT
        if "formula_version" not in fttm:
            fttm["formula_version"] = StockFTTMCalculator.FORMULA_VERSION
        fttm = fttm.loc[
            fttm["obs_date"].notna()
            & fttm["ts_code"].ne("")
            & fttm["org_name"].ne("")
            & fttm["fttm_np"].notna()
        ].copy()
        return fttm.sort_values(
            ["obs_date", "ts_code", "org_name", "selected_report_date", "fttm_np"],
            ascending=[True, True, True, False, True],
            kind="mergesort",
            na_position="last",
        ).drop_duplicates(["obs_date", "ts_code", "org_name"], keep="first")

    @staticmethod
    def _build_structural_rows(
        members: pd.DataFrame, group_keys: list[str]
    ) -> pd.DataFrame:
        def aggregate(group: pd.DataFrame) -> pd.Series:
            active = group.loc[group["is_active"]]
            valid_weight = active.loc[active["has_valid_weight"]]
            covered = active.loc[active["has_fttm"]]
            covered_weight = valid_weight.loc[valid_weight["has_fttm"]]
            weight_dates = active["weight_trade_date"].dropna()
            staleness = active["weight_staleness_days"].dropna()
            return pd.Series(
                {
                    "weight_trade_date": (
                        weight_dates.max() if not weight_dates.empty else pd.NaT
                    ),
                    "weight_staleness_days": (
                        int(staleness.max()) if not staleness.empty else np.nan
                    ),
                    "structural_member_count": int(group["ts_code"].nunique()),
                    "active_member_count": int(active["ts_code"].nunique()),
                    "weight_available_count": int(valid_weight["ts_code"].nunique()),
                    "covered_stock_count": int(covered["ts_code"].nunique()),
                    "structural_weight": valid_weight["valid_weight"].sum(min_count=1),
                    "covered_weight": covered_weight["valid_weight"].sum(min_count=1),
                }
            )

        structural = (
            members.groupby(group_keys, sort=False, dropna=False)
            .apply(aggregate, include_groups=False)
            .reset_index()
        )
        structural["covered_stock_rate"] = np.where(
            structural["active_member_count"] > 0,
            structural["covered_stock_count"] / structural["active_member_count"],
            np.nan,
        )
        structural["covered_weight_rate"] = np.where(
            structural["structural_weight"].notna()
            & (structural["structural_weight"] != 0),
            structural["covered_weight"] / structural["structural_weight"],
            np.nan,
        )
        structural["weight_data_coverage_rate"] = np.where(
            structural["active_member_count"] > 0,
            structural["weight_available_count"] / structural["active_member_count"],
            np.nan,
        )
        return structural

    @staticmethod
    def _build_org_rows(
        members: pd.DataFrame,
        fttm: pd.DataFrame,
        structural: pd.DataFrame,
        group_keys: list[str],
    ) -> pd.DataFrame:
        eligible = members.loc[members["is_active"] & members["has_valid_weight"]][
            group_keys + ["ts_code", "valid_weight"]
        ]
        joined = eligible.merge(
            fttm, on=["obs_date", "ts_code"], how="inner", sort=False
        )
        output_columns = group_keys + [
            "org_name",
            "org_index_fttm",
            "org_stock_count",
            "org_weight_coverage",
            "formula_version",
            "selected_report_date",
        ]
        if joined.empty:
            return pd.DataFrame(columns=output_columns)
        joined["weighted_fttm"] = joined["valid_weight"] * joined["fttm_np"]
        org_keys = group_keys + ["org_name"]
        org_rows = (
            joined.groupby(org_keys, sort=False, dropna=False)
            .agg(
                org_weighted_sum=("weighted_fttm", "sum"),
                org_covered_weight=("valid_weight", "sum"),
                org_stock_count=("ts_code", "nunique"),
                formula_version=("formula_version", "min"),
                selected_report_date=("selected_report_date", "max"),
            )
            .reset_index()
        )
        org_rows["org_index_fttm"] = (
            org_rows["org_weighted_sum"] / org_rows["org_covered_weight"]
        )
        org_rows = org_rows.merge(
            structural[group_keys + ["structural_weight"]],
            on=group_keys,
            how="left",
            sort=False,
        )
        org_rows["org_weight_coverage"] = np.where(
            org_rows["structural_weight"].notna()
            & (org_rows["structural_weight"] != 0),
            org_rows["org_covered_weight"] / org_rows["structural_weight"],
            np.nan,
        )
        return org_rows[output_columns]

    @staticmethod
    def _build_consensus(org_rows: pd.DataFrame, group_keys: list[str]) -> pd.DataFrame:
        output_columns = group_keys + [
            "index_fttm_np",
            "index_fttm_np_median",
            "org_count",
            "median_org_stock_count",
            "median_org_weight_coverage",
            "p25_org_weight_coverage",
            "stock_formula_version",
            "source_max_report_date",
        ]
        if org_rows.empty:
            return pd.DataFrame(columns=output_columns)
        return (
            org_rows.groupby(group_keys, sort=False, dropna=False)
            .agg(
                index_fttm_np=("org_index_fttm", "mean"),
                index_fttm_np_median=("org_index_fttm", "median"),
                org_count=("org_name", "nunique"),
                median_org_stock_count=("org_stock_count", "median"),
                median_org_weight_coverage=("org_weight_coverage", "median"),
                p25_org_weight_coverage=(
                    "org_weight_coverage",
                    lambda values: values.quantile(0.25),
                ),
                stock_formula_version=("formula_version", "min"),
                source_max_report_date=("selected_report_date", "max"),
            )
            .reset_index()
        )

    @staticmethod
    def _attach_previous_consensus(frame: pd.DataFrame) -> pd.DataFrame:
        identity = ["universe_type", "universe_code", "weight_basis"]
        previous = frame[identity + ["obs_date", "index_fttm_np"]].copy()
        previous["obs_date"] = previous["obs_date"] + pd.offsets.MonthEnd(1)
        previous = previous.rename(columns={"index_fttm_np": "previous_index_fttm_np"})
        result = frame.merge(
            previous, on=identity + ["obs_date"], how="left", sort=False
        )
        result["fttm_np_mom_abs"] = (
            result["index_fttm_np"] - result["previous_index_fttm_np"]
        )
        valid_previous = result["previous_index_fttm_np"].notna() & (
            result["previous_index_fttm_np"] != 0
        )
        result["fttm_np_mom_rate"] = np.where(
            valid_previous,
            result["fttm_np_mom_abs"] / result["previous_index_fttm_np"].abs(),
            np.nan,
        )
        return result

    @staticmethod
    def _build_diffusion(org_rows: pd.DataFrame) -> pd.DataFrame:
        identity = ["universe_type", "universe_code", "weight_basis"]
        output_columns = [
            "obs_date",
            *identity,
            "matched_org_count",
            "up_org_count",
            "down_or_flat_org_count",
        ]
        if org_rows.empty:
            return pd.DataFrame(columns=output_columns)
        previous = org_rows[
            identity + ["obs_date", "org_name", "org_index_fttm"]
        ].copy()
        previous["obs_date"] = previous["obs_date"] + pd.offsets.MonthEnd(1)
        previous = previous.rename(columns={"org_index_fttm": "previous_org_fttm"})
        current = org_rows[identity + ["obs_date", "org_name", "org_index_fttm"]]
        matched = current.merge(
            previous, on=identity + ["obs_date", "org_name"], how="inner", sort=False
        )
        if matched.empty:
            return pd.DataFrame(columns=output_columns)
        matched["is_up"] = matched["org_index_fttm"] > matched["previous_org_fttm"]
        result = (
            matched.groupby(["obs_date", *identity], sort=False, dropna=False)
            .agg(
                matched_org_count=("org_name", "nunique"),
                up_org_count=("is_up", "sum"),
            )
            .reset_index()
        )
        result["down_or_flat_org_count"] = (
            result["matched_org_count"] - result["up_org_count"]
        )
        return result[output_columns]

    def _apply_quality(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()

        def assess(row: pd.Series) -> pd.Series:
            threshold = self.thresholds[str(row["universe_type"])]
            reasons: list[str] = []
            if pd.isna(row["index_fttm_np"]) or int(row["org_count"]) == 0:
                reasons.append("no_fttm_coverage")
            if int(row["covered_stock_count"]) < threshold.min_covered_stocks:
                reasons.append("ineligible_low_stock_coverage")
            if (
                pd.isna(row["covered_weight_rate"])
                or float(row["covered_weight_rate"]) < threshold.min_covered_weight_rate
            ):
                reasons.append("ineligible_low_weight_coverage")
            if (
                pd.isna(row["weight_data_coverage_rate"])
                or float(row["weight_data_coverage_rate"])
                < threshold.min_weight_data_coverage_rate
            ):
                reasons.append("ineligible_incomplete_weight_data")
            if int(row["org_count"]) < threshold.min_org_count:
                reasons.append("ineligible_low_org_count")
            if int(row["matched_org_count"]) < threshold.min_matched_org_count:
                reasons.append("diffusion_ineligible_low_match")

            main_failures = {
                "no_fttm_coverage",
                "ineligible_low_stock_coverage",
                "ineligible_low_weight_coverage",
                "ineligible_incomplete_weight_data",
                "ineligible_low_org_count",
            }
            return pd.Series(
                {
                    "quality_reasons": sorted(reasons),
                    "is_eligible": not bool(main_failures.intersection(reasons)),
                    "is_diffusion_eligible": int(row["matched_org_count"])
                    >= threshold.min_matched_org_count,
                }
            )

        quality = result.apply(assess, axis=1)
        for column in quality.columns:
            result[column] = quality[column]
        return result

    def _apply_revision_quality(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()

        def assess(row: pd.Series) -> pd.Series:
            threshold = self.thresholds[str(row["universe_type"])]
            reasons: list[str] = []
            if pd.isna(row["revision_rate"]):
                reasons.append("no_comparable_revision")
            if (
                int(row["revision_comparable_stock_count"])
                < threshold.min_covered_stocks
            ):
                reasons.append("revision_ineligible_low_stock_coverage")
            if (
                pd.isna(row["revision_comparable_weight_rate"])
                or float(row["revision_comparable_weight_rate"])
                < threshold.min_covered_weight_rate
            ):
                reasons.append("revision_ineligible_low_weight_coverage")
            if int(row["revision_comparable_org_count"]) < threshold.min_org_count:
                reasons.append("revision_ineligible_low_org_count")
            return pd.Series(
                {
                    "revision_quality_reasons": sorted(reasons),
                    "is_revision_eligible": not bool(reasons),
                }
            )

        quality = result.apply(assess, axis=1)
        for column in quality.columns:
            result[column] = quality[column]
        return result


__all__ = [
    "DEFAULT_QUALITY_THRESHOLDS",
    "IndexFTTMCalculator",
    "IndexQualityThreshold",
]
