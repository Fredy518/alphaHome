"""Database-agnostic SW L1/L2 FTTM aggregation calculator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .stock_fttm_calculator import StockFTTMCalculator


@dataclass(frozen=True)
class IndustryQualityThreshold:
    min_covered_stocks: int
    min_covered_mv_rate: float = 0.60
    min_weight_coverage_rate: float = 0.98
    min_org_count: int = 5
    min_matched_org_count: int = 5


DEFAULT_QUALITY_THRESHOLDS: Mapping[str, IndustryQualityThreshold] = {
    "L1": IndustryQualityThreshold(min_covered_stocks=10),
    "L2": IndustryQualityThreshold(min_covered_stocks=5),
}


class IndustryFTTMCalculator:
    """Aggregate stock/broker FTTM snapshots into structural industry rows."""

    CLASSIFICATION_SOURCE = "sw"
    WEIGHT_BASIS = "total_mv"
    AGGREGATION_VERSION = "org_mv_weighted_then_equal_mean_v1"
    QUALITY_RULE_VERSION = "coverage_gate_v1"
    OUTPUT_COLUMNS = [
        "obs_date",
        "classification_source",
        "industry_level",
        "industry_code",
        "industry_name",
        "weight_basis",
        "weight_trade_date",
        "weight_staleness_days",
        "structural_member_count",
        "active_member_count",
        "weight_available_count",
        "covered_stock_count",
        "org_count",
        "median_org_stock_count",
        "median_org_mv_coverage",
        "p25_org_mv_coverage",
        "matched_org_count",
        "up_org_count",
        "down_or_flat_org_count",
        "structural_mv",
        "covered_mv",
        "covered_stock_rate",
        "covered_mv_rate",
        "weight_data_coverage_rate",
        "industry_fttm_np",
        "industry_fttm_np_median",
        "previous_industry_fttm_np",
        "fttm_np_mom_abs",
        "fttm_np_mom_rate",
        "diffusion_up",
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
        thresholds: Mapping[str, IndustryQualityThreshold] | None = None,
        max_weight_staleness_days: int = 31,
    ) -> None:
        self.thresholds = dict(thresholds or DEFAULT_QUALITY_THRESHOLDS)
        self.max_weight_staleness_days = int(max_weight_staleness_days)
        self.last_audit: dict[str, Any] = {}
        self.last_org_industry: pd.DataFrame = pd.DataFrame()

    def calculate(
        self,
        classifications: pd.DataFrame,
        stock_basic: pd.DataFrame,
        weights: pd.DataFrame,
        stock_fttm: pd.DataFrame,
        obs_dates: Sequence[date | str | pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        members = self._prepare_members(classifications)
        if obs_dates is not None:
            wanted = {pd.Timestamp(value).normalize() for value in obs_dates}
            members = members.loc[members["obs_date"].isin(wanted)].copy()
        if members.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        basics = self._prepare_stock_basic(stock_basic)
        weight_frame = self._prepare_weights(weights)
        fttm = self._prepare_stock_fttm(stock_fttm)

        members = members.merge(basics, on="ts_code", how="left", sort=False)
        members["is_active"] = (
            members["is_supported_a_share"].fillna(False)
            & members["list_date"].notna()
            & (members["list_date"] <= members["obs_date"])
            & (
                members["delist_date"].isna()
                | (members["delist_date"] > members["obs_date"])
            )
        )
        members = members.merge(
            weight_frame,
            on=["obs_date", "ts_code"],
            how="left",
            sort=False,
        )

        covered_stocks = fttm[["obs_date", "ts_code"]].drop_duplicates()
        covered_stocks["has_fttm"] = True
        members = members.merge(
            covered_stocks,
            on=["obs_date", "ts_code"],
            how="left",
            sort=False,
        )
        members["has_fttm"] = (
            members["has_fttm"].astype("boolean").fillna(False).astype(bool)
        )
        members["has_valid_weight"] = (
            members["is_active"]
            & members["valid_total_mv"].notna()
            & (members["valid_total_mv"] > 0)
        )
        members["is_covered_active"] = members["is_active"] & members["has_fttm"]
        members["covered_valid_mv"] = members["valid_total_mv"].where(
            members["is_covered_active"] & members["has_valid_weight"]
        )

        group_keys = [
            "obs_date",
            "classification_source",
            "industry_level",
            "industry_code",
            "industry_name",
        ]
        structural = self._build_structural_rows(members, group_keys)
        org_rows = self._build_org_rows(members, fttm, structural, group_keys)
        self.last_org_industry = org_rows.copy()
        consensus = self._build_consensus(org_rows, group_keys)
        result = structural.merge(consensus, on=group_keys, how="left", sort=False)

        result = self._attach_previous_consensus(result)
        diffusion = self._build_diffusion(org_rows)
        diffusion_keys = [
            "obs_date",
            "classification_source",
            "industry_level",
            "industry_code",
        ]
        result = result.merge(diffusion, on=diffusion_keys, how="left", sort=False)
        for column in (
            "org_count",
            "matched_org_count",
            "up_org_count",
            "down_or_flat_org_count",
        ):
            result[column] = result[column].astype("Int64").fillna(0).astype(int)

        result["diffusion_up"] = np.where(
            result["matched_org_count"] > 0,
            result["up_org_count"] / result["matched_org_count"],
            np.nan,
        )
        result = self._apply_quality(result)
        result["weight_basis"] = self.WEIGHT_BASIS
        result["stock_formula_version"] = result["stock_formula_version"].fillna(
            StockFTTMCalculator.FORMULA_VERSION
        )
        result["aggregation_version"] = self.AGGREGATION_VERSION
        result["quality_rule_version"] = self.QUALITY_RULE_VERSION

        result = result[self.OUTPUT_COLUMNS].sort_values(
            ["obs_date", "industry_level", "industry_code"], kind="mergesort"
        ).reset_index(drop=True)
        self.last_audit.update(
            {
                "output_row_count": int(len(result)),
                "l1_row_count": int((result["industry_level"] == "L1").sum()),
                "l2_row_count": int((result["industry_level"] == "L2").sum()),
                "valued_row_count": int(result["industry_fttm_np"].notna().sum()),
                "eligible_row_count": int(result["is_eligible"].sum()),
            }
        )
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
        source["obs_date"] = pd.to_datetime(source["obs_date"], errors="coerce").dt.normalize()
        source["data_source"] = source["data_source"].astype("string").str.strip().str.lower()
        source["ts_code"] = source["ts_code"].astype("string").str.strip()
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
            piece = source[["ts_code", "obs_date", "data_source", code_column, name_column]].copy()
            piece = piece.rename(
                columns={
                    "data_source": "classification_source",
                    code_column: "industry_code",
                    name_column: "industry_name",
                }
            )
            piece["industry_level"] = level
            piece["industry_code"] = piece["industry_code"].astype("string").fillna("").str.strip()
            piece["industry_name"] = piece["industry_name"].astype("string").fillna("").str.strip()
            missing_code_count += int(piece["industry_code"].eq("").sum())
            missing_name_count += int(piece["industry_name"].eq("").sum())
            piece = piece.loc[
                piece["industry_code"].ne("") & piece["industry_name"].ne("")
            ].copy()
            pieces.append(piece)

        members = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
        identity_keys = [
            "obs_date",
            "classification_source",
            "industry_level",
            "industry_code",
        ]
        name_counts = members.groupby(identity_keys, dropna=False)["industry_name"].nunique()
        name_conflicts = int((name_counts > 1).sum())
        canonical_names = (
            members.sort_values(identity_keys + ["industry_name"], kind="mergesort")
            .drop_duplicates(identity_keys, keep="first")[identity_keys + ["industry_name"]]
        )
        members = members.drop(columns="industry_name").merge(
            canonical_names, on=identity_keys, how="left", sort=False
        )
        members = members.sort_values(
            identity_keys + ["ts_code", "industry_name"], kind="mergesort"
        ).drop_duplicates(identity_keys + ["ts_code"], keep="first")
        self.last_audit = {
            "classification_source_row_count": int(len(source)),
            "missing_industry_code_count": missing_code_count,
            "missing_industry_name_count": missing_name_count,
            "industry_name_conflict_count": name_conflicts,
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
        basics["list_date"] = pd.to_datetime(basics["list_date"], errors="coerce").dt.normalize()
        basics["delist_date"] = pd.to_datetime(basics["delist_date"], errors="coerce").dt.normalize()
        basics["exchange"] = basics["exchange"].astype("string").str.strip().str.upper()
        basics["curr_type"] = basics["curr_type"].astype("string").str.strip().str.upper()
        basics["is_supported_a_share"] = basics["exchange"].isin({"SSE", "SZSE", "BSE"}) & basics[
            "curr_type"
        ].eq("CNY")
        basics = basics.sort_values(
            ["ts_code", "list_date", "delist_date", "exchange", "curr_type"],
            kind="mergesort",
            na_position="last",
        )
        return basics.drop_duplicates("ts_code", keep="first")

    def _prepare_weights(self, frame: pd.DataFrame) -> pd.DataFrame:
        required = {"obs_date", "ts_code", "weight_trade_date", "total_mv"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"stock_dailybasic 权重数据缺少字段: {missing}")
        weights = frame[list(required)].copy()
        weights["obs_date"] = pd.to_datetime(weights["obs_date"], errors="coerce").dt.normalize()
        weights["weight_trade_date"] = pd.to_datetime(
            weights["weight_trade_date"], errors="coerce"
        ).dt.normalize()
        weights["ts_code"] = weights["ts_code"].astype("string").str.strip()
        weights["total_mv"] = pd.to_numeric(weights["total_mv"], errors="coerce")
        weights["weight_staleness_days"] = (
            weights["obs_date"] - weights["weight_trade_date"]
        ).dt.days
        valid = (
            weights["total_mv"].notna()
            & (weights["total_mv"] > 0)
            & weights["weight_staleness_days"].between(
                0, self.max_weight_staleness_days, inclusive="both"
            )
        )
        weights["valid_total_mv"] = weights["total_mv"].where(valid)
        weights = weights.sort_values(
            ["obs_date", "ts_code", "weight_trade_date", "total_mv"],
            ascending=[True, True, False, True],
            kind="mergesort",
            na_position="last",
        )
        return weights.drop_duplicates(["obs_date", "ts_code"], keep="first")

    @staticmethod
    def _prepare_stock_fttm(frame: pd.DataFrame) -> pd.DataFrame:
        required = {"obs_date", "ts_code", "org_name", "fttm_np"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"pit_stock_fttm_monthly 缺少字段: {missing}")
        columns = list(required) + [
            column
            for column in ("formula_version", "selected_report_date")
            if column in frame.columns
        ]
        fttm = frame[columns].copy()
        fttm["obs_date"] = pd.to_datetime(fttm["obs_date"], errors="coerce").dt.normalize()
        if "selected_report_date" in fttm:
            fttm["selected_report_date"] = pd.to_datetime(
                fttm["selected_report_date"], errors="coerce"
            ).dt.normalize()
        else:
            fttm["selected_report_date"] = pd.NaT
        fttm["ts_code"] = fttm["ts_code"].astype("string").str.strip()
        fttm["org_name"] = fttm["org_name"].astype("string").fillna("").str.strip()
        fttm["fttm_np"] = pd.to_numeric(fttm["fttm_np"], errors="coerce")
        if "formula_version" not in fttm:
            fttm["formula_version"] = StockFTTMCalculator.FORMULA_VERSION
        fttm = fttm.loc[
            fttm["obs_date"].notna()
            & fttm["ts_code"].ne("")
            & fttm["org_name"].ne("")
            & fttm["fttm_np"].notna()
        ].copy()
        fttm = fttm.sort_values(
            ["obs_date", "ts_code", "org_name", "selected_report_date", "fttm_np"],
            ascending=[True, True, True, False, True],
            kind="mergesort",
            na_position="last",
        )
        return fttm.drop_duplicates(["obs_date", "ts_code", "org_name"], keep="first")

    @staticmethod
    def _build_structural_rows(
        members: pd.DataFrame, group_keys: list[str]
    ) -> pd.DataFrame:
        def aggregate(group: pd.DataFrame) -> pd.Series:
            active = group.loc[group["is_active"]]
            valid_weight = active.loc[active["has_valid_weight"]]
            covered = active.loc[active["has_fttm"]]
            covered_weight = valid_weight.loc[valid_weight["has_fttm"]]
            structural_mv = valid_weight["valid_total_mv"].sum(min_count=1)
            covered_mv = covered_weight["valid_total_mv"].sum(min_count=1)
            # The global proxy-weight date remains auditable even when every
            # constituent value is null/non-positive for one industry.
            weight_dates = active["weight_trade_date"].dropna()
            staleness = active["weight_staleness_days"].dropna()
            return pd.Series(
                {
                    "weight_trade_date": weight_dates.max() if not weight_dates.empty else pd.NaT,
                    "weight_staleness_days": int(staleness.max()) if not staleness.empty else np.nan,
                    "structural_member_count": int(group["ts_code"].nunique()),
                    "active_member_count": int(active["ts_code"].nunique()),
                    "weight_available_count": int(valid_weight["ts_code"].nunique()),
                    "covered_stock_count": int(covered["ts_code"].nunique()),
                    "structural_mv": structural_mv,
                    "covered_mv": covered_mv,
                }
            )

        structural = members.groupby(group_keys, sort=False, dropna=False).apply(
            aggregate, include_groups=False
        ).reset_index()
        structural["covered_stock_rate"] = np.where(
            structural["active_member_count"] > 0,
            structural["covered_stock_count"] / structural["active_member_count"],
            np.nan,
        )
        structural["covered_mv_rate"] = np.where(
            structural["structural_mv"].notna() & (structural["structural_mv"] != 0),
            structural["covered_mv"] / structural["structural_mv"],
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
        eligible_members = members.loc[
            members["is_active"] & members["has_valid_weight"]
        ][group_keys + ["ts_code", "valid_total_mv"]]
        joined = eligible_members.merge(
            fttm,
            on=["obs_date", "ts_code"],
            how="inner",
            sort=False,
        )
        if joined.empty:
            return pd.DataFrame(
                columns=group_keys
                + [
                    "org_name",
                    "org_industry_fttm",
                    "org_stock_count",
                    "org_mv_coverage",
                    "formula_version",
                    "selected_report_date",
                ]
            )
        joined["weighted_fttm"] = joined["valid_total_mv"] * joined["fttm_np"]
        org_keys = group_keys + ["org_name"]
        org_rows = joined.groupby(org_keys, sort=False, dropna=False).agg(
            org_weighted_sum=("weighted_fttm", "sum"),
            org_covered_mv=("valid_total_mv", "sum"),
            org_stock_count=("ts_code", "nunique"),
            formula_version=("formula_version", "min"),
            selected_report_date=("selected_report_date", "max"),
        ).reset_index()
        org_rows["org_industry_fttm"] = (
            org_rows["org_weighted_sum"] / org_rows["org_covered_mv"]
        )
        org_rows = org_rows.merge(
            structural[group_keys + ["structural_mv"]],
            on=group_keys,
            how="left",
            sort=False,
        )
        org_rows["org_mv_coverage"] = np.where(
            org_rows["structural_mv"].notna() & (org_rows["structural_mv"] != 0),
            org_rows["org_covered_mv"] / org_rows["structural_mv"],
            np.nan,
        )
        return org_rows

    @staticmethod
    def _build_consensus(org_rows: pd.DataFrame, group_keys: list[str]) -> pd.DataFrame:
        if org_rows.empty:
            return pd.DataFrame(
                columns=group_keys
                + [
                    "industry_fttm_np",
                    "industry_fttm_np_median",
                    "org_count",
                    "median_org_stock_count",
                    "median_org_mv_coverage",
                    "p25_org_mv_coverage",
                    "stock_formula_version",
                    "source_max_report_date",
                ]
            )
        return org_rows.groupby(group_keys, sort=False, dropna=False).agg(
            industry_fttm_np=("org_industry_fttm", "mean"),
            industry_fttm_np_median=("org_industry_fttm", "median"),
            org_count=("org_name", "nunique"),
            median_org_stock_count=("org_stock_count", "median"),
            median_org_mv_coverage=("org_mv_coverage", "median"),
            p25_org_mv_coverage=("org_mv_coverage", lambda values: values.quantile(0.25)),
            stock_formula_version=("formula_version", "min"),
            source_max_report_date=("selected_report_date", "max"),
        ).reset_index()

    @staticmethod
    def _attach_previous_consensus(frame: pd.DataFrame) -> pd.DataFrame:
        identity_keys = ["classification_source", "industry_level", "industry_code"]
        previous = frame[
            identity_keys + ["obs_date", "industry_fttm_np"]
        ].copy()
        previous["obs_date"] = previous["obs_date"] + pd.offsets.MonthEnd(1)
        previous = previous.rename(
            columns={"industry_fttm_np": "previous_industry_fttm_np"}
        )
        result = frame.merge(previous, on=identity_keys + ["obs_date"], how="left", sort=False)
        result["fttm_np_mom_abs"] = (
            result["industry_fttm_np"] - result["previous_industry_fttm_np"]
        )
        valid_previous = result["previous_industry_fttm_np"].notna() & (
            result["previous_industry_fttm_np"] != 0
        )
        result["fttm_np_mom_rate"] = np.where(
            valid_previous,
            result["fttm_np_mom_abs"] / result["previous_industry_fttm_np"].abs(),
            np.nan,
        )
        return result

    @staticmethod
    def _build_diffusion(org_rows: pd.DataFrame) -> pd.DataFrame:
        keys = ["classification_source", "industry_level", "industry_code"]
        output_columns = [
            "obs_date",
            *keys,
            "matched_org_count",
            "up_org_count",
            "down_or_flat_org_count",
        ]
        if org_rows.empty:
            return pd.DataFrame(columns=output_columns)
        previous = org_rows[keys + ["obs_date", "org_name", "org_industry_fttm"]].copy()
        previous["obs_date"] = previous["obs_date"] + pd.offsets.MonthEnd(1)
        previous = previous.rename(columns={"org_industry_fttm": "previous_org_fttm"})
        current = org_rows[keys + ["obs_date", "org_name", "org_industry_fttm"]]
        matched = current.merge(
            previous, on=keys + ["obs_date", "org_name"], how="inner", sort=False
        )
        if matched.empty:
            return pd.DataFrame(columns=output_columns)
        matched["is_up"] = matched["org_industry_fttm"] > matched["previous_org_fttm"]
        result = matched.groupby(["obs_date", *keys], sort=False, dropna=False).agg(
            matched_org_count=("org_name", "nunique"),
            up_org_count=("is_up", "sum"),
        ).reset_index()
        result["down_or_flat_org_count"] = (
            result["matched_org_count"] - result["up_org_count"]
        )
        return result[output_columns]

    def _apply_quality(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()

        def assess(row: pd.Series) -> pd.Series:
            threshold = self.thresholds[str(row["industry_level"])]
            reasons: list[str] = []
            if pd.isna(row["industry_fttm_np"]) or int(row["org_count"]) == 0:
                reasons.append("no_fttm_coverage")
            if int(row["covered_stock_count"]) < threshold.min_covered_stocks:
                reasons.append("ineligible_low_stock_coverage")
            if pd.isna(row["covered_mv_rate"]) or float(row["covered_mv_rate"]) < threshold.min_covered_mv_rate:
                reasons.append("ineligible_low_mv_coverage")
            if pd.isna(row["weight_data_coverage_rate"]) or float(
                row["weight_data_coverage_rate"]
            ) < threshold.min_weight_coverage_rate:
                reasons.append("ineligible_low_weight_coverage")
            if int(row["org_count"]) < threshold.min_org_count:
                reasons.append("ineligible_low_org_count")
            if int(row["matched_org_count"]) < threshold.min_matched_org_count:
                reasons.append("diffusion_ineligible_low_match")

            main_failures = {
                "no_fttm_coverage",
                "ineligible_low_stock_coverage",
                "ineligible_low_mv_coverage",
                "ineligible_low_weight_coverage",
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


__all__ = [
    "DEFAULT_QUALITY_THRESHOLDS",
    "IndustryFTTMCalculator",
    "IndustryQualityThreshold",
]
