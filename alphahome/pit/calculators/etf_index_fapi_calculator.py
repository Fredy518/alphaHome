"""FAPI calculator for ETF-tracked index constituent universes."""

from __future__ import annotations

from datetime import date
from typing import Any, Sequence

import pandas as pd

from .industry_fapi_calculator import IndustryFAPICalculator


class ETFIndexFAPICalculator(IndustryFAPICalculator):
    """Apply the source-adapted CSI800-relative FAPI to exact index members."""

    CLASSIFICATION_SOURCE = "etf_index"
    METHOD_VERSION = "source_adapted_etf_index_csi800_v1"
    INDEX_OUTPUT_COLUMNS = [
        "obs_date",
        "universe_type",
        "index_code",
        "index_name",
        "member_weight_basis",
        "member_weight_source",
        "member_source_code",
        "member_source_effective_date",
        "member_source_available_date",
        "member_source_staleness_days",
        "member_source_coverage_rate",
        "member_source_quality",
        "member_source_is_fallback",
        "member_source_is_eligible",
        "member_source_quality_reasons",
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
        "is_fapi_eligible",
        "is_ratio_fapi_eligible",
        "fapi_quality_reasons",
        "is_eligible",
        "quality_reasons",
        "method_version",
        "org_weight_version",
        "quality_rule_version",
    ]

    MEMBER_META_COLUMNS = [
        "obs_date",
        "index_code",
        "index_name",
        "weight_basis",
        "weight_source",
        "source_code",
        "source_effective_date",
        "source_available_date",
        "source_staleness_days",
        "source_coverage_rate",
        "source_quality",
        "is_fallback",
        "is_eligible",
        "quality_reasons",
    ]

    def calculate(
        self,
        members: pd.DataFrame,
        equity: pd.DataFrame,
        benchmark_members: pd.DataFrame,
        stock_fttm: pd.DataFrame,
        obs_dates: Sequence[date | str | pd.Timestamp] | None = None,
    ) -> pd.DataFrame:
        prepared, member_meta = self._adapt_members(members)
        base = super().calculate(
            prepared,
            equity,
            benchmark_members,
            stock_fttm,
            obs_dates=obs_dates,
        )
        # The parent calculator inspects both L1 and L2 slots.  Index universes
        # intentionally occupy only the L1 adapter slot, so blank L2 fields are
        # not missing index identities and must not be reported as defects.
        intentionally_absent_l2 = int(len(prepared))
        self.last_audit["missing_industry_code_count"] = max(
            int(self.last_audit.get("missing_industry_code_count", 0))
            - intentionally_absent_l2,
            0,
        )
        self.last_audit["missing_industry_name_count"] = max(
            int(self.last_audit.get("missing_industry_name_count", 0))
            - intentionally_absent_l2,
            0,
        )
        self.last_audit["intentionally_absent_l2_member_count"] = (
            intentionally_absent_l2
        )
        if base.empty:
            return pd.DataFrame(columns=self.INDEX_OUTPUT_COLUMNS)

        base = base.loc[base["industry_level"].eq("L1")].copy()
        base = base.rename(
            columns={
                "industry_code": "index_code",
                "industry_name": "index_name",
                "is_eligible": "is_fapi_eligible",
                "is_ratio_eligible": "is_ratio_fapi_eligible",
                "quality_reasons": "fapi_quality_reasons",
            }
        ).drop(columns=["classification_source", "industry_level"])
        result = base.merge(
            member_meta,
            on=["obs_date", "index_code", "index_name"],
            how="left",
            validate="one_to_one",
        )
        result["universe_type"] = "etf_tracked_index"
        result["is_eligible"] = (
            result["is_fapi_eligible"].fillna(False)
            & result["member_source_is_eligible"].fillna(False)
        )
        result["quality_reasons"] = result.apply(
            self._combined_quality_reasons, axis=1
        )
        return (
            result.reindex(columns=self.INDEX_OUTPUT_COLUMNS)
            .sort_values(["obs_date", "index_code"], kind="mergesort")
            .reset_index(drop=True)
        )

    def _adapt_members(
        self, members: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        required = set(self.MEMBER_META_COLUMNS + ["ts_code"])
        missing = sorted(required - set(members.columns))
        if missing:
            raise ValueError(f"ETF指数PIT成分缺少字段: {missing}")
        source = members[list(required)].copy()
        source["obs_date"] = pd.to_datetime(
            source["obs_date"], errors="coerce"
        ).dt.normalize()
        for column in (
            "index_code",
            "index_name",
            "ts_code",
            "weight_basis",
            "weight_source",
            "source_code",
            "source_quality",
        ):
            source[column] = source[column].astype("string").fillna("").str.strip()
        for column in ("source_effective_date", "source_available_date"):
            source[column] = pd.to_datetime(
                source[column], errors="coerce"
            ).dt.normalize()
        source["quality_reasons"] = source["quality_reasons"].apply(
            lambda value: tuple(self._as_reason_list(value))
        )
        source = source.loc[
            source["obs_date"].notna()
            & source["index_code"].ne("")
            & source["index_name"].ne("")
            & source["ts_code"].ne("")
        ].copy()

        identity = ["obs_date", "index_code", "index_name"]
        metadata_fields = [
            column for column in self.MEMBER_META_COLUMNS if column not in identity
        ]
        conflicts = (
            source.groupby(identity, sort=False, dropna=False)[metadata_fields]
            .nunique(dropna=False)
            .gt(1)
            .any(axis=1)
        )
        if bool(conflicts.any()):
            raise ValueError("同一ETF指数月末存在冲突的成分来源元数据")

        member_meta = source.sort_values(identity, kind="mergesort").drop_duplicates(
            identity, keep="first"
        )[self.MEMBER_META_COLUMNS]
        member_meta = member_meta.rename(
            columns={
                "weight_basis": "member_weight_basis",
                "weight_source": "member_weight_source",
                "source_code": "member_source_code",
                "source_effective_date": "member_source_effective_date",
                "source_available_date": "member_source_available_date",
                "source_staleness_days": "member_source_staleness_days",
                "source_coverage_rate": "member_source_coverage_rate",
                "source_quality": "member_source_quality",
                "is_fallback": "member_source_is_fallback",
                "is_eligible": "member_source_is_eligible",
                "quality_reasons": "member_source_quality_reasons",
            }
        )

        adapted = source[["obs_date", "ts_code", "index_code", "index_name"]].copy()
        adapted["data_source"] = self.CLASSIFICATION_SOURCE
        adapted["industry_code1"] = adapted["index_code"]
        adapted["industry_level1"] = adapted["index_name"]
        adapted["industry_code2"] = ""
        adapted["industry_level2"] = ""
        adapted = adapted.drop(columns=["index_code", "index_name"])
        return adapted, member_meta

    @staticmethod
    def _as_reason_list(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item) for item in value]
        if isinstance(value, tuple):
            return [str(item) for item in value]
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        return [str(value)]

    @classmethod
    def _combined_quality_reasons(cls, row: pd.Series) -> list[str]:
        reasons = cls._as_reason_list(row.get("fapi_quality_reasons"))
        member_reasons = cls._as_reason_list(
            row.get("member_source_quality_reasons")
        )
        reasons.extend(reason for reason in member_reasons if reason not in reasons)
        if not bool(row.get("member_source_is_eligible")):
            reasons.append("member_source_ineligible")
        return list(dict.fromkeys(reasons))


__all__ = ["ETFIndexFAPICalculator"]
