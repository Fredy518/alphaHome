"""PIT A-share constituent proxy for cross-market ETF-tracked indices.

The calculator first validates a complete official index-weight snapshot, then
keeps only Shanghai/Shenzhen constituents and renormalizes them inside that
subset.  The subset is always labelled as a proxy; it is never presented as
the fundamentals of the full cross-market index.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Sequence

import numpy as np
import pandas as pd

from .etf_index_members_calculator import (
    ETFIndexMemberQualityThreshold,
    ETFIndexMembersCalculator,
)


class ETFIndexAShareProxyMembersCalculator:
    """Build labelled A-share subsets from complete official index snapshots."""

    METHOD_VERSION = "official_cross_market_a_share_subset_v1"
    CONSTITUENT_SCOPE = "a_share_subset_of_cross_market_index"
    MIN_SCOPE_WEIGHT_RATE = 0.50
    MIN_SCOPE_MEMBER_COUNT = 5
    OUTPUT_COLUMNS = [
        *ETFIndexMembersCalculator.OUTPUT_COLUMNS[:-1],
        "constituent_scope",
        "is_proxy",
        "scope_weight_rate",
        "method_version",
    ]

    def __init__(
        self, threshold: ETFIndexMemberQualityThreshold | None = None
    ) -> None:
        self.threshold = threshold or ETFIndexMemberQualityThreshold()
        self.last_audit: dict[str, Any] = {}

    def calculate(
        self,
        official_weights: pd.DataFrame,
        obs_dates: Sequence[date | str | pd.Timestamp],
        index_codes: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        wanted_dates = sorted(
            {
                pd.Timestamp(value).normalize()
                for value in obs_dates
                if not pd.isna(value)
            }
        )
        official = self._prepare_official(official_weights)
        if index_codes is None:
            wanted_codes = sorted(official["index_code"].unique().tolist())
        else:
            wanted_codes = sorted(
                {
                    str(value).strip()
                    for value in index_codes
                    if str(value).strip()
                }
            )
        groups = {
            str(code): group.copy()
            for code, group in official.groupby("index_code", sort=False)
        }

        pieces: list[pd.DataFrame] = []
        unavailable = 0
        eligible_pairs = 0
        scope_rates: list[float] = []
        for obs_date in wanted_dates:
            for index_code in wanted_codes:
                selected = self._select_snapshot(groups.get(index_code), obs_date)
                if selected is None:
                    unavailable += 1
                    continue
                subset, eligible, scope_rate = selected
                pieces.append(subset)
                eligible_pairs += int(eligible)
                scope_rates.append(scope_rate)

        if pieces:
            result = (
                pd.concat(pieces, ignore_index=True)
                .reindex(columns=self.OUTPUT_COLUMNS)
                .sort_values(
                    ["obs_date", "index_code", "weight"],
                    ascending=[True, True, False],
                    kind="mergesort",
                )
                .reset_index(drop=True)
            )
        else:
            result = pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        self.last_audit = {
            "requested_month_count": len(wanted_dates),
            "requested_index_count": len(wanted_codes),
            "requested_pair_count": len(wanted_dates) * len(wanted_codes),
            "selected_pair_count": len(scope_rates),
            "eligible_pair_count": eligible_pairs,
            "unavailable_pair_count": unavailable,
            "output_row_count": int(len(result)),
            "source_pair_counts": {
                "official_index_weight_a_share_proxy": len(scope_rates),
                "unavailable": unavailable,
            },
            "scope_weight_rate_min": min(scope_rates) if scope_rates else None,
            "scope_weight_rate_median": (
                float(np.median(scope_rates)) if scope_rates else None
            ),
            "scope_weight_rate_max": max(scope_rates) if scope_rates else None,
        }
        return result

    @staticmethod
    def normalize_member_code(value: Any) -> str:
        code = str(value or "").strip().upper()
        if "." not in code:
            return code
        symbol, market = code.rsplit(".", 1)
        if market == "HK" and symbol.isdigit():
            return f"{symbol.lstrip('0').zfill(5)}.HK"
        if market in {"SH", "SZ"} and symbol.isdigit():
            return f"{symbol.zfill(6)}.{market}"
        return code

    @classmethod
    def _prepare_official(cls, frame: pd.DataFrame) -> pd.DataFrame:
        required = {
            "index_code",
            "index_name",
            "weight_trade_date",
            "ts_code",
            "raw_weight",
        }
        missing = sorted(required - set(frame.columns))
        if missing and not frame.empty:
            raise ValueError(f"官方指数权重缺少字段: {missing}")
        if frame.empty:
            return pd.DataFrame(columns=sorted(required))

        source = frame[list(required)].copy()
        for column in ("index_code", "index_name"):
            source[column] = (
                source[column].astype("string").fillna("").str.strip()
            )
        source["ts_code"] = source["ts_code"].map(cls.normalize_member_code)
        source["weight_trade_date"] = pd.to_datetime(
            source["weight_trade_date"], errors="coerce"
        ).dt.normalize()
        source["raw_weight"] = pd.to_numeric(
            source["raw_weight"], errors="coerce"
        )
        source = source.loc[
            source["index_code"].ne("")
            & source["ts_code"].ne("")
            & source["weight_trade_date"].notna()
            & source["raw_weight"].gt(0)
            & np.isfinite(source["raw_weight"])
        ].copy()
        # Some cross-market feeds contain both zero-padded and unpadded HK
        # aliases.  Normalize first, then keep one economic weight rather than
        # double-counting the same security.
        return (
            source.groupby(
                ["index_code", "index_name", "weight_trade_date", "ts_code"],
                observed=True,
                as_index=False,
            )["raw_weight"]
            .max()
            .sort_values(
                ["index_code", "weight_trade_date", "ts_code"],
                kind="mergesort",
            )
        )

    def _select_snapshot(
        self, frame: pd.DataFrame | None, obs_date: pd.Timestamp
    ) -> tuple[pd.DataFrame, bool, float] | None:
        if frame is None or frame.empty:
            return None
        threshold = self.threshold
        visible = frame.loc[
            frame["weight_trade_date"].le(obs_date)
            & frame["weight_trade_date"].ge(
                obs_date
                - pd.Timedelta(days=threshold.official_max_staleness_days)
            )
        ].copy()
        if visible.empty:
            return None
        stats = (
            visible.groupby("weight_trade_date", observed=True, as_index=False)
            .agg(
                source_member_count=("ts_code", "nunique"),
                source_weight_sum=("raw_weight", "sum"),
            )
        )
        valid = stats.loc[
            stats["source_member_count"].ge(threshold.official_min_members)
            & stats["source_weight_sum"].between(
                threshold.official_min_weight_sum,
                threshold.official_max_weight_sum,
                inclusive="both",
            )
        ]
        if valid.empty:
            return None

        source_date = valid["weight_trade_date"].max()
        snapshot = visible.loc[
            visible["weight_trade_date"].eq(source_date)
        ].copy()
        snapshot = snapshot.sort_values("ts_code", kind="mergesort").drop_duplicates(
            "ts_code", keep="last"
        )
        full_weight_sum = float(snapshot["raw_weight"].sum())
        full_member_count = int(snapshot["ts_code"].nunique())
        subset = snapshot.loc[
            snapshot["ts_code"].str.endswith((".SH", ".SZ"), na=False)
        ].copy()
        subset_weight_sum = float(subset["raw_weight"].sum())
        if subset.empty or subset_weight_sum <= 0 or full_weight_sum <= 0:
            return None

        scope_rate = subset_weight_sum / full_weight_sum
        subset_member_count = int(subset["ts_code"].nunique())
        reasons = ["a_share_subset_proxy_of_cross_market_index"]
        if scope_rate < self.MIN_SCOPE_WEIGHT_RATE:
            reasons.append("proxy_scope_weight_below_minimum")
        if subset_member_count < self.MIN_SCOPE_MEMBER_COUNT:
            reasons.append("proxy_scope_member_count_below_minimum")
        eligible = (
            scope_rate >= self.MIN_SCOPE_WEIGHT_RATE
            and subset_member_count >= self.MIN_SCOPE_MEMBER_COUNT
        )

        index_name = sorted(
            {
                str(value).strip()
                for value in subset["index_name"].dropna()
                if str(value).strip()
            }
        )
        result = subset[["index_code", "ts_code", "raw_weight"]].copy()
        result["obs_date"] = obs_date
        result["index_name"] = index_name[0] if index_name else str(
            subset["index_code"].iloc[0]
        )
        result["weight"] = result["raw_weight"] / subset_weight_sum
        result["weight_basis"] = "official_index_weight"
        result["weight_source"] = "rawdata.index_weight"
        result["source_code"] = str(subset["index_code"].iloc[0])
        result["source_effective_date"] = source_date
        result["source_available_date"] = source_date
        result["source_staleness_days"] = int((obs_date - source_date).days)
        result["source_member_count"] = full_member_count
        result["source_weight_sum"] = full_weight_sum
        result["source_coverage_rate"] = full_weight_sum / 100.0
        result["source_quality"] = "high"
        result["is_fallback"] = False
        result["is_eligible"] = eligible
        result["quality_reasons"] = [list(reasons) for _ in range(len(result))]
        result["constituent_scope"] = self.CONSTITUENT_SCOPE
        result["is_proxy"] = True
        result["scope_weight_rate"] = scope_rate
        result["method_version"] = self.METHOD_VERSION
        return result, eligible, scope_rate


__all__ = ["ETFIndexAShareProxyMembersCalculator"]
