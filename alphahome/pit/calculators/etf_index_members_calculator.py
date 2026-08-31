"""PIT source selection for ETF-tracked index constituents.

Official historical index weights are the preferred source.  When no valid
official snapshot is visible at an observation month-end, periodic ETF stock
holdings may be used as an explicitly labelled lower-tier proxy.  This module
is database agnostic so the source hierarchy and timing rules can be tested
without a live database.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ETFIndexMemberQualityThreshold:
    """Quality gates for official and disclosed-holding sources."""

    official_min_weight_sum: float = 99.0
    official_max_weight_sum: float = 101.0
    official_min_members: int = 5
    official_max_staleness_days: int = 65
    holding_full_min_weight_sum: float = 90.0
    holding_max_weight_sum: float = 105.0
    holding_partial_min_weight_sum: float = 50.0
    holding_min_members: int = 10
    holding_max_staleness_days: int = 200


class ETFIndexMembersCalculator:
    """Choose one PIT constituent source for each index and month-end."""

    METHOD_VERSION = "official_then_disclosed_etf_holdings_v1"
    OUTPUT_COLUMNS = [
        "obs_date",
        "index_code",
        "index_name",
        "ts_code",
        "weight",
        "raw_weight",
        "weight_basis",
        "weight_source",
        "source_code",
        "source_effective_date",
        "source_available_date",
        "source_staleness_days",
        "source_member_count",
        "source_weight_sum",
        "source_coverage_rate",
        "source_quality",
        "is_fallback",
        "is_eligible",
        "quality_reasons",
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
        fund_holdings: pd.DataFrame,
        obs_dates: Sequence[date | str | pd.Timestamp],
        index_codes: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Return the selected member rows for requested month-ends.

        ``fund_holdings`` may contain separate top-ten and residual disclosures
        for the same report period.  All rows visible by the observation date
        are combined and duplicate stocks keep the latest visible disclosure.
        """

        wanted_dates = sorted(
            {
                pd.Timestamp(value).normalize()
                for value in obs_dates
                if not pd.isna(value)
            }
        )
        official = self._prepare_official(official_weights)
        holdings = self._prepare_holdings(fund_holdings)

        discovered_codes = set(official["index_code"]) | set(holdings["index_code"])
        if index_codes is None:
            wanted_codes = sorted(code for code in discovered_codes if code)
        else:
            wanted_codes = sorted(
                {
                    str(code).strip()
                    for code in index_codes
                    if str(code).strip()
                }
            )

        official_groups = {
            str(code): group.copy()
            for code, group in official.groupby("index_code", sort=False)
        }
        holding_groups = {
            str(code): group.copy()
            for code, group in holdings.groupby("index_code", sort=False)
        }

        pieces: list[pd.DataFrame] = []
        source_counts = {
            "official_index_weight": 0,
            "etf_disclosed_holding": 0,
            "unavailable": 0,
        }
        quality_counts: dict[str, int] = {}
        selected_pairs = 0

        for obs_date in wanted_dates:
            for index_code in wanted_codes:
                selected = self._select_official(
                    official_groups.get(index_code), obs_date
                )
                if selected is None:
                    selected = self._select_holding(
                        holding_groups.get(index_code), obs_date
                    )
                if selected is None:
                    source_counts["unavailable"] += 1
                    continue

                source_counts[str(selected["weight_basis"].iloc[0])] += 1
                quality = str(selected["source_quality"].iloc[0])
                quality_counts[quality] = quality_counts.get(quality, 0) + 1
                selected_pairs += 1
                pieces.append(selected)

        if pieces:
            result = pd.concat(pieces, ignore_index=True)
            result = (
                result[self.OUTPUT_COLUMNS]
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
            "selected_pair_count": selected_pairs,
            "output_row_count": int(len(result)),
            "source_pair_counts": source_counts,
            "quality_pair_counts": quality_counts,
            "eligible_pair_count": int(
                result.loc[result["is_eligible"]]
                .drop_duplicates(["obs_date", "index_code"])
                .shape[0]
            )
            if not result.empty
            else 0,
        }
        return result

    @staticmethod
    def _string_column(frame: pd.DataFrame, column: str) -> pd.Series:
        return frame[column].astype("string").fillna("").str.strip()

    def _prepare_official(self, frame: pd.DataFrame) -> pd.DataFrame:
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
        for column in ("index_code", "index_name", "ts_code"):
            source[column] = self._string_column(source, column)
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
        return source.sort_values(
            ["index_code", "weight_trade_date", "ts_code"], kind="mergesort"
        ).drop_duplicates(
            ["index_code", "weight_trade_date", "ts_code"], keep="last"
        )

    def _prepare_holdings(self, frame: pd.DataFrame) -> pd.DataFrame:
        required = {
            "index_code",
            "index_name",
            "etf_code",
            "ann_date",
            "end_date",
            "ts_code",
            "raw_weight",
        }
        missing = sorted(required - set(frame.columns))
        if missing and not frame.empty:
            raise ValueError(f"ETF持仓缺少字段: {missing}")
        if frame.empty:
            return pd.DataFrame(columns=sorted(required))

        source = frame[list(required)].copy()
        for column in ("index_code", "index_name", "etf_code", "ts_code"):
            source[column] = self._string_column(source, column)
        for column in ("ann_date", "end_date"):
            source[column] = pd.to_datetime(
                source[column], errors="coerce"
            ).dt.normalize()
        source["raw_weight"] = pd.to_numeric(
            source["raw_weight"], errors="coerce"
        )
        source = source.loc[
            source["index_code"].ne("")
            & source["etf_code"].ne("")
            & source["ts_code"].ne("")
            & source["ann_date"].notna()
            & source["end_date"].notna()
            & source["raw_weight"].gt(0)
            & np.isfinite(source["raw_weight"])
        ].copy()
        return source.sort_values(
            ["index_code", "etf_code", "end_date", "ann_date", "ts_code"],
            kind="mergesort",
        )

    def _select_official(
        self, frame: pd.DataFrame | None, obs_date: pd.Timestamp
    ) -> pd.DataFrame | None:
        if frame is None or frame.empty:
            return None
        threshold = self.threshold
        visible = frame.loc[
            frame["weight_trade_date"].le(obs_date)
            & frame["weight_trade_date"].ge(
                obs_date - pd.Timedelta(days=threshold.official_max_staleness_days)
            )
        ].copy()
        if visible.empty:
            return None

        stats = (
            visible.groupby("weight_trade_date", sort=False)
            .agg(
                member_count=("ts_code", "nunique"),
                weight_sum=("raw_weight", "sum"),
            )
            .reset_index()
        )
        valid = stats.loc[
            stats["member_count"].ge(threshold.official_min_members)
            & stats["weight_sum"].between(
                threshold.official_min_weight_sum,
                threshold.official_max_weight_sum,
                inclusive="both",
            )
        ]
        if valid.empty:
            return None

        selected_date = valid["weight_trade_date"].max()
        selected = visible.loc[visible["weight_trade_date"].eq(selected_date)].copy()
        selected = selected.sort_values("ts_code", kind="mergesort").drop_duplicates(
            "ts_code", keep="last"
        )
        weight_sum = float(selected["raw_weight"].sum())
        member_count = int(selected["ts_code"].nunique())
        index_name = self._selected_name(selected)
        return self._finish_selected(
            selected,
            obs_date=obs_date,
            index_name=index_name,
            weight_basis="official_index_weight",
            weight_source="rawdata.index_weight",
            source_code=str(selected["index_code"].iloc[0]),
            source_effective_date=selected_date,
            source_available_date=selected_date,
            source_member_count=member_count,
            source_weight_sum=weight_sum,
            source_quality="high",
            is_fallback=False,
            is_eligible=True,
            quality_reasons=[],
        )

    def _select_holding(
        self, frame: pd.DataFrame | None, obs_date: pd.Timestamp
    ) -> pd.DataFrame | None:
        if frame is None or frame.empty:
            return None
        visible = frame.loc[
            frame["ann_date"].le(obs_date) & frame["end_date"].le(obs_date)
        ].copy()
        if visible.empty:
            return None

        candidates: list[dict[str, Any]] = []
        for etf_code, etf_rows in visible.groupby("etf_code", sort=False):
            end_date = etf_rows["end_date"].max()
            snapshot = etf_rows.loc[etf_rows["end_date"].eq(end_date)].copy()
            snapshot = snapshot.sort_values(
                ["ts_code", "ann_date"],
                ascending=[True, False],
                kind="mergesort",
            ).drop_duplicates("ts_code", keep="first")
            member_count = int(snapshot["ts_code"].nunique())
            weight_sum = float(snapshot["raw_weight"].sum())
            quality, eligible, reasons = self._holding_quality(
                obs_date, end_date, member_count, weight_sum
            )
            candidates.append(
                {
                    "etf_code": str(etf_code),
                    "end_date": end_date,
                    "available_date": snapshot["ann_date"].max(),
                    "member_count": member_count,
                    "weight_sum": weight_sum,
                    "quality": quality,
                    "quality_rank": {"high": 2, "partial": 1, "low": 0}[quality],
                    "eligible": eligible,
                    "reasons": reasons,
                    "snapshot": snapshot,
                }
            )
        if not candidates:
            return None

        chosen = sorted(
            candidates,
            key=lambda item: (
                item["end_date"],
                item["quality_rank"],
                -abs(item["weight_sum"] - 100.0),
                item["member_count"],
                item["etf_code"],
            ),
            reverse=True,
        )[0]
        selected = chosen["snapshot"].copy()
        return self._finish_selected(
            selected,
            obs_date=obs_date,
            index_name=self._selected_name(selected),
            weight_basis="etf_disclosed_holding",
            weight_source="rawdata.fund_portfolio",
            source_code=chosen["etf_code"],
            source_effective_date=chosen["end_date"],
            source_available_date=chosen["available_date"],
            source_member_count=chosen["member_count"],
            source_weight_sum=chosen["weight_sum"],
            source_quality=chosen["quality"],
            is_fallback=True,
            is_eligible=chosen["eligible"],
            quality_reasons=chosen["reasons"],
        )

    def _holding_quality(
        self,
        obs_date: pd.Timestamp,
        end_date: pd.Timestamp,
        member_count: int,
        weight_sum: float,
    ) -> tuple[str, bool, list[str]]:
        threshold = self.threshold
        staleness = int((obs_date - end_date).days)
        reasons = ["official_weight_unavailable", "etf_holding_fallback"]
        if staleness > threshold.holding_max_staleness_days:
            reasons.append("holding_stale_over_limit")
        if member_count < threshold.holding_min_members:
            reasons.append("holding_member_count_below_minimum")
        full = (
            threshold.holding_full_min_weight_sum
            <= weight_sum
            <= threshold.holding_max_weight_sum
            and member_count >= threshold.holding_min_members
        )
        partial = (
            threshold.holding_partial_min_weight_sum
            <= weight_sum
            <= threshold.holding_max_weight_sum
            and member_count >= threshold.holding_min_members
        )
        if full:
            quality = "high"
        elif partial:
            quality = "partial"
            reasons.append("partial_holding_disclosure")
        else:
            quality = "low"
            reasons.append("holding_coverage_below_minimum")
        eligible = quality in {"high", "partial"} and (
            staleness <= threshold.holding_max_staleness_days
        )
        return quality, eligible, reasons

    @staticmethod
    def _selected_name(frame: pd.DataFrame) -> str:
        names = sorted(
            {
                str(value).strip()
                for value in frame["index_name"].dropna()
                if str(value).strip()
            }
        )
        if names:
            return names[0]
        return str(frame["index_code"].iloc[0])

    def _finish_selected(
        self,
        selected: pd.DataFrame,
        *,
        obs_date: pd.Timestamp,
        index_name: str,
        weight_basis: str,
        weight_source: str,
        source_code: str,
        source_effective_date: pd.Timestamp,
        source_available_date: pd.Timestamp,
        source_member_count: int,
        source_weight_sum: float,
        source_quality: str,
        is_fallback: bool,
        is_eligible: bool,
        quality_reasons: list[str],
    ) -> pd.DataFrame:
        result = selected[["index_code", "ts_code", "raw_weight"]].copy()
        result["obs_date"] = obs_date
        result["index_name"] = index_name
        result["weight"] = np.where(
            source_weight_sum > 0,
            result["raw_weight"] / source_weight_sum,
            np.nan,
        )
        result["weight_basis"] = weight_basis
        result["weight_source"] = weight_source
        result["source_code"] = source_code
        result["source_effective_date"] = source_effective_date
        result["source_available_date"] = source_available_date
        result["source_staleness_days"] = int(
            (obs_date - source_effective_date).days
        )
        result["source_member_count"] = source_member_count
        result["source_weight_sum"] = source_weight_sum
        result["source_coverage_rate"] = source_weight_sum / 100.0
        result["source_quality"] = source_quality
        result["is_fallback"] = is_fallback
        result["is_eligible"] = is_eligible
        result["quality_reasons"] = [list(quality_reasons) for _ in range(len(result))]
        result["method_version"] = self.METHOD_VERSION
        return result


__all__ = [
    "ETFIndexMemberQualityThreshold",
    "ETFIndexMembersCalculator",
]
