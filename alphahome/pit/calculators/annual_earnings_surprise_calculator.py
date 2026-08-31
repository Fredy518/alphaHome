"""Annual earnings-surprise calculator with strict PIT matching."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AnnualEarningsSurpriseQualityThreshold:
    """Quality limits for a decision-usable surprise observation."""

    max_consensus_age_days: int = 183


class AnnualEarningsSurpriseCalculator:
    """Compare first annual-report actuals with prior month-end consensus.

    The actual parent net profit is stored by Tushare in CNY, while
    ``stock_report_rc.np`` is in 10,000 CNY.  The conversion is deliberately
    explicit before subtraction.  Consensus must pre-date the announcement;
    same-day and later snapshots are never eligible.
    """

    FORMULA_VERSION = "annual_actual_yuan_to_10k_vs_prior_month_consensus_v1"
    OUTPUT_COLUMNS = [
        "ts_code",
        "end_date",
        "ann_date",
        "target_year",
        "actual_np_yuan",
        "actual_np_10k",
        "actual_basic_eps",
        "actual_diluted_eps",
        "actual_source_row_count",
        "actual_source_value_conflict",
        "actual_source_update_time",
        "actual_source_selection_basis",
        "consensus_obs_date",
        "consensus_np_10k",
        "consensus_basic_eps",
        "consensus_org_count",
        "consensus_np_org_count",
        "consensus_np_dispersion_rate",
        "consensus_age_days",
        "np_surprise_abs_10k",
        "np_surprise_rate",
        "eps_surprise_abs",
        "eps_surprise_rate",
        "is_np_sign_change",
        "consensus_is_eligible",
        "is_eligible",
        "quality_reasons",
        "consensus_availability_basis",
        "formula_version",
        "consensus_source_max_report_date",
        "source_income_updated_at",
    ]

    def __init__(
        self,
        quality: AnnualEarningsSurpriseQualityThreshold | None = None,
    ) -> None:
        self.quality = quality or AnnualEarningsSurpriseQualityThreshold()
        self.last_audit: dict[str, Any] = {}

    def calculate(
        self,
        actuals: pd.DataFrame,
        consensus: pd.DataFrame,
    ) -> pd.DataFrame:
        actual = self._prepare_actuals(actuals)
        estimates = self._prepare_consensus(consensus)
        if actual.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        lookup = {
            key: group.sort_values("consensus_obs_date", kind="mergesort")
            for key, group in estimates.groupby(
                ["ts_code", "target_year"], sort=False, dropna=False
            )
        }
        rows: list[dict[str, Any]] = []
        matched_count = 0
        for item in actual.to_dict("records"):
            key = (item["ts_code"], item["target_year"])
            candidates = lookup.get(key)
            matched: dict[str, Any] = {}
            if candidates is not None and not candidates.empty:
                dates = candidates["consensus_obs_date"].to_numpy(
                    dtype="datetime64[ns]"
                )
                position = (
                    int(
                        np.searchsorted(
                            dates,
                            np.datetime64(item["ann_date"]),
                            side="left",
                        )
                    )
                    - 1
                )
                if position >= 0:
                    matched = candidates.iloc[position].to_dict()
                    matched_count += 1
            rows.append(self._build_row(item, matched))

        result = pd.DataFrame(rows, columns=self.OUTPUT_COLUMNS)
        result = result.sort_values(
            ["ann_date", "ts_code", "end_date"], kind="mergesort"
        ).reset_index(drop=True)
        self.last_audit.update(
            {
                "actual_event_count": int(len(actual)),
                "matched_consensus_count": int(matched_count),
                "eligible_count": int(result["is_eligible"].sum()),
            }
        )
        return result

    def _prepare_actuals(self, frame: pd.DataFrame) -> pd.DataFrame:
        required = {
            "ts_code",
            "end_date",
            "ann_date",
            "actual_np_yuan",
            "actual_basic_eps",
            "actual_diluted_eps",
            "actual_source_row_count",
            "actual_source_value_conflict",
            "actual_source_update_time",
            "actual_source_selection_basis",
            "source_income_updated_at",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"年度实际值缺少字段: {missing}")

        actual = frame.copy()
        self.last_audit = {"source_actual_row_count": int(len(actual))}
        actual["ts_code"] = actual["ts_code"].astype("string").str.strip()
        for column in ("end_date", "ann_date"):
            actual[column] = pd.to_datetime(
                actual[column], errors="coerce"
            ).dt.normalize()
        actual["actual_np_yuan"] = actual["actual_np_yuan"].map(self._as_decimal)
        for column in ("actual_basic_eps", "actual_diluted_eps"):
            actual[column] = pd.to_numeric(actual[column], errors="coerce")
        actual["source_income_updated_at"] = pd.to_datetime(
            actual["source_income_updated_at"], errors="coerce"
        )
        actual["actual_source_update_time"] = pd.to_datetime(
            actual["actual_source_update_time"], errors="coerce"
        )
        actual["actual_source_row_count"] = pd.to_numeric(
            actual["actual_source_row_count"], errors="coerce"
        )
        invalid = (
            actual["ts_code"].isna()
            | actual["ts_code"].eq("")
            | actual["end_date"].isna()
            | actual["ann_date"].isna()
            | actual["end_date"].dt.month.ne(12)
            | actual["end_date"].dt.day.ne(31)
        )
        self.last_audit["invalid_actual_row_count"] = int(invalid.sum())
        actual = actual.loc[~invalid].copy()
        actual["target_year"] = actual["end_date"].dt.year.astype(int)
        actual = actual.sort_values(
            ["ts_code", "end_date", "ann_date", "source_income_updated_at"],
            kind="mergesort",
            na_position="last",
        )
        duplicate_events = int(
            actual.duplicated(["ts_code", "end_date"], keep=False).sum()
        )
        self.last_audit["duplicate_actual_event_row_count"] = duplicate_events
        return actual.drop_duplicates(["ts_code", "end_date"], keep="first")

    @staticmethod
    def _prepare_consensus(frame: pd.DataFrame) -> pd.DataFrame:
        required = {
            "obs_date",
            "ts_code",
            "target_year",
            "np_consensus_median",
            "eps_consensus_median",
            "org_count",
            "np_org_count",
            "np_dispersion_rate",
            "is_eligible",
            "availability_basis",
            "source_max_report_date",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"固定财年一致预期缺少字段: {missing}")
        estimates = frame.copy().rename(
            columns={
                "obs_date": "consensus_obs_date",
                "np_consensus_median": "consensus_np_10k",
                "eps_consensus_median": "consensus_basic_eps",
                "org_count": "consensus_org_count",
                "np_org_count": "consensus_np_org_count",
                "np_dispersion_rate": "consensus_np_dispersion_rate",
                "is_eligible": "consensus_is_eligible",
                "availability_basis": "consensus_availability_basis",
                "source_max_report_date": "consensus_source_max_report_date",
            }
        )
        estimates["ts_code"] = estimates["ts_code"].astype("string").str.strip()
        estimates["consensus_obs_date"] = pd.to_datetime(
            estimates["consensus_obs_date"], errors="coerce"
        ).dt.normalize()
        estimates["target_year"] = pd.to_numeric(
            estimates["target_year"], errors="coerce"
        )
        estimates = estimates.dropna(
            subset=["ts_code", "target_year", "consensus_obs_date"]
        ).copy()
        estimates["target_year"] = estimates["target_year"].astype(int)
        estimates = estimates.sort_values(
            ["ts_code", "target_year", "consensus_obs_date"], kind="mergesort"
        )
        return estimates.drop_duplicates(
            ["ts_code", "target_year", "consensus_obs_date"], keep="last"
        )

    def _build_row(
        self,
        actual: dict[str, Any],
        consensus: dict[str, Any],
    ) -> dict[str, Any]:
        actual_np_yuan = self._as_decimal(actual.get("actual_np_yuan"))
        actual_np_10k = (
            actual_np_yuan / Decimal("10000") if actual_np_yuan is not None else None
        )
        consensus_np = self._as_decimal(consensus.get("consensus_np_10k"))
        consensus_eps = consensus.get("consensus_basic_eps", np.nan)
        actual_eps = actual.get("actual_basic_eps", np.nan)
        np_abs = (
            actual_np_10k - consensus_np
            if actual_np_10k is not None and consensus_np is not None
            else None
        )
        np_rate = (
            float(np_abs / abs(consensus_np))
            if np_abs is not None and consensus_np not in (None, Decimal("0"))
            else None
        )
        eps_abs = (
            float(actual_eps) - float(consensus_eps)
            if pd.notna(actual_eps) and pd.notna(consensus_eps)
            else np.nan
        )
        eps_rate = (
            eps_abs / abs(float(consensus_eps))
            if pd.notna(eps_abs) and float(consensus_eps) != 0
            else np.nan
        )
        consensus_date = consensus.get("consensus_obs_date", pd.NaT)
        age_days = (
            int((actual["ann_date"] - consensus_date).days)
            if pd.notna(consensus_date)
            else None
        )
        reasons: list[str] = []
        if actual_np_10k is None:
            reasons.append("missing_actual_np")
        source_conflict = actual.get("actual_source_value_conflict", False)
        if pd.notna(source_conflict) and bool(source_conflict):
            reasons.append("conflicting_actual_source")
        if pd.isna(consensus_date):
            reasons.append("missing_prior_consensus")
        elif consensus_date >= actual["ann_date"]:
            reasons.append("non_prior_consensus")
        if consensus_np is None:
            reasons.append("missing_consensus_np")
        if consensus and not bool(consensus.get("consensus_is_eligible", False)):
            reasons.append("ineligible_consensus")
        if age_days is not None and age_days > self.quality.max_consensus_age_days:
            reasons.append("stale_consensus")

        sign_change = None
        if actual_np_10k is not None and consensus_np is not None:
            sign_change = bool(actual_np_10k * consensus_np < 0)

        return {
            "ts_code": actual["ts_code"],
            "end_date": actual["end_date"],
            "ann_date": actual["ann_date"],
            "target_year": actual["target_year"],
            "actual_np_yuan": actual_np_yuan,
            "actual_np_10k": actual_np_10k,
            "actual_basic_eps": actual.get("actual_basic_eps"),
            "actual_diluted_eps": actual.get("actual_diluted_eps"),
            "actual_source_row_count": actual.get("actual_source_row_count"),
            "actual_source_value_conflict": actual.get("actual_source_value_conflict"),
            "actual_source_update_time": actual.get("actual_source_update_time"),
            "actual_source_selection_basis": actual.get(
                "actual_source_selection_basis"
            ),
            "consensus_obs_date": consensus_date,
            "consensus_np_10k": consensus_np,
            "consensus_basic_eps": consensus_eps,
            "consensus_org_count": consensus.get("consensus_org_count"),
            "consensus_np_org_count": consensus.get("consensus_np_org_count"),
            "consensus_np_dispersion_rate": consensus.get(
                "consensus_np_dispersion_rate"
            ),
            "consensus_age_days": age_days,
            "np_surprise_abs_10k": np_abs,
            "np_surprise_rate": np_rate,
            "eps_surprise_abs": eps_abs,
            "eps_surprise_rate": eps_rate,
            "is_np_sign_change": sign_change,
            "consensus_is_eligible": consensus.get("consensus_is_eligible"),
            "is_eligible": not bool(reasons),
            "quality_reasons": sorted(set(reasons)),
            "consensus_availability_basis": consensus.get(
                "consensus_availability_basis"
            ),
            "formula_version": self.FORMULA_VERSION,
            "consensus_source_max_report_date": consensus.get(
                "consensus_source_max_report_date"
            ),
            "source_income_updated_at": actual.get("source_income_updated_at"),
        }

    @staticmethod
    def _as_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            pass
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None


__all__ = [
    "AnnualEarningsSurpriseCalculator",
    "AnnualEarningsSurpriseQualityThreshold",
]
