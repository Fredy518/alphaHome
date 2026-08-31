"""Fixed-fiscal-year stock analyst-consensus PIT calculator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StockConsensusFYQualityThreshold:
    """Minimum quality needed before a stock-year consensus is consumable."""

    min_np_org_count: int = 3
    max_median_forecast_age_days: int = 183


class StockConsensusFYCalculator:
    """Build month-end, fixed-target-year analyst consensus snapshots.

    Each broker contributes at most one forecast to a stock/target-year/month.
    The latest report inside the six-month left-open/right-closed visibility
    window wins. Revisions are matched on stock, broker, and target year so
    neither horizon roll nor broker-composition changes are called revisions.
    """

    FORMULA_VERSION = "stock_fy_consensus_latest_org_median_v1"
    AVAILABILITY_BASIS = "report_date_reconstructed"
    VISIBILITY_MONTHS = 6
    ALLOWED_REPORT_YEAR_OFFSETS = (-1, 0, 1, 2)
    REVISION_WINDOWS = (1, 3)

    REVISION_COLUMNS = [
        f"{name}_{months}m"
        for months in REVISION_WINDOWS
        for name in (
            "revision_matched_org_count",
            "revision_revised_org_count",
            "revision_up_org_count",
            "revision_down_org_count",
            "np_revision_abs",
            "np_revision_rate",
            "revision_activity_rate",
            "revision_up_org_rate",
        )
    ]
    OUTPUT_COLUMNS = [
        "obs_date",
        "ts_code",
        "target_year",
        "forecast_horizon_years",
        "org_count",
        "np_org_count",
        "eps_org_count",
        "np_consensus_median",
        "np_consensus_mean",
        "eps_consensus_median",
        "eps_consensus_mean",
        "np_dispersion_mad",
        "np_dispersion_rate",
        "latest_report_date",
        "oldest_selected_report_date",
        "median_forecast_age_days",
        *REVISION_COLUMNS,
        "is_eligible",
        "quality_reasons",
        "availability_basis",
        "formula_version",
        "source_max_report_date",
    ]

    def __init__(
        self,
        quality: StockConsensusFYQualityThreshold | None = None,
    ) -> None:
        self.quality = quality or StockConsensusFYQualityThreshold()
        self.last_audit: dict[str, Any] = {}
        self.last_org_panel = pd.DataFrame()

    def calculate(
        self,
        forecasts: pd.DataFrame,
        obs_dates: Sequence[date | pd.Timestamp | str],
    ) -> pd.DataFrame:
        normalized_dates = self._normalize_obs_dates(obs_dates)
        prepared = self._prepare_forecasts(forecasts)
        if prepared.empty or not normalized_dates:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        panel = self._build_org_panel(prepared, normalized_dates)
        self.last_org_panel = panel.copy()
        if panel.empty:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        result = self._aggregate_consensus(panel)
        for months in self.REVISION_WINDOWS:
            revision = self._build_revision(panel, months)
            result = result.merge(
                revision,
                on=["obs_date", "ts_code", "target_year"],
                how="left",
                sort=False,
            )

        count_columns = [
            column
            for column in self.REVISION_COLUMNS
            if column.endswith("_count_1m") or column.endswith("_count_3m")
        ]
        for column in count_columns:
            result[column] = (
                pd.array(result[column], dtype="Int64").fillna(0).astype(int)
            )

        result = self._apply_quality(result)
        result["availability_basis"] = self.AVAILABILITY_BASIS
        result["formula_version"] = self.FORMULA_VERSION
        result["source_max_report_date"] = result["latest_report_date"]
        result = result.sort_values(
            ["obs_date", "ts_code", "target_year"], kind="mergesort"
        ).reset_index(drop=True)

        self.last_audit.update(
            {
                "org_panel_row_count": int(len(panel)),
                "snapshot_row_count": int(len(result)),
                "snapshot_month_count": int(result["obs_date"].nunique()),
                "eligible_row_count": int(result["is_eligible"].sum()),
            }
        )
        return result[self.OUTPUT_COLUMNS]

    def _prepare_forecasts(self, forecasts: pd.DataFrame) -> pd.DataFrame:
        required = {
            "ts_code",
            "org_name",
            "author_name",
            "report_date",
            "quarter",
            "np",
            "eps",
        }
        missing = sorted(required - set(forecasts.columns))
        if missing:
            raise ValueError(f"stock_report_rc 缺少字段: {missing}")

        frame = forecasts.copy()
        self.last_audit = {"source_row_count": int(len(frame))}
        frame["ts_code"] = frame["ts_code"].astype("string").fillna("").str.strip()
        frame["org_name"] = frame["org_name"].astype("string").fillna("").str.strip()
        frame["author_name"] = (
            frame["author_name"].astype("string").fillna("").str.strip()
        )
        frame["report_date"] = pd.to_datetime(
            frame["report_date"], errors="coerce"
        ).dt.normalize()
        quarter_text = frame["quarter"].astype("string").str.strip()
        parsed_year = quarter_text.str.extract(r"^(\d{4})Q4$", expand=False)
        frame["target_year"] = pd.to_numeric(parsed_year, errors="coerce")
        frame["np"] = pd.to_numeric(frame["np"], errors="coerce")
        frame["eps"] = pd.to_numeric(frame["eps"], errors="coerce")

        invalid = (
            frame["ts_code"].eq("")
            | frame["org_name"].eq("")
            | frame["report_date"].isna()
            | frame["target_year"].isna()
            | (frame[["np", "eps"]].isna().all(axis=1))
        )
        self.last_audit["invalid_or_unusable_row_count"] = int(invalid.sum())
        frame = frame.loc[~invalid].copy()
        if frame.empty:
            return frame

        frame["target_year"] = frame["target_year"].astype(int)
        frame["report_year"] = frame["report_date"].dt.year.astype(int)
        frame["report_year_offset"] = frame["target_year"] - frame["report_year"]
        relevant = frame["report_year_offset"].isin(self.ALLOWED_REPORT_YEAR_OFFSETS)
        self.last_audit["irrelevant_target_year_row_count"] = int((~relevant).sum())
        frame = frame.loc[relevant].copy()

        stable_columns = sorted(
            column for column in frame.columns if column != "_stable_source_key"
        )
        stable_values = frame[stable_columns].astype("string").fillna("<NULL>")
        frame["_stable_source_key"] = pd.util.hash_pandas_object(
            stable_values, index=False
        ).map(lambda value: f"{int(value):020d}")

        event_year_keys = [
            "ts_code",
            "org_name",
            "author_name",
            "report_date",
            "target_year",
        ]
        frame = frame.sort_values(
            event_year_keys + ["_stable_source_key"], kind="mergesort"
        )
        duplicate_count = int(frame.duplicated(event_year_keys, keep=False).sum())
        self.last_audit["duplicate_event_year_row_count"] = duplicate_count
        return frame.drop_duplicates(event_year_keys, keep="first").reset_index(
            drop=True
        )

    def _build_org_panel(
        self,
        prepared: pd.DataFrame,
        obs_dates: Sequence[pd.Timestamp],
    ) -> pd.DataFrame:
        snapshots: list[pd.DataFrame] = []
        for obs_date in obs_dates:
            window_start = obs_date - pd.DateOffset(months=self.VISIBILITY_MONTHS)
            candidates = prepared.loc[
                (prepared["report_date"] > window_start)
                & (prepared["report_date"] <= obs_date)
                & (prepared["target_year"] >= obs_date.year - 1)
                & (prepared["target_year"] <= obs_date.year + 2)
            ].copy()
            if candidates.empty:
                continue

            candidates = candidates.sort_values(
                [
                    "ts_code",
                    "org_name",
                    "target_year",
                    "report_date",
                    "author_name",
                    "_stable_source_key",
                ],
                ascending=[True, True, True, False, True, True],
                kind="mergesort",
                na_position="last",
            )
            selected = candidates.drop_duplicates(
                ["ts_code", "org_name", "target_year"], keep="first"
            ).copy()
            selected["obs_date"] = obs_date
            selected["selected_report_date"] = selected["report_date"]
            selected["selected_author_name"] = selected["author_name"]
            selected["forecast_age_days"] = (obs_date - selected["report_date"]).dt.days
            snapshots.append(
                selected[
                    [
                        "obs_date",
                        "ts_code",
                        "org_name",
                        "target_year",
                        "np",
                        "eps",
                        "selected_report_date",
                        "selected_author_name",
                        "forecast_age_days",
                    ]
                ]
            )

        if not snapshots:
            return pd.DataFrame()
        return (
            pd.concat(snapshots, ignore_index=True)
            .sort_values(
                ["obs_date", "ts_code", "target_year", "org_name"], kind="mergesort"
            )
            .reset_index(drop=True)
        )

    def _aggregate_consensus(self, panel: pd.DataFrame) -> pd.DataFrame:
        keys = ["obs_date", "ts_code", "target_year"]
        grouped = panel.groupby(keys, sort=False, dropna=False)
        result = grouped.agg(
            org_count=("org_name", "nunique"),
            np_org_count=("np", "count"),
            eps_org_count=("eps", "count"),
            np_consensus_median=("np", "median"),
            np_consensus_mean=("np", "mean"),
            eps_consensus_median=("eps", "median"),
            eps_consensus_mean=("eps", "mean"),
            latest_report_date=("selected_report_date", "max"),
            oldest_selected_report_date=("selected_report_date", "min"),
            median_forecast_age_days=("forecast_age_days", "median"),
        ).reset_index()
        np_values = panel.loc[panel["np"].notna(), keys + ["np"]].merge(
            result[keys + ["np_consensus_median"]],
            on=keys,
            how="left",
            sort=False,
        )
        np_values["absolute_deviation"] = (
            np_values["np"] - np_values["np_consensus_median"]
        ).abs()
        mad = (
            np_values.groupby(keys, sort=False, dropna=False)["absolute_deviation"]
            .median()
            .rename("np_dispersion_mad")
            .reset_index()
        )
        result = result.merge(mad, on=keys, how="left", sort=False)
        denominator = result["np_consensus_median"].abs()
        result["np_dispersion_rate"] = np.where(
            denominator.notna() & denominator.ne(0),
            result["np_dispersion_mad"] / denominator,
            np.nan,
        )
        result["forecast_horizon_years"] = (
            result["target_year"] - result["obs_date"].dt.year
        ).astype(int)
        return result

    def _build_revision(self, panel: pd.DataFrame, months: int) -> pd.DataFrame:
        keys = ["obs_date", "ts_code", "org_name", "target_year"]
        current = panel[keys + ["np"]].rename(columns={"np": "current_np"})
        previous = panel[keys + ["np"]].rename(columns={"np": "previous_np"})
        previous["obs_date"] = previous["obs_date"] + pd.offsets.MonthEnd(months)
        matched = current.merge(previous, on=keys, how="inner", sort=False)
        matched = matched.loc[
            matched["current_np"].notna() & matched["previous_np"].notna()
        ].copy()

        output_columns = [
            "obs_date",
            "ts_code",
            "target_year",
            *[
                f"{name}_{months}m"
                for name in (
                    "revision_matched_org_count",
                    "revision_revised_org_count",
                    "revision_up_org_count",
                    "revision_down_org_count",
                    "np_revision_abs",
                    "np_revision_rate",
                    "revision_activity_rate",
                    "revision_up_org_rate",
                )
            ],
        ]
        if matched.empty:
            return pd.DataFrame(columns=output_columns)

        group_keys = ["obs_date", "ts_code", "target_year"]
        matched["change"] = matched["current_np"] - matched["previous_np"]
        matched["tolerance"] = np.maximum(
            matched["previous_np"].abs() * 1e-12,
            1e-8,
        )
        matched["revised"] = matched["change"].abs() > matched["tolerance"]
        matched["up"] = matched["change"] > matched["tolerance"]
        matched["down"] = matched["change"] < -matched["tolerance"]
        summary = (
            matched.groupby(group_keys, sort=False, dropna=False)
            .agg(
                matched_count=("org_name", "size"),
                revised_count=("revised", "sum"),
                up_count=("up", "sum"),
                down_count=("down", "sum"),
                current_level=("current_np", "median"),
                previous_level=("previous_np", "median"),
            )
            .reset_index()
        )
        summary["revision_abs"] = summary["current_level"] - summary["previous_level"]
        summary["revision_rate"] = np.where(
            summary["previous_level"].ne(0),
            summary["revision_abs"] / summary["previous_level"].abs(),
            np.nan,
        )
        summary["activity_rate"] = summary["revised_count"] / summary["matched_count"]
        summary["up_rate"] = np.where(
            summary["revised_count"].gt(0),
            summary["up_count"] / summary["revised_count"],
            np.nan,
        )
        renamed = summary.rename(
            columns={
                "matched_count": f"revision_matched_org_count_{months}m",
                "revised_count": f"revision_revised_org_count_{months}m",
                "up_count": f"revision_up_org_count_{months}m",
                "down_count": f"revision_down_org_count_{months}m",
                "revision_abs": f"np_revision_abs_{months}m",
                "revision_rate": f"np_revision_rate_{months}m",
                "activity_rate": f"revision_activity_rate_{months}m",
                "up_rate": f"revision_up_org_rate_{months}m",
            }
        )
        return renamed[output_columns]

    def _apply_quality(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        missing_np = result["np_consensus_median"].isna()
        low_count = result["np_org_count"].fillna(0).lt(self.quality.min_np_org_count)
        stale = result["median_forecast_age_days"].isna() | result[
            "median_forecast_age_days"
        ].gt(self.quality.max_median_forecast_age_days)
        result["is_eligible"] = ~(missing_np | low_count | stale)
        result["quality_reasons"] = [
            [
                reason
                for reason, active in (
                    ("missing_np_consensus", bool(missing_np.iloc[index])),
                    ("low_np_org_count", bool(low_count.iloc[index])),
                    ("stale_consensus", bool(stale.iloc[index])),
                )
                if active
            ]
            for index in range(len(result))
        ]
        return result

    @staticmethod
    def _normalize_obs_dates(
        obs_dates: Iterable[date | pd.Timestamp | str],
    ) -> list[pd.Timestamp]:
        normalized: list[pd.Timestamp] = []
        for value in obs_dates:
            parsed = pd.Timestamp(value).normalize()
            if parsed != parsed + pd.offsets.MonthEnd(0):
                raise ValueError(f"obs_date 必须是自然月末: {value}")
            normalized.append(parsed)
        return sorted(set(normalized))


__all__ = [
    "StockConsensusFYCalculator",
    "StockConsensusFYQualityThreshold",
]
