"""Point-in-time stock/broker FTTM calculator.

The calculator is deliberately database agnostic.  It implements the immutable
event pairing and visibility rules; managers only load source frames and replace
monthly snapshots transactionally.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


class StockFTTMCalculator:
    """Build deterministic month-end FTTM snapshots from research reports."""

    FORMULA_VERSION = "fttm_q4_report_event_linear_v1"
    EVENT_KEYS = ["ts_code", "org_name", "author_name", "report_date"]
    OUTPUT_COLUMNS = [
        "ts_code",
        "org_name",
        "obs_date",
        "selected_report_date",
        "selected_author_name",
        "report_quarter",
        "fy1_year",
        "fy2_year",
        "fy1_np_raw",
        "fy2_np_raw",
        "fy1_np_used",
        "fy2_np_used",
        "fy1_value_source",
        "fy2_value_source",
        "fy1_weight",
        "fy2_weight",
        "fttm_np",
        "selected_total_share",
        "fttm_eps",
        "estimate_pair_status",
        "is_single_year_fallback",
        "source_window_start",
        "source_window_end",
        "formula_version",
        "source_max_report_date",
    ]

    def __init__(self) -> None:
        self.last_audit: dict[str, Any] = {}

    def calculate(
        self,
        forecasts: pd.DataFrame,
        obs_dates: Sequence[date | pd.Timestamp | str],
        daily_shares: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Calculate snapshots for every supplied natural month end.

        ``daily_shares`` is optional when ``forecasts`` already contains an
        exact-report-date ``total_share`` column (the production query does).
        """

        event_rows = self.build_event_estimates(forecasts, daily_shares=daily_shares)
        normalized_dates = self._normalize_obs_dates(obs_dates)
        if event_rows.empty or not normalized_dates:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        snapshots: list[pd.DataFrame] = []
        for obs_date in normalized_dates:
            window_start = obs_date - pd.DateOffset(months=6)
            candidates = event_rows.loc[
                (event_rows["report_date"] > window_start)
                & (event_rows["report_date"] <= obs_date)
            ].copy()
            if candidates.empty:
                continue

            candidates = candidates.sort_values(
                [
                    "ts_code",
                    "org_name",
                    "report_date",
                    "author_name",
                    "_stable_event_key",
                ],
                ascending=[True, True, False, True, True],
                kind="mergesort",
                na_position="last",
            )
            selected = candidates.drop_duplicates(
                subset=["ts_code", "org_name"], keep="first"
            ).copy()
            selected["obs_date"] = obs_date
            selected["selected_report_date"] = selected["report_date"]
            selected["selected_author_name"] = selected["author_name"]
            selected["source_window_start"] = window_start
            selected["source_window_end"] = obs_date
            selected["formula_version"] = self.FORMULA_VERSION
            selected["source_max_report_date"] = selected["report_date"]
            snapshots.append(selected[self.OUTPUT_COLUMNS])

        if not snapshots:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        result = pd.concat(snapshots, ignore_index=True)
        result = result.sort_values(
            ["obs_date", "ts_code", "org_name"], kind="mergesort"
        ).reset_index(drop=True)
        self.last_audit["event_count"] = int(len(event_rows))
        self.last_audit["snapshot_row_count"] = int(len(result))
        self.last_audit["snapshot_month_count"] = int(result["obs_date"].nunique())
        return result

    def build_event_estimates(
        self,
        forecasts: pd.DataFrame,
        daily_shares: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Pair FY1/FY2 only inside one exact research-report event."""

        frame = self._prepare_forecasts(forecasts, daily_shares=daily_shares)
        if frame.empty:
            return self._empty_event_frame()

        event_year_keys = self.EVENT_KEYS + ["report_year", "forecast_year"]
        frame = frame.sort_values(
            event_year_keys + ["_stable_source_key"],
            kind="mergesort",
            na_position="last",
        )
        duplicate_count = int(frame.duplicated(event_year_keys, keep=False).sum())
        frame = frame.drop_duplicates(event_year_keys, keep="first")
        self.last_audit["duplicate_event_year_row_count"] = duplicate_count

        value_columns = [
            *self.EVENT_KEYS,
            "report_year",
            "forecast_year",
            "np",
            "annual_np",
            "value_source",
            "total_share",
            "_stable_source_key",
        ]
        fy1 = frame.loc[frame["forecast_year"] == frame["report_year"], value_columns].copy()
        fy2 = frame.loc[
            frame["forecast_year"] == frame["report_year"] + 1, value_columns
        ].copy()

        rename_fy1 = {
            "forecast_year": "fy1_year_source",
            "np": "fy1_np_raw",
            "annual_np": "fy1_annual_np",
            "value_source": "fy1_value_source",
            "total_share": "fy1_total_share",
            "_stable_source_key": "fy1_source_key",
        }
        rename_fy2 = {
            "forecast_year": "fy2_year_source",
            "np": "fy2_np_raw",
            "annual_np": "fy2_annual_np",
            "value_source": "fy2_value_source",
            "total_share": "fy2_total_share",
            "_stable_source_key": "fy2_source_key",
        }
        fy1 = fy1.rename(columns=rename_fy1)
        fy2 = fy2.rename(columns=rename_fy2)
        merge_keys = self.EVENT_KEYS + ["report_year"]
        events = fy1.merge(fy2, on=merge_keys, how="outer", sort=False)

        has_fy1 = events["fy1_annual_np"].notna()
        has_fy2 = events["fy2_annual_np"].notna()
        events = events.loc[has_fy1 | has_fy2].copy()
        has_fy1 = events["fy1_annual_np"].notna()
        has_fy2 = events["fy2_annual_np"].notna()

        events["fy1_year"] = events["report_year"].astype(int)
        events["fy2_year"] = events["fy1_year"] + 1
        events["fy1_np_used"] = events["fy1_annual_np"].where(
            has_fy1, events["fy2_annual_np"]
        )
        events["fy2_np_used"] = events["fy2_annual_np"].where(
            has_fy2, events["fy1_annual_np"]
        )
        events["estimate_pair_status"] = np.select(
            [has_fy1 & has_fy2, has_fy1, has_fy2],
            ["both", "fy1_only", "fy2_only"],
            default="",
        )
        events["is_single_year_fallback"] = ~(has_fy1 & has_fy2)
        events["report_quarter"] = events["report_date"].dt.quarter.astype(int)
        events["fy1_weight"] = (5 - events["report_quarter"]) / 4.0
        events["fy2_weight"] = (events["report_quarter"] - 1) / 4.0
        events["fttm_np"] = (
            events["fy1_weight"] * events["fy1_np_used"]
            + events["fy2_weight"] * events["fy2_np_used"]
        )
        events["selected_total_share"] = events["fy1_total_share"].combine_first(
            events["fy2_total_share"]
        )
        valid_share = events["selected_total_share"].notna() & (
            events["selected_total_share"] != 0
        )
        events["fttm_eps"] = np.where(
            valid_share,
            events["fttm_np"] / events["selected_total_share"],
            np.nan,
        )
        events["_stable_event_key"] = (
            events["fy1_source_key"].fillna("").astype(str)
            + "|"
            + events["fy2_source_key"].fillna("").astype(str)
        )

        events = events.sort_values(
            self.EVENT_KEYS + ["_stable_event_key"], kind="mergesort"
        ).reset_index(drop=True)
        self.last_audit.update(
            {
                "paired_event_count": int((events["estimate_pair_status"] == "both").sum()),
                "fy1_only_event_count": int(
                    (events["estimate_pair_status"] == "fy1_only").sum()
                ),
                "fy2_only_event_count": int(
                    (events["estimate_pair_status"] == "fy2_only").sum()
                ),
                "negative_event_count": int((events["fttm_np"] < 0).sum()),
                "zero_event_count": int((events["fttm_np"] == 0).sum()),
            }
        )
        return events

    def _prepare_forecasts(
        self,
        forecasts: pd.DataFrame,
        daily_shares: pd.DataFrame | None,
    ) -> pd.DataFrame:
        required = {"ts_code", "org_name", "author_name", "report_date", "quarter", "np", "eps"}
        missing = sorted(required - set(forecasts.columns))
        if missing:
            raise ValueError(f"stock_report_rc 缺少字段: {missing}")

        frame = forecasts.copy()
        self.last_audit = {"source_row_count": int(len(frame))}
        frame["_org_name_original"] = frame["org_name"].astype("string").fillna("")
        frame["org_name"] = frame["_org_name_original"].str.strip()
        frame["author_name"] = (
            frame["author_name"].astype("string").fillna("").str.strip()
        )
        frame["ts_code"] = frame["ts_code"].astype("string").fillna("").str.strip()
        frame["report_date"] = pd.to_datetime(frame["report_date"], errors="coerce").dt.normalize()
        quarter_text = frame["quarter"].astype("string").str.strip()
        parsed_year = quarter_text.str.extract(r"^(\d{4})Q4$", expand=False)
        frame["forecast_year"] = pd.to_numeric(parsed_year, errors="coerce")

        invalid_date = frame["report_date"].isna()
        blank_org = frame["org_name"].eq("")
        malformed_q4 = frame["forecast_year"].isna()
        self.last_audit.update(
            {
                "invalid_report_date_count": int(invalid_date.sum()),
                "blank_org_count": int(blank_org.sum()),
                "malformed_or_non_q4_count": int(malformed_q4.sum()),
            }
        )

        valid = ~(invalid_date | blank_org | malformed_q4) & frame["ts_code"].ne("")
        frame = frame.loc[valid].copy()
        if frame.empty:
            return frame

        frame["forecast_year"] = frame["forecast_year"].astype(int)
        frame["report_year"] = frame["report_date"].dt.year.astype(int)
        relevant_year = frame["forecast_year"].eq(
            frame["report_year"]
        ) | frame["forecast_year"].eq(frame["report_year"] + 1)
        self.last_audit["irrelevant_q4_year_count"] = int((~relevant_year).sum())
        frame = frame.loc[relevant_year].copy()

        if daily_shares is not None:
            shares = self._prepare_daily_shares(daily_shares)
            frame = frame.drop(columns=["total_share"], errors="ignore").merge(
                shares,
                on=["ts_code", "report_date"],
                how="left",
                sort=False,
            )
        elif "total_share" not in frame.columns:
            frame["total_share"] = np.nan

        for column in ("np", "eps", "total_share"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["annual_np"] = frame["np"]
        eps_fallback = frame["np"].isna() & frame["eps"].notna() & frame["total_share"].notna()
        frame.loc[eps_fallback, "annual_np"] = (
            frame.loc[eps_fallback, "eps"] * frame.loc[eps_fallback, "total_share"]
        )
        frame["value_source"] = np.select(
            [frame["np"].notna(), eps_fallback], ["np", "eps_share"], default=None
        )
        self.last_audit.update(
            {
                "np_value_count": int(frame["np"].notna().sum()),
                "eps_share_value_count": int(eps_fallback.sum()),
                "unusable_annual_value_count": int(frame["annual_np"].isna().sum()),
            }
        )
        frame = frame.loc[frame["annual_np"].notna()].copy()

        # Hash every business field after normalization.  This removes any
        # dependence on database natural order while avoiding a slow Python row
        # loop for the multi-million-row source table.
        stable_columns = sorted(
            column
            for column in frame.columns
            if column not in {"_stable_source_key"}
        )
        stable_values = frame[stable_columns].astype("string").fillna("<NULL>")
        frame["_stable_source_key"] = pd.util.hash_pandas_object(
            stable_values, index=False
        ).map(lambda value: f"{int(value):020d}")

        collisions = (
            frame[["_org_name_original", "org_name"]]
            .drop_duplicates()
            .groupby("org_name", dropna=False)["_org_name_original"]
            .nunique()
        )
        self.last_audit["trimmed_org_collision_count"] = int((collisions > 1).sum())
        return frame

    @staticmethod
    def _prepare_daily_shares(daily_shares: pd.DataFrame) -> pd.DataFrame:
        required = {"ts_code", "trade_date", "total_share"}
        missing = sorted(required - set(daily_shares.columns))
        if missing:
            raise ValueError(f"stock_dailybasic 缺少字段: {missing}")
        shares = daily_shares[list(required)].copy()
        shares["ts_code"] = shares["ts_code"].astype("string").str.strip()
        shares["report_date"] = pd.to_datetime(
            shares.pop("trade_date"), errors="coerce"
        ).dt.normalize()
        shares["total_share"] = pd.to_numeric(shares["total_share"], errors="coerce")
        shares = shares.sort_values(
            ["ts_code", "report_date", "total_share"],
            kind="mergesort",
            na_position="last",
        )
        return shares.drop_duplicates(["ts_code", "report_date"], keep="first")

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

    @classmethod
    def _empty_event_frame(cls) -> pd.DataFrame:
        columns = [
            *cls.EVENT_KEYS,
            "report_quarter",
            "fy1_year",
            "fy2_year",
            "fy1_np_raw",
            "fy2_np_raw",
            "fy1_np_used",
            "fy2_np_used",
            "fy1_value_source",
            "fy2_value_source",
            "fy1_weight",
            "fy2_weight",
            "fttm_np",
            "selected_total_share",
            "fttm_eps",
            "estimate_pair_status",
            "is_single_year_fallback",
            "_stable_event_key",
        ]
        return pd.DataFrame(columns=columns)


__all__ = ["StockFTTMCalculator"]
