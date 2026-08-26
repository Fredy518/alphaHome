"""Matched-sample decomposition for adjacent-month FTTM changes.

The existing aggregate FTTM month-on-month fields compare two independently
constructed snapshots.  They are useful level diagnostics, but the difference
can mix forecast revisions, horizon roll, and changes in broker/stock coverage.

This module isolates the first two effects on a strict common sample:

* same stock and broker in adjacent natural months;
* the same FY1/FY2 target years in both observations;
* both annual estimates present in both observations;
* current-month constituents and weights used for both sides.

It deliberately leaves year-roll and coverage-composition effects outside the
decomposition and reports their loss of coverage explicitly.
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd


REVISION_VERSION = "common_stock_org_decomp_v1"

REVISION_SOURCE_COLUMNS = [
    "obs_date",
    "ts_code",
    "org_name",
    "fttm_np",
    "fy1_year",
    "fy2_year",
    "fy1_np_raw",
    "fy2_np_raw",
    "fy1_weight",
    "fy2_weight",
]

REVISION_OUTPUT_COLUMNS = [
    "revision_comparable_stock_count",
    "revision_comparable_org_count",
    "revision_median_org_count",
    "revision_comparable_weight_rate",
    "revision_rate",
    "horizon_roll_rate",
    "revision_activity_rate",
    "revision_up_stock_rate",
    "revision_up_weight_rate",
    "revision_version",
]


def build_matched_revision_metrics(
    members: pd.DataFrame,
    stock_fttm: pd.DataFrame,
    *,
    group_keys: Sequence[str],
    weight_column: str,
) -> pd.DataFrame:
    """Return common-sample revision diagnostics for each aggregate row.

    ``members`` must contain current-month eligible constituents and one
    positive weight per ``group_keys + ts_code``.  ``stock_fttm`` may contain
    many months, but must expose the annual estimates and event weights listed
    in :data:`REVISION_SOURCE_COLUMNS`.
    """

    keys = list(group_keys)
    output_columns = [*keys, *REVISION_OUTPUT_COLUMNS]
    required_members = {*keys, "ts_code", weight_column}
    required_fttm = set(REVISION_SOURCE_COLUMNS)
    if not required_members.issubset(members.columns) or not required_fttm.issubset(
        stock_fttm.columns
    ):
        return pd.DataFrame(columns=output_columns)

    member_frame = members[[*keys, "ts_code", weight_column]].copy()
    member_frame["obs_date"] = pd.to_datetime(
        member_frame["obs_date"], errors="coerce"
    ).dt.normalize()
    member_frame["ts_code"] = member_frame["ts_code"].astype("string").str.strip()
    member_frame[weight_column] = pd.to_numeric(
        member_frame[weight_column], errors="coerce"
    )
    member_frame = member_frame.loc[
        member_frame["obs_date"].notna()
        & member_frame["ts_code"].ne("")
        & member_frame[weight_column].notna()
        & (member_frame[weight_column] > 0)
    ].copy()
    member_frame = member_frame.sort_values(
        [*keys, "ts_code", weight_column],
        ascending=[*[True] * len(keys), True, False],
        kind="mergesort",
    ).drop_duplicates([*keys, "ts_code"], keep="first")
    if member_frame.empty:
        return pd.DataFrame(columns=output_columns)

    fttm = stock_fttm[REVISION_SOURCE_COLUMNS].copy()
    fttm["obs_date"] = pd.to_datetime(fttm["obs_date"], errors="coerce").dt.normalize()
    fttm["ts_code"] = fttm["ts_code"].astype("string").str.strip()
    fttm["org_name"] = fttm["org_name"].astype("string").fillna("").str.strip()
    for column in (
        "fttm_np",
        "fy1_year",
        "fy2_year",
        "fy1_np_raw",
        "fy2_np_raw",
        "fy1_weight",
        "fy2_weight",
    ):
        fttm[column] = pd.to_numeric(fttm[column], errors="coerce")
    fttm = fttm.loc[
        fttm["obs_date"].notna()
        & fttm["ts_code"].ne("")
        & fttm["org_name"].ne("")
        & fttm["fttm_np"].notna()
    ].copy()
    fttm = fttm.sort_values(
        ["obs_date", "ts_code", "org_name"], kind="mergesort"
    ).drop_duplicates(["obs_date", "ts_code", "org_name"], keep="first")
    if fttm.empty:
        return pd.DataFrame(columns=output_columns)

    previous = fttm.copy()
    previous["obs_date"] = previous["obs_date"] + pd.offsets.MonthEnd(1)
    previous = previous.rename(
        columns={
            column: f"previous_{column}"
            for column in REVISION_SOURCE_COLUMNS
            if column not in {"obs_date", "ts_code", "org_name"}
        }
    )
    matched = fttm.merge(
        previous,
        on=["obs_date", "ts_code", "org_name"],
        how="inner",
        sort=False,
    )
    same_targets = matched["fy1_year"].eq(matched["previous_fy1_year"]) & matched[
        "fy2_year"
    ].eq(matched["previous_fy2_year"])
    complete_pairs = (
        matched[
            [
                "fy1_np_raw",
                "fy2_np_raw",
                "previous_fy1_np_raw",
                "previous_fy2_np_raw",
                "fy1_weight",
                "fy2_weight",
                "previous_fy1_weight",
                "previous_fy2_weight",
            ]
        ]
        .notna()
        .all(axis=1)
    )
    matched = matched.loc[same_targets & complete_pairs].copy()
    if matched.empty:
        return pd.DataFrame(columns=output_columns)

    matched["revision_component"] = matched["fy1_weight"] * (
        matched["fy1_np_raw"] - matched["previous_fy1_np_raw"]
    ) + matched["fy2_weight"] * (matched["fy2_np_raw"] - matched["previous_fy2_np_raw"])
    matched["horizon_roll_component"] = (
        matched["fy1_weight"] - matched["previous_fy1_weight"]
    ) * matched["previous_fy1_np_raw"] + (
        matched["fy2_weight"] - matched["previous_fy2_weight"]
    ) * matched[
        "previous_fy2_np_raw"
    ]

    joined = member_frame.merge(
        matched,
        on=["obs_date", "ts_code"],
        how="inner",
        sort=False,
    )
    if joined.empty:
        return pd.DataFrame(columns=output_columns)

    stock_keys = [*keys, "ts_code"]
    stock_rows = (
        joined.groupby(stock_keys, sort=False, dropna=False)
        .agg(
            revision_weight=(weight_column, "first"),
            previous_common_fttm=("previous_fttm_np", "mean"),
            revision_component=("revision_component", "mean"),
            horizon_roll_component=("horizon_roll_component", "mean"),
            matched_org_count=("org_name", "nunique"),
        )
        .reset_index()
    )

    structural = (
        member_frame.groupby(keys, sort=False, dropna=False)
        .agg(structural_revision_weight=(weight_column, "sum"))
        .reset_index()
    )
    org_counts = (
        joined.groupby(keys, sort=False, dropna=False)
        .agg(revision_comparable_org_count=("org_name", "nunique"))
        .reset_index()
    )

    def aggregate(group: pd.DataFrame) -> pd.Series:
        weights = group["revision_weight"]
        previous_level = (weights * group["previous_common_fttm"]).sum()
        revision_change = (weights * group["revision_component"]).sum()
        horizon_roll = (weights * group["horizon_roll_component"]).sum()
        comparable_weight = weights.sum()
        active_tolerance = np.maximum(
            1e-8, group["previous_common_fttm"].abs().to_numpy(dtype=float) * 1e-10
        )
        active = (
            group["revision_component"].abs().to_numpy(dtype=float) > active_tolerance
        )
        up = group["revision_component"].to_numpy(dtype=float) > active_tolerance
        active_count = int(active.sum())
        active_weight = float(weights.to_numpy(dtype=float)[active].sum())
        return pd.Series(
            {
                "revision_comparable_stock_count": int(group["ts_code"].nunique()),
                "revision_median_org_count": float(group["matched_org_count"].median()),
                "revision_comparable_weight": comparable_weight,
                "revision_rate": (
                    revision_change / abs(previous_level)
                    if pd.notna(previous_level) and previous_level != 0
                    else np.nan
                ),
                "horizon_roll_rate": (
                    horizon_roll / abs(previous_level)
                    if pd.notna(previous_level) and previous_level != 0
                    else np.nan
                ),
                "revision_activity_rate": (
                    active_count / len(group) if len(group) else np.nan
                ),
                "revision_up_stock_rate": (
                    int(up.sum()) / active_count if active_count else np.nan
                ),
                "revision_up_weight_rate": (
                    float(weights.to_numpy(dtype=float)[up].sum()) / active_weight
                    if active_weight > 0
                    else np.nan
                ),
            }
        )

    aggregate_rows = (
        stock_rows.groupby(keys, sort=False, dropna=False)
        .apply(aggregate, include_groups=False)
        .reset_index()
    )
    aggregate_rows = aggregate_rows.merge(org_counts, on=keys, how="left", sort=False)
    aggregate_rows = aggregate_rows.merge(structural, on=keys, how="left", sort=False)
    aggregate_rows["revision_comparable_weight_rate"] = np.where(
        aggregate_rows["structural_revision_weight"].notna()
        & (aggregate_rows["structural_revision_weight"] != 0),
        aggregate_rows["revision_comparable_weight"]
        / aggregate_rows["structural_revision_weight"],
        np.nan,
    )
    for column in (
        "revision_comparable_weight_rate",
        "revision_activity_rate",
        "revision_up_stock_rate",
        "revision_up_weight_rate",
    ):
        aggregate_rows[column] = aggregate_rows[column].clip(lower=0.0, upper=1.0)
    aggregate_rows["revision_version"] = REVISION_VERSION
    return aggregate_rows[output_columns]


__all__ = [
    "REVISION_OUTPUT_COLUMNS",
    "REVISION_SOURCE_COLUMNS",
    "REVISION_VERSION",
    "build_matched_revision_metrics",
]
