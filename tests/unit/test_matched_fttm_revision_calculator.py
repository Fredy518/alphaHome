from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.matched_fttm_revision_calculator import (
    REVISION_VERSION,
    build_matched_revision_metrics,
)


GROUP_KEYS = ["obs_date", "universe_type", "universe_code"]


def _members(dates=("2024-03-31", "2024-04-30")) -> pd.DataFrame:
    rows = []
    for obs_date in dates:
        rows.extend(
            [
                {
                    "obs_date": obs_date,
                    "universe_type": "all_a",
                    "universe_code": "ALL_A",
                    "ts_code": "000001.SZ",
                    "weight": 1.0,
                },
                {
                    "obs_date": obs_date,
                    "universe_type": "all_a",
                    "universe_code": "ALL_A",
                    "ts_code": "000002.SZ",
                    "weight": 3.0,
                },
            ]
        )
    return pd.DataFrame(rows)


def _row(
    obs_date: str,
    ts_code: str,
    org_name: str,
    *,
    fy1: float,
    fy2: float,
    fy1_weight: float,
    fy2_weight: float,
    fy1_year: int = 2024,
    fy2_year: int = 2025,
) -> dict:
    return {
        "obs_date": obs_date,
        "ts_code": ts_code,
        "org_name": org_name,
        "fttm_np": fy1_weight * fy1 + fy2_weight * fy2,
        "fy1_year": fy1_year,
        "fy2_year": fy2_year,
        "fy1_np_raw": fy1,
        "fy2_np_raw": fy2,
        "fy1_weight": fy1_weight,
        "fy2_weight": fy2_weight,
    }


def test_decomposes_revision_and_horizon_roll_on_common_sample():
    fttm = pd.DataFrame(
        [
            _row(
                "2024-03-31",
                "000001.SZ",
                "机构A",
                fy1=100.0,
                fy2=200.0,
                fy1_weight=1.0,
                fy2_weight=0.0,
            ),
            _row(
                "2024-04-30",
                "000001.SZ",
                "机构A",
                fy1=110.0,
                fy2=220.0,
                fy1_weight=0.75,
                fy2_weight=0.25,
            ),
            _row(
                "2024-03-31",
                "000002.SZ",
                "机构A",
                fy1=300.0,
                fy2=400.0,
                fy1_weight=1.0,
                fy2_weight=0.0,
            ),
            _row(
                "2024-04-30",
                "000002.SZ",
                "机构A",
                fy1=300.0,
                fy2=400.0,
                fy1_weight=0.75,
                fy2_weight=0.25,
            ),
        ]
    )

    result = build_matched_revision_metrics(
        _members(), fttm, group_keys=GROUP_KEYS, weight_column="weight"
    )
    april = result.loc[result.obs_date.eq(pd.Timestamp("2024-04-30"))].iloc[0]

    # Previous weighted level: (1*100 + 3*300) / implicit common scale = 1000.
    # Revision numerator: 1*(.75*10 + .25*20) + 3*0 = 12.5.
    # Horizon roll: 1*25 + 3*25 = 100.
    assert april.revision_rate == pytest.approx(12.5 / 1000.0)
    assert april.horizon_roll_rate == pytest.approx(100.0 / 1000.0)
    assert april.revision_comparable_stock_count == 2
    assert april.revision_comparable_org_count == 1
    assert april.revision_comparable_weight_rate == pytest.approx(1.0)
    assert april.revision_activity_rate == pytest.approx(0.5)
    assert april.revision_up_stock_rate == pytest.approx(1.0)
    assert april.revision_up_weight_rate == pytest.approx(1.0)
    assert april.revision_version == REVISION_VERSION


def test_year_roll_and_incomplete_pairs_are_excluded_from_comparable_coverage():
    fttm = pd.DataFrame(
        [
            _row(
                "2024-03-31",
                "000001.SZ",
                "机构A",
                fy1=100.0,
                fy2=200.0,
                fy1_weight=1.0,
                fy2_weight=0.0,
            ),
            _row(
                "2024-04-30",
                "000001.SZ",
                "机构A",
                fy1=200.0,
                fy2=300.0,
                fy1_weight=0.75,
                fy2_weight=0.25,
                fy1_year=2025,
                fy2_year=2026,
            ),
            _row(
                "2024-03-31",
                "000002.SZ",
                "机构A",
                fy1=300.0,
                fy2=400.0,
                fy1_weight=1.0,
                fy2_weight=0.0,
            ),
            _row(
                "2024-04-30",
                "000002.SZ",
                "机构A",
                fy1=310.0,
                fy2=410.0,
                fy1_weight=0.75,
                fy2_weight=0.25,
            ),
        ]
    )
    fttm.loc[
        (fttm.obs_date == "2024-04-30") & (fttm.ts_code == "000002.SZ"),
        "fy2_np_raw",
    ] = None

    result = build_matched_revision_metrics(
        _members(), fttm, group_keys=GROUP_KEYS, weight_column="weight"
    )

    assert result.empty


def test_unchanged_forecasts_are_inactive_not_down_revisions():
    fttm = pd.DataFrame(
        [
            _row(
                "2024-03-31",
                "000001.SZ",
                "机构A",
                fy1=100.0,
                fy2=200.0,
                fy1_weight=1.0,
                fy2_weight=0.0,
            ),
            _row(
                "2024-04-30",
                "000001.SZ",
                "机构A",
                fy1=100.0,
                fy2=200.0,
                fy1_weight=0.75,
                fy2_weight=0.25,
            ),
        ]
    )
    members = _members().loc[lambda frame: frame.ts_code.eq("000001.SZ")]

    result = build_matched_revision_metrics(
        members, fttm, group_keys=GROUP_KEYS, weight_column="weight"
    )
    april = result.loc[result.obs_date.eq(pd.Timestamp("2024-04-30"))].iloc[0]

    assert april.revision_rate == pytest.approx(0.0)
    assert april.horizon_roll_rate == pytest.approx(0.25)
    assert april.revision_activity_rate == pytest.approx(0.0)
    assert pd.isna(april.revision_up_stock_rate)
    assert pd.isna(april.revision_up_weight_rate)
