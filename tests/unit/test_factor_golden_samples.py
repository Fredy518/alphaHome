import pandas as pd
import numpy as np

from alphahome.factors.core import GFactorCalculator, PFactorCalculator
from alphahome.factors.core.computation import compute_g_snapshot, compute_p_snapshot
from alphahome.factors.persistence import (
    G_FACTOR_COLUMNS,
    P_FACTOR_COLUMNS,
    factor_frame_checksum,
)


class _NoIOContext:
    db_manager = object()

    def query_dataframe(self, *_args, **_kwargs):
        raise AssertionError("pure calculation attempted database I/O")


def _p_inputs():
    indicators = pd.DataFrame(
        {
            "ts_code": ["A", "B", "C", "D"],
            "end_date": ["2025-12-31"] * 4,
            "ann_date": ["2026-03-01"] * 4,
            "data_source": ["report"] * 4,
            "gpa_ttm": [10.0, 20.0, 30.0, 40.0],
            "roe_excl_ttm": [5.0, 10.0, 15.0, 20.0],
            "roa_excl_ttm": [2.0, 4.0, 6.0, 8.0],
            "net_margin_ttm": [1.0, 2.0, 3.0, 4.0],
            "operating_margin_ttm": [2.0, 3.0, 4.0, 5.0],
            "roi_ttm": [3.0, 4.0, 5.0, 6.0],
            "asset_turnover_ttm": [0.5, 0.6, 0.7, 0.8],
            "equity_multiplier": [1.2, 1.3, 1.4, 1.5],
            "debt_to_asset_ratio": [30.0, 40.0, 50.0, 60.0],
            "equity_ratio": [70.0, 60.0, 50.0, 40.0],
            "revenue_yoy_growth": [5.0, 10.0, 15.0, 20.0],
            "n_income_yoy_growth": [6.0, 12.0, 18.0, 24.0],
            "operate_profit_yoy_growth": [7.0, 14.0, 21.0, 28.0],
            "data_quality": ["high"] * 4,
            "calculation_status": ["success"] * 4,
        }
    )
    industry = pd.DataFrame(
        {
            "ts_code": ["A", "B", "C", "D"],
            "requires_special_gpa_handling": [False, False, True, False],
            "gpa_calculation_method": ["standard", "standard", "null", "standard"],
        }
    )
    return indicators, industry


def _g_history():
    rows = []
    for code, old, new, revenue, profit in (
        ("A", 30, 60, 10, 20),
        ("B", 50, 40, 30, 5),
        ("C", 20, 80, -5, 50),
    ):
        for calc_date, score in (("2025-03-07", old), ("2026-03-06", new)):
            rows.append(
                {
                    "ts_code": code,
                    "calc_date": calc_date,
                    "p_score": score,
                    "data_source": "report",
                    "ann_date": calc_date,
                    "gpa": 1,
                    "roe_excl": 2,
                    "roa_excl": 3,
                    "revenue_yoy_growth": revenue,
                    "n_income_yoy_growth": profit,
                }
            )
    return pd.DataFrame(rows)


def test_p_v2_golden_sample_is_stable_at_database_precision():
    indicators, industry = _p_inputs()
    calculator = PFactorCalculator(context=_NoIOContext())
    result = compute_p_snapshot(
        indicators,
        industry,
        "2026-03-06",
        calculator._calculate_p_factors_from_mvp_indicators_pit,
    )

    assert result["ts_code"].tolist() == ["A", "B", "C", "D"]
    assert result["p_score"].round(6).tolist() == [
        0.0,
        40.0,
        66.666667,
        100.0,
    ]
    assert result["p_rank"].tolist() == [4, 3, 2, 1]
    assert pd.isna(result.loc[result["ts_code"] == "C", "gpa"]).all()
    assert (
        factor_frame_checksum(result, P_FACTOR_COLUMNS)
        == "7d282014e3fdcde14ad0746aa9c5fb4e5a7df4ef89f91ba314dcb1251e993517"
    )


def test_g_v1_1_golden_sample_is_stable_at_database_precision():
    calculator = GFactorCalculator(context=_NoIOContext())
    result = compute_g_snapshot(
        _g_history(),
        "2026-03-06",
        calculator._calculate_g_factors_from_p_data_pit,
    ).sort_values("ts_code")

    assert result["ts_code"].tolist() == ["A", "B", "C"]
    assert result["g_score"].round(6).tolist() == [
        66.666667,
        50.0,
        83.333333,
    ]
    assert (
        factor_frame_checksum(result, G_FACTOR_COLUMNS)
        == "977be3ff0367535843967e1febb091eb1e9bfbed6cf0e15999ae6a0a1b7eef0f"
    )


def test_optimized_g_yoy_series_matches_frozen_reference_logic():
    calculator = GFactorCalculator(context=_NoIOContext())
    frame = pd.DataFrame(
        {
            "calc_date": pd.date_range("2024-01-05", periods=110, freq="7D"),
            "p_score": [
                np.nan if index in {3, 57} else (index * 7) % 101
                for index in range(110)
            ],
        }
    )

    expected = []
    sorted_frame = frame.sort_values("calc_date")
    for _, current in sorted_frame.iterrows():
        target = current["calc_date"] - pd.DateOffset(weeks=52)
        candidates = sorted_frame[
            (sorted_frame["calc_date"] >= target - pd.DateOffset(days=45))
            & (sorted_frame["calc_date"] <= target + pd.DateOffset(days=45))
        ].copy()
        if candidates.empty:
            continue
        candidates["date_diff"] = abs(candidates["calc_date"] - target)
        match = candidates.loc[candidates["date_diff"].idxmin()]
        if pd.notna(current["p_score"]) and pd.notna(match["p_score"]):
            expected.append(float(current["p_score"]) - float(match["p_score"]))

    assert calculator._build_yoy_delta_series_52w(frame) == expected


def test_g_efficiency_surprise_treats_float_noise_as_zero_dispersion():
    calculator = GFactorCalculator(context=_NoIOContext())
    base = -24.886850491046097
    noisy_equal = [base, np.nextafter(base, np.inf), base]
    assert np.std(noisy_equal) > 0
    calculator._build_yoy_delta_series_52w = lambda _group: noisy_equal

    result = calculator._calculate_efficiency_surprise(
        pd.DataFrame(),
        pd.Series({"p_score": 54.39561950895391}),
        pd.Series({"p_score": 79.28247}),
    )

    assert result == base
