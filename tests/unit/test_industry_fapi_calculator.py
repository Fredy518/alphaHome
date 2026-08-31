from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from alphahome.pit.calculators.industry_fapi_calculator import (
    IndustryFAPICalculator,
)


JAN = pd.Timestamp("2026-01-31")
FEB = pd.Timestamp("2026-02-28")


def _sources(current_report_date: str = "2026-02-20"):
    stocks = [f"S{index:03d}.SZ" for index in range(1, 21)]
    orgs = [f"机构{index}" for index in range(1, 6)]
    classification_rows = []
    for index, ts_code in enumerate(stocks, start=1):
        first_industry = index <= 2
        classification_rows.append(
            {
                "obs_date": FEB,
                "ts_code": ts_code,
                "data_source": "sw",
                "industry_code1": "801010.SI" if first_industry else "801020.SI",
                "industry_level1": "行业甲" if first_industry else "行业乙",
                "industry_code2": "801011.SI" if first_industry else "801021.SI",
                "industry_level2": "行业甲二级" if first_industry else "行业乙二级",
            }
        )
    classification_rows.append(
        {
            "obs_date": FEB,
            "ts_code": "X001.SZ",
            "data_source": "sw",
            "industry_code1": "801030.SI",
            "industry_level1": "仅结构行业",
            "industry_code2": "801031.SI",
            "industry_level2": "仅结构二级",
        }
    )

    fttm_rows = []
    for org_index, org_name in enumerate(orgs, start=1):
        for stock_index, ts_code in enumerate(stocks, start=1):
            fttm_rows.append(
                {
                    "obs_date": JAN,
                    "ts_code": ts_code,
                    "org_name": org_name,
                    "fttm_np": 10.0,
                    "selected_report_date": pd.Timestamp("2026-01-20"),
                    "formula_version": "test_fttm_v1",
                }
            )
            current_value = 10.0
            if stock_index <= 2:
                current_value = 20.0 if org_index <= 3 else 5.0
            fttm_rows.append(
                {
                    "obs_date": FEB,
                    "ts_code": ts_code,
                    "org_name": org_name,
                    "fttm_np": current_value,
                    "selected_report_date": pd.Timestamp(current_report_date),
                    "formula_version": "test_fttm_v1",
                }
            )

    equity = pd.DataFrame(
        [
            {
                "obs_date": FEB,
                "ts_code": ts_code,
                "equity_trade_date": pd.Timestamp("2026-02-27"),
                "total_mv": 100.0,
                "pb": 1.0,
            }
            for ts_code in stocks
        ]
    )
    benchmark = pd.DataFrame(
        [
            {
                "obs_date": FEB,
                "benchmark_code": "000906.SH",
                "benchmark_name": "中证800",
                "benchmark_weight_trade_date": pd.Timestamp("2026-02-27"),
                "ts_code": ts_code,
                "weight": 5.0,
            }
            for ts_code in stocks
        ]
    )
    return (
        pd.DataFrame(classification_rows),
        equity,
        benchmark,
        pd.DataFrame(fttm_rows),
    )


def test_source_adapted_fapi_matches_declared_common_sample_formula():
    calculator = IndustryFAPICalculator()

    result = calculator.calculate(*_sources(), obs_dates=[date(2026, 2, 28)])

    industry = result.loc[
        result["industry_level"].eq("L1") & result["industry_code"].eq("801010.SI")
    ].iloc[0]
    assert industry["matched_org_count"] == 5
    assert industry["matched_stock_count"] == 2
    assert industry["median_common_stock_count"] == 2
    assert industry["spread_up_org_count"] == 3
    assert industry["spread_down_or_flat_org_count"] == 2
    assert industry["ratio_up_org_count"] == 3
    assert industry["ratio_down_or_flat_org_count"] == 2
    assert industry["fapi_spread_equal"] == pytest.approx(0.6)
    assert industry["fapi_spread_weighted"] == pytest.approx(0.6)
    assert industry["fapi_ratio_equal"] == pytest.approx(0.6)
    assert industry["fapi_ratio_weighted"] == pytest.approx(0.6)
    assert industry["expected_roe_equal"] == pytest.approx(0.14)
    assert industry["benchmark_expected_roe_equal"] == pytest.approx(0.104)
    assert industry["is_eligible"] == np.bool_(True)
    assert industry["is_ratio_eligible"] == np.bool_(True)
    assert industry["quality_reasons"] == []
    assert industry["stock_formula_versions"] == ["test_fttm_v1"]


def test_structural_industries_are_preserved_without_manufactured_fapi():
    calculator = IndustryFAPICalculator()

    result = calculator.calculate(*_sources(), obs_dates=[FEB])

    structural_only = result.loc[
        result["industry_level"].eq("L1") & result["industry_code"].eq("801030.SI")
    ].iloc[0]
    assert structural_only["structural_member_count"] == 1
    assert structural_only["equity_available_member_count"] == 0
    assert structural_only["matched_org_count"] == 0
    assert pd.isna(structural_only["fapi_spread_equal"])
    assert not structural_only["is_eligible"]
    assert structural_only["quality_reasons"] == [
        "insufficient_matched_orgs",
        "no_spread_fapi",
    ]
    assert set(result["industry_level"]) == {"L1", "L2"}


def test_future_report_date_is_explicitly_ineligible():
    calculator = IndustryFAPICalculator()

    result = calculator.calculate(
        *_sources(current_report_date="2026-03-01"), obs_dates=[FEB]
    )

    industry = result.loc[
        result["industry_level"].eq("L1") & result["industry_code"].eq("801010.SI")
    ].iloc[0]
    assert not industry["is_eligible"]
    assert "future_current_report_date" in industry["quality_reasons"]


def test_four_fapi_definitions_remain_bounded_and_count_consistent():
    result = IndustryFAPICalculator().calculate(*_sources(), obs_dates=[FEB])
    valued = result.loc[result["fapi_spread_equal"].notna()]

    for column in (
        "fapi_spread_equal",
        "fapi_spread_weighted",
        "fapi_ratio_equal",
        "fapi_ratio_weighted",
    ):
        assert valued[column].dropna().between(0, 1).all()
    assert (
        valued["matched_org_count"]
        == valued["spread_up_org_count"] + valued["spread_down_or_flat_org_count"]
    ).all()
    assert (
        valued["ratio_matched_org_count"]
        == valued["ratio_up_org_count"] + valued["ratio_down_or_flat_org_count"]
    ).all()
