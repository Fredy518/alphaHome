from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.etf_index_fapi_calculator import (
    ETFIndexFAPICalculator,
)


JAN = pd.Timestamp("2026-01-31")
FEB = pd.Timestamp("2026-02-28")


def _sources(member_source_eligible: bool = True):
    stocks = [f"S{index:03d}.SZ" for index in range(1, 21)]
    orgs = [f"机构{index}" for index in range(1, 6)]
    members = pd.DataFrame(
        [
            {
                "obs_date": FEB,
                "index_code": "IDX",
                "index_name": "测试指数",
                "ts_code": ts_code,
                "weight_basis": "official_index_weight",
                "weight_source": "rawdata.index_weight",
                "source_code": "IDX",
                "source_effective_date": pd.Timestamp("2026-02-27"),
                "source_available_date": pd.Timestamp("2026-02-27"),
                "source_staleness_days": 1,
                "source_coverage_rate": 1.0,
                "source_quality": "high",
                "is_fallback": False,
                "is_eligible": member_source_eligible,
                "quality_reasons": []
                if member_source_eligible
                else ["member_test_ineligible"],
            }
            for ts_code in stocks[:2]
        ]
    )
    fttm_rows = []
    for org_index, org_name in enumerate(orgs, start=1):
        for stock_index, ts_code in enumerate(stocks, start=1):
            for obs_date, value in (
                (JAN, 10.0),
                (
                    FEB,
                    20.0
                    if stock_index <= 2 and org_index <= 3
                    else (5.0 if stock_index <= 2 else 10.0),
                ),
            ):
                fttm_rows.append(
                    {
                        "obs_date": obs_date,
                        "ts_code": ts_code,
                        "org_name": org_name,
                        "fttm_np": value,
                        "selected_report_date": obs_date - pd.Timedelta(days=8),
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
    return members, equity, benchmark, pd.DataFrame(fttm_rows)


def test_etf_index_fapi_reuses_exact_member_universe_and_source_metadata():
    calculator = ETFIndexFAPICalculator()
    result = calculator.calculate(*_sources(), obs_dates=[FEB])

    assert len(result) == 1
    row = result.iloc[0]
    assert row["index_code"] == "IDX"
    assert row["member_weight_basis"] == "official_index_weight"
    assert row["structural_member_count"] == 2
    assert row["fapi_spread_equal"] == pytest.approx(0.6)
    assert row["expected_roe_equal"] == pytest.approx(0.14)
    assert bool(row["is_fapi_eligible"])
    assert bool(row["is_eligible"])
    assert calculator.last_audit["missing_industry_code_count"] == 0
    assert calculator.last_audit["intentionally_absent_l2_member_count"] == 2


def test_member_source_ineligibility_cannot_be_hidden_by_valid_fapi():
    result = ETFIndexFAPICalculator().calculate(
        *_sources(member_source_eligible=False), obs_dates=[FEB]
    )

    row = result.iloc[0]
    assert bool(row["is_fapi_eligible"])
    assert not bool(row["is_eligible"])
    assert "member_test_ineligible" in row["quality_reasons"]
    assert "member_source_ineligible" in row["quality_reasons"]
