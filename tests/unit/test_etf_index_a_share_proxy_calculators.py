from __future__ import annotations

from datetime import date
import logging

import pandas as pd
import pytest

from alphahome.pit.calculators.etf_index_a_share_proxy_fapi_calculator import (
    ETFIndexAShareProxyFAPICalculator,
)
from alphahome.pit.calculators.etf_index_a_share_proxy_members_calculator import (
    ETFIndexAShareProxyMembersCalculator,
)
from alphahome.pit.pit_etf_index_a_share_proxy_fapi_manager import (
    PITETFIndexAShareProxyFAPIMonthlyManager,
)


JAN = pd.Timestamp("2026-01-31")
FEB = pd.Timestamp("2026-02-28")


def _cross_market_weights() -> pd.DataFrame:
    rows = [
        (f"00000{index}.SZ", 14.0) for index in range(1, 6)
    ] + [
        ("00178.HK", 15.0),
        ("178.HK", 15.0),
        ("02899.HK", 15.0),
    ]
    return pd.DataFrame(
        {
            "index_code": ["931238.CSI"] * len(rows),
            "index_name": ["沪深港黄金产业"] * len(rows),
            "weight_trade_date": ["2026-02-27"] * len(rows),
            "ts_code": [row[0] for row in rows],
            "raw_weight": [row[1] for row in rows],
        }
    )


def test_a_share_proxy_normalizes_hk_aliases_and_keeps_explicit_scope():
    calculator = ETFIndexAShareProxyMembersCalculator()

    result = calculator.calculate(
        _cross_market_weights(), [FEB], ["931238.CSI"]
    )

    assert len(result) == 5
    assert result["ts_code"].str.endswith((".SH", ".SZ")).all()
    assert result["weight"].sum() == pytest.approx(1.0)
    assert result["source_weight_sum"].unique().tolist() == [100.0]
    assert result["scope_weight_rate"].unique().tolist() == [pytest.approx(0.70)]
    assert result["constituent_scope"].unique().tolist() == [
        "a_share_subset_of_cross_market_index"
    ]
    assert result["is_proxy"].all()
    assert result["is_eligible"].all()
    assert result["method_version"].unique().tolist() == [
        calculator.METHOD_VERSION
    ]
    assert calculator.last_audit["selected_pair_count"] == 1


def _proxy_fapi_sources():
    stocks = [f"S{index:03d}.SZ" for index in range(1, 21)]
    orgs = [f"机构{index}" for index in range(1, 6)]
    members = pd.DataFrame(
        [
            {
                "obs_date": FEB,
                "index_code": "931238.CSI",
                "index_name": "沪深港黄金产业",
                "ts_code": ts_code,
                "weight_basis": "official_index_weight",
                "weight_source": "rawdata.index_weight",
                "source_code": "931238.CSI",
                "source_effective_date": pd.Timestamp("2026-02-27"),
                "source_available_date": pd.Timestamp("2026-02-27"),
                "source_staleness_days": 1,
                "source_coverage_rate": 1.0,
                "source_quality": "high",
                "is_fallback": False,
                "is_eligible": True,
                "quality_reasons": [
                    "a_share_subset_proxy_of_cross_market_index"
                ],
                "constituent_scope": "a_share_subset_of_cross_market_index",
                "is_proxy": True,
                "scope_weight_rate": 0.72,
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
                        "selected_report_date": obs_date
                        - pd.Timedelta(days=8),
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


def test_proxy_fapi_preserves_scope_metadata_and_method_version():
    calculator = ETFIndexAShareProxyFAPICalculator()

    result = calculator.calculate(*_proxy_fapi_sources(), obs_dates=[FEB])

    row = result.iloc[0]
    assert row["member_constituent_scope"] == (
        "a_share_subset_of_cross_market_index"
    )
    assert bool(row["member_is_proxy"])
    assert row["member_scope_weight_rate"] == pytest.approx(0.72)
    assert row["method_version"] == calculator.METHOD_VERSION
    assert "a_share_subset_proxy_of_cross_market_index" in row[
        "quality_reasons"
    ]
    assert bool(row["is_eligible"])


def test_proxy_fapi_manager_clears_months_with_no_proxy_members():
    manager = PITETFIndexAShareProxyFAPIMonthlyManager()
    manager.logger = logging.getLogger(__name__)
    manager._ensure_table_exists = lambda: None
    manager._resolve_index_codes = lambda _codes: ["931238.CSI"]
    manager._load_sources = lambda _months, _stock_months, _codes: {
        "members": pd.DataFrame(),
        "equity": pd.DataFrame(),
        "benchmark_members": pd.DataFrame(),
        "stock_fttm": pd.DataFrame(),
    }
    manager._validate_dependencies = lambda _sources, _months, _stock_months: None
    captured: list[pd.DataFrame] = []

    def replace(frame, _months, _codes):
        captured.append(frame.copy())
        return 0

    manager._atomic_replace_scope = replace
    manager._dependency_freshness = lambda: {}

    result = manager._run_months(
        [date(2022, 12, 31)],
        batch_size=1,
        index_codes=None,
        result_key="backfilled_records",
    )

    assert result["backfilled_records"] == 0
    assert len(captured) == 1 and captured[0].empty
    assert set(captured[0].columns) == set(manager.calculator.INDEX_OUTPUT_COLUMNS)
    assert result["batch_audits"][0]["missing_member_pair_count"] == 1
