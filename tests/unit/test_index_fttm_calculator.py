from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.index_fttm_calculator import (
    IndexFTTMCalculator,
    IndexQualityThreshold,
)


def _members(dates=("2024-01-31", "2024-02-29")):
    rows = []
    for obs_date in dates:
        for universe_type, universe_code, universe_name, weight_basis, source in (
            ("index", "000300.SH", "沪深300", "official_weight", "rawdata.index_weight"),
            ("all_a", "ALL_A", "全A", "total_mv", "rawdata.stock_dailybasic"),
        ):
            rows.extend(
                [
                    {
                        "obs_date": obs_date,
                        "universe_type": universe_type,
                        "universe_code": universe_code,
                        "universe_name": universe_name,
                        "weight_basis": weight_basis,
                        "weight_source": source,
                        "weight_trade_date": obs_date,
                        "ts_code": "000001.SZ",
                        "raw_weight": 25.0 if universe_type == "index" else 100.0,
                    },
                    {
                        "obs_date": obs_date,
                        "universe_type": universe_type,
                        "universe_code": universe_code,
                        "universe_name": universe_name,
                        "weight_basis": weight_basis,
                        "weight_source": source,
                        "weight_trade_date": obs_date,
                        "ts_code": "000002.SZ",
                        "raw_weight": 75.0 if universe_type == "index" else 300.0,
                    },
                ]
            )
    return pd.DataFrame(rows)


def _stock_basic(delist_second=None):
    return pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "list_date": "2000-01-01",
                "delist_date": None,
                "exchange": "SZSE",
                "curr_type": "CNY",
            },
            {
                "ts_code": "000002.SZ",
                "list_date": "2000-01-01",
                "delist_date": delist_second,
                "exchange": "SZSE",
                "curr_type": "CNY",
            },
        ]
    )


def _fttm():
    return pd.DataFrame(
        [
            {
                "obs_date": "2024-01-31",
                "ts_code": "000001.SZ",
                "org_name": "机构A",
                "fttm_np": 10.0,
                "formula_version": "fttm_q4_report_event_linear_v1",
                "selected_report_date": "2024-01-20",
            },
            {
                "obs_date": "2024-01-31",
                "ts_code": "000002.SZ",
                "org_name": "机构A",
                "fttm_np": 20.0,
                "formula_version": "fttm_q4_report_event_linear_v1",
                "selected_report_date": "2024-01-20",
            },
            {
                "obs_date": "2024-01-31",
                "ts_code": "000001.SZ",
                "org_name": "机构B",
                "fttm_np": 30.0,
                "formula_version": "fttm_q4_report_event_linear_v1",
                "selected_report_date": "2024-01-21",
            },
            {
                "obs_date": "2024-02-29",
                "ts_code": "000001.SZ",
                "org_name": "机构A",
                "fttm_np": 20.0,
                "formula_version": "fttm_q4_report_event_linear_v1",
                "selected_report_date": "2024-02-20",
            },
            {
                "obs_date": "2024-02-29",
                "ts_code": "000002.SZ",
                "org_name": "机构A",
                "fttm_np": 20.0,
                "formula_version": "fttm_q4_report_event_linear_v1",
                "selected_report_date": "2024-02-20",
            },
            {
                "obs_date": "2024-02-29",
                "ts_code": "000001.SZ",
                "org_name": "机构B",
                "fttm_np": 25.0,
                "formula_version": "fttm_q4_report_event_linear_v1",
                "selected_report_date": "2024-02-21",
            },
        ]
    )


def _calculator(min_match=2):
    thresholds = {
        universe_type: IndexQualityThreshold(
            min_covered_stocks=2,
            min_covered_weight_rate=0.60,
            min_weight_data_coverage_rate=0.98,
            min_org_count=2,
            min_matched_org_count=min_match,
        )
        for universe_type in ("index", "all_a")
    }
    return IndexFTTMCalculator(thresholds=thresholds)


def test_official_index_and_all_a_use_two_stage_weighting():
    calculator = _calculator()
    result = calculator.calculate(_members(), _stock_basic(), _fttm())

    january = result.loc[result.obs_date.eq(pd.Timestamp("2024-01-31"))]
    assert set(january.universe_type) == {"index", "all_a"}
    assert january.index_fttm_np.tolist() == pytest.approx([23.75, 23.75])
    assert january.covered_weight_rate.tolist() == pytest.approx([1.0, 1.0])
    assert january.org_count.tolist() == [2, 2]

    index_org = calculator.last_org_universe.loc[
        (calculator.last_org_universe.obs_date == pd.Timestamp("2024-01-31"))
        & (calculator.last_org_universe.universe_code == "000300.SH")
    ]
    assert sorted(index_org.org_index_fttm.tolist()) == pytest.approx([17.5, 30.0])


def test_diffusion_uses_same_org_immediate_previous_month():
    result = _calculator().calculate(_members(), _stock_basic(), _fttm())
    february = result.loc[result.obs_date.eq(pd.Timestamp("2024-02-29"))]

    assert february.index_fttm_np.tolist() == pytest.approx([22.5, 22.5])
    assert february.previous_index_fttm_np.tolist() == pytest.approx([23.75, 23.75])
    assert february.matched_org_count.tolist() == [2, 2]
    assert february.up_org_count.tolist() == [1, 1]
    assert february.down_or_flat_org_count.tolist() == [1, 1]
    assert february.diffusion_up.tolist() == pytest.approx([0.5, 0.5])


def test_first_month_diffusion_is_null_not_neutral():
    result = _calculator(min_match=1).calculate(
        _members(("2024-01-31",)),
        _stock_basic(),
        _fttm().loc[lambda frame: frame.obs_date.eq("2024-01-31")],
    )

    assert result.matched_org_count.eq(0).all()
    assert result.diffusion_up.isna().all()
    assert result.quality_reasons.map(
        lambda reasons: "diffusion_ineligible_low_match" in reasons
    ).all()


def test_weight_staleness_is_enforced_by_weight_basis():
    members = _members(("2024-02-29",))
    members.loc[members.universe_type.eq("index"), "weight_trade_date"] = "2023-12-01"
    members.loc[members.universe_type.eq("all_a"), "weight_trade_date"] = "2024-01-01"
    result = _calculator().calculate(
        members,
        _stock_basic(),
        _fttm().loc[lambda frame: frame.obs_date.eq("2024-02-29")],
    )

    assert result.weight_available_count.eq(0).all()
    assert result.index_fttm_np.isna().all()
    assert result.quality_reasons.map(
        lambda reasons: "ineligible_incomplete_weight_data" in reasons
    ).all()


def test_delisted_stock_is_removed_from_active_denominator():
    result = _calculator().calculate(
        _members(("2024-02-29",)),
        _stock_basic(delist_second="2024-02-01"),
        _fttm().loc[lambda frame: frame.obs_date.eq("2024-02-29")],
    )

    assert result.structural_member_count.eq(2).all()
    assert result.active_member_count.eq(1).all()
    assert result.covered_stock_count.eq(1).all()
