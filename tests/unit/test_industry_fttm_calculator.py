from __future__ import annotations

import pandas as pd
import pytest

from alphahome.pit.calculators.industry_fttm_calculator import (
    IndustryFTTMCalculator,
    IndustryQualityThreshold,
)


def _classification(dates=("2024-01-31", "2024-02-29")):
    rows = []
    for obs_date in dates:
        rows.extend(
            [
                {
                    "ts_code": "000001.SZ",
                    "obs_date": obs_date,
                    "data_source": "sw",
                    "industry_code1": "801010.SI",
                    "industry_level1": "一级A",
                    "industry_code2": "801011.SI",
                    "industry_level2": "二级A",
                },
                {
                    "ts_code": "000002.SZ",
                    "obs_date": obs_date,
                    "data_source": "sw",
                    "industry_code1": "801010.SI",
                    "industry_level1": "一级A",
                    "industry_code2": "801011.SI",
                    "industry_level2": "二级A",
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


def _weights(dates=("2024-01-31", "2024-02-29")):
    rows = []
    for obs_date in dates:
        rows.extend(
            [
                {
                    "obs_date": obs_date,
                    "ts_code": "000001.SZ",
                    "weight_trade_date": obs_date,
                    "total_mv": 100.0,
                },
                {
                    "obs_date": obs_date,
                    "ts_code": "000002.SZ",
                    "weight_trade_date": obs_date,
                    "total_mv": 300.0,
                },
            ]
        )
    return pd.DataFrame(rows)


def _fttm():
    frame = pd.DataFrame(
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
    frame["fy1_year"] = 2024
    frame["fy2_year"] = 2025
    frame["fy1_np_raw"] = frame["fttm_np"]
    frame["fy2_np_raw"] = frame["fttm_np"]
    frame["fy1_weight"] = 1.0
    frame["fy2_weight"] = 0.0
    return frame


def _calculator(min_stocks=2, min_orgs=2, min_match=2):
    thresholds = {
        level: IndustryQualityThreshold(
            min_covered_stocks=min_stocks,
            min_covered_mv_rate=0.60,
            min_weight_coverage_rate=0.98,
            min_org_count=min_orgs,
            min_matched_org_count=min_match,
        )
        for level in ("L1", "L2")
    }
    return IndustryFTTMCalculator(thresholds=thresholds)


def test_two_stage_weighting_l1_l2_and_equal_org_consensus():
    calculator = _calculator()
    result = calculator.calculate(
        _classification(), _stock_basic(), _weights(), _fttm()
    )

    january = result.loc[result.obs_date.eq(pd.Timestamp("2024-01-31"))]
    assert set(january.industry_level) == {"L1", "L2"}
    assert january.industry_fttm_np.tolist() == pytest.approx([23.75, 23.75])
    assert january.industry_fttm_np_median.tolist() == pytest.approx([23.75, 23.75])
    assert january.org_count.tolist() == [2, 2]
    assert january.covered_stock_count.tolist() == [2, 2]
    assert january.covered_mv_rate.tolist() == pytest.approx([1.0, 1.0])
    # A: (100*10 + 300*20)/400 = 17.5; B is normalized over its one stock = 30.
    l1_org = calculator.last_org_industry.loc[
        (calculator.last_org_industry.obs_date == pd.Timestamp("2024-01-31"))
        & (calculator.last_org_industry.industry_level == "L1")
    ].sort_values("org_name")
    assert sorted(l1_org.org_industry_fttm.tolist()) == pytest.approx([17.5, 30.0])


def test_diffusion_uses_immediate_previous_month_and_same_org_intersection():
    result = _calculator().calculate(
        _classification(), _stock_basic(), _weights(), _fttm()
    )
    february = result.loc[result.obs_date.eq(pd.Timestamp("2024-02-29"))]

    assert february.industry_fttm_np.tolist() == pytest.approx([22.5, 22.5])
    assert february.previous_industry_fttm_np.tolist() == pytest.approx([23.75, 23.75])
    assert february.matched_org_count.tolist() == [2, 2]
    assert february.up_org_count.tolist() == [1, 1]
    assert february.down_or_flat_org_count.tolist() == [1, 1]
    assert february.diffusion_up.tolist() == pytest.approx([0.5, 0.5])
    assert february.is_diffusion_eligible.all()
    assert february.revision_rate.tolist() == pytest.approx([0.03125, 0.03125])
    assert february.horizon_roll_rate.tolist() == pytest.approx([0.0, 0.0])
    assert february.revision_activity_rate.tolist() == pytest.approx([0.5, 0.5])
    assert february.revision_up_stock_rate.tolist() == pytest.approx([1.0, 1.0])
    assert february.is_revision_eligible.all()


def test_no_previous_match_is_null_not_neutral_half():
    one_month = _calculator(min_match=1).calculate(
        _classification(("2024-01-31",)),
        _stock_basic(),
        _weights(("2024-01-31",)),
        _fttm().loc[lambda value: value.obs_date.eq("2024-01-31")],
    )

    assert one_month.matched_org_count.eq(0).all()
    assert one_month.diffusion_up.isna().all()
    assert one_month.quality_reasons.map(
        lambda reasons: "diffusion_ineligible_low_match" in reasons
    ).all()


def test_delisted_stock_is_excluded_from_active_denominator_after_delist():
    result = _calculator(min_stocks=1).calculate(
        _classification(),
        _stock_basic(delist_second="2024-02-01"),
        _weights(),
        _fttm(),
    )
    february = result.loc[result.obs_date.eq(pd.Timestamp("2024-02-29"))]

    assert february.structural_member_count.eq(2).all()
    assert february.active_member_count.eq(1).all()
    assert february.covered_stock_count.eq(1).all()


def test_low_coverage_retains_structural_rows_and_marks_ineligible():
    fttm = _fttm().loc[lambda value: value.ts_code.eq("000001.SZ")]
    result = _calculator(min_stocks=2).calculate(
        _classification(("2024-01-31",)),
        _stock_basic(),
        _weights(("2024-01-31",)),
        fttm.loc[lambda value: value.obs_date.eq("2024-01-31")],
    )

    assert len(result) == 2
    assert not result.is_eligible.any()
    assert result.quality_reasons.map(
        lambda reasons: "ineligible_low_stock_coverage" in reasons
    ).all()


def test_weight_older_than_31_days_is_not_available():
    stale = _weights(("2024-02-29",))
    stale["weight_trade_date"] = "2024-01-01"
    result = _calculator(min_stocks=1).calculate(
        _classification(("2024-02-29",)),
        _stock_basic(),
        stale,
        _fttm().loc[lambda value: value.obs_date.eq("2024-02-29")],
    )

    assert result.weight_available_count.eq(0).all()
    assert result.industry_fttm_np.isna().all()
    assert result.quality_reasons.map(
        lambda reasons: "ineligible_low_weight_coverage" in reasons
    ).all()
