import numpy as np
import pandas as pd
import pytest

from fundpos.constants import ASSETS, SW_CODES
from fundpos.errors import DataUnavailable
from fundpos.factors import personalized_panel
from fundpos.normalization import (
    aum_at_date,
    historical_membership_fallback,
    normalize_holdings,
    separate_nav_availability,
)
from fundpos.pit import available_nav, last_full_holdings, map_membership
from fundpos.validation import partial_industry_errors


def holdings_fixture():
    # 100 correctly rounded positions lose 0.4 percentage points in total.
    weights = np.repeat(.00004, 100)
    h = pd.DataFrame({"fund_code": "A", "report_date": pd.Timestamp("2023-12-31"),
        "ann_date": pd.Timestamp("2024-03-20"),
        "security_code": [f"{600000+i}.SH" for i in range(100)],
        "market_value": weights * 1e9, "weight": np.round(weights, 4)})
    a = pd.DataFrame({"fund_code": ["A"], "report_date": [pd.Timestamp("2023-12-31")],
        "aum": [1e9], "stock_market_value": [4e6], "stock_weight": [.004]})
    return h, a


def test_rounding_does_not_discard_complete_amounts_or_make_zero_positions():
    h, a = holdings_fixture()
    result = normalize_holdings(h, a)
    assert result.full_report_verified.all()
    assert result.weight_disclosed.sum() == 0
    assert result.weight.sum() == pytest.approx(.004)
    assert (result.weight > 0).all()
    pd.testing.assert_frame_equal(result, normalize_holdings(result, a))


@pytest.mark.parametrize("case", ["missing", "duplicate", "negative", "wrong_amount"])
def test_money_control_does_not_normalize_away_real_defects(case):
    h, a = holdings_fixture()
    if case == "missing":
        h = h.iloc[:-1]
    elif case == "duplicate":
        h = pd.concat([h, h.iloc[:1]])
    elif case == "negative":
        h.loc[0, "market_value"] = -1
    else:
        a.loc[0, "stock_market_value"] += 2e4
    result = normalize_holdings(h, a)
    assert not result.full_report_verified.any()
    with pytest.raises(DataUnavailable, match="NO_VERIFIED_FULL_HOLDINGS"):
        last_full_holdings(result, "A", "2024-04-01")


def financial_fixture():
    return pd.DataFrame({"fund_code": ["A", "C", "A", "C"],
        "report_date": pd.to_datetime(["2023-12-31"] * 2 + ["2024-03-31"] * 2),
        "ann_date": pd.to_datetime(["2024-01-20"] * 2 + ["2024-04-20"] * 2),
        "net_asset": [100., 25., 200., 50.]})


def test_weekend_report_aum_and_share_family_obey_real_publication_dates():
    f = financial_fixture()
    result = aum_at_date(pd.DataFrame(), f, ["A", "C"], "2024-03-29", "2024-03-30")
    assert result["aum"] == 125 and result["aum_date"] == "2023-12-31"
    f.loc[f.report_date == pd.Timestamp("2024-03-31"), "net_asset"] *= 100
    assert aum_at_date(pd.DataFrame(), f, ["A", "C"], "2024-03-29", "2024-03-30") == result
    assert aum_at_date(pd.DataFrame(), f.iloc[[0]], ["A", "C"],
                       "2024-03-29", "2024-03-30")["aum"] is None


def test_aum_does_not_mix_share_classes_from_different_report_dates():
    f = financial_fixture().iloc[[0, 1, 2]]
    result = aum_at_date(pd.DataFrame(), f, ["A", "C"], "2024-05-01", "2024-05-02")
    assert result["aum"] == 125 and result["aum_date"] == "2023-12-31"
    f = f.iloc[[1, 2]]
    assert aum_at_date(pd.DataFrame(), f, ["A", "C"], "2024-05-01", "2024-05-02")["aum"] is None


def test_nav_timing_is_independent_from_report_aum_timing():
    n = pd.DataFrame({"fund_code": ["A"], "date": ["2023-12-31"],
                      "ann_date": ["2024-01-20"], "net_asset": [100.]})
    n = separate_nav_availability(n)
    n["nav_available_at"] = pd.Timestamp("2024-01-01")
    assert len(available_nav(n, "2024-01-01")) == 1
    assert aum_at_date(n, pd.DataFrame(), ["A"], "2023-12-31", "2024-01-01")["aum"] is None


def test_future_conflicting_aum_row_cannot_change_earlier_result():
    f = financial_fixture().iloc[:2]
    before = aum_at_date(pd.DataFrame(), f, ["A", "C"], "2024-03-29", "2024-03-30")
    future = f.copy()
    future["ann_date"] = pd.Timestamp("2024-05-01")
    future["net_asset"] *= 2
    after = aum_at_date(pd.DataFrame(), pd.concat([f, future]), ["A", "C"],
                        "2024-03-29", "2024-03-30")
    assert before == after


def test_industry_gap_fill_never_backfills_first_membership_or_overrides_primary():
    h = pd.DataFrame({"security_code": ["600001.SH"], "weight": [.0001]})
    primary = pd.DataFrame({"security_code": ["600001.SH"], "industry": [SW_CODES[0]],
        "in_date": ["2022-01-01"], "out_date": ["2022-06-01"]})
    source = pd.DataFrame({"security_code": ["600001.SH", "600001.SH"],
        "in_date": ["2022-06-10", "2023-01-01"], "industry_l1": ["申万电子", "申万医药生物"]})
    fallback = historical_membership_fallback(source)
    assert map_membership(h, primary, "2022-05-01", fallback=fallback).industry.iloc[0] == SW_CODES[0]
    assert map_membership(h, primary, "2022-06-05", fallback=fallback,
                           allow_unknown=True).industry.iloc[0] == "unknown_a"
    assert map_membership(h, primary, "2022-06-30", fallback=fallback).industry.iloc[0] == "801080.SI"
    appended = pd.concat([source, pd.DataFrame({"security_code": ["600001.SH"],
        "in_date": ["2024-01-01"], "industry_l1": ["申万基础化工"]})])
    pd.testing.assert_frame_equal(map_membership(h, primary, "2022-06-30", fallback=fallback),
        map_membership(h, primary, "2022-06-30", fallback=historical_membership_fallback(appended)))


def test_unknown_positive_equity_stays_in_prior_and_proxy_diagnostics():
    idx = pd.bdate_range("2023-01-02", periods=4)
    base = pd.DataFrame(.001, index=idx[1:], columns=ASSETS)
    h = pd.DataFrame({"security_code": ["600000.SH", "600001.SH"],
                      "industry": [SW_CODES[0], "unknown_a"], "weight": [.799, .001]})
    prices = pd.DataFrame({"date": idx, "security_code": "600000.SH",
                           "adjusted_close": [10, 10.1, 10.2, 10.3]})
    _, diagnostics, prior = personalized_panel(base, h, prices, idx[0], cutoff=idx[-1])
    assert prior.sum() == pytest.approx(1)
    assert prior.cash == pytest.approx(.2)
    assert prior[SW_CODES[0]] == pytest.approx(.8)
    assert diagnostics["proxy_ratio"] == pytest.approx(.001/.8)


def test_partial_error_bounds_contain_all_nonnegative_unknown_allocations():
    rng = np.random.default_rng(401)
    known = rng.dirichlet(np.ones(31)) * .799
    estimate = rng.dirichlet(np.ones(31)) * .8
    labels = pd.DataFrame([{"fund_code": "A", "valuation_date": "2023-06-30",
                            **dict(zip(SW_CODES, known)), "unclassified_weight": .001}])
    prediction = pd.DataFrame([{"fund_code": "A", "valuation_date": "2023-06-30",
                                "status": "degraded", **dict(zip(SW_CODES, estimate))}])
    result = partial_industry_errors(prediction, labels).iloc[0]
    for allocation in np.r_[np.eye(31), rng.dirichlet(np.ones(31), 100)]:
        error = abs(estimate - known - .001 * allocation).sum()
        assert result.industry_l1_lower - 1e-12 <= error <= result.industry_l1_upper + 1e-12
    assert "industry_l1" not in result.index


def test_v2_independent_labels_use_money_and_separate_partial_truth(monkeypatch):
    from pathlib import Path

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    import recalculate_coverage

    from fundpos.data import DataBundle

    universe = pd.DataFrame({"fund_code": ["A"], "valuation_date": ["2023-06-30"],
                             "report_date": ["2023-06-30"]})
    h = pd.DataFrame({"fund_code": ["A", "A"], "report_date": ["2023-06-30"]*2,
        "ann_date": ["2023-08-31"]*2, "security_code": ["600000.SH", "600001.SH"],
        "market_value": [799100000., 900000.], "weight_disclosed": [.7991, .0009],
        "weight": [.4, .4], "full_report_verified": [False, False]})
    a = pd.DataFrame({"fund_code": ["A"], "report_date": ["2023-06-30"],
                      "ann_date": [None], "aum": [1e9], "stock_market_value": [8e8],
                      "stock_weight": [.8]})
    m = pd.DataFrame({"security_code": ["600000.SH"], "industry": [SW_CODES[0]],
                      "in_date": ["2022-01-01"], "out_date": [None]})
    bundle = DataBundle({"holdings": h, "asset_reports": a, "membership": m})
    full, stock, excluded, partial = recalculate_coverage.independent_labels(bundle, ["A"], universe)
    assert full.empty and len(stock) == 1 and len(partial) == 1
    assert excluded.reason.iloc[0] == "SMALL_UNCLASSIFIED_A_PARTIAL_LABEL"
    assert partial[SW_CODES[0]].iloc[0] == pytest.approx(.7991)
    assert partial.unclassified_weight.iloc[0] == pytest.approx(.0009)
    # A next-period classification is never pulled into the report-period label.
    bundle.frames["membership_fallback"] = pd.DataFrame({"security_code": ["600001.SH"],
        "industry": [SW_CODES[1]], "in_date": ["2023-07-01"], "out_date": [None]})
    again = recalculate_coverage.independent_labels(bundle, ["A"], universe)
    pd.testing.assert_frame_equal(partial, again[3])
