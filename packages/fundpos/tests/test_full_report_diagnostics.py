import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from fundpos.constants import ASSETS, SW_CODES
from fundpos.data import DataBundle


def script(name):
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def example():
    truth, predictions = [], []
    for code, shift in [("A", .1), ("B", -.1)]:
        weights = dict.fromkeys(SW_CODES, 0.0)
        weights.update({SW_CODES[0]: .4, SW_CODES[1]: .4})
        truth.append({"fund_code": code, "valuation_date": "2023-06-30",
                      **weights, "stock_true": .8, "hk_true": 0,
                      "label_ann_verified": True})
        weights[SW_CODES[0]] += shift
        weights[SW_CODES[1]] -= shift
        predictions.append({"fund_code": code, "valuation_date": "2023-06-30",
                            **weights, "cash": .2, "bond": 0, "hk": 0,
                            "stock_weight": .8, "status": "degraded", "aum": 1e9})
    return pd.DataFrame(predictions), pd.DataFrame(truth)


def test_twenty_point_error_is_not_relative_error_or_portfolio_bias():
    module = script("full_test_summary")
    predictions, labels = example()
    errors = module.matched_errors(predictions, labels)
    np.testing.assert_allclose(errors.industry_l1, .2)
    np.testing.assert_allclose(errors.per_industry_mae, .2/31)
    np.testing.assert_allclose(errors.equity_normalized_industry_l1, .25)
    assert errors.meets_20pp.all()
    assert module.grouped_bias(errors)["industry_l1"] == pytest.approx(0, abs=1e-12)


def test_missing_truth_and_duplicate_keys_cannot_improve_accuracy():
    module = script("full_test_summary")
    predictions, labels = example()
    labels.loc[0, SW_CODES[0]] = np.nan
    with pytest.raises(ValueError, match="Missing truth"):
        module.matched_errors(predictions, labels)
    with pytest.raises(ValueError, match="Duplicate"):
        module.matched_errors(pd.concat([predictions, predictions]), labels)


def test_turnover_proxy_excludes_same_report_future_and_stale_reports():
    module = script("full_test_summary")
    errors = pd.DataFrame({"fund_code": ["A", "B", "C"],
                           "valuation_date": ["2023-06-30"] * 3})
    proxy = pd.DataFrame({"fund_code": ["A", "A", "A", "B", "C"],
                          "report_date": ["2022-12-31", "2023-06-30", "2023-12-31",
                                          "2021-12-31", "2023-12-31"],
                          "turnover_proxy": [.8, 99, 100, 2, 3]})
    grouped = module.attach_turnover_proxy(errors, proxy).set_index("fund_code")
    assert grouped.loc["A", "turnover_proxy"] == .8
    assert grouped.loc[["B", "C"], "turnover_proxy"].isna().all()
    assert grouped.loc["A", "turnover_proxy_group"] == "1倍以内"


def test_nav_availability_sensitivity_never_releases_report_aum_early():
    module = script("full_report_test")
    nav = pd.DataFrame({"fund_code": ["A", "A"],
                        "date": ["2023-06-29", "2023-06-30"],
                        "ann_date": ["2023-06-30", "2023-07-20"],
                        "adj_nav": [1.0, 1.01], "net_asset": [np.nan, 1e9],
                        "total_netasset": [np.nan, 1e9]})
    holdings = pd.DataFrame({"fund_code": ["A"], "ann_date": ["2023-08-31"]})
    original = DataBundle({"nav": nav, "holdings": holdings})
    sensitivity = module.nav_sensitivity(original)
    assert sensitivity.frames["nav"].loc[1, "ann_date"] == pd.Timestamp("2023-07-01")
    assert sensitivity.frames["nav"].loc[1, ["net_asset", "total_netasset"]].isna().all()
    pd.testing.assert_frame_equal(original.frames["nav"], nav)
    pd.testing.assert_frame_equal(sensitivity.frames["holdings"], holdings)


def test_independent_labels_exclude_unmapped_stock_even_at_small_weight():
    module = script("full_report_test")
    u = pd.DataFrame({"fund_code": ["A"], "report_date": ["2023-06-30"],
                      "valuation_date": ["2023-06-30"]})
    frames = {
        "holdings": pd.DataFrame({"fund_code": ["A", "A"],
            "report_date": ["2023-06-30"]*2, "ann_date": ["2023-08-31"]*2,
            "security_code": ["600000.SH", "600001.SH"], "weight": [.7999, .0001]}),
        "asset_reports": pd.DataFrame({"fund_code": ["A"], "report_date": ["2023-06-30"],
            "ann_date": [None], "stock_weight": [.8], "aum": [1e9]}),
        "membership": pd.DataFrame({"security_code": ["600000.SH"],
            "industry": [SW_CODES[0]], "in_date": ["2021-01-01"], "out_date": [None]}),
    }
    labels, stocks, exclusions = module.independent_labels(DataBundle(frames), ["A"], u)
    assert labels.empty and len(stocks) == 1
    assert exclusions.reason.iloc[0] == "UNMAPPED_SW_LABEL"
    assert exclusions.missing_weight.iloc[0] == pytest.approx(.0001)


def test_market_price_partition_does_not_change_results(settings, bundle):
    from fundpos.pipeline import compute_date

    before = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    # Irrelevant market quotes must not enter a fund's factor basket or change its order.
    extra = bundle.frames["prices"].copy()
    extra["security_code"] = "UNRELATED"
    extra["adjusted_close"] = 1234.0
    bundle.frames["prices"] = pd.concat([extra, bundle.frames["prices"]]).sample(frac=1, random_state=1)
    after = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    np.testing.assert_allclose(before[list(ASSETS)], after[list(ASSETS)], atol=1e-12, rtol=0)
