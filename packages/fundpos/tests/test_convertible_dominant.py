import numpy as np
import pandas as pd
import pytest

from fundpos.constants import FIXED_INCOME_ASSETS, FIXED_INCOME_FACTOR_COLUMNS, SW_CODES
from fundpos.convertible_dominant import (
    CBOND_STYLE_COLUMNS,
    convertible_dominant_constraints,
    convertible_rotation_mix,
    convertible_style_factor_panel,
    convertible_total_return_levels,
    disclosed_convertible_style_prior,
    disclosure_anchored_stock_constraints,
    estimate_convertible_nav_drift_sparse_trade,
    estimate_convertible_state_space,
    estimate_convertible_style_endpoint,
    latest_published_convertible_holdings,
    mark_to_market_convertible_weight,
    personalized_convertible_panel,
)
from fundpos.convertible_dominant_validation import (
    ConvertibleDominantValidationState,
    convertible_dominant_assessment,
    historical_contract_sample_gaps,
)
from fundpos.errors import DataUnavailable, ProtocolError
from fundpos.fixed_income import FixedIncomeConstraints, estimate_financing_scenarios
from fundpos.storage import atomic_json, code_fingerprint


def test_historical_contract_preflight_requires_every_fund_and_date():
    constraints = pd.DataFrame(
        {
            "fund_code": ["F1", "F2", "F3"],
            "ann_date": pd.to_datetime(
                ["2021-01-01", "2024-01-01", "2021-01-01"]
            ),
            "effective_date": pd.to_datetime(
                ["2021-01-01", "2021-01-01", "2021-01-01"]
            ),
            "verified": [True, True, True],
            "stock_denominator": ["fund_nav", "fund_nav", "fund_assets"],
        }
    )
    samples = pd.DataFrame(
        {
            "fund_code": ["F1", "F2", "F3", "F4"],
            "valuation_date": pd.to_datetime(["2022-12-31"] * 4),
        }
    )
    gaps = historical_contract_sample_gaps(constraints, samples)
    assert set(gaps.fund_code) == {"F2", "F4"}


def test_historical_contract_preflight_keeps_conditional_rows_out_of_strict_gate():
    constraints = pd.DataFrame(
        {
            "fund_code": ["F1"],
            "ann_date": pd.to_datetime(["2021-01-01"]),
            "effective_date": pd.to_datetime(["2021-01-01"]),
            "verified": [True],
            "formal_verified": [False],
        }
    )
    samples = pd.DataFrame(
        {"fund_code": ["F1"], "valuation_date": pd.to_datetime(["2022-12-31"])}
    )
    assert len(historical_contract_sample_gaps(constraints, samples)) == 1
    assert historical_contract_sample_gaps(
        constraints, samples, allow_conditional=True
    ).empty


def test_convertible_return_level_uses_adjusted_preclose_return():
    raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-12-30", "2024-12-31"]),
            "security_code": ["B", "B"],
            "pre_close": [100.0, 101.0],
            "close": [103.0, 102.01],
            "pct_chg": [3.0, 1.0],
        }
    )
    result = convertible_total_return_levels(raw)
    assert result.adjusted_close.iloc[1] / result.adjusted_close.iloc[0] - 1 == pytest.approx(
        0.01
    )
    assert result.evidence_status.eq("conditional_security_total_return_proxy").all()


def test_convertible_return_level_rejects_another_return_definition():
    raw = pd.DataFrame(
        {
            "date": ["2024-12-30"],
            "security_code": ["B"],
            "pre_close": [100.0],
            "close": [102.0],
            "pct_chg": [1.0],
        }
    )
    with pytest.raises(DataUnavailable, match="CBOND_RETURN_FORMULA_MISMATCH"):
        convertible_total_return_levels(raw)


def test_convertible_return_level_reconstructs_reference_cashflow_ablation():
    raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-12-30", "2024-12-31"]),
            "security_code": ["B", "B"],
            "pre_close": [100.0, 101.0],
            "close": [103.0, 102.01],
            "pct_chg": [3.0, 1.0],
        }
    )
    result = convertible_total_return_levels(
        raw, return_mode="reference_cashflow_reconstructed"
    )
    expected = (102.01 + 2.0) / 103.0 - 1
    assert result["return"].iloc[1] == pytest.approx(expected)
    assert result.adjusted_close.iloc[1] / result.adjusted_close.iloc[0] - 1 == pytest.approx(
        expected
    )
    assert result.evidence_status.eq(
        "conditional_reference_cashflow_reconstruction"
    ).all()


def test_convertible_return_level_keeps_vendor_return_for_reference_noise():
    raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-12-30", "2024-12-31"]),
            "security_code": ["B", "B"],
            "pre_close": [100.0, 102.999],
            "close": [103.0, 104.029],
            "pct_chg": [3.0, 1.0],
        }
    )
    result = convertible_total_return_levels(
        raw, return_mode="reference_cashflow_reconstructed"
    )
    assert result["return"].iloc[1] == pytest.approx(0.01)


def test_convertible_style_factor_uses_previous_observation_classification():
    index = pd.bdate_range("2024-01-02", periods=4)
    panel = pd.DataFrame({"convertible_bond": [0.01] * 4}, index=index)
    prices = pd.DataFrame(
        {
            "date": np.repeat(index, 3),
            "ann_date": np.repeat(index, 3),
            "security_code": ["B1", "B2", "B3"] * 4,
            "return": [0.0, 0.0, 0.0, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10],
            # B1 moves from equity-like to bond-like on day two. Its day-two
            # return must still use the day-one category.
            "cb_over_rate": [10.0, 30.0, 60.0, 60.0, 30.0, 60.0, 60.0, 30.0, 60.0, 60.0, 30.0, 60.0],
        }
    )
    result, meta = convertible_style_factor_panel(
        panel, prices, index[-1], minimum_constituents=1
    )
    assert result.loc[index[1], CBOND_STYLE_COLUMNS[0]] == pytest.approx(0.02)
    assert result.loc[index[2], CBOND_STYLE_COLUMNS[2]] == pytest.approx((0.05 + 0.07) / 2)
    assert meta["cbond_style_lookahead_control"] == "previous_security_observation"


def test_disclosed_convertible_style_prior_reconciles_control_and_proxy():
    holdings = pd.DataFrame(
        {
            "security_code": ["B1", "B2", "B3"],
            "weight": [0.20, 0.25, 0.10],
        }
    )
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-12-29", "2023-12-29"]),
            "ann_date": pd.to_datetime(["2023-12-29", "2023-12-29"]),
            "security_code": ["B1", "B2"],
            "cb_over_rate": [10.0, 60.0],
        }
    )
    prior, meta = disclosed_convertible_style_prior(
        holdings,
        prices,
        "2023-12-31",
        0.80,
        "2024-01-02",
    )
    assert prior[CBOND_STYLE_COLUMNS[0]] == pytest.approx(0.20)
    assert prior[CBOND_STYLE_COLUMNS[2]] == pytest.approx(0.25)
    # Missing B3 plus the undisclosed 25% residual are explicit proxies.
    assert prior[CBOND_STYLE_COLUMNS[1]] == pytest.approx(0.35)
    assert prior.sum() == pytest.approx(0.80)
    assert meta["cbond_style_prior_proxy_weight"] == pytest.approx(0.35)


def test_disclosed_convertible_style_prior_accepts_display_rounding_gap():
    holdings = pd.DataFrame(
        {"security_code": ["B1", "B2"], "weight": [0.4002, 0.4001]}
    )
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-12-29", "2023-12-29"]),
            "ann_date": pd.to_datetime(["2023-12-29", "2023-12-29"]),
            "security_code": ["B1", "B2"],
            "cb_over_rate": [10.0, 60.0],
        }
    )
    prior, meta = disclosed_convertible_style_prior(
        holdings, prices, "2023-12-31", 0.80, "2024-01-02"
    )
    assert prior.sum() == pytest.approx(0.80)
    assert meta["cbond_style_prior_reconciliation_adjustment"] == pytest.approx(
        -0.0003
    )


def test_disclosed_convertible_style_prior_rejects_material_over_control():
    holdings = pd.DataFrame(
        {"security_code": ["B1", "B2"], "weight": [0.45, 0.40]}
    )
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-12-29", "2023-12-29"]),
            "ann_date": pd.to_datetime(["2023-12-29", "2023-12-29"]),
            "security_code": ["B1", "B2"],
            "cb_over_rate": [10.0, 60.0],
        }
    )
    with pytest.raises(DataUnavailable, match="CBOND_STYLE_PRIOR_RECONCILIATION"):
        disclosed_convertible_style_prior(
            holdings, prices, "2023-12-31", 0.80, "2024-01-02"
        )


def test_convertible_dominant_bound_allows_cbond_above_nav():
    result = convertible_dominant_constraints(
        FixedIncomeConstraints(cbond_upper=1.0, gross_assets_upper=1.4), 1.4
    )
    assert result.cbond_upper == pytest.approx(1.4)
    assert "cbond_up_to_gross_assets" in result.source


def test_latest_convertible_holdings_keep_unreported_residual():
    reports = pd.DataFrame(
        {
            "fund_code": ["F", "F"],
            "report_date": ["2023-12-31", "2024-03-31"],
            "ann_date": ["2024-03-20", "2024-04-20"],
            "convertible_bond_weight": [1.2, 1.1],
        }
    )
    holdings = pd.DataFrame(
        {
            "fund_code": ["F", "F", "F"],
            "report_date": ["2023-12-31", "2023-12-31", "2024-03-31"],
            "ann_date": ["2024-03-20", "2024-03-20", "2024-04-20"],
            "security_code": ["B1", "B2", "B3"],
            "weight": [0.5, 0.4, 1.0],
        }
    )
    result, meta = latest_published_convertible_holdings(
        holdings, reports, "F", "2024-03-31", "2024-04-01"
    )
    assert set(result.security_code) == {"B1", "B2"}
    assert result.weight.sum() == pytest.approx(0.9)
    assert meta["cbond_control_weight"] == pytest.approx(1.2)
    assert meta["cbond_disclosure_coverage"] == pytest.approx(0.75)


def test_stock_total_anchor_uses_only_the_latest_public_report():
    reports = pd.DataFrame(
        {
            "fund_code": ["F", "F"],
            "report_date": ["2023-12-31", "2024-03-31"],
            "ann_date": ["2024-03-20", "2024-04-20"],
            "stock_weight": [0.03, 0.20],
        }
    )
    result, meta = disclosure_anchored_stock_constraints(
        FixedIncomeConstraints(stock_upper=0.4),
        reports,
        "F",
        "2024-03-31",
        "2024-04-01",
    )
    assert result.stock_lower == result.stock_upper == pytest.approx(0.03)
    assert meta["stock_anchor_report_date"] == "2023-12-31"
    assert meta["stock_anchor_age_days"] == 91


def test_personalized_convertible_factor_mixes_priced_and_broad_residual():
    index = pd.bdate_range("2024-01-02", periods=4)
    panel = pd.DataFrame({"convertible_bond": [0.0, 0.01, 0.01, 0.01]}, index=index)
    holdings = pd.DataFrame(
        {"security_code": ["B1", "B2"], "weight": [0.6, 0.2]}
    )
    prices = pd.DataFrame(
        {
            "date": index,
            "ann_date": index,
            "security_code": ["B1"] * 4,
            "adjusted_close": [1.0, 1.10, 1.10, 1.10],
        }
    )
    result, meta = personalized_convertible_panel(
        panel, holdings, prices, index[0], 1.0, index[-1]
    )
    # 60% priced B1 rises 10%; the remaining 40% receives the 1% broad return.
    assert result.loc[index[1], "convertible_bond"] == pytest.approx(0.064)
    assert meta["cbond_priced_coverage"] == pytest.approx(0.6)
    assert meta["cbond_proxy_ratio"] == pytest.approx(0.4)
    assert meta["cbond_unusable_security_count"] == 1


def test_convertible_mark_to_market_uses_fund_nav_denominator():
    nav = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-03-29", "2024-06-28"]),
            "ann_date": pd.to_datetime(["2024-03-30", "2024-06-29"]),
            "adj_nav": [1.0, 1.05],
        }
    )
    weight, meta = mark_to_market_convertible_weight(
        nav,
        control_weight=0.80,
        anchor_date="2024-03-31",
        endpoint_date="2024-06-28",
        factor_growth=1.10,
        cutoff="2024-06-29",
    )
    assert weight == pytest.approx(0.80 * 1.10 / 1.05)
    assert meta["cbond_mark_nav_anchor_date"] == "2024-03-29"


def test_convertible_state_space_moves_toward_a_changed_endpoint():
    rng = np.random.default_rng(20260914)
    index = pd.bdate_range("2024-01-02", periods=60)
    x = pd.DataFrame(
        rng.normal(0, 0.006, (60, len(FIXED_INCOME_FACTOR_COLUMNS))),
        columns=FIXED_INCOME_FACTOR_COLUMNS,
        index=index,
    )
    x["cash"] = 0.0001
    x["financing_cost"] = 0.00008
    path = np.r_[np.repeat(0.4, 30), np.linspace(0.4, 0.9, 30)]
    y = pd.Series(
        path * x.convertible_bond.to_numpy()
        + (1 - path) * x.cash.to_numpy(),
        index=index,
    )
    replacement = pd.Series(0.0, index=FIXED_INCOME_ASSETS)
    replacement.cash = 1.0
    result, diagnostics = estimate_convertible_state_space(
        x,
        y,
        FixedIncomeConstraints(cbond_upper=1.4, gross_assets_upper=1.4),
        cbond_anchor=0.4,
        anchor_date=index[0],
        replacement_weights=replacement,
        state_penalty=0.1,
    )
    assert result.convertible_bond > 0.7
    assert diagnostics["cbond_state_start"] == pytest.approx(0.4, abs=0.03)
    assert diagnostics["constraint_error"] <= 1e-6


def test_convertible_state_space_honors_fixed_equity_structure():
    rng = np.random.default_rng(20260913)
    index = pd.bdate_range("2024-01-02", periods=60)
    x = pd.DataFrame(
        rng.normal(0, 0.006, (60, len(FIXED_INCOME_FACTOR_COLUMNS))),
        columns=FIXED_INCOME_FACTOR_COLUMNS,
        index=index,
    )
    x["cash"] = 0.0001
    x["financing_cost"] = 0.00008
    fixed = pd.Series(np.nan, index=FIXED_INCOME_ASSETS)
    fixed[["hk", *SW_CODES]] = 0.0
    fixed["801010.SI"] = 0.12
    fixed["801030.SI"] = 0.08
    path = np.linspace(0.55, 0.75, len(index))
    y = pd.Series(
        path * x.convertible_bond.to_numpy()
        + 0.20 * (
            0.60 * x["801010.SI"].to_numpy()
            + 0.40 * x["801030.SI"].to_numpy()
        )
        + (0.80 - path) * x.cash.to_numpy(),
        index=index,
    )
    replacement = pd.Series(0.0, index=FIXED_INCOME_ASSETS)
    replacement.cash = 1.0
    result, diagnostics = estimate_convertible_state_space(
        x,
        y,
        FixedIncomeConstraints(
            stock_lower=0.20,
            stock_upper=0.20,
            cbond_upper=1.4,
            financing_upper=0.0,
            gross_assets_upper=1.4,
        ),
        cbond_anchor=0.55,
        anchor_date=index[0],
        replacement_weights=replacement,
        state_penalty=0.1,
        fixed_weights=fixed,
    )
    assert result["801010.SI"] == pytest.approx(0.12, abs=1e-6)
    assert result["801030.SI"] == pytest.approx(0.08, abs=1e-6)
    assert result.convertible_bond == pytest.approx(0.75, abs=0.03)
    assert diagnostics["fixed_asset_coefficients"] == 32
    assert diagnostics["constraint_error"] <= 1e-6


def test_nav_drift_sparse_trade_separates_mechanical_drift_and_jump():
    rng = np.random.default_rng(20260915)
    index = pd.bdate_range("2024-01-02", periods=60)
    x = pd.DataFrame(
        rng.normal(0, 0.004, (60, len(FIXED_INCOME_FACTOR_COLUMNS))),
        columns=FIXED_INCOME_FACTOR_COLUMNS,
        index=index,
    )
    x["cash"] = 0.0001
    x["convertible_bond"] = rng.normal(0.0004, 0.01, len(x))
    x["financing_cost"] = 0.00008
    true_path = np.empty(len(x))
    true_path[0] = 0.4
    y = np.empty(len(x))
    for position in range(len(x)):
        y[position] = (
            true_path[position] * x.convertible_bond.iloc[position]
            + (1 - true_path[position]) * x.cash.iloc[position]
        )
        if position + 1 < len(x):
            drifted = true_path[position] * (
                (1 + x.convertible_bond.iloc[position]) / (1 + y[position])
            )
            true_path[position + 1] = drifted + (0.30 if position == 34 else 0.0)
    true_endpoint = true_path[-1] * (
        (1 + x.convertible_bond.iloc[-1]) / (1 + y[-1])
    )
    replacement = pd.Series(0.0, index=FIXED_INCOME_ASSETS)
    replacement.cash = 1.0
    fixed = pd.Series(0.0, index=FIXED_INCOME_ASSETS)
    fixed[["cash", "convertible_bond"]] = np.nan
    result, diagnostics = estimate_convertible_nav_drift_sparse_trade(
        x,
        pd.Series(y, index=index),
        FixedIncomeConstraints(
            cbond_upper=1.4,
            financing_upper=0.0,
            gross_assets_upper=1.4,
        ),
        cbond_anchor=0.4,
        anchor_date=index[0] - pd.offsets.BDay(1),
        replacement_weights=replacement,
        trade_penalty=0.03,
        anchor_penalty=10.0,
        fixed_weights=fixed,
    )
    assert result.convertible_bond == pytest.approx(true_endpoint, abs=0.02)
    assert diagnostics["cbond_active_change_count_1pp"] == 1
    assert diagnostics["cbond_active_change_max"] == pytest.approx(0.30, abs=0.03)
    assert diagnostics["cbond_natural_drift_total"] > 0
    assert diagnostics["constraint_error"] <= 1e-6


def test_convertible_style_endpoint_recovers_total_and_internal_mix():
    rng = np.random.default_rng(20260916)
    index = pd.bdate_range("2024-01-02", periods=60)
    columns = [*FIXED_INCOME_FACTOR_COLUMNS, *CBOND_STYLE_COLUMNS]
    x = pd.DataFrame(
        rng.normal(0, 0.006, (len(index), len(columns))),
        columns=columns,
        index=index,
    )
    x["cash"] = 0.0001
    x["financing_cost"] = 0.00008
    anchor = np.array([0.10, 0.30, 0.20])
    endpoint = np.array([0.40, 0.15, 0.05])
    elapsed = (index[-1] - index[0]).days
    progress = ((index - index[0]).days / elapsed).to_numpy(float)
    path = (1 - progress[:, None]) * anchor + progress[:, None] * endpoint
    y = pd.Series(
        (path * x[list(CBOND_STYLE_COLUMNS)].to_numpy()).sum(axis=1)
        + 0.40 * x.cash.to_numpy(),
        index=index,
    )
    replacement = pd.Series(0.0, index=FIXED_INCOME_ASSETS)
    replacement.cash = 1.0
    fixed = pd.Series(0.0, index=FIXED_INCOME_ASSETS)
    fixed[["cash", "convertible_bond"]] = np.nan
    result, diagnostics = estimate_convertible_style_endpoint(
        x,
        y,
        FixedIncomeConstraints(
            cbond_upper=1.4,
            financing_upper=0.0,
            gross_assets_upper=1.4,
        ),
        cbond_anchor=float(anchor.sum()),
        anchor_date=index[0],
        replacement_weights=replacement,
        cbond_style_prior=pd.Series(anchor, index=CBOND_STYLE_COLUMNS),
        fixed_weights=fixed,
    )
    recovered = np.array(
        [diagnostics[f"cbond_style_{column.removeprefix('cbond_')}"] for column in CBOND_STYLE_COLUMNS]
    )
    np.testing.assert_allclose(recovered, endpoint, atol=1e-5, rtol=0)
    assert result.convertible_bond == pytest.approx(endpoint.sum(), abs=1e-5)
    assert diagnostics["constraint_error"] <= 1e-6


def test_convertible_rotation_mix_excludes_equity_and_convertibles():
    prior = pd.Series(np.nan, index=[*FIXED_INCOME_ASSETS, "financing"])
    prior.convertible_bond = 0.8
    prior["801010.SI"] = 0.1
    prior[["rate_short", "rate_long", "credit_short", "credit_long"]] = 0.02
    prior[[code for code in FIXED_INCOME_ASSETS if code.startswith("801")][1:]] = 0.0
    prior.hk = 0.0
    mix, meta = convertible_rotation_mix(prior, balance_sheet_leverage=0.0)
    assert mix.sum() == pytest.approx(1.0)
    assert mix[["convertible_bond", "hk", "801010.SI"]].sum() == 0
    assert meta["cbond_rotation_ordinary_share"] > 0


def test_convertible_free_financing_scenario_recovers_leveraged_cbond():
    rng = np.random.default_rng(20260913)
    x = pd.DataFrame(
        rng.normal(0, 0.005, (100, len(FIXED_INCOME_FACTOR_COLUMNS))),
        columns=FIXED_INCOME_FACTOR_COLUMNS,
    )
    x["cash"] = 0.00005
    x["financing_cost"] = 0.00008
    assets = np.zeros(len(FIXED_INCOME_ASSETS))
    assets[FIXED_INCOME_ASSETS.index("convertible_bond")] = 1.2
    y = pd.Series(x.iloc[:, :-1].to_numpy() @ assets - x.financing_cost.to_numpy() * 0.2)
    bounds = FixedIncomeConstraints(
        cbond_upper=1.4, financing_upper=0.4, gross_assets_upper=1.4
    )
    result, diagnostics, scenarios = estimate_financing_scenarios(
        x,
        y,
        bounds,
        disclosed_financing=None,
        contract_financing_upper=0.4,
        include_free_financing=True,
        prefer_free_financing=True,
    )
    assert diagnostics["primary_scenario"] == "estimated_financing"
    assert set(scenarios.scenario) == {
        "no_financing",
        "estimated_financing",
        "contract_upper",
    }
    assert result.convertible_bond == pytest.approx(1.2, abs=1e-5)
    assert result.financing == pytest.approx(0.2, abs=1e-5)


def test_convertible_assessment_selects_cbond_accuracy_first():
    base = {
        "status": "evaluated",
        "stock_mae": 0.04,
        "cbond_mae": 0.12,
        "stock_p90": 0.09,
        "cbond_p90": 0.20,
        "industry_l1_mean": 0.10,
        "industry_l1_p90": 0.20,
        "hk_mae": 0.01,
        "full_asset_label_coverage": 1.0,
    }
    personalized = base | {
        "stock_mae": 0.03,
        "cbond_mae": 0.04,
        "stock_p90": 0.07,
        "cbond_p90": 0.08,
        "industry_l1_mean": 0.15,
        "industry_l1_p90": 0.25,
        "hk_mae": 0.02,
    }
    prior = personalized | {
        "cbond_mae": 0.03,
        "cbond_p90": 0.07,
        "industry_l1_mean": 0.11,
        "industry_l1_p90": 0.20,
        "hk_mae": 0.01,
    }
    result = convertible_dominant_assessment(
        {"index": base, "personalized": personalized, "prior1": prior},
        {
            "stock_mae_max": 0.05,
            "convertible_bond_mae_max": 0.05,
            "industry_l1_reference": 0.20,
            "industry_relative_improvement": 0.10,
            "dedicated_cbond_relative_improvement": 0.10,
            "coverage_min": 0.95,
        },
        candidate_names=["prior1"],
        baseline_names=["personalized"],
    )
    assert result["candidate"] == "prior1"
    assert result["all_development_gates_pass"]
    assert result["references"]["is_acceptance_gate"] is False


def test_convertible_industry_reference_does_not_become_a_single_fund_gate():
    baseline = {
        "status": "evaluated",
        "stock_mae": 0.04,
        "cbond_mae": 0.08,
        "stock_p90": 0.10,
        "cbond_p90": 0.16,
        "industry_l1_mean": 0.30,
        "industry_l1_p90": 0.50,
        "hk_mae": 0.01,
        "full_asset_label_coverage": 1.0,
    }
    candidate = baseline | {
        "stock_mae": 0.03,
        "cbond_mae": 0.04,
        "stock_p90": 0.08,
        "cbond_p90": 0.10,
        "industry_l1_mean": 0.24,
        "industry_l1_p90": 0.40,
    }
    result = convertible_dominant_assessment(
        {"personalized": baseline, "candidate": candidate},
        {
            "stock_mae_max": 0.05,
            "convertible_bond_mae_max": 0.05,
            "group_industry_l1_reference": 0.20,
            "industry_relative_improvement": 0.10,
            "dedicated_cbond_relative_improvement": 0.10,
            "coverage_min": 0.95,
        },
        candidate_names=["candidate"],
        baseline_names=["personalized"],
    )
    assert result["references"]["single_fund_mean_below_reference"] is False
    assert result["all_development_gates_pass"]


def test_convertible_final_remains_locked_after_conditional_development(settings):
    state = ConvertibleDominantValidationState(settings)
    atomic_json(
        state.path,
        {
            "protocol_hash": state.protocol_hash,
            "code_hash": code_fingerprint(settings.root),
            "research_candidate": "personalized",
            "development_gates": {"all_development_gates_pass": True},
            "evidence_scope": "conditional",
        },
    )
    with pytest.raises(ProtocolError, match="Freeze the strict"):
        state.assert_open_allowed("final")
    with pytest.raises(ProtocolError, match="no clean selection window"):
        state.assert_open_allowed("selection")
