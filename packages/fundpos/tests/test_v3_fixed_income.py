import copy

import numpy as np
import pandas as pd
import pytest

from fundpos.aggregation import aggregate_exposures
from fundpos.constants import FIXED_INCOME_ASSETS, FIXED_INCOME_FACTOR_COLUMNS, SW_CODES
from fundpos.data import (
    DataBundle,
    canonicalize_convertible_holding_codes,
    financing_cost_returns,
)
from fundpos.enhanced_index import (
    evaluate_tracking_index_prior,
    tracking_index_industry_prior,
)
from fundpos.errors import DataUnavailable, ProtocolError
from fundpos.fixed_income import (
    FixedIncomeConstraints,
    estimate_financing_scenarios,
    estimate_fixed_income,
    fixed_income_constraint_error,
)
from fundpos.fixed_income_data import (
    apply_report_evidence,
    classify_bond_disclosures,
    disclosed_balance_sheet_leverage,
    disclosed_financing,
    recover_asset_control_announcements,
    recover_report_announcements,
)
from fundpos.fixed_income_pipeline import (
    _market_cap_equity_proxy,
    _prior_from_disclosure,
    fixed_income_constraints_from_row,
    fixed_income_panel,
)
from fundpos.fixed_income_validation import (
    MAIN_VALIDATION_GROUP,
    FixedIncomeValidationState,
    factor_registry_gaps,
    fixed_income_selection_assessment,
    fixed_income_validation_group,
)
from fundpos.normalization import conditional_next_day_nav_availability
from fundpos.report_evidence import _challenge_cookie, report_period_from_title
from fundpos.storage import atomic_json, code_fingerprint
from fundpos.universe import fixed_income_plus_universe, select_fixed_count


def fi_matrix(seed=3):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(rng.normal(0, 0.008, (60, len(FIXED_INCOME_FACTOR_COLUMNS))),
                        columns=FIXED_INCOME_FACTOR_COLUMNS)


def test_conditional_nav_scenario_preserves_vendor_and_aum_dates():
    nav = pd.DataFrame(
        {
            "fund_code": ["F"],
            "date": pd.to_datetime(["2023-03-31"]),
            "ann_date": pd.to_datetime(["2023-04-21"]),
            "adj_nav": [1.0],
        }
    )
    result = conditional_next_day_nav_availability(nav)
    assert result.ann_date.iloc[0] == pd.Timestamp("2023-04-21")
    assert result.aum_available_at.iloc[0] == pd.Timestamp("2023-04-21")
    assert result.nav_available_at.iloc[0] == pd.Timestamp("2023-04-01")
    assert bool(result.nav_availability_conditional.iloc[0])


def test_fixed_income_comparison_cohorts_do_not_enter_main_acceptance_group():
    assert fixed_income_validation_group(
        contract_pool=False,
        style_pool=True,
        comparison_group="convertible_dominant",
    ) == "convertible_dominant"
    assert fixed_income_validation_group(
        contract_pool=False,
        style_pool=True,
        comparison_group="pure_bond_control",
    ) == "pure_bond_control"
    assert fixed_income_validation_group(
        contract_pool=False,
        style_pool=True,
        comparison_group=None,
    ) == MAIN_VALIDATION_GROUP


def test_fixed_income_selection_assessment_applies_absolute_and_relative_gates():
    base = {
        "industry_l1_mean": 0.16,
        "industry_l1_p90": 0.25,
        "stock_mae": 0.047,
        "cbond_mae": 0.25,
        "stock_p90": 0.10,
        "cbond_p90": 0.80,
        "hk_mae": 0.003,
        "full_asset_label_coverage": 0.99,
    }
    candidate = dict(
        base,
        industry_l1_mean=0.148,
        industry_l1_p90=0.22,
        stock_mae=0.043,
        cbond_mae=0.059,
        stock_p90=0.09,
        cbond_p90=0.13,
        hk_mae=0.002,
    )
    result = fixed_income_selection_assessment(
        {"index": base, "personalized": base, "candidate": candidate},
        {
            "stock_mae_max": 0.05,
            "convertible_bond_mae_max": 0.05,
            "industry_l1_relative_improvement": 0.10,
        },
    )
    assert result["candidate"] == "candidate"
    assert result["tests"]["stock_mae_absolute"]
    assert not result["tests"]["cbond_mae_absolute"]
    assert not result["tests"]["industry_improvement"]
    assert not result["all_selection_gates_pass"]


def test_fixed_income_final_rejects_failed_selection_gates(settings):
    state = FixedIncomeValidationState(settings)
    atomic_json(
        state.path,
        {
            "selection_frozen": "prior1_smooth1",
            "protocol_hash": state.protocol_hash,
            "code_hash": code_fingerprint(settings.root),
            "selection_gates": {"all_selection_gates_pass": False},
        },
    )
    with pytest.raises(ProtocolError, match="gates"):
        state.assert_open_allowed("final")


def test_no_holdings_proxy_uses_historical_sw_market_weights():
    weights = pd.DataFrame(
        {
            "date": [pd.Timestamp("2022-12-30")] * len(SW_CODES),
            "asset": SW_CODES,
            "weight": np.repeat(1 / len(SW_CODES), len(SW_CODES)),
        }
    )
    reports = pd.DataFrame(
        {
            "fund_code": ["F"],
            "report_date": pd.to_datetime(["2022-12-31"]),
            "ann_date": pd.to_datetime(["2023-01-20"]),
            "stock_weight": [0.2],
        }
    )
    prior, metadata = _market_cap_equity_proxy(
        DataBundle({"asset_reports": reports, "industry_market_weights": weights}),
        "F",
        "2023-03-31",
        "2023-04-01",
    )
    assert prior[list(SW_CODES)].sum() == pytest.approx(0.2)
    assert prior.hk == 0
    assert metadata["proxy_ratio"] == pytest.approx(0.2)
    assert metadata["holdings_proxy_used"] is True


def test_fixed_income_prior_preserves_known_components_when_bond_total_is_unknown():
    report = pd.DataFrame(
        {
            "fund_code": ["F"],
            "report_date": pd.to_datetime(["2022-12-31"]),
            "ann_date": pd.to_datetime(["2023-01-20"]),
            "ordinary_bond_weight": [np.nan],
            "convertible_bond_weight": [0.1],
        }
    )
    equity = pd.Series(0.0, index=("cash", "bond", "hk", *SW_CODES))
    equity["801010.SI"] = 0.2
    prior = _prior_from_disclosure(
        DataBundle({"asset_reports": report}),
        "F",
        equity,
        "2023-03-31",
        "2023-04-01",
    )
    assert prior.convertible_bond == pytest.approx(0.1)
    assert prior["801010.SI"] == pytest.approx(0.2)
    assert prior[["rate_short", "rate_long", "credit_short", "credit_long"]].isna().all()
    assert pd.isna(prior.cash)


def test_fixed_income_solver_recovers_assets_and_financing():
    x = fi_matrix()
    assets = np.random.default_rng(4).dirichlet(np.ones(len(FIXED_INCOME_ASSETS))) * 1.10
    financing = 0.10
    true = pd.Series(np.r_[assets, financing], index=[*FIXED_INCOME_ASSETS, "financing"])
    y = x.iloc[:, :-1].to_numpy() @ assets - x.financing_cost.to_numpy() * financing
    result, stats = estimate_fixed_income(x, pd.Series(y),
        FixedIncomeConstraints(stock_upper=1, financing_upper=.2, gross_assets_upper=1.2))
    np.testing.assert_allclose(result[[*FIXED_INCOME_ASSETS, "financing"]], true, atol=1e-5)
    assert stats["constraint_error"] <= 1e-6
    assert result.ordinary_bond == pytest.approx(result[["rate_short", "rate_long", "credit_short", "credit_long"]].sum())


def test_fixed_income_balance_sheet_allows_bond_above_nav():
    x = fi_matrix(5)
    assets = pd.Series(0.0, index=FIXED_INCOME_ASSETS)
    assets.rate_long, assets.credit_short = .8, .4
    y = x.iloc[:, :-1] @ assets - x.financing_cost * .2
    result, _ = estimate_fixed_income(x, y, FixedIncomeConstraints(financing_lower=.2,
        financing_upper=.2, gross_assets_upper=1.2))
    assert result.ordinary_bond == pytest.approx(1.2, abs=1e-5)
    assert result.gross_assets == pytest.approx(1.2, abs=1e-6)
    assert result.financing == pytest.approx(.2, abs=1e-6)


def test_contract_denominators_map_to_distinct_linear_constraints():
    bounds = fixed_income_constraints_from_row(
        pd.Series(
            {
                "source": "verified_contract",
                "denominator_interpretation": "gross_balance_sheet",
                "stock_lower": 0.0,
                "stock_upper": 0.2,
                "stock_denominator": "fund_assets",
                "bond_lower": 0.8,
                "bond_denominator": "fund_assets",
                "cbond_lower": 0.8,
                "cbond_denominator": "non_cash_fund_assets",
                "gross_assets_upper": 1.4,
            }
        )
    )
    assert bounds.stock_upper == 0.4
    assert bounds.stock_gross_upper == 0.2
    assert bounds.fixed_income_gross_lower == 0.8
    assert bounds.cbond_non_cash_lower == 0.8
    assert bounds.cbond_lower == 0

    nav_bounds = fixed_income_constraints_from_row(
        pd.Series(
            {
                "denominator_interpretation": "nav_investment_ratio",
                "stock_upper": 0.2,
                "stock_denominator": "fund_assets",
                "bond_lower": 0.8,
                "bond_denominator": "fund_assets",
                "cbond_lower": 0.8,
                "cbond_denominator": "non_cash_fund_assets",
            }
        )
    )
    assert nav_bounds.stock_upper == 0.2
    assert nav_bounds.fixed_income_nav_lower == 0.8
    assert nav_bounds.cbond_non_cash_nav_lower == 0.8


def test_ambiguous_fund_assets_denominator_is_not_silently_assumed():
    with pytest.raises(DataUnavailable, match="UNSUPPORTED_CONSTRAINT_DENOMINATOR"):
        fixed_income_constraints_from_row(
            pd.Series(
                {
                    "stock_upper": 0.2,
                    "stock_denominator": "fund_assets",
                }
            )
        )


def test_contract_grace_period_requires_point_in_time_compliance_state():
    row = pd.Series(
        {
            "denominator_interpretation": "nav_investment_ratio",
            "bond_lower": 0.8,
            "bond_denominator": "fund_assets",
            "portfolio_ratio_exception_days": 10,
        }
    )
    with pytest.raises(DataUnavailable, match="CONTRACT_COMPLIANCE_STATE_UNKNOWN"):
        fixed_income_constraints_from_row(row)
    diagnostic = fixed_income_constraints_from_row(
        pd.concat(
            [row, pd.Series({"constraint_enforcement_mode": "unconditional_diagnostic"})]
        )
    )
    assert diagnostic.fixed_income_nav_lower == 0.8


def test_fixed_income_solver_enforces_documented_asset_denominators():
    x = fi_matrix(28)
    bounds = FixedIncomeConstraints(
        stock_gross_upper=0.2,
        fixed_income_gross_lower=0.8,
        cbond_non_cash_lower=0.8,
        financing_upper=0.2,
        gross_assets_upper=1.2,
    )
    result, diagnostics = estimate_fixed_income(x, x.rate_long, bounds)
    gross = result[list(FIXED_INCOME_ASSETS)].sum()
    fixed_income = result[
        ["rate_short", "rate_long", "credit_short", "credit_long", "convertible_bond"]
    ].sum()
    stock = result[["hk", *SW_CODES]].sum()
    assert stock <= 0.2 * gross + 1e-6
    assert fixed_income >= 0.8 * gross - 1e-6
    assert result.convertible_bond >= 0.8 * (gross - result.cash) - 1e-6
    assert diagnostics["constraint_error"] <= 1e-6


def test_financing_scenarios_do_not_claim_unknown_bond_total():
    x = fi_matrix(6)
    assets = np.zeros(len(FIXED_INCOME_ASSETS))
    assets[1] = 1
    result, diagnostics, scenarios = estimate_financing_scenarios(
        x, pd.Series(x.iloc[:, :-1].to_numpy() @ assets), FixedIncomeConstraints(),
        disclosed_financing=None, contract_financing_upper=.3)
    assert diagnostics["primary_scenario"] == "no_financing"
    assert diagnostics["ordinary_bond_quality"] == "unavailable"
    assert set(scenarios.scenario) == {"no_financing", "contract_upper"}
    assert result.ordinary_bond >= 0


def test_financing_disclosure_outside_constraint_is_not_silently_used():
    x = fi_matrix(26)
    assets = np.zeros(len(FIXED_INCOME_ASSETS))
    assets[0] = 1
    _, diagnostics, scenarios = estimate_financing_scenarios(
        x,
        pd.Series(x.iloc[:, :-1].to_numpy() @ assets),
        FixedIncomeConstraints(financing_upper=.2),
        disclosed_financing=.3,
        contract_financing_upper=.2,
    )
    assert diagnostics["financing_input_status"] == "outside_verified_constraint"
    assert "last_disclosed" not in set(scenarios.scenario)


def test_fixed_income_prior_penalizes_only_disclosed_components():
    x = fi_matrix(16)
    assets = np.random.default_rng(17).dirichlet(np.ones(len(FIXED_INCOME_ASSETS)))
    y = pd.Series(x.iloc[:, :-1].to_numpy() @ assets)
    prior = pd.Series(assets, index=FIXED_INCOME_ASSETS)
    prior["cash"] = np.nan
    prior["financing"] = np.nan
    _, diagnostics = estimate_fixed_income(
        x,
        y,
        FixedIncomeConstraints(stock_upper=1),
        prior=prior,
        prior_penalty=.1,
    )
    assert diagnostics["prior_known_coefficients"] == len(FIXED_INCOME_ASSETS) - 1

    previous = prior.copy()
    _, smooth_diagnostics = estimate_fixed_income(
        x,
        y,
        FixedIncomeConstraints(stock_upper=1),
        previous=previous,
        smooth_penalty=.1,
    )
    assert smooth_diagnostics["solver_status"] in {"optimal", "optimal_inaccurate"}


def test_fixed_income_constraint_validation_and_error():
    with pytest.raises(DataUnavailable, match="INVALID_CONSTRAINT"):
        FixedIncomeConstraints(financing_lower=.5, financing_upper=.1).validate()
    solution = pd.Series(0.0, index=[*FIXED_INCOME_ASSETS, "financing"])
    solution.cash = 1
    assert fixed_income_constraint_error(solution, FixedIncomeConstraints()) == 0


def test_report_announcement_recovery_requires_same_security_and_amount():
    target = pd.DataFrame({"fund_code": ["F1", "F2"], "report_date": ["2023-12-31"]*2,
                           "ann_date": [None, None], "security_code": ["B1", "B2"],
                           "market_value": [100.0, 200.0]})
    evidence = pd.DataFrame({"fund_code": ["F1", "F2", "F2"], "report_date": ["2023-12-31"]*3,
                             "ann_date": ["2024-03-28", "2024-03-27", "2024-03-28"],
                             "security_code": ["B1", "B2", "B2"],
                             "market_value": [100.0, 200.0, 199.0]})
    result = recover_report_announcements(target, evidence)
    assert result.loc[result.fund_code.eq("F1"), "ann_date"].iloc[0] == pd.Timestamp("2024-03-28")
    assert result.loc[result.fund_code.eq("F2"), "ann_date"].iloc[0] == pd.Timestamp("2024-03-27")


def test_report_level_date_without_document_evidence_is_not_borrowed():
    target = pd.DataFrame({"fund_code": ["F1"], "report_date": ["2023-12-31"],
                           "ann_date": [None]})
    evidence = pd.DataFrame({"fund_code": ["F1"], "report_date": ["2023-12-31"],
                             "ann_date": ["2024-03-28"]})
    assert pd.isna(recover_report_announcements(target, evidence).ann_date.iloc[0])


def test_asset_control_uses_same_period_financial_announcement_conditionally():
    target = pd.DataFrame(
        {
            "fund_code": ["F"],
            "report_date": ["2022-03-31"],
            "ann_date": [None],
            "stock_weight": [.2],
        }
    )
    evidence = pd.DataFrame(
        {
            "fund_code": ["F"],
            "report_date": ["2022-03-31"],
            "ann_date": ["2022-04-22"],
        }
    )
    result = recover_asset_control_announcements(target, evidence).iloc[0]
    assert result.ann_date == pd.Timestamp("2022-04-22")
    assert result.announcement_evidence_status == "cross_table_verified_not_document_hash"


def test_formal_report_hash_can_supply_same_report_announcement_date():
    frames = {
        "report_evidence": pd.DataFrame(
            {
                "fund_code": ["F"],
                "report_date": ["2023-12-31"],
                "ann_date": ["2024-03-28"],
                "document_sha256": ["a" * 64],
                "document_path": ["report.pdf"],
                "verified": [True],
            }
        ),
        "asset_reports": pd.DataFrame(
            {
                "fund_code": ["F"],
                "report_date": ["2023-12-31"],
                "ann_date": [None],
            }
        ),
    }
    result = apply_report_evidence(frames)["asset_reports"].iloc[0]
    assert result.ann_date == pd.Timestamp("2024-03-28")
    assert result.announcement_source == "formal_report_document_hash"


def test_report_title_period_and_pdf_challenge_parser():
    assert report_period_from_title("甲基金2023年年度报告") == pd.Timestamp("2023-12-31")
    assert report_period_from_title("甲基金2024年第1季度报告") == pd.Timestamp("2024-03-31")
    assert report_period_from_title("甲基金2023年年度报告摘要") is None
    challenge = b'var e={a:1000000,b:2000000,c:function(x){return x}},t=0;(t,3000000);EO_Bot_Ssid'
    assert _challenge_cookie(challenge) == "__tst_status=3000000#; EO_Bot_Ssid=3000000"


def test_bond_categories_reconcile_without_double_counting_convertibles():
    assets = pd.DataFrame({"fund_code": ["F"], "report_date": ["2023-12-31"],
        "ann_date": ["2024-03-28"], "bond_weight": [1.1]})
    allocation = pd.DataFrame({"fund_code": ["F", "F"], "report_date": ["2023-12-31"]*2,
        "ann_date": ["2024-03-28"]*2, "bond_category": ["可转换债券", "金融债券"], "weight": [.2, .9]})
    result = classify_bond_disclosures(assets, allocation).iloc[0]
    assert result.bond_allocation_complete
    assert result.convertible_bond_weight == pytest.approx(.2)
    assert result.ordinary_bond_weight == pytest.approx(.9)


def test_bond_category_gap_is_unknown_not_renormalized():
    assets = pd.DataFrame({"fund_code": ["F"], "report_date": ["2023-12-31"],
        "ann_date": ["2024-03-28"], "bond_weight": [1.1]})
    allocation = pd.DataFrame({"fund_code": ["F"], "report_date": ["2023-12-31"],
        "ann_date": ["2024-03-28"], "bond_category": ["可转换债券"], "weight": [.2]})
    result = classify_bond_disclosures(assets, allocation).iloc[0]
    assert not result.bond_allocation_complete
    assert pd.isna(result.convertible_bond_weight) and pd.isna(result.ordinary_bond_weight)


def test_financing_requires_explicit_repo_instead_of_balance_residual():
    reports = pd.DataFrame({"fund_code": ["F", "F"], "report_date": ["2023-12-31", "2024-03-31"],
        "ann_date": ["2024-03-28", "2024-04-20"], "total_asset_value": [120, 180],
        "aum": [100, 100], "repo_sold_value": [10, 30], "repo_sold_weight": [None, None]})
    value, reason = disclosed_financing(reports, "F", "2024-03-29", "2024-03-29")
    assert value == pytest.approx(.1) and reason is None
    value, reason = disclosed_financing(reports, "F", "2023-12-31", "2024-01-01")
    assert value is None and reason == "NO_EXPLICIT_REPO_FINANCING"
    residual_only = reports.drop(columns=["repo_sold_value", "repo_sold_weight"])
    value, reason = disclosed_financing(residual_only, "F", "2024-03-29", "2024-03-29")
    assert value is None and reason == "NO_EXPLICIT_REPO_FINANCING"


def test_balance_sheet_leverage_is_a_separate_dated_scenario():
    reports = pd.DataFrame(
        {
            "fund_code": ["F", "F"],
            "report_date": ["2023-12-31", "2024-03-31"],
            "ann_date": ["2024-03-20", "2024-04-20"],
            "total_asset_value": [120.0, 140.0],
            "aum": [100.0, 100.0],
        }
    )
    value, reason = disclosed_balance_sheet_leverage(
        reports, "F", "2024-03-31", "2024-04-01"
    )
    assert reason is None
    assert value == pytest.approx(0.2)

    x = fi_matrix(39)
    assets = np.zeros(len(FIXED_INCOME_ASSETS))
    assets[FIXED_INCOME_ASSETS.index("convertible_bond")] = 1.2
    y = pd.Series(x.iloc[:, :-1].to_numpy() @ assets - x.financing_cost * 0.2)
    result, diagnostics, scenarios = estimate_financing_scenarios(
        x,
        y,
        FixedIncomeConstraints(
            cbond_upper=1.4, financing_upper=0.4, gross_assets_upper=1.4
        ),
        disclosed_financing=None,
        balance_sheet_leverage=value,
        contract_financing_upper=0.4,
        prefer_balance_sheet_leverage=True,
    )
    assert diagnostics["primary_scenario"] == "last_disclosed_leverage"
    assert "last_disclosed_leverage" in set(scenarios.scenario)
    assert result.financing == pytest.approx(0.2, abs=1e-6)

def universe_frames():
    funds = pd.DataFrame({"fund_code": ["F1", "F2"], "fund_name": ["一", "二"],
        "master_code": ["F1", "F2"], "found_date": ["2020-01-01"]*2,
        "liquidation_date": [None, None], "category": ["偏债混合"]*2})
    classification = pd.DataFrame({"fund_code": ["F1", "F2"], "category": ["偏债混合"]*2,
        "in_date": ["2020-01-01"]*2, "out_date": [None, None]})
    reports = []
    for code, stock in [("F1", .2), ("F2", .5)]:
        for date, ann in [("2023-06-30", "2023-08-30"), ("2023-12-31", "2024-03-28")]:
            reports.append({"fund_code": code, "report_date": date, "ann_date": ann,
                            "stock_weight": stock, "bond_weight": .7, "convertible_bond_weight": .1})
    constraints = pd.DataFrame({"fund_code": ["F1"], "ann_date": ["2020-01-01"],
        "effective_date": ["2020-01-01"], "verified": [True], "fixed_income_primary": [True],
        "allows_equity_or_cbond": [True], "stock_denominator": ["fund_nav"], "stock_upper": [.4]})
    return funds, classification, pd.DataFrame(reports), constraints


def test_contract_and_style_pools_remain_separate():
    result = fixed_income_plus_universe(*universe_frames(), "2024-06-28", "2024-06-29")
    f1, f2 = result.set_index("fund_code").loc["F1"], result.set_index("fund_code").loc["F2"]
    assert f1.contract_pool and f1.style_pool and f1.in_fixed_income_plus
    assert not f2.contract_pool and not f2.style_pool and not f2.in_fixed_income_plus


def test_future_reports_do_not_change_historical_style_pool():
    frames = list(universe_frames())
    before = fixed_income_plus_universe(*frames, "2024-01-31", "2024-02-01")
    future = copy.deepcopy(frames)
    extra = future[2].iloc[[0]].assign(report_date="2024-12-31", ann_date="2025-03-28", stock_weight=0)
    future[2] = pd.concat([future[2], extra], ignore_index=True)
    after = fixed_income_plus_universe(*future, "2024-01-31", "2024-02-01")
    pd.testing.assert_frame_equal(before, after)


def test_selection_has_fixed_quotas_and_visible_shortfall():
    frame = fixed_income_plus_universe(*universe_frames(), "2024-06-28", "2024-06-29")
    selected, gaps = select_fixed_count(frame, {"偏债混合": 2, "pure_bond_control": 1})
    assert selected.master_code.is_unique
    assert gaps.set_index("selection_group").loc["pure_bond_control", "shortfall"] == 1


def test_generic_aggregation_keeps_unknown_assets_missing():
    results = pd.DataFrame({"master_code": ["A", "B"], "valuation_date": ["2024-01-02"]*2,
        "category": ["偏债混合"]*2, "status": ["ok", "unavailable"], "aum": [100, None],
        "ordinary_bond": [1.1, None], "convertible_bond": [.1, None]})
    agg = aggregate_exposures(results, ("ordinary_bond", "convertible_bond"))
    assert agg.status.eq("partial").all() and agg.aum_coverage.isna().all()
    assert agg.loc[agg.weighting.eq("equal"), "ordinary_bond"].iloc[0] == pytest.approx(1.1)


def test_group_aggregation_keeps_field_specific_coverage():
    results = pd.DataFrame(
        {
            "master_code": ["A", "B"],
            "valuation_date": ["2024-01-02"] * 2,
            "category": ["偏债混合"] * 2,
            "status": ["degraded", "degraded"],
            "aum": [100.0, 100.0],
            "convertible_bond": [.1, .2],
            "ordinary_bond": [None, None],
            "cbond_quality": ["estimated", "estimated"],
            "ordinary_bond_quality": ["unavailable", "unavailable"],
        }
    )
    aggregate = aggregate_exposures(
        results, ("ordinary_bond", "convertible_bond")
    )
    equal = aggregate.loc[
        aggregate.category.eq("偏债混合") & aggregate.weighting.eq("equal")
    ].iloc[0]
    assert pd.isna(equal.ordinary_bond)
    assert equal.convertible_bond == pytest.approx(.15)
    assert equal.convertible_bond_count_coverage == 1
    assert equal.ordinary_bond_count_coverage == 0


def test_enhanced_index_prior_uses_only_dated_mapping_and_components():
    bundle = DataBundle(
        {
            "tracking_index_history": pd.DataFrame(
                {
                    "fund_code": ["F", "F"],
                    "index_code": ["I1", "I2"],
                    "effective_date": ["2020-01-01", "2025-01-01"],
                    "ann_date": ["2020-01-01", "2025-01-01"],
                    "source": ["contract", "contract"],
                }
            ),
            "index_membership": pd.DataFrame(
                {
                    "index_code": ["I1", "I1", "I2"],
                    "security_code": ["S1", "S2", "S3"],
                    "weight": [.6, .4, 1.0],
                    "as_of_date": ["2023-12-31", "2023-12-31", "2025-01-01"],
                    "ann_date": ["2024-01-02", "2024-01-02", "2025-01-02"],
                    "source": ["index_file"] * 3,
                }
            ),
            "membership": pd.DataFrame(
                {
                    "security_code": ["S1", "S2", "S3"],
                    "industry": ["801010.SI", "801030.SI", "801040.SI"],
                    "in_date": ["2020-01-01"] * 3,
                    "out_date": [None, None, None],
                }
            ),
            "membership_fallback": pd.DataFrame(),
        }
    )
    prior, meta = tracking_index_industry_prior(bundle, "F", "2024-06-28", "2024-06-29")
    assert meta["tracking_index_code"] == "I1"
    assert prior["801010.SI"] == pytest.approx(.6)
    assert prior["801040.SI"] == 0


def test_enhanced_index_current_metadata_is_not_historical_evidence():
    with pytest.raises(DataUnavailable, match="NO_HISTORICAL_TRACKING_INDEX"):
        tracking_index_industry_prior(
            DataBundle({}), "F", "2024-06-28", "2024-06-29"
        )


def test_conditional_tracking_mapping_is_never_marked_strict():
    bundle = DataBundle(
        {
            "tracking_index_history": pd.DataFrame(
                {
                    "fund_code": ["F"],
                    "index_code": ["I1"],
                    "effective_date": ["2020-01-01"],
                    "ann_date": ["2020-01-01"],
                    "source": ["current_metadata"],
                    "evidence_status": ["conditional_current_metadata"],
                    "strict_pit": [False],
                }
            ),
            "index_membership": pd.DataFrame(
                {
                    "index_code": ["I1"],
                    "security_code": ["S1"],
                    "weight": [1.0],
                    "as_of_date": ["2023-12-31"],
                    "ann_date": ["2023-12-31"],
                    "source": ["official"],
                }
            ),
            "membership": pd.DataFrame(
                {
                    "security_code": ["S1"],
                    "industry": ["801010.SI"],
                    "in_date": ["2020-01-01"],
                    "out_date": [None],
                }
            ),
            "membership_fallback": pd.DataFrame(),
        }
    )
    _, meta = tracking_index_industry_prior(
        bundle, "F", "2024-06-28", "2024-06-29"
    )
    assert not meta["tracking_index_strict_pit"]
    assert meta["tracking_index_evidence_status"] == "conditional_current_metadata"


def test_fixed_income_cash_falls_back_to_the_same_dr007_accrual():
    dates = pd.bdate_range("2023-01-02", periods=62)
    starts = pd.Series(dates).shift(1)
    equity_assets = ["cash", "hk", *SW_CODES]
    equity = pd.DataFrame(
        [
            {
                "date": day,
                "start_date": starts.iloc[i],
                "ann_date": day,
                "asset": asset,
                "return": np.nan if asset == "cash" else 0.001,
            }
            for i, day in enumerate(dates)
            for asset in equity_assets
        ]
    )
    fixed_assets = [
        "rate_short",
        "rate_long",
        "credit_short",
        "credit_long",
        "convertible_bond",
        "financing_cost",
    ]
    fixed = pd.DataFrame(
        [
            {
                "date": day,
                "start_date": starts.iloc[i],
                "ann_date": day,
                "asset": asset,
                "return": 0.0001,
            }
            for i, day in enumerate(dates)
            for asset in fixed_assets
        ]
    )
    panel = fixed_income_panel(
        DataBundle({"factors": equity, "fixed_income_factors": fixed}),
        dates[-1],
        dates,
    )
    assert panel.cash.dropna().eq(panel.financing_cost.dropna()).all()


def test_financing_cost_preserves_dr007_and_extends_with_prior_day_fr007():
    calendar = pd.DatetimeIndex(pd.to_datetime(
        ["2025-05-26", "2025-05-27", "2025-05-28", "2025-05-29"]))
    dr007 = pd.DataFrame({
        "date": pd.to_datetime(["2025-05-26", "2025-05-27"]),
        "annual_rate_pct": [2.0, 2.1],
        "source_workbook_sha256": ["hash", "hash"],
        "evidence_status": ["historical", "historical"],
    })
    repo = pd.DataFrame({"date": calendar, "fr007": [1.5, 1.6, 1.7, 1.8]})

    result = financing_cost_returns(dr007, repo, calendar).set_index("date")

    assert result.loc["2025-05-28", "source"] == "rawdata.macro_dr007_history:L001619493"
    assert result.loc["2025-05-29", "source"] == "rawdata.macro_repo_rate:fr007"
    assert result.loc["2025-05-28", "return"] == pytest.approx((1 + 2.1 / 100 / 365) - 1)
    assert result.loc["2025-05-29", "return"] == pytest.approx((1 + 1.7 / 100 / 365) - 1)
    assert result.loc["2025-05-28", "source_hash"] == "hash"
    assert pd.isna(result.loc["2025-05-29", "source_hash"])


def test_convertible_codes_use_official_exchange_and_remove_source_twins():
    holdings = pd.DataFrame({
        "fund_code": ["F", "F", "F"],
        "report_date": pd.to_datetime(["2026-06-30"] * 3),
        "cbond_code_raw": ["SZ118062", "SH118062", "SZ123001"],
        "security_code": ["118062.SZ", "118062.SH", "123001.SZ"],
        "quantity": [10.0, 10.0, 20.0],
        "market_value": [100.0, 100.0, 220.0],
        "weight": [0.1, 0.1, 0.22],
        "rank_no": [1, 1, 2],
    })
    master = pd.DataFrame({"ts_code": ["118062.SH", "123001.SZ"]})

    result, stats = canonicalize_convertible_holding_codes(holdings, master)

    assert result.security_code.tolist() == ["118062.SH", "123001.SZ"]
    assert result.cbond_code_raw.tolist()[0] == "SH118062"
    assert stats == {
        "input_rows": 3,
        "output_rows": 2,
        "rewritten_codes": 1,
        "duplicate_rows_removed": 1,
        "unresolved_codes": 0,
        "source": "rawdata.cbond_basic:unique_numeric_code",
    }


def test_tracking_index_comparison_uses_equity_normalized_industries():
    mix = dict.fromkeys(SW_CODES, 0.0)
    mix["801010.SI"], mix["801030.SI"] = .6, .4
    predictions = pd.DataFrame(
        [
            {
                "fund_code": "F",
                "valuation_date": "2024-06-28",
                "tracking_index_status": "available_point_in_time",
                "tracking_index_code": "I",
                "tracking_index_prior": mix,
            }
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "fund_code": "F",
                "valuation_date": "2024-06-28",
                **{key: value * .5 for key, value in mix.items()},
            }
        ]
    )
    metrics, errors = evaluate_tracking_index_prior(predictions, labels)
    assert metrics["status"] == "evaluated"
    assert errors.equity_normalized_industry_l1.iloc[0] == pytest.approx(0)


def test_v3_factor_registry_separates_strict_and_conditional_sources(settings):
    required = [
        "rate_short",
        "rate_long",
        "credit_short",
        "credit_long",
        "convertible_bond",
        "financing_cost",
    ]
    strict = factor_registry_gaps(settings, required, "2022-01-01", "2023-12-31")
    assert {item["asset"] for item in strict} == {"financing_cost"}
    conditional = factor_registry_gaps(
        settings,
        required,
        "2022-01-01",
        "2023-12-31",
        allow_conditional=True,
    )
    assert conditional == []
