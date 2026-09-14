import numpy as np
import pandas as pd
import pytest

from fundpos.constants import ASSETS
from fundpos.data import AlphaDB
from fundpos.errors import DataUnavailable
from fundpos.estimation import InvestmentConstraints, estimate_weights
from fundpos.pipeline import compute_date, select_constraints
from fundpos.pit import map_membership, recover_holding_announcements
from fundpos.storage import git_revision
from fundpos.validation import ValidationState, run_validation


def test_quarterly_reconstruction_runs_through_verified_bridge(bundle):
    from fundpos.pipeline import build_holdings

    full = bundle["holdings"]
    previous = full.loc[
        (full.fund_code == "DEMO001.OF") & (full.report_date == pd.Timestamp("2022-12-31"))
    ].copy()
    heavy = previous.nsmallest(10, "rank_no").copy()
    heavy["report_date"] = pd.Timestamp("2023-03-31")
    heavy["ann_date"] = pd.Timestamp("2023-04-25")
    heavy["full_report_verified"] = False
    bundle.frames["holdings"] = pd.concat([full, heavy], ignore_index=True)
    groups = previous[["security_code", "weight"]].merge(
        bundle["membership"][["security_code", "industry"]], on="security_code", how="left"
    )
    groups.loc[groups.security_code.str.endswith(".HK"), "industry"] = "hk"
    groups["group"] = np.where(groups.industry == "hk", "HK", "C")
    allocation = groups.groupby("group", as_index=False).weight.sum()
    allocation["fund_code"] = "DEMO001.OF"
    allocation["report_date"] = pd.Timestamp("2023-03-31")
    allocation["ann_date"] = pd.Timestamp("2023-04-25")
    bundle.frames["allocations"] = allocation
    groups["date"] = pd.Timestamp("2023-03-31")
    groups["ann_date"] = pd.Timestamp("2023-03-31")
    groups["float_mv"] = 1e8
    bundle.frames["stock_groups"] = groups.drop(columns="weight")
    result, metadata = build_holdings(bundle, "DEMO001.OF", "2023-05-27", "2023-05-26")
    assert metadata["holdings_mode"] == "quarterly_reconstruction"
    assert result.weight.sum() == pytest.approx(0.88, abs=1e-6)
    pd.testing.assert_series_equal(
        result.groupby("group").weight.sum(),
        allocation.set_index("group").weight,
        check_names=False,
    )


def test_future_recomputed_state_is_not_a_smoothing_prior(settings, bundle):
    model = settings.with_model(name="prior01_smooth01", prior_penalty=0.1, smooth_penalty=0.1)
    prior = compute_date(model, bundle, "2023-09-28", "2023-09-29")
    prior["information_cutoff"] = "2024-01-01"
    plain = compute_date(model, bundle, "2023-09-29", "2023-09-30")
    guarded = compute_date(model, bundle, "2023-09-29", "2023-09-30", prior)
    pd.testing.assert_frame_equal(plain, guarded)


def test_failed_day_keeps_model_state_without_filling_output(settings, bundle):
    from fundpos.pipeline import advance_state

    prior = compute_date(settings, bundle, "2023-09-28", "2023-09-29")
    failed = prior.copy()
    failed["status"] = "unavailable"
    failed["valuation_date"] = "2023-09-29"
    state = advance_state(prior, failed)
    pd.testing.assert_frame_equal(state, prior)
    assert failed.status.eq("unavailable").all()


def test_rapid_rebalance_is_a_measurable_window_limitation():
    rng = np.random.default_rng(716)
    x = pd.DataFrame(rng.normal(0, 0.01, (60, 34)), columns=ASSETS)
    old = pd.Series(0.0, index=ASSETS)
    old.iloc[0], old.iloc[3] = 0.15, 0.85
    new = old.copy()
    new.iloc[3], new.iloc[10] = 0.0, 0.85
    y = x @ old
    y.iloc[-5:] = (x @ new).iloc[-5:]
    weights, stats = estimate_weights(x, y, InvestmentConstraints())
    assert stats["constraint_error"] <= 1e-6
    assert abs(weights - new).sum() > 0.2
    assert stats["return_mae"] > 0.0001


def test_missing_proxy_constituent_is_not_silently_dropped():
    from fundpos.reconstruction import market_cap_bridge

    frame = pd.DataFrame(
        {
            "security_code": ["A", "B"],
            "group": ["C", "C"],
            "industry": ["801010.SI", None],
            "float_mv": [30.0, 70.0],
        }
    )
    with pytest.raises(DataUnavailable, match="INCOMPLETE_MARKET_CAP_BRIDGE"):
        market_cap_bridge(frame)


def test_explicit_raw_hk_code_recovers_announcements():
    h = pd.DataFrame(
        {
            "fund_code": ["F"],
            "report_date": ["2023-06-30"],
            "ann_date": [None],
            "security_code": [None],
            "security_code_raw": ["HK00700"],
            "market_value": [100.0],
        }
    )
    d = pd.DataFrame(
        {
            "fund_code": ["F"],
            "report_date": ["2023-06-30"],
            "ann_date": ["2023-07-20"],
            "security_code": ["0700.HK"],
            "market_value": [100.0],
        }
    )
    result = recover_holding_announcements(h, d)
    assert result.security_code.iloc[0] == "00700.HK"
    assert result.ann_date.iloc[0] == pd.Timestamp("2023-07-20")


def test_unrelated_ambiguous_stock_does_not_fail_every_fund():
    h = pd.DataFrame({"security_code": ["A"], "weight": [0.5]})
    m = pd.DataFrame(
        {
            "security_code": ["A", "B", "B"],
            "industry": ["801010.SI", "801010.SI", "801030.SI"],
            "in_date": ["2020-01-01"] * 3,
            "out_date": [None] * 3,
        }
    )
    assert len(map_membership(h, m, "2023-01-01")) == 1


@pytest.mark.parametrize(
    "prior_penalty,smooth_penalty", [(p, s) for p in [0.1, 1] for s in [0, 0.1, 1]]
)
def test_fixed_candidate_grid_solves(prior_penalty, smooth_penalty):
    rng = np.random.default_rng(14)
    x = pd.DataFrame(rng.normal(0, 0.01, (60, 34)), columns=ASSETS)
    truth = pd.Series(1 / 34, index=ASSETS)
    prior = truth.copy()
    prior.iloc[3], prior.iloc[4] = 0.02, 2 / 34 - 0.02
    w, stats = estimate_weights(
        x,
        x @ truth,
        InvestmentConstraints(),
        prior=prior,
        previous=prior,
        prior_penalty=prior_penalty,
        smooth_penalty=smooth_penalty,
    )
    assert stats["constraint_error"] <= 1e-6
    assert np.isfinite(w).all()


def test_quarterly_bridge_respects_announcement_time(settings, bundle):
    # New quarter exists but remains unavailable; no row may alter the prior snapshot.
    before = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    bundle.frames["allocations"] = pd.DataFrame(
        {
            "fund_code": ["DEMO001.OF"],
            "report_date": ["2023-09-29"],
            "ann_date": ["2023-10-20"],
            "group": ["C"],
            "weight": [0.8],
        }
    )
    after = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    pd.testing.assert_frame_equal(before, after)


def test_contract_effective_after_valuation_is_not_applied(bundle):
    fund = type("Fund", (), {"fund_code": "DEMO001.OF", "category": "普通股票型"})()
    new = bundle.frames["constraints"].iloc[:1].copy()
    new["ann_date"] = pd.Timestamp("2023-09-20")
    new["effective_date"] = pd.Timestamp("2023-09-30")
    new["stock_lower"] = 0.2
    bundle.frames["constraints"] = pd.concat([bundle.frames["constraints"], new], ignore_index=True)
    before = select_constraints(bundle, fund, "2023-09-30", "2023-09-29")
    after = select_constraints(bundle, fund, "2023-09-30", "2023-09-30")
    assert before.stock_lower > 0.2 and after.stock_lower == 0.2


def test_missing_preconditions_do_not_open_final_holdout(settings, bundle):
    from fundpos.storage import atomic_json, code_fingerprint

    bundle.provenance = {
        "provider": "fixture_for_gate",
        "universe_scope": "all_active_equity_metadata",
    }
    state = ValidationState(settings)
    atomic_json(
        state.path,
        {
            "selection_frozen": "prior01_smooth0",
            "protocol_hash": state.protocol_hash,
            "code_hash": code_fingerprint(settings.root),
            "selection_assessment": {"all_selection_gates_pass": True},
        },
    )
    run_validation(settings, bundle, "final")
    assert not state.read().get("final_opened_at")


def test_readonly_database_rejects_mutation(settings):
    with pytest.raises(ValueError, match="Read-only"):
        AlphaDB(settings).query("DELETE FROM rawdata.fund_nav")


def test_family_missing_master_has_unknown_aum(settings, bundle):
    bundle.frames["funds"] = bundle.frames["funds"].loc[
        bundle.frames["funds"].fund_code != "DEMO001.OF"
    ]
    result = compute_date(settings, bundle, "2023-09-29", "2023-09-30")
    family = result.loc[result.master_code == "DEMO001.OF"]
    assert family.aum.isna().all()


def test_git_revision_uses_frozen_release_identity(monkeypatch, tmp_path):
    monkeypatch.setenv("FUNDPOS_SOURCE_REVISION", "a" * 40)

    assert git_revision(tmp_path) == "a" * 40
