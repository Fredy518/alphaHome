import numpy as np
import pandas as pd
import pytest

from fundpos.constants import ASSETS
from fundpos.errors import DataUnavailable
from fundpos.estimation import (
    InvestmentConstraints,
    constraint_error,
    estimate_weights,
    observation_weights,
)


def matrix(seed=17):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(rng.normal(0, 0.01, (60, 34)), columns=ASSETS)


def test_known_synthetic_exposures_are_recovered():
    x = matrix()
    true = np.random.default_rng(10).dirichlet(np.ones(34))
    w, stats = estimate_weights(x, pd.Series(x.to_numpy() @ true), InvestmentConstraints())
    np.testing.assert_allclose(w, true, atol=1e-6)
    assert stats["constraint_error"] <= 1e-6
    assert stats["return_mae"] < 1e-8


def test_hk_cap_uses_actual_equity_not_global_upper_bound():
    x = matrix()
    old = np.zeros(34)
    old[0], old[2], old[3] = 0.2, 0.45, 0.35
    bounds = InvestmentConstraints(stock_lower=0.8, stock_upper=0.95, hk_upper_equity=0.5)
    assert old[2] <= 0.5 * 0.95 and old[2] / sum(old[2:]) == 0.5625
    assert constraint_error(old, bounds) == pytest.approx(0.05)
    w, _ = estimate_weights(x, x @ old, bounds)
    assert w.hk <= 0.5 * w.iloc[2:].sum() + 1e-6


def test_documented_weights_are_not_accidentally_squared():
    documented = observation_weights(60)
    legacy = observation_weights(60, "legacy_squared")
    assert documented[-1] / documented[0] == pytest.approx(np.exp(59 / 60))
    assert legacy[-1] / legacy[0] == pytest.approx(np.exp(118 / 60))
    e = np.arange(60) / 1000
    assert np.dot(documented, e**2) == pytest.approx(np.sum((np.sqrt(documented) * e) ** 2))


def test_infeasible_contract_has_explicit_failure():
    x = matrix()
    with pytest.raises(DataUnavailable, match="SOLVER_STATUS"):
        estimate_weights(x, x.iloc[:, 3], InvestmentConstraints(stock_upper=0.2, hk_lower_nav=0.5))


def test_high_correlation_is_reported_not_claimed_accurate():
    x = matrix()
    x.iloc[:, 4] = x.iloc[:, 3]
    w, stats = estimate_weights(
        x, x.iloc[:, 3] * 0.85 + x.iloc[:, 0] * 0.15, InvestmentConstraints()
    )
    assert stats["r2"] > 0.999
    assert stats["condition_number"] > 1e12
    assert w.iloc[3] + w.iloc[4] == pytest.approx(0.85, abs=1e-6)


def test_candidate_prior_requires_explicit_information():
    x = matrix()
    with pytest.raises(DataUnavailable, match="NO_PRIOR"):
        estimate_weights(x, x.iloc[:, 3], InvestmentConstraints(), prior_penalty=0.1)
    with pytest.raises(DataUnavailable, match="NO_PREVIOUS_ESTIMATE"):
        estimate_weights(x, x.iloc[:, 3], InvestmentConstraints(), smooth_penalty=1)
