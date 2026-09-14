from __future__ import annotations

from dataclasses import dataclass

import cvxpy as cp
import numpy as np
import pandas as pd

from .constants import ASSETS
from .errors import DataUnavailable


@dataclass(frozen=True)
class InvestmentConstraints:
    stock_lower: float = 0.0
    stock_upper: float = 1.0
    hk_lower_nav: float = 0.0
    hk_upper_nav: float = 1.0
    hk_upper_equity: float = 1.0
    hk_lower_equity: float = 0.0
    source: str = "economic_long_only_bounds"

    def validate(self):
        if not (0 <= self.stock_lower <= self.stock_upper <= 1):
            raise DataUnavailable(
                "INVALID_CONSTRAINT", "Stock bounds must satisfy 0 <= lower <= upper <= 1"
            )
        if not (0 <= self.hk_lower_nav <= self.hk_upper_nav <= 1):
            raise DataUnavailable("INVALID_CONSTRAINT", "Invalid HK/NAV bounds")
        if not (0 <= self.hk_lower_equity <= self.hk_upper_equity <= 1):
            raise DataUnavailable("INVALID_CONSTRAINT", "Invalid HK/equity bounds")


def observation_weights(n: int, mode="documented") -> np.ndarray:
    if mode not in ("documented", "legacy_squared"):
        raise ValueError(mode)
    weights = np.exp(np.arange(1, n + 1) / n)
    if mode == "legacy_squared":
        weights = weights**2
    return weights / weights.sum()


def constraint_error(weights: np.ndarray, bounds: InvestmentConstraints) -> float:
    stock = weights[2:].sum()
    hk = weights[2]
    return float(
        max(
            0,
            abs(weights.sum() - 1),
            -weights.min(),
            bounds.stock_lower - stock,
            stock - bounds.stock_upper,
            bounds.hk_lower_nav - hk,
            hk - bounds.hk_upper_nav,
            bounds.hk_lower_equity * stock - hk,
            hk - bounds.hk_upper_equity * stock,
        )
    )


def estimate_weights(
    x: pd.DataFrame,
    y: pd.Series,
    bounds: InvestmentConstraints,
    *,
    prior: pd.Series | None = None,
    previous: pd.Series | None = None,
    prior_penalty=0.0,
    smooth_penalty=0.0,
    weighting="documented",
    tolerance=1e-6,
) -> tuple[pd.Series, dict]:
    bounds.validate()
    if list(x.columns) != list(ASSETS):
        raise DataUnavailable("FACTOR_SCHEMA", "All 34 ordered factors are required")
    if not x.index.equals(y.index) or len(y) < 35:
        raise DataUnavailable("RETURN_ALIGNMENT", "Need >=35 aligned observations")
    matrix = x.to_numpy(dtype=float)
    target = y.to_numpy(dtype=float)
    if not np.isfinite(matrix).all() or not np.isfinite(target).all():
        raise DataUnavailable("MISSING_RETURN", "NaN/inf cannot enter the optimization")
    if np.var(target) < 1e-12:
        raise DataUnavailable("CONSTANT_NAV", "Fund returns have no identifiable variance")
    if prior_penalty and prior is None:
        raise DataUnavailable("NO_PRIOR", "Candidate requires a published holdings prior")
    if smooth_penalty and previous is None:
        raise DataUnavailable("NO_PREVIOUS_ESTIMATE", "Warm the candidate chronologically first")
    weights = cp.Variable(len(ASSETS))
    obs_weights = observation_weights(len(y), weighting)
    variance = max(
        float(
            np.average((target - np.average(target, weights=obs_weights)) ** 2, weights=obs_weights)
        ),
        1e-12,
    )
    objective = cp.sum_squares(
        cp.multiply(np.sqrt(obs_weights / variance), matrix @ weights - target)
    )
    if prior_penalty:
        p = prior.reindex(ASSETS).to_numpy(dtype=float)
        if not np.isfinite(p[2:]).all():
            raise DataUnavailable("INVALID_PRIOR", "Equity prior incomplete")
        objective += prior_penalty * cp.sum_squares(weights[2:] - p[2:])
    if smooth_penalty:
        prev = previous.reindex(ASSETS).to_numpy(dtype=float)
        if not np.isfinite(prev).all():
            raise DataUnavailable("INVALID_PREVIOUS", "Previous weights incomplete")
        objective += smooth_penalty * cp.sum_squares(weights - prev)
    equity = cp.sum(weights[2:])
    constraints = [
        weights >= 0,
        cp.sum(weights) == 1,
        equity >= bounds.stock_lower,
        equity <= bounds.stock_upper,
        weights[2] >= bounds.hk_lower_nav,
        weights[2] <= bounds.hk_upper_nav,
        weights[2] <= bounds.hk_upper_equity * equity,
        weights[2] >= bounds.hk_lower_equity * equity,
    ]
    problem = cp.Problem(cp.Minimize(objective), constraints)
    try:
        problem.solve(
            solver="OSQP",
            eps_abs=1e-8,
            eps_rel=1e-8,
            max_iter=100000,
            polishing=True,
            warm_start=False,
        )
    except cp.error.SolverError as exc:
        raise DataUnavailable("SOLVER_FAILURE", type(exc).__name__) from exc
    if problem.status != cp.OPTIMAL or weights.value is None:
        raise DataUnavailable("SOLVER_STATUS", str(problem.status))
    solution = np.asarray(weights.value).ravel()
    error = constraint_error(solution, bounds)
    if not np.isfinite(solution).all() or error > tolerance:
        raise DataUnavailable("CONSTRAINT_VIOLATION", f"error={error}")
    # Do not renormalize/round the solver result; that can break coupled HK constraints.
    predicted = matrix @ solution
    residual = target - predicted
    cash_bond_diff = float(np.std(matrix[:, 0] - matrix[:, 1]))
    diagnostics = {
        "solver_status": problem.status,
        "constraint_error": error,
        "return_mae": float(np.abs(residual).mean()),
        "r2": float(1 - (residual @ residual) / np.sum((target - target.mean()) ** 2)),
        "residual_mean": float(residual.mean()),
        "window_observations": len(target),
        "condition_number": float(min(np.linalg.cond(matrix), 1e300)),
        "non_equity_identification": "weak" if cash_bond_diff < 0.0005 else "proxy_estimate",
        "weighting": weighting,
        "constraint_source": bounds.source,
    }
    return pd.Series(solution, index=ASSETS), diagnostics
