from __future__ import annotations

from dataclasses import dataclass, replace

import cvxpy as cp
import numpy as np
import pandas as pd

from .constants import FIXED_INCOME_ASSETS, FIXED_INCOME_FACTOR_COLUMNS, SW_CODES
from .errors import DataUnavailable
from .estimation import observation_weights


@dataclass(frozen=True)
class FixedIncomeConstraints:
    stock_lower: float = 0.0
    stock_upper: float = 0.40
    stock_gross_lower: float | None = None
    stock_gross_upper: float | None = None
    fixed_income_nav_lower: float | None = None
    fixed_income_gross_lower: float | None = None
    cbond_lower: float = 0.0
    cbond_upper: float = 1.0
    cbond_gross_lower: float | None = None
    cbond_non_cash_lower: float | None = None
    cbond_non_cash_nav_lower: float | None = None
    cbond_fixed_income_lower: float | None = None
    financing_lower: float = 0.0
    financing_upper: float = 0.40
    gross_assets_upper: float = 1.40
    hk_upper_equity: float = 1.0
    source: str = "unverified_category_diagnostic"

    def validate(self):
        values = [
            self.stock_lower,
            self.stock_upper,
            self.cbond_lower,
            self.cbond_upper,
            self.financing_lower,
            self.financing_upper,
            self.gross_assets_upper,
            self.hk_upper_equity,
        ]
        if not np.isfinite(values).all() or min(values) < 0:
            raise DataUnavailable("INVALID_CONSTRAINT", "Finite nonnegative bounds required")
        ratio_values = [
            value
            for value in (
                self.stock_gross_lower,
                self.stock_gross_upper,
                self.fixed_income_nav_lower,
                self.fixed_income_gross_lower,
                self.cbond_gross_lower,
                self.cbond_non_cash_lower,
                self.cbond_non_cash_nav_lower,
                self.cbond_fixed_income_lower,
            )
            if value is not None
        ]
        if ratio_values and (
            not np.isfinite(ratio_values).all()
            or min(ratio_values) < 0
            or max(ratio_values) > 1
        ):
            raise DataUnavailable(
                "INVALID_CONSTRAINT", "Portfolio ratios must be between zero and one"
            )
        if self.stock_lower > self.stock_upper or self.cbond_lower > self.cbond_upper:
            raise DataUnavailable("INVALID_CONSTRAINT", "Lower bound exceeds upper bound")
        if (
            self.stock_gross_lower is not None
            and self.stock_gross_upper is not None
            and self.stock_gross_lower > self.stock_gross_upper
        ):
            raise DataUnavailable("INVALID_CONSTRAINT", "Gross stock lower exceeds upper")
        if self.financing_lower > self.financing_upper:
            raise DataUnavailable("INVALID_CONSTRAINT", "Invalid financing interval")
        if self.gross_assets_upper < 1 + self.financing_lower:
            raise DataUnavailable("INVALID_CONSTRAINT", "Gross assets conflict with financing")


def portfolio_ratio_constraints(
    weights: cp.Expression, bounds: FixedIncomeConstraints
) -> list[cp.Constraint]:
    """Build linear constraints using the denominator stated in a fund contract."""
    cash = weights[FIXED_INCOME_ASSETS.index("cash")]
    cbond = weights[FIXED_INCOME_ASSETS.index("convertible_bond")]
    stock_positions = [FIXED_INCOME_ASSETS.index(a) for a in ("hk", *SW_CODES)]
    ordinary_positions = [
        FIXED_INCOME_ASSETS.index(a)
        for a in ("rate_short", "rate_long", "credit_short", "credit_long")
    ]
    stock = cp.sum(weights[stock_positions])
    ordinary = cp.sum(weights[ordinary_positions])
    fixed_income = ordinary + cbond
    gross_assets = cp.sum(weights)
    non_cash_assets = gross_assets - cash
    constraints = []
    if bounds.stock_gross_lower is not None:
        constraints.append(stock >= bounds.stock_gross_lower * gross_assets)
    if bounds.stock_gross_upper is not None:
        constraints.append(stock <= bounds.stock_gross_upper * gross_assets)
    if bounds.fixed_income_nav_lower is not None:
        constraints.append(fixed_income >= bounds.fixed_income_nav_lower)
    if bounds.fixed_income_gross_lower is not None:
        constraints.append(fixed_income >= bounds.fixed_income_gross_lower * gross_assets)
    if bounds.cbond_gross_lower is not None:
        constraints.append(cbond >= bounds.cbond_gross_lower * gross_assets)
    if bounds.cbond_non_cash_lower is not None:
        constraints.append(cbond >= bounds.cbond_non_cash_lower * non_cash_assets)
    if bounds.cbond_non_cash_nav_lower is not None:
        constraints.append(cbond >= bounds.cbond_non_cash_nav_lower * (1 - cash))
    if bounds.cbond_fixed_income_lower is not None:
        constraints.append(cbond >= bounds.cbond_fixed_income_lower * fixed_income)
    return constraints


def fixed_income_constraint_error(solution: pd.Series, bounds: FixedIncomeConstraints) -> float:
    assets = solution.reindex(FIXED_INCOME_ASSETS).to_numpy(float)
    financing = float(solution.financing)
    stock = float(solution[["hk", *SW_CODES]].sum())
    gross_assets = float(assets.sum())
    cash = float(solution.cash)
    ordinary = float(
        solution[["rate_short", "rate_long", "credit_short", "credit_long"]].sum()
    )
    fixed_income = ordinary + float(solution.convertible_bond)
    ratio_errors = []
    if bounds.stock_gross_lower is not None:
        ratio_errors.append(bounds.stock_gross_lower * gross_assets - stock)
    if bounds.stock_gross_upper is not None:
        ratio_errors.append(stock - bounds.stock_gross_upper * gross_assets)
    if bounds.fixed_income_nav_lower is not None:
        ratio_errors.append(bounds.fixed_income_nav_lower - fixed_income)
    if bounds.fixed_income_gross_lower is not None:
        ratio_errors.append(bounds.fixed_income_gross_lower * gross_assets - fixed_income)
    if bounds.cbond_gross_lower is not None:
        ratio_errors.append(
            bounds.cbond_gross_lower * gross_assets - solution.convertible_bond
        )
    if bounds.cbond_non_cash_lower is not None:
        ratio_errors.append(
            bounds.cbond_non_cash_lower * (gross_assets - cash)
            - solution.convertible_bond
        )
    if bounds.cbond_non_cash_nav_lower is not None:
        ratio_errors.append(
            bounds.cbond_non_cash_nav_lower * (1 - cash)
            - solution.convertible_bond
        )
    if bounds.cbond_fixed_income_lower is not None:
        ratio_errors.append(
            bounds.cbond_fixed_income_lower * fixed_income
            - solution.convertible_bond
        )
    return float(max(0, *ratio_errors, -assets.min(), -financing, abs(assets.sum() - financing - 1),
        bounds.stock_lower - stock, stock - bounds.stock_upper,
        bounds.cbond_lower - solution.convertible_bond,
        solution.convertible_bond - bounds.cbond_upper,
        bounds.financing_lower - financing, financing - bounds.financing_upper,
        assets.sum() - bounds.gross_assets_upper,
        solution.hk - bounds.hk_upper_equity * stock))


def estimate_fixed_income(
    x: pd.DataFrame,
    y: pd.Series,
    bounds: FixedIncomeConstraints,
    *,
    prior: pd.Series | None = None,
    previous: pd.Series | None = None,
    fixed_weights: pd.Series | None = None,
    prior_penalty=0.0,
    smooth_penalty=0.0,
    weighting="documented",
    tolerance=1e-6,
) -> tuple[pd.Series, dict]:
    bounds.validate()
    if list(x.columns) != list(FIXED_INCOME_FACTOR_COLUMNS) or not x.index.equals(y.index) or len(y) < 35:
        raise DataUnavailable("RETURN_ALIGNMENT", "Need >=35 aligned fixed-income factor observations")
    matrix, target = x.to_numpy(float), y.to_numpy(float)
    if not np.isfinite(matrix).all() or not np.isfinite(target).all():
        raise DataUnavailable("MISSING_RETURN", "NaN/inf cannot enter the optimization")
    if np.var(target) < 1e-12:
        raise DataUnavailable("CONSTANT_NAV", "Fund returns have no identifiable variance")
    if prior_penalty and prior is None:
        raise DataUnavailable("NO_PRIOR", "Candidate requires published holdings")
    if smooth_penalty and previous is None:
        raise DataUnavailable("NO_PREVIOUS_ESTIMATE", "Candidate requires previous estimate")
    weights = cp.Variable(len(FIXED_INCOME_ASSETS))
    financing = cp.Variable()
    asset_matrix = matrix[:, : len(FIXED_INCOME_ASSETS)]
    predicted = asset_matrix @ weights - matrix[:, -1] * financing
    obs = observation_weights(len(y), weighting)
    variance = max(float(np.average((target - np.average(target, weights=obs)) ** 2, weights=obs)), 1e-12)
    objective = cp.sum_squares(cp.multiply(np.sqrt(obs / variance), predicted - target))
    if prior_penalty:
        p = prior.reindex([*FIXED_INCOME_ASSETS, "financing"]).to_numpy(float)
        known = np.flatnonzero(np.isfinite(p))
        if not len(known):
            raise DataUnavailable("INVALID_PRIOR", "Fixed-income prior has no known assets")
        variables = cp.hstack([weights, cp.reshape(financing, (1,), order="C")])
        objective += prior_penalty * cp.sum_squares(variables[known] - p[known])
    if smooth_penalty:
        p = previous.reindex([*FIXED_INCOME_ASSETS, "financing"]).to_numpy(float)
        known = np.flatnonzero(np.isfinite(p))
        if not len(known):
            raise DataUnavailable("INVALID_PREVIOUS", "Previous estimate has no known assets")
        variables = cp.hstack([weights, cp.reshape(financing, (1,), order="C")])
        objective += smooth_penalty * cp.sum_squares(variables[known] - p[known])
    stock_positions = [FIXED_INCOME_ASSETS.index(a) for a in ("hk", *SW_CODES)]
    stock = cp.sum(weights[stock_positions])
    cbond = weights[FIXED_INCOME_ASSETS.index("convertible_bond")]
    hk = weights[FIXED_INCOME_ASSETS.index("hk")]
    constraints = [weights >= 0, financing >= 0, cp.sum(weights) - financing == 1,
        stock >= bounds.stock_lower, stock <= bounds.stock_upper,
        cbond >= bounds.cbond_lower, cbond <= bounds.cbond_upper,
        financing >= bounds.financing_lower, financing <= bounds.financing_upper,
        cp.sum(weights) <= bounds.gross_assets_upper, hk <= bounds.hk_upper_equity * stock]
    constraints.extend(portfolio_ratio_constraints(weights, bounds))
    fixed_count = 0
    if fixed_weights is not None:
        fixed = fixed_weights.reindex(FIXED_INCOME_ASSETS).to_numpy(float)
        known = np.flatnonzero(np.isfinite(fixed))
        if np.any(fixed[known] < -tolerance):
            raise DataUnavailable("INVALID_FIXED_ASSET_WEIGHT", "Negative fixed weight")
        constraints.extend(weights[position] == fixed[position] for position in known)
        fixed_count = len(known)
    problem = cp.Problem(cp.Minimize(objective), constraints)
    try:
        problem.solve(solver="OSQP", eps_abs=1e-8, eps_rel=1e-8, max_iter=100000,
                      polishing=True, warm_start=False)
    except cp.error.SolverError as exc:
        raise DataUnavailable("SOLVER_FAILURE", type(exc).__name__) from exc
    if problem.status != cp.OPTIMAL or weights.value is None or financing.value is None:
        raise DataUnavailable("SOLVER_STATUS", str(problem.status))
    result = pd.Series(np.r_[np.asarray(weights.value).ravel(), float(financing.value)],
                       index=[*FIXED_INCOME_ASSETS, "financing"])
    error = fixed_income_constraint_error(result, bounds)
    if error > tolerance or not np.isfinite(result).all():
        raise DataUnavailable("CONSTRAINT_VIOLATION", f"error={error}")
    residual = target - (asset_matrix @ result.iloc[:-1].to_numpy() - matrix[:, -1] * result.financing)
    result["ordinary_bond"] = result[["rate_short", "rate_long", "credit_short", "credit_long"]].sum()
    result["stock_weight"] = result[["hk", *SW_CODES]].sum()
    result["a_stock_weight"] = result[list(SW_CODES)].sum()
    result["gross_assets"] = result[list(FIXED_INCOME_ASSETS)].sum()
    result["non_equity"] = 1 - result.stock_weight
    stats = {"solver_status": problem.status, "constraint_error": error,
             "return_mae": float(np.abs(residual).mean()),
             "r2": float(1 - residual @ residual / np.sum((target-target.mean())**2)),
             "condition_number": float(min(np.linalg.cond(matrix), 1e300)),
             "window_observations": len(y), "constraint_source": bounds.source,
             "weighting": weighting,
             "prior_known_coefficients": int(np.isfinite(p).sum()) if prior_penalty else 0,
             "fixed_asset_coefficients": fixed_count}
    return result, stats


def estimate_financing_scenarios(
    x: pd.DataFrame,
    y: pd.Series,
    bounds: FixedIncomeConstraints,
    *,
    disclosed_financing: float | None,
    balance_sheet_leverage: float | None = None,
    contract_financing_upper: float | None,
    sensitivity_limit=0.05,
    include_free_financing=False,
    prefer_free_financing=False,
    prefer_balance_sheet_leverage=False,
    estimator=estimate_fixed_income,
    **kwargs,
) -> tuple[pd.Series, dict, pd.DataFrame]:
    upper = min(bounds.financing_upper, contract_financing_upper) if contract_financing_upper is not None else bounds.financing_upper
    scenarios = {"no_financing": replace(bounds, financing_lower=0, financing_upper=0)}
    financing_input_status = "not_disclosed"
    if disclosed_financing is not None and 0 <= disclosed_financing <= upper:
        scenarios["last_disclosed"] = replace(bounds, financing_lower=disclosed_financing,
                                                financing_upper=disclosed_financing,
                                                gross_assets_upper=max(bounds.gross_assets_upper, 1 + disclosed_financing))
        financing_input_status = "used"
    elif disclosed_financing is not None:
        financing_input_status = "outside_verified_constraint"
    balance_sheet_leverage_status = "not_disclosed"
    if balance_sheet_leverage is not None and 0 <= balance_sheet_leverage <= upper:
        scenarios["last_disclosed_leverage"] = replace(
            bounds,
            financing_lower=balance_sheet_leverage,
            financing_upper=balance_sheet_leverage,
            gross_assets_upper=max(bounds.gross_assets_upper, 1 + balance_sheet_leverage),
        )
        balance_sheet_leverage_status = "used_as_total_liability_scenario"
    elif balance_sheet_leverage is not None:
        balance_sheet_leverage_status = "outside_verified_constraint"
    if include_free_financing and upper > 0:
        scenarios["estimated_financing"] = replace(
            bounds,
            financing_lower=0,
            financing_upper=upper,
            gross_assets_upper=max(bounds.gross_assets_upper, 1 + upper),
        )
    scenarios["contract_upper"] = replace(bounds, financing_lower=upper, financing_upper=upper,
                                            gross_assets_upper=max(bounds.gross_assets_upper, 1 + upper))
    solutions, diagnostics, failures = {}, {}, {}
    for name, scenario in scenarios.items():
        try:
            solutions[name], diagnostics[name] = estimator(x, y, scenario, **kwargs)
        except DataUnavailable as exc:
            failures[name] = {"code": exc.code, "detail": exc.detail}
    intended_primary = (
        "last_disclosed"
        if "last_disclosed" in scenarios
        else (
            "last_disclosed_leverage"
            if prefer_balance_sheet_leverage and "last_disclosed_leverage" in scenarios
            else (
                "estimated_financing"
                if prefer_free_financing and "estimated_financing" in scenarios
                else "no_financing"
            )
        )
    )
    preferred_order = [intended_primary, "last_disclosed"]
    if prefer_balance_sheet_leverage:
        preferred_order.append("last_disclosed_leverage")
    if prefer_free_financing:
        preferred_order.append("estimated_financing")
    preferred_order.extend(
        ["no_financing", "last_disclosed_leverage", "estimated_financing", "contract_upper"]
    )
    primary_name = next((name for name in preferred_order if name in solutions), None)
    if primary_name is None:
        raise DataUnavailable("NO_FEASIBLE_FINANCING_SCENARIO", str(failures))
    comparison = pd.DataFrame(solutions).T
    stock_sensitivity = float(comparison.stock_weight.max() - comparison.stock_weight.min())
    cbond_sensitivity = float(comparison.convertible_bond.max() - comparison.convertible_bond.min())
    status = {
        **diagnostics[primary_name], "primary_scenario": primary_name,
        "stock_sensitivity": stock_sensitivity, "cbond_sensitivity": cbond_sensitivity,
        "stock_quality": "diagnostic" if stock_sensitivity > sensitivity_limit else "estimated",
        "cbond_quality": "diagnostic" if cbond_sensitivity > sensitivity_limit else "estimated",
        "ordinary_bond_quality": "estimated" if primary_name == "last_disclosed" else "unavailable",
        "financing_input_status": financing_input_status,
        "balance_sheet_leverage_status": balance_sheet_leverage_status,
        "scenario_count": len(comparison),
        "scenario_failure_count": len(failures),
        "scenario_failures": failures or None,
        "primary_scenario_fallback": primary_name != intended_primary,
    }
    return solutions[primary_name], status, comparison.reset_index(names="scenario")
