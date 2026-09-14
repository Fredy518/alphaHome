from __future__ import annotations

from dataclasses import replace

import cvxpy as cp
import numpy as np
import pandas as pd

from .constants import FIXED_INCOME_ASSETS, FIXED_INCOME_FACTOR_COLUMNS, SW_CODES
from .errors import DataUnavailable
from .estimation import observation_weights
from .fixed_income import (
    FixedIncomeConstraints,
    fixed_income_constraint_error,
    portfolio_ratio_constraints,
)
from .pit import available, available_nav, dates, require_unique

CONVERTIBLE_DOMINANT_GROUP = "convertible_dominant"
CBOND_STYLE_COLUMNS = (
    "cbond_equity_like",
    "cbond_balanced",
    "cbond_bond_like",
)


def convertible_rotation_mix(
    prior: pd.Series,
    *,
    balance_sheet_leverage: float | None = None,
) -> tuple[pd.Series, dict]:
    """Build the disclosed non-convertible sleeve used for exposure rotation.

    The endpoint model lets convertible exposure move from the last public
    report to the valuation date.  To keep gross assets conserved over that
    path, the opposite leg is a fixed mix of the last disclosed ordinary-bond
    sleeve and any identifiable cash residual.  Missing ordinary-bond
    structure falls back to cash rather than learning another highly collinear
    return coefficient.
    """
    mix = pd.Series(0.0, index=FIXED_INCOME_ASSETS, dtype=float)
    ordinary_assets = ("rate_short", "rate_long", "credit_short", "credit_long")
    ordinary = prior.reindex(ordinary_assets).astype(float)
    ordinary = ordinary.where(np.isfinite(ordinary), 0.0).clip(lower=0.0)
    mix.loc[list(ordinary_assets)] = ordinary

    known = prior.reindex(["convertible_bond", "hk", *SW_CODES]).astype(float)
    if known.notna().all():
        gross = 1.0 + max(0.0, float(balance_sheet_leverage or 0.0))
        cash = gross - float(known.sum()) - float(ordinary.sum())
        if np.isfinite(cash) and cash > 0:
            mix.cash = cash
    total = float(mix.sum())
    if total <= 1e-12:
        mix.cash = 1.0
        mode = "cash_fallback"
    else:
        mix /= total
        mode = (
            "last_public_ordinary_bond_and_cash_mix"
            if mix.cash > 0
            else "last_public_ordinary_bond_mix"
        )
    return mix, {
        "cbond_rotation_counterpart": mode,
        "cbond_rotation_cash_share": float(mix.cash),
        "cbond_rotation_ordinary_share": float(mix[list(ordinary_assets)].sum()),
    }


def estimate_convertible_endpoint(
    x: pd.DataFrame,
    y: pd.Series,
    bounds: FixedIncomeConstraints,
    *,
    cbond_anchor: float,
    anchor_date,
    replacement_weights: pd.Series,
    prior: pd.Series | None = None,
    previous: pd.Series | None = None,
    prior_penalty=0.0,
    smooth_penalty=0.0,
    weighting="documented",
    tolerance=1e-6,
) -> tuple[pd.Series, dict]:
    """Estimate valuation-date exposure with an anchored linear CB path.

    A static 60-observation regression estimates an average exposure, while
    validation labels describe the final report date.  Here the convertible
    coefficient moves linearly from the latest public report weight to an
    unknown endpoint.  The opposite movement is assigned to a frozen,
    disclosed non-convertible sleeve, so every point on the path conserves
    gross assets.  Only the endpoint is returned as the estimated position.
    """
    bounds.validate()
    if (
        list(x.columns) != list(FIXED_INCOME_FACTOR_COLUMNS)
        or not x.index.equals(y.index)
        or len(y) < 35
    ):
        raise DataUnavailable(
            "RETURN_ALIGNMENT", "Need >=35 aligned fixed-income factor observations"
        )
    matrix = x.to_numpy(float)
    target = y.to_numpy(float)
    if not np.isfinite(matrix).all() or not np.isfinite(target).all():
        raise DataUnavailable("MISSING_RETURN", "NaN/inf cannot enter the optimization")
    if np.var(target) < 1e-12:
        raise DataUnavailable("CONSTANT_NAV", "Fund returns have no identifiable variance")
    anchor = float(cbond_anchor)
    if (
        not np.isfinite(anchor)
        or anchor < bounds.cbond_lower - tolerance
        or anchor > bounds.cbond_upper + tolerance
    ):
        raise DataUnavailable(
            "CBOND_ANCHOR_OUTSIDE_CONSTRAINT",
            f"anchor={anchor:.6f}, bounds={bounds.cbond_lower:.6f}/{bounds.cbond_upper:.6f}",
        )
    if prior_penalty and prior is None:
        raise DataUnavailable("NO_PRIOR", "Candidate requires published holdings")
    if smooth_penalty and previous is None:
        raise DataUnavailable("NO_PREVIOUS_ESTIMATE", "Candidate requires previous estimate")

    replacement = replacement_weights.reindex(FIXED_INCOME_ASSETS).fillna(0.0).astype(float)
    forbidden = ["convertible_bond", "hk", *SW_CODES]
    if (
        not np.isfinite(replacement).all()
        or (replacement < 0).any()
        or replacement[forbidden].abs().sum() > tolerance
        or abs(float(replacement.sum()) - 1.0) > tolerance
    ):
        raise DataUnavailable(
            "INVALID_CBOND_ROTATION_MIX",
            "Replacement must be a nonnegative unit mix of cash and ordinary bonds",
        )

    observation_dates = pd.DatetimeIndex(x.index).normalize()
    endpoint_date = observation_dates[-1]
    anchor_timestamp = pd.Timestamp(anchor_date).normalize()
    elapsed = max(1, (endpoint_date - anchor_timestamp).days)
    progress = np.clip(
        (observation_dates - anchor_timestamp).days.to_numpy(float) / elapsed,
        0.0,
        1.0,
    )
    if progress[-1] < 1 - 1e-12:
        raise DataUnavailable("INVALID_CBOND_ENDPOINT", str(endpoint_date.date()))

    asset_matrix = matrix[:, : len(FIXED_INCOME_ASSETS)].copy()
    cbond_position = FIXED_INCOME_ASSETS.index("convertible_bond")
    replacement_return = asset_matrix @ replacement.to_numpy(float)
    spread = asset_matrix[:, cbond_position] - replacement_return
    known_path_return = (1.0 - progress) * anchor * spread
    # The transformed coefficient is the return earned by one unit of endpoint
    # CB exposure after accounting for the earlier disclosed anchor.
    asset_matrix[:, cbond_position] -= (1.0 - progress) * spread
    adjusted_target = target - known_path_return

    weights = cp.Variable(len(FIXED_INCOME_ASSETS))
    financing = cp.Variable()
    predicted = asset_matrix @ weights - matrix[:, -1] * financing
    obs = observation_weights(len(y), weighting)
    variance = max(
        float(
            np.average(
                (adjusted_target - np.average(adjusted_target, weights=obs)) ** 2,
                weights=obs,
            )
        ),
        1e-12,
    )
    objective = cp.sum_squares(
        cp.multiply(np.sqrt(obs / variance), predicted - adjusted_target)
    )
    variables = cp.hstack([weights, cp.reshape(financing, (1,), order="C")])
    prior_array = None
    if prior_penalty:
        prior_array = prior.reindex([*FIXED_INCOME_ASSETS, "financing"]).to_numpy(float)
        known = np.flatnonzero(np.isfinite(prior_array))
        if not len(known):
            raise DataUnavailable("INVALID_PRIOR", "Fixed-income prior has no known assets")
        objective += prior_penalty * cp.sum_squares(variables[known] - prior_array[known])
    if smooth_penalty:
        previous_array = previous.reindex(
            [*FIXED_INCOME_ASSETS, "financing"]
        ).to_numpy(float)
        known = np.flatnonzero(np.isfinite(previous_array))
        if not len(known):
            raise DataUnavailable("INVALID_PREVIOUS", "Previous estimate has no known assets")
        objective += smooth_penalty * cp.sum_squares(
            variables[known] - previous_array[known]
        )

    stock_positions = [
        FIXED_INCOME_ASSETS.index(asset) for asset in ("hk", *SW_CODES)
    ]
    replacement_positions = np.flatnonzero(replacement.to_numpy(float) > 0)
    stock = cp.sum(weights[stock_positions])
    cbond = weights[cbond_position]
    hk = weights[FIXED_INCOME_ASSETS.index("hk")]
    constraints = [
        weights >= 0,
        financing >= 0,
        cp.sum(weights) - financing == 1,
        stock >= bounds.stock_lower,
        stock <= bounds.stock_upper,
        cbond >= bounds.cbond_lower,
        cbond <= bounds.cbond_upper,
        financing >= bounds.financing_lower,
        financing <= bounds.financing_upper,
        cp.sum(weights) <= bounds.gross_assets_upper,
        hk <= bounds.hk_upper_equity * stock,
        # When the anchor exceeds the endpoint, the replacement sleeve is
        # smaller at the start.  This constraint keeps it nonnegative there;
        # linear interpolation then makes every intermediate date feasible.
    ]
    constraints.extend(portfolio_ratio_constraints(weights, bounds))
    constraints.extend(
        weights[position]
        + float(replacement.iloc[position]) * (cbond - anchor)
        >= 0
        for position in replacement_positions
    )
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
    if problem.status != cp.OPTIMAL or weights.value is None or financing.value is None:
        raise DataUnavailable("SOLVER_STATUS", str(problem.status))

    result = pd.Series(
        np.r_[np.asarray(weights.value).ravel(), float(financing.value)],
        index=[*FIXED_INCOME_ASSETS, "financing"],
    )
    error = fixed_income_constraint_error(result, bounds)
    start_replacement = (
        result.iloc[replacement_positions].to_numpy()
        + replacement.iloc[replacement_positions].to_numpy()
        * (result.convertible_bond - anchor)
    )
    error = max(error, float(max(0.0, -start_replacement.min())))
    if error > tolerance or not np.isfinite(result).all():
        raise DataUnavailable("CONSTRAINT_VIOLATION", f"error={error}")

    fitted = asset_matrix @ result.iloc[:-1].to_numpy() - matrix[:, -1] * result.financing
    residual = adjusted_target - fitted
    result["ordinary_bond"] = result[
        ["rate_short", "rate_long", "credit_short", "credit_long"]
    ].sum()
    result["stock_weight"] = result[["hk", *SW_CODES]].sum()
    result["a_stock_weight"] = result[list(SW_CODES)].sum()
    result["gross_assets"] = result[list(FIXED_INCOME_ASSETS)].sum()
    result["non_equity"] = 1 - result.stock_weight
    stats = {
        "solver_status": problem.status,
        "constraint_error": float(error),
        "return_mae": float(np.abs(residual).mean()),
        "r2": float(
            1 - residual @ residual / np.sum((target - target.mean()) ** 2)
        ),
        "condition_number": float(min(np.linalg.cond(asset_matrix), 1e300)),
        "window_observations": len(y),
        "constraint_source": bounds.source,
        "weighting": weighting,
        "prior_known_coefficients": (
            int(np.isfinite(prior_array).sum()) if prior_array is not None else 0
        ),
        "cbond_exposure_mode": "last_public_anchor_linear_endpoint",
        "cbond_anchor_weight": anchor,
        "cbond_anchor_date": str(anchor_timestamp.date()),
        "cbond_endpoint_date": str(endpoint_date.date()),
        "cbond_transition_start_progress": float(progress[0]),
        "cbond_start_replacement_weight": float(start_replacement.sum()),
    }
    return result, stats


def estimate_convertible_state_space(
    x: pd.DataFrame,
    y: pd.Series,
    bounds: FixedIncomeConstraints,
    *,
    cbond_anchor: float,
    anchor_date,
    replacement_weights: pd.Series,
    state_penalty: float,
    prior: pd.Series | None = None,
    previous: pd.Series | None = None,
    fixed_weights: pd.Series | None = None,
    prior_penalty=0.0,
    smooth_penalty=0.0,
    weighting="documented",
    tolerance=1e-6,
) -> tuple[pd.Series, dict]:
    """Estimate a smooth convertible exposure path and return its endpoint.

    The remaining assets are endpoint coefficients.  Daily changes in the
    convertible state are offset against the frozen disclosed replacement mix,
    preserving gross assets throughout the window.  A quadratic transition
    penalty controls noise; this is a bounded state-space regression expressed
    as one deterministic convex program.
    """
    bounds.validate()
    if state_penalty <= 0 or not np.isfinite(state_penalty):
        raise DataUnavailable("INVALID_CBOND_STATE_PENALTY", str(state_penalty))
    if (
        list(x.columns) != list(FIXED_INCOME_FACTOR_COLUMNS)
        or not x.index.equals(y.index)
        or len(y) < 35
    ):
        raise DataUnavailable(
            "RETURN_ALIGNMENT", "Need >=35 aligned fixed-income factor observations"
        )
    matrix = x.to_numpy(float)
    target = y.to_numpy(float)
    if not np.isfinite(matrix).all() or not np.isfinite(target).all():
        raise DataUnavailable("MISSING_RETURN", "NaN/inf cannot enter the optimization")
    if np.var(target) < 1e-12:
        raise DataUnavailable("CONSTANT_NAV", "Fund returns have no identifiable variance")
    anchor = float(cbond_anchor)
    if (
        not np.isfinite(anchor)
        or anchor < bounds.cbond_lower - tolerance
        or anchor > bounds.cbond_upper + tolerance
    ):
        raise DataUnavailable(
            "CBOND_ANCHOR_OUTSIDE_CONSTRAINT",
            f"anchor={anchor:.6f}, bounds={bounds.cbond_lower:.6f}/{bounds.cbond_upper:.6f}",
        )
    if prior_penalty and prior is None:
        raise DataUnavailable("NO_PRIOR", "Candidate requires published holdings")
    if smooth_penalty and previous is None:
        raise DataUnavailable("NO_PREVIOUS_ESTIMATE", "Candidate requires previous estimate")

    replacement = replacement_weights.reindex(FIXED_INCOME_ASSETS).fillna(0.0).astype(float)
    forbidden = ["convertible_bond", "hk", *SW_CODES]
    if (
        not np.isfinite(replacement).all()
        or (replacement < 0).any()
        or replacement[forbidden].abs().sum() > tolerance
        or abs(float(replacement.sum()) - 1.0) > tolerance
    ):
        raise DataUnavailable(
            "INVALID_CBOND_ROTATION_MIX",
            "Replacement must be a nonnegative unit mix of cash and ordinary bonds",
        )

    asset_matrix = matrix[:, : len(FIXED_INCOME_ASSETS)]
    cbond_position = FIXED_INCOME_ASSETS.index("convertible_bond")
    replacement_return = asset_matrix @ replacement.to_numpy(float)
    spread = asset_matrix[:, cbond_position] - replacement_return
    weights = cp.Variable(len(FIXED_INCOME_ASSETS))
    financing = cp.Variable()
    cbond_path = cp.Variable(len(y))
    # Static endpoint exposure earns the replacement return; the path state
    # earns the incremental convertible-minus-replacement spread.
    endpoint_matrix = asset_matrix.copy()
    endpoint_matrix[:, cbond_position] = replacement_return
    predicted = (
        endpoint_matrix @ weights
        + cp.multiply(spread, cbond_path)
        - matrix[:, -1] * financing
    )
    obs = observation_weights(len(y), weighting)
    variance = max(
        float(np.average((target - np.average(target, weights=obs)) ** 2, weights=obs)),
        1e-12,
    )
    objective = cp.sum_squares(
        cp.multiply(np.sqrt(obs / variance), predicted - target)
    )
    day_gap = max(
        1,
        (pd.DatetimeIndex(x.index)[0].normalize() - pd.Timestamp(anchor_date).normalize()).days,
    )
    objective += state_penalty * (
        cp.square(cbond_path[0] - anchor) / day_gap
        + cp.sum_squares(cbond_path[1:] - cbond_path[:-1])
    )
    variables = cp.hstack([weights, cp.reshape(financing, (1,), order="C")])
    prior_array = None
    if prior_penalty:
        prior_array = prior.reindex([*FIXED_INCOME_ASSETS, "financing"]).to_numpy(float)
        known = np.flatnonzero(np.isfinite(prior_array))
        if not len(known):
            raise DataUnavailable("INVALID_PRIOR", "Fixed-income prior has no known assets")
        objective += prior_penalty * cp.sum_squares(variables[known] - prior_array[known])
    if smooth_penalty:
        previous_array = previous.reindex(
            [*FIXED_INCOME_ASSETS, "financing"]
        ).to_numpy(float)
        known = np.flatnonzero(np.isfinite(previous_array))
        if not len(known):
            raise DataUnavailable("INVALID_PREVIOUS", "Previous estimate has no known assets")
        objective += smooth_penalty * cp.sum_squares(
            variables[known] - previous_array[known]
        )

    stock_positions = [
        FIXED_INCOME_ASSETS.index(asset) for asset in ("hk", *SW_CODES)
    ]
    replacement_positions = np.flatnonzero(replacement.to_numpy(float) > 0)
    stock = cp.sum(weights[stock_positions])
    cbond = weights[cbond_position]
    hk = weights[FIXED_INCOME_ASSETS.index("hk")]
    constraints = [
        weights >= 0,
        financing >= 0,
        cp.sum(weights) - financing == 1,
        stock >= bounds.stock_lower,
        stock <= bounds.stock_upper,
        cbond >= bounds.cbond_lower,
        cbond <= bounds.cbond_upper,
        cbond_path >= bounds.cbond_lower,
        cbond_path <= bounds.cbond_upper,
        cbond_path[-1] == cbond,
        financing >= bounds.financing_lower,
        financing <= bounds.financing_upper,
        cp.sum(weights) <= bounds.gross_assets_upper,
        hk <= bounds.hk_upper_equity * stock,
    ]
    constraints.extend(portfolio_ratio_constraints(weights, bounds))
    constraints.extend(
        weights[position]
        + float(replacement.iloc[position]) * (cbond - cbond_path)
        >= 0
        for position in replacement_positions
    )
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
    if (
        problem.status != cp.OPTIMAL
        or weights.value is None
        or financing.value is None
        or cbond_path.value is None
    ):
        raise DataUnavailable("SOLVER_STATUS", str(problem.status))

    result = pd.Series(
        np.r_[np.asarray(weights.value).ravel(), float(financing.value)],
        index=[*FIXED_INCOME_ASSETS, "financing"],
    )
    path = np.asarray(cbond_path.value).ravel()
    path_replacement_min = min(
        float(
            (
                result.iloc[position]
                + replacement.iloc[position] * (result.convertible_bond - path)
            ).min()
        )
        for position in replacement_positions
    )
    error = max(
        fixed_income_constraint_error(result, bounds),
        float(max(0.0, -path.min(), path.max() - bounds.cbond_upper)),
        abs(float(path[-1] - result.convertible_bond)),
        float(max(0.0, -path_replacement_min)),
    )
    if error > tolerance or not np.isfinite(result).all() or not np.isfinite(path).all():
        raise DataUnavailable("CONSTRAINT_VIOLATION", f"error={error}")
    fitted = (
        endpoint_matrix @ result.iloc[:-1].to_numpy()
        + spread * path
        - matrix[:, -1] * result.financing
    )
    residual = target - fitted
    result["ordinary_bond"] = result[
        ["rate_short", "rate_long", "credit_short", "credit_long"]
    ].sum()
    result["stock_weight"] = result[["hk", *SW_CODES]].sum()
    result["a_stock_weight"] = result[list(SW_CODES)].sum()
    result["gross_assets"] = result[list(FIXED_INCOME_ASSETS)].sum()
    result["non_equity"] = 1 - result.stock_weight
    stats = {
        "solver_status": problem.status,
        "constraint_error": float(error),
        "return_mae": float(np.abs(residual).mean()),
        "r2": float(1 - residual @ residual / np.sum((target - target.mean()) ** 2)),
        "condition_number": float(min(np.linalg.cond(endpoint_matrix), 1e300)),
        "window_observations": len(y),
        "constraint_source": bounds.source,
        "weighting": weighting,
        "prior_known_coefficients": (
            int(np.isfinite(prior_array).sum()) if prior_array is not None else 0
        ),
        "fixed_asset_coefficients": fixed_count,
        "cbond_exposure_mode": "bounded_state_space_endpoint",
        "cbond_anchor_weight": anchor,
        "cbond_anchor_date": str(pd.Timestamp(anchor_date).date()),
        "cbond_endpoint_date": str(pd.Timestamp(x.index[-1]).date()),
        "cbond_state_penalty": float(state_penalty),
        "cbond_state_start": float(path[0]),
        "cbond_state_min": float(path.min()),
        "cbond_state_max": float(path.max()),
        "cbond_state_total_variation": float(np.abs(np.diff(path)).sum()),
    }
    return result, stats


def estimate_convertible_nav_drift_sparse_trade(
    x: pd.DataFrame,
    y: pd.Series,
    bounds: FixedIncomeConstraints,
    *,
    cbond_anchor: float,
    anchor_date,
    replacement_weights: pd.Series,
    trade_penalty: float,
    anchor_penalty: float = 1.0,
    prior: pd.Series | None = None,
    previous: pd.Series | None = None,
    fixed_weights: pd.Series | None = None,
    prior_penalty=0.0,
    smooth_penalty=0.0,
    weighting="documented",
    tolerance=1e-6,
) -> tuple[pd.Series, dict]:
    """Estimate a CB endpoint after NAV drift and sparse active changes.

    ``cbond_path[t]`` is the exposure that earns the return in observation t.
    With no active change, its next value is the current exposure multiplied
    by ``(1 + cbond_return) / (1 + fund_nav_return)``. The L1 penalty applies
    only to the remaining innovation. This distinguishes mechanical weight
    drift from occasional active reallocation while retaining the existing
    gross-asset and financing scenario constraints.

    The final reported exposure is the last interval's post-return value. A
    trade after that return is not identifiable from the available NAV and is
    therefore outside this estimator's claim.
    """
    bounds.validate()
    if trade_penalty <= 0 or not np.isfinite(trade_penalty):
        raise DataUnavailable("INVALID_CBOND_TRADE_PENALTY", str(trade_penalty))
    if anchor_penalty <= 0 or not np.isfinite(anchor_penalty):
        raise DataUnavailable("INVALID_CBOND_ANCHOR_PENALTY", str(anchor_penalty))
    if (
        list(x.columns) != list(FIXED_INCOME_FACTOR_COLUMNS)
        or not x.index.equals(y.index)
        or len(y) < 35
    ):
        raise DataUnavailable(
            "RETURN_ALIGNMENT", "Need >=35 aligned fixed-income factor observations"
        )
    matrix = x.to_numpy(float)
    target = y.to_numpy(float)
    if not np.isfinite(matrix).all() or not np.isfinite(target).all():
        raise DataUnavailable("MISSING_RETURN", "NaN/inf cannot enter the optimization")
    if np.var(target) < 1e-12:
        raise DataUnavailable("CONSTANT_NAV", "Fund returns have no identifiable variance")
    if np.any(target <= -1):
        raise DataUnavailable("INVALID_NAV_RETURN", "NAV return must exceed -100%")
    anchor = float(cbond_anchor)
    if (
        not np.isfinite(anchor)
        or anchor < bounds.cbond_lower - tolerance
        or anchor > bounds.cbond_upper + tolerance
    ):
        raise DataUnavailable(
            "CBOND_ANCHOR_OUTSIDE_CONSTRAINT",
            f"anchor={anchor:.6f}, bounds={bounds.cbond_lower:.6f}/{bounds.cbond_upper:.6f}",
        )
    if prior_penalty and prior is None:
        raise DataUnavailable("NO_PRIOR", "Candidate requires published holdings")
    if smooth_penalty and previous is None:
        raise DataUnavailable("NO_PREVIOUS_ESTIMATE", "Candidate requires previous estimate")

    replacement = (
        replacement_weights.reindex(FIXED_INCOME_ASSETS).fillna(0.0).astype(float)
    )
    forbidden = ["convertible_bond", "hk", *SW_CODES]
    if (
        not np.isfinite(replacement).all()
        or (replacement < 0).any()
        or replacement[forbidden].abs().sum() > tolerance
        or abs(float(replacement.sum()) - 1.0) > tolerance
    ):
        raise DataUnavailable(
            "INVALID_CBOND_ROTATION_MIX",
            "Replacement must be a nonnegative unit mix of cash and ordinary bonds",
        )

    asset_matrix = matrix[:, : len(FIXED_INCOME_ASSETS)]
    cbond_position = FIXED_INCOME_ASSETS.index("convertible_bond")
    replacement_return = asset_matrix @ replacement.to_numpy(float)
    spread = asset_matrix[:, cbond_position] - replacement_return
    weights = cp.Variable(len(FIXED_INCOME_ASSETS))
    financing = cp.Variable()
    cbond_path = cp.Variable(len(y))
    endpoint_matrix = asset_matrix.copy()
    endpoint_matrix[:, cbond_position] = replacement_return
    predicted = (
        endpoint_matrix @ weights
        + cp.multiply(spread, cbond_path)
        - matrix[:, -1] * financing
    )
    obs = observation_weights(len(y), weighting)
    variance = max(
        float(
            np.average(
                (target - np.average(target, weights=obs)) ** 2,
                weights=obs,
            )
        ),
        1e-12,
    )
    objective = cp.sum_squares(
        cp.multiply(np.sqrt(obs / variance), predicted - target)
    )
    drift_growth = (1 + asset_matrix[:, cbond_position]) / (1 + target)
    if not np.isfinite(drift_growth).all() or np.any(drift_growth <= 0):
        raise DataUnavailable("INVALID_CBOND_NAV_DRIFT", "Nonpositive drift multiplier")
    active_change = cbond_path[1:] - cp.multiply(
        drift_growth[:-1], cbond_path[:-1]
    )
    objective += trade_penalty * cp.norm1(active_change)

    observation_dates = pd.DatetimeIndex(x.index).normalize()
    anchor_timestamp = pd.Timestamp(anchor_date).normalize()
    anchor_position = int(
        np.searchsorted(
            observation_dates.to_numpy(), anchor_timestamp.to_datetime64(), side="right"
        )
    )
    anchor_position = min(anchor_position, len(observation_dates) - 1)
    anchor_gap_days = max(
        1, abs((observation_dates[anchor_position] - anchor_timestamp).days)
    )
    objective += (
        anchor_penalty
        * cp.square(cbond_path[anchor_position] - anchor)
        / anchor_gap_days
    )

    variables = cp.hstack([weights, cp.reshape(financing, (1,), order="C")])
    prior_array = None
    if prior_penalty:
        prior_array = prior.reindex(
            [*FIXED_INCOME_ASSETS, "financing"]
        ).to_numpy(float)
        known = np.flatnonzero(np.isfinite(prior_array))
        if not len(known):
            raise DataUnavailable("INVALID_PRIOR", "Fixed-income prior has no known assets")
        objective += prior_penalty * cp.sum_squares(
            variables[known] - prior_array[known]
        )
    if smooth_penalty:
        previous_array = previous.reindex(
            [*FIXED_INCOME_ASSETS, "financing"]
        ).to_numpy(float)
        known = np.flatnonzero(np.isfinite(previous_array))
        if not len(known):
            raise DataUnavailable(
                "INVALID_PREVIOUS", "Previous estimate has no known assets"
            )
        objective += smooth_penalty * cp.sum_squares(
            variables[known] - previous_array[known]
        )

    stock_positions = [
        FIXED_INCOME_ASSETS.index(asset) for asset in ("hk", *SW_CODES)
    ]
    replacement_positions = np.flatnonzero(replacement.to_numpy(float) > 0)
    stock = cp.sum(weights[stock_positions])
    cbond = weights[cbond_position]
    hk = weights[FIXED_INCOME_ASSETS.index("hk")]
    endpoint_growth = float(drift_growth[-1])
    constraints = [
        weights >= 0,
        financing >= 0,
        cp.sum(weights) - financing == 1,
        stock >= bounds.stock_lower,
        stock <= bounds.stock_upper,
        cbond >= bounds.cbond_lower,
        cbond <= bounds.cbond_upper,
        cbond_path >= bounds.cbond_lower,
        cbond_path <= bounds.cbond_upper,
        cbond == endpoint_growth * cbond_path[-1],
        financing >= bounds.financing_lower,
        financing <= bounds.financing_upper,
        cp.sum(weights) <= bounds.gross_assets_upper,
        hk <= bounds.hk_upper_equity * stock,
    ]
    constraints.extend(portfolio_ratio_constraints(weights, bounds))
    constraints.extend(
        weights[position]
        + float(replacement.iloc[position]) * (cbond - cbond_path)
        >= 0
        for position in replacement_positions
    )
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
    if (
        problem.status != cp.OPTIMAL
        or weights.value is None
        or financing.value is None
        or cbond_path.value is None
    ):
        raise DataUnavailable("SOLVER_STATUS", str(problem.status))

    result = pd.Series(
        np.r_[np.asarray(weights.value).ravel(), float(financing.value)],
        index=[*FIXED_INCOME_ASSETS, "financing"],
    )
    path = np.asarray(cbond_path.value).ravel()
    active = path[1:] - drift_growth[:-1] * path[:-1]
    path_replacement_min = min(
        float(
            (
                result.iloc[position]
                + replacement.iloc[position] * (result.convertible_bond - path)
            ).min()
        )
        for position in replacement_positions
    )
    error = max(
        fixed_income_constraint_error(result, bounds),
        float(max(0.0, -path.min(), path.max() - bounds.cbond_upper)),
        abs(float(endpoint_growth * path[-1] - result.convertible_bond)),
        float(max(0.0, -path_replacement_min)),
    )
    if error > tolerance or not np.isfinite(result).all() or not np.isfinite(path).all():
        raise DataUnavailable("CONSTRAINT_VIOLATION", f"error={error}")
    fitted = (
        endpoint_matrix @ result.iloc[:-1].to_numpy()
        + spread * path
        - matrix[:, -1] * result.financing
    )
    residual = target - fitted
    result["ordinary_bond"] = result[
        ["rate_short", "rate_long", "credit_short", "credit_long"]
    ].sum()
    result["stock_weight"] = result[["hk", *SW_CODES]].sum()
    result["a_stock_weight"] = result[list(SW_CODES)].sum()
    result["gross_assets"] = result[list(FIXED_INCOME_ASSETS)].sum()
    result["non_equity"] = 1 - result.stock_weight
    stats = {
        "solver_status": problem.status,
        "constraint_error": float(error),
        "return_mae": float(np.abs(residual).mean()),
        "r2": float(1 - residual @ residual / np.sum((target - target.mean()) ** 2)),
        "condition_number": float(min(np.linalg.cond(endpoint_matrix), 1e300)),
        "window_observations": len(y),
        "constraint_source": bounds.source,
        "weighting": weighting,
        "prior_known_coefficients": (
            int(np.isfinite(prior_array).sum()) if prior_array is not None else 0
        ),
        "fixed_asset_coefficients": fixed_count,
        "cbond_exposure_mode": "nav_drift_sparse_trade_endpoint",
        "cbond_anchor_weight": anchor,
        "cbond_anchor_date": str(anchor_timestamp.date()),
        "cbond_anchor_path_position": anchor_position,
        "cbond_anchor_path_date": str(observation_dates[anchor_position].date()),
        "cbond_endpoint_date": str(observation_dates[-1].date()),
        "cbond_trade_penalty": float(trade_penalty),
        "cbond_anchor_penalty": float(anchor_penalty),
        "cbond_endpoint_nav_drift_multiplier": endpoint_growth,
        "cbond_state_start": float(path[0]),
        "cbond_state_min": float(path.min()),
        "cbond_state_max": float(path.max()),
        "cbond_state_total_variation": float(np.abs(np.diff(path)).sum()),
        "cbond_active_change_total": float(np.abs(active).sum()),
        "cbond_active_change_max": float(np.abs(active).max()),
        "cbond_active_change_count_1pp": int((np.abs(active) > 0.01).sum()),
        "cbond_natural_drift_total": float(
            np.abs((drift_growth[:-1] - 1) * path[:-1]).sum()
        ),
    }
    return result, stats


def estimate_convertible_style_endpoint(
    x: pd.DataFrame,
    y: pd.Series,
    bounds: FixedIncomeConstraints,
    *,
    cbond_anchor: float,
    anchor_date,
    replacement_weights: pd.Series,
    cbond_style_prior: pd.Series,
    prior: pd.Series | None = None,
    previous: pd.Series | None = None,
    fixed_weights: pd.Series | None = None,
    prior_penalty=0.0,
    smooth_penalty=0.0,
    weighting="documented",
    tolerance=1e-6,
) -> tuple[pd.Series, dict]:
    """Estimate endpoint CB total and style mix with a linear disclosed anchor.

    Three nonnegative endpoint style sleeves sum to the reported convertible
    total. Their path starts at the disclosed style mix and moves linearly to
    the endpoint. This separates a change in CB composition from a change in
    aggregate CB NAV weight without adding the CB option beta to direct stock.
    """
    bounds.validate()
    required_columns = [*FIXED_INCOME_FACTOR_COLUMNS, *CBOND_STYLE_COLUMNS]
    if list(x.columns) != required_columns or not x.index.equals(y.index) or len(y) < 35:
        raise DataUnavailable(
            "RETURN_ALIGNMENT", "Need >=35 aligned fixed-income and CB-style observations"
        )
    base = x[list(FIXED_INCOME_FACTOR_COLUMNS)].to_numpy(float)
    styles = x[list(CBOND_STYLE_COLUMNS)].to_numpy(float)
    target = y.to_numpy(float)
    if (
        not np.isfinite(base).all()
        or not np.isfinite(styles).all()
        or not np.isfinite(target).all()
    ):
        raise DataUnavailable("MISSING_RETURN", "NaN/inf cannot enter the optimization")
    if np.var(target) < 1e-12:
        raise DataUnavailable("CONSTANT_NAV", "Fund returns have no identifiable variance")
    anchor = float(cbond_anchor)
    anchor_style = cbond_style_prior.reindex(CBOND_STYLE_COLUMNS).to_numpy(float)
    if (
        not np.isfinite(anchor_style).all()
        or np.any(anchor_style < 0)
        or abs(float(anchor_style.sum()) - anchor) > tolerance
    ):
        raise DataUnavailable(
            "INVALID_CBOND_STYLE_PRIOR",
            f"style={anchor_style.sum():.6f}, control={anchor:.6f}",
        )
    if prior_penalty and prior is None:
        raise DataUnavailable("NO_PRIOR", "Candidate requires published holdings")
    if smooth_penalty and previous is None:
        raise DataUnavailable("NO_PREVIOUS_ESTIMATE", "Candidate requires previous estimate")
    replacement = (
        replacement_weights.reindex(FIXED_INCOME_ASSETS).fillna(0.0).astype(float)
    )
    forbidden = ["convertible_bond", "hk", *SW_CODES]
    if (
        not np.isfinite(replacement).all()
        or (replacement < 0).any()
        or replacement[forbidden].abs().sum() > tolerance
        or abs(float(replacement.sum()) - 1.0) > tolerance
    ):
        raise DataUnavailable(
            "INVALID_CBOND_ROTATION_MIX",
            "Replacement must be a nonnegative unit mix of cash and ordinary bonds",
        )

    dates_index = pd.DatetimeIndex(x.index).normalize()
    endpoint_date = dates_index[-1]
    anchor_timestamp = pd.Timestamp(anchor_date).normalize()
    elapsed = max(1, (endpoint_date - anchor_timestamp).days)
    progress = np.clip(
        (dates_index - anchor_timestamp).days.to_numpy(float) / elapsed,
        0.0,
        1.0,
    )
    if progress[-1] < 1 - 1e-12:
        raise DataUnavailable("INVALID_CBOND_ENDPOINT", str(endpoint_date.date()))

    asset_matrix = base[:, : len(FIXED_INCOME_ASSETS)]
    cbond_position = FIXED_INCOME_ASSETS.index("convertible_bond")
    replacement_return = asset_matrix @ replacement.to_numpy(float)
    endpoint_matrix = asset_matrix.copy()
    endpoint_matrix[:, cbond_position] = replacement_return
    known_anchor = (1 - progress) * (
        styles @ anchor_style - anchor * replacement_return
    )
    endpoint_style_matrix = progress[:, None] * (
        styles - replacement_return[:, None]
    )
    weights = cp.Variable(len(FIXED_INCOME_ASSETS))
    financing = cp.Variable()
    endpoint_style = cp.Variable(len(CBOND_STYLE_COLUMNS))
    predicted = (
        endpoint_matrix @ weights
        + endpoint_style_matrix @ endpoint_style
        + known_anchor
        - base[:, -1] * financing
    )
    obs = observation_weights(len(y), weighting)
    variance = max(
        float(
            np.average(
                (target - np.average(target, weights=obs)) ** 2,
                weights=obs,
            )
        ),
        1e-12,
    )
    objective = cp.sum_squares(
        cp.multiply(np.sqrt(obs / variance), predicted - target)
    )
    variables = cp.hstack([weights, cp.reshape(financing, (1,), order="C")])
    prior_array = None
    if prior_penalty:
        prior_array = prior.reindex(
            [*FIXED_INCOME_ASSETS, "financing"]
        ).to_numpy(float)
        known = np.flatnonzero(np.isfinite(prior_array))
        if not len(known):
            raise DataUnavailable("INVALID_PRIOR", "Fixed-income prior has no known assets")
        objective += prior_penalty * cp.sum_squares(
            variables[known] - prior_array[known]
        )
    if smooth_penalty:
        previous_array = previous.reindex(
            [*FIXED_INCOME_ASSETS, "financing"]
        ).to_numpy(float)
        known = np.flatnonzero(np.isfinite(previous_array))
        if not len(known):
            raise DataUnavailable(
                "INVALID_PREVIOUS", "Previous estimate has no known assets"
            )
        objective += smooth_penalty * cp.sum_squares(
            variables[known] - previous_array[known]
        )

    stock_positions = [
        FIXED_INCOME_ASSETS.index(asset) for asset in ("hk", *SW_CODES)
    ]
    replacement_positions = np.flatnonzero(replacement.to_numpy(float) > 0)
    stock = cp.sum(weights[stock_positions])
    cbond = weights[cbond_position]
    hk = weights[FIXED_INCOME_ASSETS.index("hk")]
    style_path_total = (1 - progress) * anchor + progress * cp.sum(endpoint_style)
    constraints = [
        weights >= 0,
        endpoint_style >= 0,
        financing >= 0,
        cp.sum(weights) - financing == 1,
        cp.sum(endpoint_style) == cbond,
        stock >= bounds.stock_lower,
        stock <= bounds.stock_upper,
        cbond >= bounds.cbond_lower,
        cbond <= bounds.cbond_upper,
        financing >= bounds.financing_lower,
        financing <= bounds.financing_upper,
        cp.sum(weights) <= bounds.gross_assets_upper,
        hk <= bounds.hk_upper_equity * stock,
    ]
    constraints.extend(portfolio_ratio_constraints(weights, bounds))
    constraints.extend(
        weights[position]
        + float(replacement.iloc[position]) * (cbond - style_path_total)
        >= 0
        for position in replacement_positions
    )
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
    if (
        problem.status != cp.OPTIMAL
        or weights.value is None
        or financing.value is None
        or endpoint_style.value is None
    ):
        raise DataUnavailable("SOLVER_STATUS", str(problem.status))
    result = pd.Series(
        np.r_[np.asarray(weights.value).ravel(), float(financing.value)],
        index=[*FIXED_INCOME_ASSETS, "financing"],
    )
    style_result = np.asarray(endpoint_style.value).ravel()
    path_total = (1 - progress) * anchor + progress * style_result.sum()
    path_replacement_min = min(
        float(
            (
                result.iloc[position]
                + replacement.iloc[position]
                * (result.convertible_bond - path_total)
            ).min()
        )
        for position in replacement_positions
    )
    error = max(
        fixed_income_constraint_error(result, bounds),
        abs(float(style_result.sum() - result.convertible_bond)),
        float(max(0.0, -style_result.min(), -path_replacement_min)),
    )
    if error > tolerance or not np.isfinite(result).all():
        raise DataUnavailable("CONSTRAINT_VIOLATION", f"error={error}")
    fitted = (
        endpoint_matrix @ result.iloc[:-1].to_numpy()
        + endpoint_style_matrix @ style_result
        + known_anchor
        - base[:, -1] * result.financing
    )
    residual = target - fitted
    result["ordinary_bond"] = result[
        ["rate_short", "rate_long", "credit_short", "credit_long"]
    ].sum()
    result["stock_weight"] = result[["hk", *SW_CODES]].sum()
    result["a_stock_weight"] = result[list(SW_CODES)].sum()
    result["gross_assets"] = result[list(FIXED_INCOME_ASSETS)].sum()
    result["non_equity"] = 1 - result.stock_weight
    stats = {
        "solver_status": problem.status,
        "constraint_error": float(error),
        "return_mae": float(np.abs(residual).mean()),
        "r2": float(1 - residual @ residual / np.sum((target - target.mean()) ** 2)),
        "condition_number": float(
            min(np.linalg.cond(np.c_[endpoint_matrix, endpoint_style_matrix]), 1e300)
        ),
        "window_observations": len(y),
        "constraint_source": bounds.source,
        "weighting": weighting,
        "prior_known_coefficients": (
            int(np.isfinite(prior_array).sum()) if prior_array is not None else 0
        ),
        "fixed_asset_coefficients": fixed_count,
        "cbond_exposure_mode": "style_decomposed_linear_endpoint",
        "cbond_anchor_weight": anchor,
        "cbond_anchor_date": str(anchor_timestamp.date()),
        "cbond_endpoint_date": str(endpoint_date.date()),
        "cbond_style_equity_like": float(style_result[0]),
        "cbond_style_balanced": float(style_result[1]),
        "cbond_style_bond_like": float(style_result[2]),
        "cbond_style_total": float(style_result.sum()),
        "cbond_style_shift_l1": float(np.abs(style_result - anchor_style).sum()),
    }
    return result, stats


def convertible_total_return_levels(
    raw: pd.DataFrame,
    *,
    return_mode: str = "vendor_adjusted_preclose",
    cashflow_threshold: float = 0.05,
    cashflow_upper: float = 5.0,
) -> pd.DataFrame:
    """Turn the exchange full-price return field into a constituent return index.

    ``pct_chg`` is calculated from the vendor's adjusted previous close.  The
    adjustment is material on coupon dates, while the exchange quote itself is
    a full price.  ``reference_cashflow_reconstructed`` is a frozen data
    ablation: when the previous observed close exceeds the current reference
    close by a plausible coupon amount, it uses that difference as an inferred
    cash flow and calculates the exact holder return.  It remains conditional
    evidence because the entitlement and payment documents are not available.

    In either mode a constituent with a long suspension, redemption or
    delisting gap is rejected later and its portfolio weight falls back to the
    official broad total-return index.
    """
    columns = [
        "date",
        "security_code",
        "adjusted_close",
        "return",
        "ann_date",
        "source",
        "return_basis",
        "evidence_status",
        "close",
        "bond_value",
        "bond_over_rate",
        "cb_value",
        "cb_over_rate",
    ]
    if raw.empty:
        return pd.DataFrame(columns=columns)
    allowed_modes = {"vendor_adjusted_preclose", "reference_cashflow_reconstructed"}
    if return_mode not in allowed_modes:
        raise DataUnavailable("INVALID_CBOND_RETURN_MODE", return_mode)
    if cashflow_threshold < 0 or cashflow_upper <= cashflow_threshold:
        raise DataUnavailable(
            "INVALID_CBOND_CASHFLOW_RANGE",
            f"{cashflow_threshold} / {cashflow_upper}",
        )
    required = {"date", "security_code", "pct_chg"}
    if not required.issubset(raw):
        raise DataUnavailable(
            "CBOND_RETURN_SCHEMA", ", ".join(sorted(required - set(raw)))
        )
    data = dates(raw, ("date",)).sort_values(["security_code", "date"]).copy()
    require_unique(data, ["date", "security_code"], "convertible-bond daily returns")
    data["return"] = pd.to_numeric(data.pct_chg, errors="coerce") / 100.0
    invalid = data["return"].isna() | ~np.isfinite(data["return"]) | data["return"].le(-1)
    if invalid.any():
        sample = data.loc[invalid, ["security_code", "date"]].head(5).to_dict("records")
        raise DataUnavailable("INVALID_CBOND_RETURN", str(sample))
    if {"close", "pre_close"}.issubset(data):
        positive = data.close.gt(0) & data.pre_close.gt(0)
        formula = data.close / data.pre_close - 1
        # pct_chg has four decimal places in percent units.  One basis point in
        # percent units comfortably covers its rounding error without accepting
        # a different return definition.
        mismatch = positive & (formula.sub(data["return"]).abs() > 0.0001)
        if mismatch.any():
            sample = data.loc[
                mismatch, ["security_code", "date", "close", "pre_close", "pct_chg"]
            ].head(5)
            raise DataUnavailable("CBOND_RETURN_FORMULA_MISMATCH", sample.to_json())
    elif return_mode == "reference_cashflow_reconstructed":
        raise DataUnavailable("CBOND_CASHFLOW_SCHEMA", "close, pre_close")
    if return_mode == "reference_cashflow_reconstructed":
        previous_close = data.groupby("security_code", sort=False)["close"].shift()
        reference_adjustment = previous_close - data["pre_close"]
        inferred = (
            previous_close.gt(0)
            & data["close"].gt(0)
            & data["pre_close"].gt(0)
            & reference_adjustment.gt(cashflow_threshold)
            & reference_adjustment.le(cashflow_upper)
        )
        reconstructed = (data["close"] + reference_adjustment) / previous_close - 1
        data.loc[inferred, "return"] = reconstructed.loc[inferred]
        data["source"] = "rawdata.cbond_daily:close+reference_adjustment"
        data["return_basis"] = "exchange_full_price_inferred_coupon_cashflow_CNY"
        data["evidence_status"] = "conditional_reference_cashflow_reconstruction"
    else:
        data["source"] = "rawdata.cbond_daily:pct_chg"
        data["return_basis"] = "exchange_full_price_adjusted_preclose_CNY"
        data["evidence_status"] = "conditional_security_total_return_proxy"
    data["adjusted_close"] = (
        data.groupby("security_code", sort=False)["return"]
        .transform(lambda value: (1 + value).cumprod())
    )
    data["ann_date"] = data.date
    for column in (
        "close",
        "bond_value",
        "bond_over_rate",
        "cb_value",
        "cb_over_rate",
    ):
        data[column] = pd.to_numeric(data.get(column), errors="coerce")
    return data[columns]


def convertible_style_factor_panel(
    panel: pd.DataFrame,
    prices: pd.DataFrame,
    cutoff,
    *,
    equity_premium_max: float = 20.0,
    bond_premium_min: float = 50.0,
    minimum_constituents: int = 5,
) -> tuple[pd.DataFrame, dict]:
    """Add three lag-classified convertible style returns to a factor panel.

    A security's return on date t is assigned using its conversion premium on
    its preceding observed trading date. This avoids classifying a same-day
    return with a close-dependent end-of-day premium. Sparse buckets fall back
    to the official broad convertible return and are counted explicitly.
    """
    if not 0 <= equity_premium_max < bond_premium_min:
        raise DataUnavailable(
            "INVALID_CBOND_STYLE_THRESHOLDS",
            f"{equity_premium_max} / {bond_premium_min}",
        )
    if minimum_constituents < 1:
        raise DataUnavailable(
            "INVALID_CBOND_STYLE_MINIMUM", str(minimum_constituents)
        )
    required = {"date", "security_code", "return", "cb_over_rate"}
    if not required.issubset(prices):
        raise DataUnavailable(
            "NO_CBOND_STYLE_HISTORY", ", ".join(sorted(required - set(prices)))
        )
    data = dates(prices, ("date", "ann_date"))
    data = available(
        data.loc[data.date.le(pd.Timestamp(cutoff))], pd.Timestamp(cutoff)
    ).sort_values(["security_code", "date"])
    require_unique(data, ["date", "security_code"], "convertible style history")
    data["return"] = pd.to_numeric(data["return"], errors="coerce")
    data["cb_over_rate"] = pd.to_numeric(data["cb_over_rate"], errors="coerce")
    data["lagged_cb_over_rate"] = data.groupby("security_code", sort=False)[
        "cb_over_rate"
    ].shift()
    valid = (
        data["return"].notna()
        & data["lagged_cb_over_rate"].notna()
        & np.isfinite(data["return"])
        & np.isfinite(data["lagged_cb_over_rate"])
        & data["return"].gt(-1)
    )
    data = data.loc[valid].copy()
    if data.empty:
        raise DataUnavailable("NO_CBOND_STYLE_HISTORY", "No lag-classified returns")
    data["style"] = np.select(
        [
            data.lagged_cb_over_rate.le(equity_premium_max),
            data.lagged_cb_over_rate.ge(bond_premium_min),
        ],
        [CBOND_STYLE_COLUMNS[0], CBOND_STYLE_COLUMNS[2]],
        default=CBOND_STYLE_COLUMNS[1],
    )
    grouped = data.groupby(["date", "style"], observed=True).agg(
        style_return=("return", "mean"),
        constituent_count=("security_code", "nunique"),
    )
    returns = grouped.style_return.unstack("style")
    counts = grouped.constituent_count.unstack("style")
    result = panel.copy()
    fallback_counts = {}
    for column in CBOND_STYLE_COLUMNS:
        values = returns.get(column, pd.Series(dtype=float)).reindex(result.index)
        count = counts.get(column, pd.Series(dtype=float)).reindex(result.index)
        fallback = values.isna() | count.fillna(0).lt(minimum_constituents)
        result[column] = values.where(~fallback, result["convertible_bond"])
        fallback_counts[column] = int(fallback.sum())
    if result[list(CBOND_STYLE_COLUMNS)].isna().any().any():
        raise DataUnavailable(
            "MISSING_CBOND_STYLE_FACTOR", "Broad fallback is also missing"
        )
    return result, {
        "cbond_style_definition": (
            f"lagged_conversion_premium:<={equity_premium_max:g}/"
            f">={bond_premium_min:g}pct"
        ),
        "cbond_style_weighting": "equal_weight_available_securities",
        "cbond_style_minimum_constituents": int(minimum_constituents),
        "cbond_style_fallback_days": fallback_counts,
        "cbond_style_lookahead_control": "previous_security_observation",
    }


def disclosed_convertible_style_prior(
    holdings: pd.DataFrame,
    prices: pd.DataFrame,
    report_date,
    total_cbond_weight: float,
    cutoff,
    *,
    equity_premium_max: float = 20.0,
    bond_premium_min: float = 50.0,
    max_price_age_days: int = 14,
    disclosure_rounding_tolerance: float = 0.0005,
) -> tuple[pd.Series, dict]:
    """Classify a disclosed CB basket using prices known at its report date."""
    if not 0 <= equity_premium_max < bond_premium_min:
        raise DataUnavailable("INVALID_CBOND_STYLE_THRESHOLDS", "style prior")
    total = float(total_cbond_weight)
    if not np.isfinite(total) or total <= 0:
        raise DataUnavailable("INVALID_CBOND_CONTROL", str(total_cbond_weight))
    if holdings.empty or holdings.weight.isna().any() or (holdings.weight < 0).any():
        raise DataUnavailable("INVALID_CBOND_HOLDINGS", "style prior")
    target = pd.Timestamp(report_date).normalize()
    data = dates(prices, ("date", "ann_date"))
    data = available(
        data.loc[
            data.security_code.isin(holdings.security_code)
            & data.date.le(target)
        ],
        cutoff,
    )
    if "cb_over_rate" not in data:
        raise DataUnavailable("NO_CBOND_STYLE_HISTORY", "cb_over_rate")
    data["cb_over_rate"] = pd.to_numeric(data.cb_over_rate, errors="coerce")
    latest = (
        data.sort_values(["security_code", "date"])
        .groupby("security_code", as_index=False)
        .tail(1)
    )
    latest["age_days"] = (target - latest.date).dt.days
    latest = latest.loc[
        latest.cb_over_rate.notna()
        & latest.age_days.between(0, max_price_age_days)
    ]
    detail = holdings[["security_code", "weight"]].merge(
        latest[["security_code", "cb_over_rate"]],
        on="security_code",
        how="left",
        validate="one_to_one",
    )
    disclosed_total = float(detail.weight.sum())
    reconciliation_adjustment = 0.0
    if disclosed_total > total:
        overage = disclosed_total - total
        if overage > disclosure_rounding_tolerance:
            raise DataUnavailable(
                "CBOND_STYLE_PRIOR_RECONCILIATION",
                f"holdings={disclosed_total:.6f}, control={total:.6f}",
            )
        # Source weights are rounded to four decimals.  When their displayed
        # sum is one to five basis points above the independently reported
        # asset-allocation control, reconcile the basket proportionally rather
        # than losing the whole validation row.
        detail["weight"] *= total / disclosed_total
        reconciliation_adjustment = total - disclosed_total
    detail["style"] = np.select(
        [
            detail.cb_over_rate.le(equity_premium_max),
            detail.cb_over_rate.ge(bond_premium_min),
        ],
        [CBOND_STYLE_COLUMNS[0], CBOND_STYLE_COLUMNS[2]],
        default=CBOND_STYLE_COLUMNS[1],
    )
    missing = detail.cb_over_rate.isna()
    prior = (
        detail.groupby("style", observed=True).weight.sum()
        .reindex(CBOND_STYLE_COLUMNS, fill_value=0.0)
        .astype(float)
    )
    residual = max(0.0, total - float(prior.sum()))
    # The balanced bucket is the declared proxy for undisclosed or unclassified
    # NAV weight; it is never silently moved to cash.
    prior.loc[CBOND_STYLE_COLUMNS[1]] += residual
    if abs(float(prior.sum()) - total) > 1e-6:
        raise DataUnavailable(
            "CBOND_STYLE_PRIOR_RECONCILIATION",
            f"prior={prior.sum():.6f}, control={total:.6f}",
        )
    return prior, {
        "cbond_style_prior_equity_like": float(prior.iloc[0]),
        "cbond_style_prior_balanced": float(prior.iloc[1]),
        "cbond_style_prior_bond_like": float(prior.iloc[2]),
        "cbond_style_prior_proxy_weight": float(
            detail.loc[missing, "weight"].sum() + residual
        ),
        "cbond_style_prior_classified_security_count": int((~missing).sum()),
        "cbond_style_prior_reconciliation_adjustment": float(
            reconciliation_adjustment
        ),
    }


def convertible_dominant_constraints(
    bounds: FixedIncomeConstraints,
    configured_cbond_upper: float | None = None,
    configured_stock_upper: float | None = None,
) -> FixedIncomeConstraints:
    """Allow convertible assets to use the verified gross-asset capacity.

    A convertible-dominant fund can disclose convertible assets above NAV when
    it is leveraged.  Capping the convertible coefficient at one mechanically
    transfers the excess return to direct stocks.  The balance-sheet identity
    and gross-assets bound still apply.
    """
    requested = (
        bounds.gross_assets_upper
        if configured_cbond_upper is None
        else float(configured_cbond_upper)
    )
    return replace(
        bounds,
        stock_upper=(
            bounds.stock_upper
            if configured_stock_upper is None
            else max(bounds.stock_upper, float(configured_stock_upper))
        ),
        cbond_upper=min(
            bounds.gross_assets_upper, max(bounds.cbond_upper, requested)
        ),
        source=f"{bounds.source}|cbond_up_to_gross_assets",
    )


def disclosure_anchored_stock_constraints(
    bounds: FixedIncomeConstraints,
    asset_reports: pd.DataFrame,
    fund_code: str,
    at_date,
    cutoff,
) -> tuple[FixedIncomeConstraints, dict]:
    """Fix direct-stock total at the latest independently disclosed NAV weight.

    Convertible returns contain an embedded equity option.  A return regression
    cannot reliably distinguish that option beta from direct stock.  For this
    model family the direct-stock *total* is therefore carried from the latest
    public asset allocation, while the regression can still distribute that
    fixed total across SW industries.  The anchor date and age are always
    exposed as diagnostics.
    """
    target = pd.Timestamp(at_date).normalize()
    reports = dates(asset_reports, ("report_date", "ann_date"))
    reports = available(
        reports.loc[
            reports.fund_code.eq(fund_code)
            & reports.report_date.le(target)
            & reports.stock_weight.notna()
        ],
        cutoff,
    )
    if reports.empty:
        raise DataUnavailable("NO_PUBLISHED_STOCK_ANCHOR", fund_code)
    report = reports.sort_values(["report_date", "ann_date"]).iloc[-1]
    stock = float(report.stock_weight)
    if (
        not np.isfinite(stock)
        or stock < bounds.stock_lower - 1e-8
        or stock > bounds.stock_upper + 1e-8
    ):
        raise DataUnavailable(
            "STOCK_ANCHOR_OUTSIDE_CONTRACT",
            f"stock={stock:.6f}, bounds={bounds.stock_lower:.6f}/{bounds.stock_upper:.6f}",
        )
    return (
        replace(
            bounds,
            stock_lower=stock,
            stock_upper=stock,
            source=f"{bounds.source}|stock_total_last_public_disclosure",
        ),
        {
            "stock_total_method": "last_public_asset_allocation_anchor",
            "stock_anchor_weight": stock,
            "stock_anchor_report_date": str(report.report_date.date()),
            "stock_anchor_ann_date": str(report.ann_date.date()),
            "stock_anchor_age_days": int((target - report.report_date).days),
        },
    )


def latest_published_convertible_holdings(
    holdings: pd.DataFrame,
    asset_reports: pd.DataFrame,
    fund_code: str,
    at_date,
    cutoff,
    *,
    reconciliation_tolerance: float = 0.005,
) -> tuple[pd.DataFrame, dict]:
    """Return the latest PIT convertible disclosure and its NAV control total."""
    target = pd.Timestamp(at_date).normalize()
    cutoff = pd.Timestamp(cutoff).normalize()
    reports = dates(asset_reports, ("report_date", "ann_date"))
    reports = available(
        reports.loc[
            reports.fund_code.eq(fund_code)
            & reports.report_date.le(target)
            & reports.convertible_bond_weight.notna()
        ],
        cutoff,
    )
    if reports.empty:
        raise DataUnavailable(
            "NO_PUBLISHED_CBOND_CONTROL", f"{fund_code} at {cutoff.date()}"
        )
    report = reports.sort_values(["report_date", "ann_date"]).iloc[-1]
    total = float(report.convertible_bond_weight)
    if not np.isfinite(total) or total <= 0:
        raise DataUnavailable(
            "NO_POSITIVE_CBOND_CONTROL", f"{fund_code}: {total}"
        )
    detail = dates(holdings, ("report_date", "ann_date"))
    detail = available(
        detail.loc[
            detail.fund_code.eq(fund_code)
            & detail.report_date.eq(report.report_date)
        ],
        cutoff,
    )
    if detail.empty:
        raise DataUnavailable(
            "NO_PUBLISHED_CBOND_HOLDINGS",
            f"{fund_code}: {report.report_date.date()}",
        )
    if detail.security_code.isna().any() or detail.weight.isna().any():
        raise DataUnavailable(
            "INCOMPLETE_CBOND_HOLDINGS",
            f"{fund_code}: {report.report_date.date()}",
        )
    detail = detail.loc[detail.weight.gt(0)].copy()
    if detail.empty or (detail.weight < 0).any():
        raise DataUnavailable("INVALID_CBOND_HOLDINGS", fund_code)
    # Some source reports split one security into repeated lines.  Combining the
    # reported NAV weights is deterministic and avoids double-counting.
    descriptive = [
        column
        for column in ("security_name", "source", "announcement_source")
        if column in detail
    ]
    aggregations = {"weight": "sum"} | {column: "first" for column in descriptive}
    detail = detail.groupby("security_code", as_index=False).agg(aggregations)
    require_unique(detail, ["security_code"], "published convertible holdings")
    disclosed = float(detail.weight.sum())
    if disclosed > total + reconciliation_tolerance:
        raise DataUnavailable(
            "CBOND_HOLDINGS_EXCEED_CONTROL",
            f"detail={disclosed:.6f}, control={total:.6f}",
        )
    coverage = min(1.0, disclosed / total)
    return detail, {
        "cbond_holdings_report_date": str(report.report_date.date()),
        "cbond_holdings_ann_date": str(report.ann_date.date()),
        "cbond_control_weight": total,
        "cbond_disclosed_weight": disclosed,
        "cbond_disclosure_coverage": coverage,
        "cbond_disclosed_security_count": int(detail.security_code.nunique()),
    }


def personalized_convertible_panel(
    panel: pd.DataFrame,
    holdings: pd.DataFrame,
    prices: pd.DataFrame,
    report_date,
    total_cbond_weight: float,
    cutoff,
    *,
    max_price_age_days: int = 14,
    endpoint_date=None,
) -> tuple[pd.DataFrame, dict]:
    """Replace the broad convertible factor with a PIT disclosed basket.

    Each security is held buy-and-hold from the report-date anchor.  Its NAV
    weight is divided by the independently disclosed total convertible weight.
    Unreported or unusable securities retain the official broad total-return
    factor.  The basket is applied only after the report date; earlier returns
    stay broad because end-of-period holdings do not describe the prior period.
    """
    result = panel.copy()
    if "convertible_bond" not in result:
        raise DataUnavailable("NO_BROAD_CBOND_FACTOR", "convertible_bond")
    if not np.isfinite(total_cbond_weight) or total_cbond_weight <= 0:
        raise DataUnavailable("INVALID_CBOND_CONTROL", str(total_cbond_weight))
    target_index = pd.DatetimeIndex(result.index).normalize().sort_values().unique()
    factor_end = min(
        pd.Timestamp(cutoff).normalize(),
        (
            pd.Timestamp(endpoint_date).normalize()
            if endpoint_date is not None
            else pd.Timestamp(cutoff).normalize()
        ),
    )
    usable_index = target_index[target_index <= factor_end]
    anchor_candidates = usable_index[usable_index <= pd.Timestamp(report_date)]
    if not len(anchor_candidates):
        raise DataUnavailable("CBOND_ANCHOR_OUTSIDE_FACTOR_HISTORY", str(report_date))
    anchor = anchor_candidates[-1]
    active_index = usable_index[usable_index >= anchor]
    if len(active_index) < 2:
        raise DataUnavailable("CBOND_FACTOR_WINDOW_EMPTY", str(anchor.date()))
    broad_returns = result.loc[active_index, "convertible_bond"]
    if broad_returns.isna().any() or (broad_returns <= -1).any():
        raise DataUnavailable("INVALID_BROAD_CBOND_FACTOR", str(anchor.date()))
    broad_level = (1 + broad_returns).cumprod()
    broad_level = broad_level / broad_level.iloc[0]

    data = dates(prices, ("date", "ann_date"))
    data = available(
        data.loc[
            data.security_code.isin(holdings.security_code)
            & data.date.le(pd.Timestamp(cutoff))
        ],
        cutoff,
    )
    require_unique(data, ["date", "security_code"], "convertible total-return levels")
    weights = holdings.set_index("security_code").weight.astype(float) / total_cbond_weight
    valid_levels: dict[str, pd.Series] = {}
    invalid: list[dict] = []
    target_dates = pd.Series(active_index, index=active_index)
    for security_code, weight in weights.items():
        observed = (
            data.loc[data.security_code.eq(security_code), ["date", "adjusted_close"]]
            .dropna()
            .set_index("date")
            .adjusted_close
            .sort_index()
        )
        if observed.empty:
            invalid.append({"security_code": security_code, "reason": "no_return_history"})
            continue
        joined = observed.index.union(active_index).sort_values()
        carried = observed.reindex(joined).ffill().reindex(active_index)
        source_dates = (
            pd.Series(observed.index, index=observed.index)
            .reindex(joined)
            .ffill()
            .reindex(active_index)
        )
        age = target_dates - source_dates
        if (
            carried.isna().any()
            or (carried <= 0).any()
            or age.isna().any()
            or age.dt.days.gt(max_price_age_days).any()
        ):
            invalid.append(
                {"security_code": security_code, "reason": "missing_or_stale_return_history"}
            )
            continue
        valid_levels[security_code] = carried / carried.loc[anchor]

    priced_weight = float(weights.reindex(valid_levels).sum()) if valid_levels else 0.0
    priced_weight = min(1.0, max(0.0, priced_weight))
    composite = broad_level * (1 - priced_weight)
    for security_code, level in valid_levels.items():
        composite = composite + level * float(weights[security_code])
    personalized = composite.pct_change(fill_method=None)
    personalized.iloc[0] = broad_returns.iloc[0]
    result.loc[active_index[1:], "convertible_bond"] = personalized.iloc[1:]
    return result, {
        "cbond_factor_mode": (
            "published_constituents_plus_official_broad_residual"
            if priced_weight > 0
            else "official_broad_fallback"
        ),
        "cbond_priced_coverage": priced_weight,
        "cbond_proxy_ratio": 1 - priced_weight,
        "cbond_personalized_security_count": len(valid_levels),
        "cbond_unusable_security_count": len(invalid),
        "cbond_unusable_securities": invalid[:20],
        "cbond_factor_anchor_date": str(anchor.date()),
        "cbond_factor_endpoint_date": str(active_index[-1].date()),
        "cbond_factor_growth_since_anchor": float(composite.iloc[-1]),
        "cbond_constituent_return_basis": "exchange_full_price_adjusted_preclose_CNY",
        "cbond_constituent_evidence_status": "conditional_security_total_return_proxy",
    }


def mark_to_market_convertible_weight(
    nav: pd.DataFrame,
    *,
    control_weight: float,
    anchor_date,
    endpoint_date,
    factor_growth: float,
    cutoff,
) -> tuple[float, dict]:
    """Roll the last disclosed convertible NAV weight to the current date.

    This is the no-trading accounting identity: disclosed market value grows by
    the reconstructed convertible-bond basket return, then is divided by the
    fund's adjusted-NAV growth over the same dates.  It is an observable anchor
    for the dedicated model, not an assertion that the manager did not trade.
    """
    anchor = pd.Timestamp(anchor_date).normalize()
    endpoint = pd.Timestamp(endpoint_date).normalize()
    if endpoint <= anchor:
        raise DataUnavailable(
            "CBOND_MARK_ENDPOINT_NOT_AFTER_ANCHOR",
            f"{anchor.date()} / {endpoint.date()}",
        )
    if (
        not np.isfinite(control_weight)
        or control_weight < 0
        or not np.isfinite(factor_growth)
        or factor_growth <= 0
    ):
        raise DataUnavailable("INVALID_CBOND_MARK_INPUT", "weight or growth")
    values = available_nav(dates(nav, ("date", "ann_date")), cutoff).sort_values(
        "date"
    )
    anchor_rows = values.loc[values.date.le(anchor)]
    endpoint_rows = values.loc[values.date.eq(endpoint)]
    if anchor_rows.empty or endpoint_rows.empty:
        raise DataUnavailable(
            "CBOND_MARK_NAV_MISSING", f"{anchor.date()} / {endpoint.date()}"
        )
    anchor_row = anchor_rows.iloc[-1]
    endpoint_row = endpoint_rows.iloc[-1]
    nav_growth = float(endpoint_row.adj_nav / anchor_row.adj_nav)
    if not np.isfinite(nav_growth) or nav_growth <= 0:
        raise DataUnavailable("INVALID_CBOND_MARK_NAV_GROWTH", str(nav_growth))
    estimate = float(control_weight * factor_growth / nav_growth)
    return estimate, {
        "cbond_mark_to_market_weight": estimate,
        "cbond_mark_nav_anchor_date": str(anchor_row.date.date()),
        "cbond_mark_nav_endpoint_date": str(endpoint_row.date.date()),
        "cbond_mark_nav_growth": nav_growth,
        "cbond_mark_method": (
            "published_holdings_full_price_growth_over_fund_nav_growth"
        ),
    }
