#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Full Barra Style Factor Calculators.

This module implements production-grade Barra CNE5-style factor calculations:
- Multi-indicator composites (Value, Liquidity, Growth, Leverage)
- Industry neutralization
- EWMA weighting (Beta, Momentum, ResVol)
- Bayesian shrinkage (Beta)
- Robust winsorization and standardization

Reference: MSCI Barra CNE5 Model Handbook
"""

from __future__ import annotations

from typing import Optional, Tuple
import numpy as np
import pandas as pd


# =============================================================================
# Utility Functions
# =============================================================================

def winsorize_series(
    s: pd.Series, 
    lower_q: float = 0.01, 
    upper_q: float = 0.99,
    lower_bound: Optional[float] = None,
    upper_bound: Optional[float] = None,
) -> pd.Series:
    """Winsorize a series by quantiles or absolute bounds.
    
    Args:
        s: Input series
        lower_q: Lower quantile for clipping (default 1%)
        upper_q: Upper quantile for clipping (default 99%)
        lower_bound: Optional absolute lower bound (overrides lower_q)
        upper_bound: Optional absolute upper bound (overrides upper_q)
    
    Returns:
        Winsorized series
    """
    s = pd.to_numeric(s, errors="coerce")
    valid = s.dropna()
    if valid.empty:
        return s
    
    lo = lower_bound if lower_bound is not None else float(valid.quantile(lower_q))
    hi = upper_bound if upper_bound is not None else float(valid.quantile(upper_q))
    return s.clip(lower=lo, upper=hi)


def weighted_zscore(x: pd.Series, w: pd.Series) -> pd.Series:
    """Compute market-cap weighted z-score.
    
    Args:
        x: Values to standardize
        w: Weights (typically sqrt(market_cap))
    
    Returns:
        Standardized series with weighted mean=0, std≈1
    """
    x = pd.to_numeric(x, errors="coerce")
    w = pd.to_numeric(w, errors="coerce")
    mask = x.notna() & w.notna() & (w > 0)
    
    if mask.sum() < 2:
        return pd.Series(np.nan, index=x.index)
    
    xv = x[mask].astype(float)
    wv = w[mask].astype(float)
    wsum = float(wv.sum())
    
    if wsum <= 0:
        return pd.Series(np.nan, index=x.index)
    
    mean = float((xv * wv).sum() / wsum)
    var = float(((xv - mean) ** 2 * wv).sum() / wsum)
    std = float(np.sqrt(var))
    
    if not np.isfinite(std) or std <= 0:
        return pd.Series(np.nan, index=x.index)
    
    return (x - mean) / std


def industry_neutralize(
    x: pd.Series, 
    industry_codes: pd.Series,
    weights: Optional[pd.Series] = None,
) -> pd.Series:
    """Neutralize a factor by industry (regress out industry effects).
    
    Args:
        x: Factor values to neutralize
        industry_codes: Industry classification for each stock
        weights: Optional weights for weighted regression
    
    Returns:
        Residuals after removing industry effects (demeaned within each industry)
    """
    x = pd.to_numeric(x, errors="coerce")
    mask = x.notna() & industry_codes.notna()
    
    if mask.sum() < 10:
        return x
    
    # Simple approach: demean within each industry
    result = x.copy()
    
    if weights is not None:
        w = weights.copy()
        w = w.fillna(0)
        w = np.maximum(w, 0)
    else:
        w = pd.Series(1.0, index=x.index)
    
    # Calculate weighted mean per industry and subtract
    df = pd.DataFrame({"x": x, "industry": industry_codes, "w": w})
    df = df[mask]
    
    # Weighted mean per industry
    def weighted_mean(group):
        w_sum = group["w"].sum()
        if w_sum <= 0:
            return group["x"].mean()
        return (group["x"] * group["w"]).sum() / w_sum
    
    industry_means = df.groupby("industry").apply(weighted_mean, include_groups=False)
    
    # Subtract industry mean from each observation
    result_masked = x[mask] - industry_codes[mask].map(industry_means)
    
    result = pd.Series(np.nan, index=x.index)
    result.loc[mask] = result_masked
    
    return result


def exponential_weights(window: int, half_life: int) -> np.ndarray:
    """Generate exponential decay weights for EWMA calculations.
    
    Args:
        window: Number of periods
        half_life: Half-life in periods
    
    Returns:
        Normalized weights array (most recent weight first)
    """
    decay = np.log(2) / half_life
    weights = np.exp(-decay * np.arange(window))
    return weights / weights.sum()


# =============================================================================
# Size Factor
# =============================================================================

def calculate_size(
    ff_mcap: pd.Series,
    industry_codes: Optional[pd.Series] = None,
    neutralize: bool = False,
    weights: Optional[pd.Series] = None,
) -> pd.Series:
    """Calculate Size factor (log market cap, optionally industry-neutralized).
    
    Args:
        ff_mcap: Free-float market cap
        industry_codes: Industry classification for neutralization
        neutralize: Whether to apply industry neutralization
        weights: Weights for zscore calculation
    
    Returns:
        Standardized Size factor
    """
    # Log transform
    log_mcap = ff_mcap.apply(lambda x: np.log(x) if pd.notna(x) and x > 0 else np.nan)
    
    # Optional industry neutralization
    if neutralize and industry_codes is not None:
        log_mcap = industry_neutralize(log_mcap, industry_codes, weights)
    
    # Winsorize and standardize
    log_mcap = winsorize_series(log_mcap)
    
    if weights is not None:
        return weighted_zscore(log_mcap, weights)
    return log_mcap


# =============================================================================
# Value Factor (Multi-indicator Composite)
# =============================================================================

def calculate_value(
    pe_ttm: pd.Series,
    pb: pd.Series,
    ps_ttm: pd.Series,
    dv_ttm: pd.Series,
    weights: pd.Series,
    cf_to_price: Optional[pd.Series] = None,
) -> pd.Series:
    """Calculate Value factor as composite of E/P, B/P, S/P, DY, CF/P.
    
    Args:
        pe_ttm: Price-to-earnings (TTM)
        pb: Price-to-book
        ps_ttm: Price-to-sales (TTM)
        dv_ttm: Dividend yield (TTM, in %)
        weights: Market cap weights for zscore
        cf_to_price: Optional cash flow to price ratio
    
    Returns:
        Composite Value factor (standardized)
    """
    indicators = []
    
    # E/P (earnings yield)
    ep = pe_ttm.apply(lambda x: 1.0 / x if pd.notna(x) and x != 0 else np.nan)
    ep = winsorize_series(ep)
    ep_z = weighted_zscore(ep, weights)
    indicators.append(ep_z)
    
    # B/P (book-to-price)
    bp = pb.apply(lambda x: 1.0 / x if pd.notna(x) and x != 0 else np.nan)
    bp = winsorize_series(bp)
    bp_z = weighted_zscore(bp, weights)
    indicators.append(bp_z)
    
    # S/P (sales yield)
    sp = ps_ttm.apply(lambda x: 1.0 / x if pd.notna(x) and x != 0 else np.nan)
    sp = winsorize_series(sp)
    sp_z = weighted_zscore(sp, weights)
    indicators.append(sp_z)
    
    # DY (dividend yield, already in %)
    dy = dv_ttm / 100.0  # Convert from % to decimal
    dy = winsorize_series(dy)
    dy_z = weighted_zscore(dy, weights)
    indicators.append(dy_z)
    
    # CF/P (optional)
    if cf_to_price is not None:
        cfp = winsorize_series(cf_to_price)
        cfp_z = weighted_zscore(cfp, weights)
        indicators.append(cfp_z)
    
    # Equal-weight composite
    composite = pd.concat(indicators, axis=1)
    value_raw = composite.mean(axis=1, skipna=True)
    
    # Final standardization
    return weighted_zscore(value_raw, weights)


# =============================================================================
# Liquidity Factor (Multi-window Composite)
# =============================================================================

def calculate_liquidity(
    turnover_21d: pd.Series,
    turnover_63d: pd.Series,
    turnover_252d: pd.Series,
    amount_to_mv: Optional[pd.Series],
    weights: pd.Series,
) -> pd.Series:
    """Calculate Liquidity factor as composite of multi-window turnover.
    
    Args:
        turnover_21d: 21-day average turnover rate
        turnover_63d: 63-day average turnover rate
        turnover_252d: 252-day average turnover rate
        amount_to_mv: Optional trading amount / market value ratio
        weights: Market cap weights for zscore
    
    Returns:
        Composite Liquidity factor (standardized)
    """
    indicators = []
    
    # Log-transform turnover rates (they are typically right-skewed)
    for turn, name in [
        (turnover_21d, "turn_21d"),
        (turnover_63d, "turn_63d"),
        (turnover_252d, "turn_252d"),
    ]:
        log_turn = turn.apply(lambda x: np.log(x) if pd.notna(x) and x > 0 else np.nan)
        log_turn = winsorize_series(log_turn)
        z = weighted_zscore(log_turn, weights)
        indicators.append(z)
    
    # Amount/MV ratio (optional)
    if amount_to_mv is not None:
        log_amv = amount_to_mv.apply(lambda x: np.log(x) if pd.notna(x) and x > 0 else np.nan)
        log_amv = winsorize_series(log_amv)
        amv_z = weighted_zscore(log_amv, weights)
        indicators.append(amv_z)
    
    # Equal-weight composite
    composite = pd.concat(indicators, axis=1)
    liquidity_raw = composite.mean(axis=1, skipna=True)
    
    # Final standardization
    return weighted_zscore(liquidity_raw, weights)


# =============================================================================
# Beta Factor (EWMA + Bayesian Shrinkage)
# =============================================================================

def calculate_beta(
    stock_returns: pd.DataFrame,
    market_returns: pd.Series,
    weights: pd.Series,
    window: int = 252,
    half_life: int = 63,
    shrinkage_factor: float = 0.3,
) -> pd.Series:
    """Calculate Beta factor with EWMA weighting and Bayesian shrinkage.
    
    Args:
        stock_returns: DataFrame with columns as tickers, rows as dates
        market_returns: Market index returns (same dates as rows)
        weights: Market cap weights for final zscore
        window: Lookback window for regression
        half_life: Half-life for exponential weighting
        shrinkage_factor: Shrinkage toward 1.0 (0 = no shrink, 1 = full shrink)
    
    Returns:
        Standardized Beta factor
    """
    # Compute exponential weights
    exp_weights = exponential_weights(window, half_life)
    
    betas = {}
    for ticker in stock_returns.columns:
        stock_ret = stock_returns[ticker].dropna()
        common_idx = stock_ret.index.intersection(market_returns.index)
        
        if len(common_idx) < 60:  # Minimum observations
            betas[ticker] = np.nan
            continue
        
        # Take most recent 'window' observations
        common_idx = common_idx[-window:] if len(common_idx) > window else common_idx
        
        y = stock_ret.loc[common_idx].values
        x = market_returns.loc[common_idx].values
        w = exp_weights[-len(common_idx):]  # Align weights
        
        # Weighted OLS: β = Σ(w*x*y) / Σ(w*x²)
        try:
            x_mean = np.sum(w * x)
            y_mean = np.sum(w * y)
            cov_xy = np.sum(w * (x - x_mean) * (y - y_mean))
            var_x = np.sum(w * (x - x_mean) ** 2)
            
            if var_x > 1e-10:
                beta_raw = cov_xy / var_x
            else:
                beta_raw = 1.0
        except Exception:
            beta_raw = 1.0
        
        # Bayesian shrinkage toward 1.0
        beta_shrunk = shrinkage_factor * 1.0 + (1 - shrinkage_factor) * beta_raw
        betas[ticker] = beta_shrunk
    
    beta_series = pd.Series(betas)
    beta_series = winsorize_series(beta_series, lower_q=0.005, upper_q=0.995)
    
    # Reindex to match weights
    beta_series = beta_series.reindex(weights.index)
    
    return weighted_zscore(beta_series, weights)


# =============================================================================
# Momentum Factor (Multi-window with Short-term Reversal Adjustment)
# =============================================================================

def calculate_momentum(
    cumret_252_21: pd.Series,
    cumret_126_21: pd.Series,
    cumret_21_1: pd.Series,
    industry_codes: pd.Series,
    weights: pd.Series,
    reversal_adj: float = 0.1,
    neutralize: bool = True,
) -> pd.Series:
    """Calculate Momentum factor with multi-window and reversal adjustment.
    
    Args:
        cumret_252_21: Cumulative return from t-252 to t-21
        cumret_126_21: Cumulative return from t-126 to t-21  
        cumret_21_1: Short-term return from t-21 to t-1 (for reversal)
        industry_codes: Industry classification for neutralization
        weights: Market cap weights for zscore
        reversal_adj: Weight for short-term reversal adjustment
        neutralize: Whether to apply industry neutralization
    
    Returns:
        Standardized Momentum factor
    """
    # Multi-window composite (50/50 long/medium term)
    mom_raw = 0.5 * cumret_252_21 + 0.5 * cumret_126_21
    
    # Short-term reversal adjustment
    if reversal_adj > 0:
        mom_raw = mom_raw - reversal_adj * cumret_21_1
    
    # Winsorize
    mom_raw = winsorize_series(mom_raw, lower_q=0.01, upper_q=0.99)
    
    # Industry neutralization
    if neutralize:
        mom_raw = industry_neutralize(mom_raw, industry_codes, weights)
    
    # Final standardization
    return weighted_zscore(mom_raw, weights)


# =============================================================================
# Growth Factor (Multi-dimensional)
# =============================================================================

def calculate_growth(
    netprofit_yoy: pd.Series,
    revenue_yoy: pd.Series,
    ocf_yoy: pd.Series,
    weights: pd.Series,
) -> pd.Series:
    """Calculate Growth factor as composite of profit, revenue, and cash flow growth.
    
    Args:
        netprofit_yoy: Net profit year-over-year growth (%)
        revenue_yoy: Revenue year-over-year growth (%)
        ocf_yoy: Operating cash flow year-over-year growth (%)
        weights: Market cap weights for zscore
    
    Returns:
        Composite Growth factor (standardized)
    """
    indicators = []
    
    for yoy_series in [netprofit_yoy, revenue_yoy, ocf_yoy]:
        # Robust winsorization for growth rates (can have extreme values)
        yoy_w = winsorize_series(yoy_series, lower_bound=-100, upper_bound=500)
        yoy_z = weighted_zscore(yoy_w, weights)
        indicators.append(yoy_z)
    
    # Equal-weight composite
    composite = pd.concat(indicators, axis=1)
    growth_raw = composite.mean(axis=1, skipna=True)
    
    # Final standardization
    return weighted_zscore(growth_raw, weights)


# =============================================================================
# Leverage Factor (Multi-dimensional)
# =============================================================================

def calculate_leverage(
    debt_to_assets: pd.Series,
    debt_to_equity: pd.Series,
    weights: pd.Series,
) -> pd.Series:
    """Calculate Leverage factor as composite of debt ratios.
    
    Args:
        debt_to_assets: Total debt / Total assets (%)
        debt_to_equity: Total debt / Shareholders equity
        weights: Market cap weights for zscore
    
    Returns:
        Composite Leverage factor (standardized)
    """
    indicators = []
    
    # Debt-to-assets (already in %)
    da = winsorize_series(debt_to_assets, lower_q=0.01, upper_q=0.99)
    da_z = weighted_zscore(da, weights)
    indicators.append(da_z)
    
    # Debt-to-equity (can be very large, need robust winsorization)
    de = winsorize_series(debt_to_equity, lower_q=0.01, upper_q=0.99)
    de_z = weighted_zscore(de, weights)
    indicators.append(de_z)
    
    # Equal-weight composite
    composite = pd.concat(indicators, axis=1)
    leverage_raw = composite.mean(axis=1, skipna=True)
    
    # Final standardization
    return weighted_zscore(leverage_raw, weights)


# =============================================================================
# Residual Volatility Factor
# =============================================================================

def calculate_resvol(
    residual_returns: pd.DataFrame,
    industry_codes: pd.Series,
    weights: pd.Series,
    window: int = 252,
    half_life: int = 42,
    neutralize: bool = True,
) -> pd.Series:
    """Calculate Residual Volatility factor with EWMA and industry adjustment.
    
    Args:
        residual_returns: DataFrame with tickers as columns, dates as rows
        industry_codes: Industry classification for neutralization
        weights: Market cap weights for final zscore
        window: Lookback window
        half_life: Half-life for exponential weighting
        neutralize: Whether to apply industry neutralization
    
    Returns:
        Standardized ResVol factor
    """
    exp_weights = exponential_weights(window, half_life)
    
    resvols = {}
    for ticker in residual_returns.columns:
        resid = residual_returns[ticker].dropna()
        
        if len(resid) < 60:
            resvols[ticker] = np.nan
            continue
        
        # Take most recent observations
        resid = resid.iloc[-window:] if len(resid) > window else resid
        w = exp_weights[-len(resid):]
        
        # EWMA variance
        resid_vals = resid.values
        mean_resid = np.sum(w * resid_vals)
        var_resid = np.sum(w * (resid_vals - mean_resid) ** 2)
        resvols[ticker] = np.sqrt(var_resid) * np.sqrt(252)  # Annualized
    
    resvol_series = pd.Series(resvols)
    
    # Reindex to match weights
    resvol_series = resvol_series.reindex(weights.index)
    
    # Winsorize
    resvol_series = winsorize_series(resvol_series)
    
    # Industry neutralization
    if neutralize:
        resvol_series = industry_neutralize(resvol_series, industry_codes, weights)
    
    return weighted_zscore(resvol_series, weights)


# =============================================================================
# Non-linear Size Factor
# =============================================================================

def calculate_nlsize(size_factor: pd.Series, weights: pd.Series) -> pd.Series:
    """Calculate Non-linear Size factor as Size³ orthogonalized to Size.
    
    Args:
        size_factor: Already-calculated Size factor (standardized)
        weights: Market cap weights for zscore
    
    Returns:
        Standardized Non-linear Size factor
    """
    size_cubed = size_factor ** 3
    
    # Regress Size³ on Size to get residual
    mask = size_factor.notna() & size_cubed.notna()
    if mask.sum() < 10:
        return pd.Series(np.nan, index=size_factor.index)
    
    x = size_factor[mask].values.reshape(-1, 1)
    y = size_cubed[mask].values
    
    try:
        beta = np.linalg.lstsq(x, y, rcond=None)[0]
        fitted = x @ beta
        residuals = y - fitted.flatten()
    except np.linalg.LinAlgError:
        return pd.Series(np.nan, index=size_factor.index)
    
    result = pd.Series(np.nan, index=size_factor.index)
    result.loc[mask] = residuals
    
    result = winsorize_series(result)
    return weighted_zscore(result, weights)
