#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Barra risk model estimation.

Implements factor covariance and specific variance estimation for portfolio risk.

Portfolio variance decomposition:
    σ²_p = x' Ω x + Σ_i w_i² δ_i²

Where:
    Ω = factor covariance matrix (K × K)
    δ_i² = specific (idiosyncratic) variance for stock i
    x = portfolio factor exposures (K × 1)
    w = portfolio weights (N × 1)

Estimation methods:
1. Factor Covariance (Ω):
   - Sample covariance from rolling window of factor returns
   - Optional: Exponential weighting (half-life decay)
   - Optional: Newey-West adjustment for autocorrelation

2. Specific Variance (δ²):
   - Sample variance from rolling window of specific returns
   - Optional: Exponential weighting
   - Optional: Bayesian shrinkage toward cross-sectional mean

References:
- MSCI Barra Risk Model Handbook
- Menchero, J. et al. (2011) "Eigen-Adjusted Covariance Matrices"
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd


@dataclass
class RiskModelConfig:
    """Configuration for risk model estimation."""
    
    # Rolling window for covariance estimation (trading days)
    cov_window: int = 252
    
    # Minimum observations required
    min_observations: int = 60
    
    # Exponential decay half-life (None = equal weight)
    half_life: Optional[int] = 126
    
    # Newey-West lags for autocorrelation adjustment (0 = no adjustment)
    newey_west_lags: int = 2
    
    # Specific variance shrinkage intensity (0 = no shrinkage, 1 = full shrinkage)
    specific_var_shrinkage: float = 0.2
    
    # Annualization factor (252 trading days)
    annualization_factor: float = 252.0
    
    # Floor for specific variance (prevent near-zero values)
    specific_var_floor: float = 1e-6


def _compute_exp_weights(n: int, half_life: Optional[int] = None) -> np.ndarray:
    """Compute exponential decay weights for a time series.
    
    Args:
        n: Number of observations
        half_life: Half-life in periods. If None, returns equal weights.
    
    Returns:
        Array of weights summing to 1.0, most recent observation last.
    """
    if half_life is None or half_life <= 0:
        return np.ones(n) / n
    
    # Lambda = 0.5^(1/half_life)
    lam = 0.5 ** (1.0 / half_life)
    
    # Weights: λ^0, λ^1, ..., λ^(n-1) (most recent = λ^0 = 1)
    # But we want chronological order, so reverse
    weights = lam ** np.arange(n - 1, -1, -1)
    weights /= weights.sum()
    
    return weights


def _newey_west_adjustment(
    returns: np.ndarray,
    cov: np.ndarray,
    lags: int = 2,
    weights: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Apply Newey-West adjustment for autocorrelation.
    
    Args:
        returns: T × K matrix of factor returns (demeaned)
        cov: K × K sample covariance matrix
        lags: Number of lags to include
        weights: Optional T-length weight vector
    
    Returns:
        Adjusted K × K covariance matrix
    """
    if lags <= 0:
        return cov
    
    T, K = returns.shape
    
    if weights is None:
        weights = np.ones(T) / T
    
    adjusted_cov = cov.copy()
    
    for lag in range(1, lags + 1):
        bartlett_weight = 1.0 - lag / (lags + 1)
        
        # Compute autocovariance at this lag
        r1 = returns[:-lag]  # (T-lag) × K
        r2 = returns[lag:]   # (T-lag) × K
        w = weights[lag:]    # (T-lag)
        w = w / w.sum()      # renormalize
        
        # Weighted autocovariance
        autocov = (r1.T * w) @ r2  # K × K
        
        # Add symmetric adjustment
        adjusted_cov += bartlett_weight * (autocov + autocov.T)
    
    return adjusted_cov


def estimate_factor_covariance(
    factor_returns: pd.DataFrame,
    config: Optional[RiskModelConfig] = None,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Estimate factor covariance matrix from historical factor returns.
    
    Args:
        factor_returns: DataFrame with trade_date index and factor columns.
                       Should be daily factor returns (not cumulative).
        config: Risk model configuration.
    
    Returns:
        Tuple of:
        - Covariance matrix as DataFrame (annualized)
        - Diagnostics dict with n_obs, effective_n, etc.
    """
    if config is None:
        config = RiskModelConfig()
    
    # Get factor columns (exclude metadata like n_obs, r2, rmse)
    factor_cols = [c for c in factor_returns.columns 
                   if c.startswith("fr_")]
    
    if not factor_cols:
        raise ValueError("No factor return columns (fr_*) found")
    
    # Extract factor returns matrix
    F = factor_returns[factor_cols].dropna(how="all")
    
    if len(F) < config.min_observations:
        raise ValueError(f"Insufficient observations: {len(F)} < {config.min_observations}")
    
    # Use most recent window
    if len(F) > config.cov_window:
        F = F.iloc[-config.cov_window:]
    
    # Convert to numpy
    F_np = F.values.astype(float)
    T, K = F_np.shape
    
    # Handle NaN: fill with 0 (factor didn't move) or column mean
    col_means = np.nanmean(F_np, axis=0)
    for j in range(K):
        mask = np.isnan(F_np[:, j])
        F_np[mask, j] = col_means[j] if not np.isnan(col_means[j]) else 0.0
    
    # Compute weights
    weights = _compute_exp_weights(T, config.half_life)
    
    # Demean (weighted)
    weighted_mean = (weights[:, np.newaxis] * F_np).sum(axis=0)
    F_demeaned = F_np - weighted_mean
    
    # Weighted covariance
    # Cov = Σ w_t (r_t - μ)(r_t - μ)'
    cov = (F_demeaned.T * weights) @ F_demeaned
    
    # Newey-West adjustment
    if config.newey_west_lags > 0:
        cov = _newey_west_adjustment(F_demeaned, cov, config.newey_west_lags, weights)
    
    # Annualize
    cov *= config.annualization_factor
    
    # Ensure positive semi-definite
    eigvals, eigvecs = np.linalg.eigh(cov)
    eigvals = np.maximum(eigvals, 0)
    cov = eigvecs @ np.diag(eigvals) @ eigvecs.T
    
    # Convert to DataFrame
    cov_df = pd.DataFrame(cov, index=factor_cols, columns=factor_cols)
    
    # Effective number of observations (for exp weighting)
    effective_n = 1.0 / (weights ** 2).sum() if config.half_life else T
    
    diagnostics = {
        "n_obs": T,
        "effective_n": effective_n,
        "n_factors": K,
        "half_life": config.half_life,
        "annualization_factor": config.annualization_factor,
    }
    
    return cov_df, diagnostics


def estimate_specific_variance(
    specific_returns: pd.DataFrame,
    config: Optional[RiskModelConfig] = None,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """Estimate specific (idiosyncratic) variance for each stock.
    
    Args:
        specific_returns: DataFrame with columns [trade_date, ticker, specific_return].
                         Should contain rolling window of specific returns.
        config: Risk model configuration.
    
    Returns:
        Tuple of:
        - DataFrame with [ticker, specific_var, n_obs] (annualized variance)
        - Diagnostics dict
    """
    if config is None:
        config = RiskModelConfig()
    
    # Pivot to wide format: trade_date × ticker
    if "trade_date" in specific_returns.columns:
        wide = specific_returns.pivot(
            index="trade_date", 
            columns="ticker", 
            values="specific_return"
        )
    else:
        wide = specific_returns
    
    # Use most recent window
    if len(wide) > config.cov_window:
        wide = wide.iloc[-config.cov_window:]
    
    T = len(wide)
    
    if T < config.min_observations:
        raise ValueError(f"Insufficient observations: {T} < {config.min_observations}")
    
    # Compute weights
    weights = _compute_exp_weights(T, config.half_life)
    
    # Compute weighted variance for each stock
    results = []
    for ticker in wide.columns:
        series = wide[ticker].values.astype(float)
        valid_mask = ~np.isnan(series)
        n_valid = valid_mask.sum()
        
        if n_valid < config.min_observations // 2:
            # Not enough data for this stock
            continue
        
        # Subset weights and values
        vals = series[valid_mask]
        w = weights[valid_mask]
        w = w / w.sum()  # renormalize
        
        # Weighted mean and variance
        mean = (w * vals).sum()
        var = (w * (vals - mean) ** 2).sum()
        
        # Annualize
        var *= config.annualization_factor
        
        results.append({
            "ticker": ticker,
            "specific_var": var,
            "n_obs": n_valid,
        })
    
    if not results:
        return pd.DataFrame(columns=["ticker", "specific_var", "n_obs"]), {}
    
    result_df = pd.DataFrame(results)
    
    # Apply shrinkage toward cross-sectional mean
    if config.specific_var_shrinkage > 0:
        cross_mean = result_df["specific_var"].mean()
        result_df["specific_var"] = (
            (1 - config.specific_var_shrinkage) * result_df["specific_var"]
            + config.specific_var_shrinkage * cross_mean
        )
    
    # Apply floor
    result_df["specific_var"] = result_df["specific_var"].clip(lower=config.specific_var_floor)
    
    diagnostics = {
        "n_stocks": len(result_df),
        "n_periods": T,
        "shrinkage": config.specific_var_shrinkage,
        "mean_specific_var": result_df["specific_var"].mean(),
        "median_specific_var": result_df["specific_var"].median(),
    }
    
    return result_df, diagnostics


def compute_portfolio_risk(
    weights: pd.Series,
    exposures: pd.DataFrame,
    factor_cov: pd.DataFrame,
    specific_var: pd.DataFrame,
) -> Dict[str, float]:
    """Compute portfolio risk from factor model.
    
    Args:
        weights: Series with ticker index and portfolio weights
        exposures: DataFrame with ticker index and factor exposure columns
        factor_cov: K × K factor covariance matrix
        specific_var: DataFrame with [ticker, specific_var]
    
    Returns:
        Dict with:
        - total_var: Total portfolio variance
        - factor_var: Variance from factor exposures
        - specific_var: Variance from specific risk
        - total_vol: Total portfolio volatility (std dev)
        - factor_vol: Factor volatility
        - specific_vol: Specific volatility
    """
    # Align weights to exposures
    common = weights.index.intersection(exposures.index)
    w = weights.loc[common].values
    w = w / w.sum()  # ensure weights sum to 1
    
    # Portfolio exposures: x = Σ w_i * X_i
    factor_cols = [c for c in exposures.columns if c.startswith("style_") or c.startswith("ind_")]
    X = exposures.loc[common, factor_cols].fillna(0).values
    portfolio_exposure = (w[:, np.newaxis] * X).sum(axis=0)  # K-vector
    
    # Map factor exposure columns to factor return columns
    fr_cols = ["fr_" + c for c in factor_cols]
    
    # Subset covariance matrix to available factors
    available_fr = [c for c in fr_cols if c in factor_cov.index]
    if not available_fr:
        raise ValueError("No matching factor columns in covariance matrix")
    
    cov = factor_cov.loc[available_fr, available_fr].values
    
    # Map portfolio exposure to available factors
    factor_map = {f"fr_{c}": i for i, c in enumerate(factor_cols)}
    x = np.array([portfolio_exposure[factor_map.get(c, 0)] for c in available_fr])
    
    # Factor variance: x' Ω x
    factor_variance = x @ cov @ x
    
    # Specific variance: Σ w_i² δ_i²
    spec_var_dict = specific_var.set_index("ticker")["specific_var"].to_dict()
    specific_variance = sum(
        (w[i] ** 2) * spec_var_dict.get(t, 0)
        for i, t in enumerate(common)
    )
    
    total_variance = factor_variance + specific_variance
    
    return {
        "total_var": float(total_variance),
        "factor_var": float(factor_variance),
        "specific_var": float(specific_variance),
        "total_vol": float(np.sqrt(total_variance)),
        "factor_vol": float(np.sqrt(factor_variance)),
        "specific_vol": float(np.sqrt(specific_variance)),
        "factor_var_pct": float(factor_variance / total_variance) if total_variance > 0 else 0,
        "specific_var_pct": float(specific_variance / total_variance) if total_variance > 0 else 0,
    }


class RiskModel:
    """Convenience class for risk model estimation and usage.
    
    Example:
        >>> model = RiskModel(config=RiskModelConfig(cov_window=252))
        >>> model.fit(factor_returns_df, specific_returns_df)
        >>> risk = model.compute_risk(portfolio_weights, exposures)
        >>> print(f"Portfolio volatility: {risk['total_vol']:.2%}")
    """
    
    def __init__(self, config: Optional[RiskModelConfig] = None):
        self.config = config or RiskModelConfig()
        self.factor_cov: Optional[pd.DataFrame] = None
        self.specific_var: Optional[pd.DataFrame] = None
        self.factor_cov_diag: Dict[str, float] = {}
        self.specific_var_diag: Dict[str, float] = {}
    
    def fit(
        self,
        factor_returns: pd.DataFrame,
        specific_returns: pd.DataFrame,
    ) -> "RiskModel":
        """Estimate factor covariance and specific variance.
        
        Args:
            factor_returns: Historical factor returns
            specific_returns: Historical specific returns
        
        Returns:
            Self for chaining
        """
        self.factor_cov, self.factor_cov_diag = estimate_factor_covariance(
            factor_returns, self.config
        )
        self.specific_var, self.specific_var_diag = estimate_specific_variance(
            specific_returns, self.config
        )
        return self
    
    def compute_risk(
        self,
        weights: pd.Series,
        exposures: pd.DataFrame,
    ) -> Dict[str, float]:
        """Compute portfolio risk.
        
        Args:
            weights: Portfolio weights indexed by ticker
            exposures: Factor exposures indexed by ticker
        
        Returns:
            Risk decomposition dict
        """
        if self.factor_cov is None or self.specific_var is None:
            raise RuntimeError("Must call fit() before compute_risk()")
        
        return compute_portfolio_risk(
            weights, exposures, self.factor_cov, self.specific_var
        )
    
    def get_factor_volatility(self) -> pd.Series:
        """Get annualized volatility for each factor."""
        if self.factor_cov is None:
            raise RuntimeError("Must call fit() first")
        return pd.Series(np.sqrt(np.diag(self.factor_cov)), index=self.factor_cov.index)
    
    def get_factor_correlation(self) -> pd.DataFrame:
        """Get factor correlation matrix."""
        if self.factor_cov is None:
            raise RuntimeError("Must call fit() first")
        vols = np.sqrt(np.diag(self.factor_cov))
        corr = self.factor_cov.values / np.outer(vols, vols)
        np.fill_diagonal(corr, 1.0)
        return pd.DataFrame(corr, index=self.factor_cov.index, columns=self.factor_cov.columns)
