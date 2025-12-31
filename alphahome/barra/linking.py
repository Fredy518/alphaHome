#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Multi-period attribution linking algorithms.

Implements Carino and Menchero methods for linking single-period attributions
to form consistent multi-period attribution results.

References:
- Carino, D. (1999) "Combining Attribution Effects Over Time"
- Menchero, J. (2000) "An Optimized Approach to Linking Attribution Effects Over Time"
"""

from __future__ import annotations

from typing import Dict, List, Optional, Union
import numpy as np
import pandas as pd


def _log_ratio(r: float) -> float:
    """Compute ln(1+r)/r with handling for r near zero."""
    if abs(r) < 1e-10:
        return 1.0
    return np.log(1 + r) / r


def _carino_factor(total_return: float, period_return: float) -> float:
    """Compute Carino scaling factor for a single period.
    
        k_t = (R / ln(1 + R)) * (ln(1 + r_t) / r_t)
                = log_ratio(r_t) / log_ratio(R)
    
    Where:
      R = total multi-period return
      r_t = single-period return
    """
    # Carino scaling factor k_t must satisfy:
    #   \sum_t k_t * r_t = R
    # given \sum_k a_{k,t} = r_t for each period.
    #
    # Using k_t = (R / ln(1+R)) * (ln(1+r_t) / r_t) yields:
    #   \sum_t k_t r_t = (R / ln(1+R)) \sum_t ln(1+r_t) = (R / ln(1+R)) ln(1+R) = R
    #
    # With helper log_ratio(x)=ln(1+x)/x, this becomes:
    #   k_t = log_ratio(r_t) / log_ratio(R)
    if abs(total_return) < 1e-12:
        return 1.0

    log_R = _log_ratio(total_return)
    log_rt = _log_ratio(period_return)
    if abs(log_R) < 1e-12:
        return 1.0
    return log_rt / log_R


def link_carino(
    period_returns: List[float],
    period_contributions: List[Dict[str, float]],
    factor_names: Optional[List[str]] = None,
) -> Dict[str, float]:
    """Link multi-period attribution using Carino method.
    
    The Carino method ensures that the sum of linked contributions
    equals the geometrically compounded total return.
    
    Args:
        period_returns: List of single-period total returns (as decimals, e.g. 0.01 for 1%)
        period_contributions: List of dicts, each containing factor contributions for that period
        factor_names: Optional list of factor names to include. If None, uses all keys
                     from first period's contributions.
    
    Returns:
        Dict mapping factor names to linked multi-period contributions
    
    Example:
        >>> returns = [0.01, 0.02, -0.01]
        >>> contribs = [
        ...     {"size": 0.005, "value": 0.003, "specific": 0.002},
        ...     {"size": 0.008, "value": 0.007, "specific": 0.005},
        ...     {"size": -0.003, "value": -0.005, "specific": -0.002},
        ... ]
        >>> link_carino(returns, contribs)
        {'size': ..., 'value': ..., 'specific': ...}
    """
    if not period_returns or not period_contributions:
        return {}
    
    n = len(period_returns)
    if len(period_contributions) != n:
        raise ValueError("period_returns and period_contributions must have same length")
    
    # Compute total geometric return
    total_return = np.prod([1 + r for r in period_returns]) - 1
    
    # Get factor names
    if factor_names is None:
        factor_names = list(period_contributions[0].keys())
    
    # Compute Carino scaling factors for each period
    k_factors = [_carino_factor(total_return, r) for r in period_returns]
    
    # Link each factor's contributions
    linked = {}
    for factor in factor_names:
        linked_contrib = 0.0
        for t in range(n):
            contrib_t = period_contributions[t].get(factor, 0.0)
            linked_contrib += k_factors[t] * contrib_t
        linked[factor] = linked_contrib
    
    return linked


def link_menchero(
    period_returns: List[float],
    period_contributions: List[Dict[str, float]],
    factor_names: Optional[List[str]] = None,
) -> Dict[str, float]:
    """Link multi-period attribution using Menchero (optimized) method.
    
    The Menchero method is an alternative to Carino that uses a different
    smoothing approach. It computes an "adjusted" return for each factor
    that accounts for compounding.
    
    Menchero formula:
      A_k = Σ_t [a_{k,t} * Π_{s<t}(1+r_s) * (1 + (r_t - a_{k,t})/2)]
    
    Where a_{k,t} is the contribution of factor k in period t.
    
    Args:
        period_returns: List of single-period total returns
        period_contributions: List of dicts with factor contributions
        factor_names: Optional list of factor names
    
    Returns:
        Dict mapping factor names to linked contributions
    """
    if not period_returns or not period_contributions:
        return {}
    
    n = len(period_returns)
    if len(period_contributions) != n:
        raise ValueError("period_returns and period_contributions must have same length")
    
    if factor_names is None:
        factor_names = list(period_contributions[0].keys())
    
    # Compute cumulative products for compounding
    cum_products = [1.0]
    for r in period_returns:
        cum_products.append(cum_products[-1] * (1 + r))
    
    linked = {}
    for factor in factor_names:
        linked_contrib = 0.0
        for t in range(n):
            a_kt = period_contributions[t].get(factor, 0.0)
            r_t = period_returns[t]
            
            # Compound up to time t
            compound_before = cum_products[t]
            
            # Menchero adjustment
            adjustment = 1 + (r_t - a_kt) / 2
            
            linked_contrib += a_kt * compound_before * adjustment
        
        linked[factor] = linked_contrib
    
    return linked


def link_simple_compound(
    period_contributions: List[Dict[str, float]],
    factor_names: Optional[List[str]] = None,
) -> Dict[str, float]:
    """Simple additive linking (no compounding adjustment).
    
    Just sums contributions across periods. This is a naive approach
    that doesn't account for compounding, but can be useful for
    very short periods or when compounding effects are negligible.
    
    Args:
        period_contributions: List of dicts with factor contributions
        factor_names: Optional list of factor names
    
    Returns:
        Dict mapping factor names to summed contributions
    """
    if not period_contributions:
        return {}
    
    if factor_names is None:
        factor_names = list(period_contributions[0].keys())
    
    linked = {f: 0.0 for f in factor_names}
    for contrib in period_contributions:
        for f in factor_names:
            linked[f] += contrib.get(f, 0.0)
    
    return linked


class MultiPeriodLinker:
    """Helper class for multi-period attribution linking.
    
    Provides a convenient interface for accumulating single-period
    attributions and computing linked results.
    
    Example:
        >>> linker = MultiPeriodLinker(method="carino")
        >>> linker.add_period(return_=0.01, contributions={"size": 0.005, "value": 0.005})
        >>> linker.add_period(return_=0.02, contributions={"size": 0.01, "value": 0.01})
        >>> result = linker.get_linked()
        >>> print(result["total_return"], result["linked_contributions"])
    """
    
    METHODS = {"carino", "menchero", "simple"}
    
    def __init__(self, method: str = "carino"):
        if method not in self.METHODS:
            raise ValueError(f"method must be one of {self.METHODS}")
        self.method = method
        self._returns: List[float] = []
        self._contributions: List[Dict[str, float]] = []
        self._dates: List[str] = []
    
    def add_period(
        self, 
        return_: float, 
        contributions: Dict[str, float],
        date: Optional[str] = None
    ) -> None:
        """Add a single-period attribution result."""
        self._returns.append(return_)
        self._contributions.append(contributions)
        self._dates.append(date or "")
    
    def clear(self) -> None:
        """Clear all accumulated periods."""
        self._returns.clear()
        self._contributions.clear()
        self._dates.clear()
    
    @property
    def n_periods(self) -> int:
        return len(self._returns)
    
    @property
    def total_return(self) -> float:
        """Geometric total return across all periods."""
        if not self._returns:
            return 0.0
        return np.prod([1 + r for r in self._returns]) - 1
    
    def get_linked(self) -> Dict[str, Union[float, Dict[str, float]]]:
        """Compute and return linked attribution.
        
        Returns:
            Dict with:
              - total_return: float
              - n_periods: int
              - linked_contributions: Dict[str, float]
              - recon_error: float (difference between sum of contributions and total return)
        """
        if not self._contributions:
            return {
                "total_return": 0.0,
                "n_periods": 0,
                "linked_contributions": {},
                "recon_error": 0.0,
            }
        
        if self.method == "carino":
            linked = link_carino(self._returns, self._contributions)
        elif self.method == "menchero":
            linked = link_menchero(self._returns, self._contributions)
        else:
            linked = link_simple_compound(self._contributions)
        
        total = self.total_return
        contrib_sum = sum(linked.values())
        recon_error = total - contrib_sum
        
        return {
            "total_return": total,
            "n_periods": self.n_periods,
            "linked_contributions": linked,
            "recon_error": recon_error,
        }
    
    def to_dataframe(self) -> pd.DataFrame:
        """Return period-by-period contributions as DataFrame."""
        if not self._contributions:
            return pd.DataFrame()
        
        df = pd.DataFrame(self._contributions)
        df["return"] = self._returns
        if any(self._dates):
            df["date"] = self._dates
            df = df.set_index("date")
        return df


def link_attribution_series(
    attribution_df: pd.DataFrame,
    return_col: str = "active_return",
    contrib_prefix: str = "contrib_",
    specific_contrib_col: str = "specific_contrib",
    method: str = "carino",
) -> Dict[str, Union[float, Dict[str, float]]]:
    """Link attribution from a DataFrame of period-by-period results.
    
    Convenience function for linking attributions stored in a DataFrame
    (e.g., from portfolio_attribution_daily table).
    
    Args:
        attribution_df: DataFrame with one row per period, containing return and contribution columns
        return_col: Name of the column containing period returns
        contrib_prefix: Prefix for contribution columns (e.g., "contrib_" for "contrib_size", etc.)
        specific_contrib_col: Name of the specific contribution column (handled separately)
        method: Linking method ("carino", "menchero", or "simple")
    
    Returns:
        Linked attribution result dict with keys:
          - total_return: geometric total return
          - n_periods: number of periods linked
          - linked_contributions: dict of factor_name -> linked contribution
          - specific_contrib: linked specific contribution
          - recon_error: difference between total_return and sum of contributions
    """
    if attribution_df.empty:
        return {
            "total_return": 0.0,
            "n_periods": 0,
            "linked_contributions": {},
            "specific_contrib": 0.0,
            "recon_error": 0.0,
        }
    
    # Extract contribution columns (factor contributions)
    contrib_cols = [c for c in attribution_df.columns if c.startswith(contrib_prefix)]
    factor_names = [c[len(contrib_prefix):] for c in contrib_cols]
    
    # Check if specific_contrib column exists
    has_specific = specific_contrib_col in attribution_df.columns
    
    linker = MultiPeriodLinker(method=method)
    
    for _, row in attribution_df.iterrows():
        r = float(row[return_col])
        contribs = {f: float(row[f"{contrib_prefix}{f}"]) for f in factor_names}
        
        # Include specific_contrib in the linking process
        if has_specific:
            contribs["_specific_"] = float(row[specific_contrib_col])
        
        linker.add_period(return_=r, contributions=contribs)
    
    result = linker.get_linked()
    
    # Extract specific_contrib from linked_contributions
    linked_contribs = result.get("linked_contributions", {})
    specific_contrib = linked_contribs.pop("_specific_", 0.0)
    
    result["linked_contributions"] = linked_contribs
    result["specific_contrib"] = specific_contrib
    
    return result
