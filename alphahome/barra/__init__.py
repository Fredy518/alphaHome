"""Barra-style factor model & attribution.

This package provides the storage schema initializer and core math utilities.
Computation tasks (exposures/regression/attribution) are implemented incrementally.
"""

from .constants import (
    BARRA_SCHEMA,
    STYLE_FACTOR_COLUMNS,
    EXTENDED_FACTOR_COLUMNS,
    ALL_STYLE_FACTOR_COLUMNS,
    FACTOR_PARAMS,
    MARKET_INDEX_CODE,
)

from .factor_calculators import (
    winsorize_series,
    weighted_zscore,
    industry_neutralize,
    calculate_size,
    calculate_value,
    calculate_liquidity,
    calculate_beta,
    calculate_momentum,
    calculate_growth,
    calculate_leverage,
    calculate_resvol,
    calculate_nlsize,
)

from .linking import (
    link_carino,
    link_menchero,
    link_simple_compound,
    link_attribution_series,
    MultiPeriodLinker,
)

from .risk_model import (
    RiskModel,
    RiskModelConfig,
    estimate_factor_covariance,
    estimate_specific_variance,
    compute_portfolio_risk,
)

__all__ = [
    "BARRA_SCHEMA",
    "STYLE_FACTOR_COLUMNS",
    # Linking
    "link_carino",
    "link_menchero",
    "link_simple_compound",
    "link_attribution_series",
    "MultiPeriodLinker",
    # Risk Model
    "RiskModel",
    "RiskModelConfig",
    "estimate_factor_covariance",
    "estimate_specific_variance",
    "compute_portfolio_risk",
]

