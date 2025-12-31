from __future__ import annotations

BARRA_SCHEMA = "barra"

# Full Barra CNE5-style factor columns stored in barra.exposures_daily
# (Industry one-hot columns are generated from rawdata.index_swmember l1_code list)

# Core style factors (MVP + Post-MVP upgrades)
STYLE_FACTOR_COLUMNS: tuple[str, ...] = (
    "style_size",           # Log market cap (industry-neutralized)
    "style_beta",           # EWMA market beta with Bayesian shrinkage
    "style_momentum",       # Multi-window momentum (12-1m, 6-1m) with reversal adj
    "style_value",          # Composite: E/P + B/P + S/P + DY + CF/P
    "style_liquidity",      # Multi-window turnover (21d/63d/252d) + amount/mv
    "style_resvol",         # EWMA residual volatility (industry-adjusted)
)

# Extended style factors (Post-MVP Phase 2B)
EXTENDED_FACTOR_COLUMNS: tuple[str, ...] = (
    "style_nlsize",         # Non-linear size (Size³ orthogonalized to Size)
    "style_growth",         # Composite: netprofit_yoy + revenue_yoy + ocf_yoy
    "style_leverage",       # Composite: debt/assets + debt/equity
    "style_dividend",       # Dividend yield (dv_ttm)
    "style_earnings_quality",  # OCF/income + accruals ratio
)

# All factor columns for full model
ALL_STYLE_FACTOR_COLUMNS: tuple[str, ...] = STYLE_FACTOR_COLUMNS + EXTENDED_FACTOR_COLUMNS

# Legacy alias for backward compatibility (MVP version used this)
STYLE_FACTOR_COLUMNS_MVP: tuple[str, ...] = (
    "style_size",
    "style_beta",
    "style_mom_12m1m",  # Old name
    "style_value_bp",   # Old name (single-indicator)
    "style_liquidity",
    "style_resvol",
)

# Factor calculation parameters
FACTOR_PARAMS = {
    "beta": {
        "window": 252,
        "half_life": 63,
        "shrinkage_factor": 0.3,
    },
    "momentum": {
        "reversal_adjustment": 0.1,
        "neutralize_industry": True,
    },
    "resvol": {
        "window": 252,
        "half_life": 42,
        "neutralize_industry": True,
    },
    "liquidity": {
        "windows": [21, 63, 252],
        "include_amount_mv": True,
    },
    "value": {
        "indicators": ["ep", "bp", "sp", "dy"],  # CF/P optional
    },
    "growth": {
        "indicators": ["netprofit_yoy", "revenue_yoy", "ocf_yoy"],
    },
    "leverage": {
        "indicators": ["debt_to_assets", "debt_to_equity"],
    },
}

# Market index for Beta calculation
MARKET_INDEX_CODE = "000300.SH"  # CSI 300
