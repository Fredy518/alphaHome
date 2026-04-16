#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Schema and constants for the lightweight fund analysis module."""

DEFAULT_PERIODS_PER_YEAR = 250
DEFAULT_RISK_FREE_RATE = 0.0
FFILL_LIMIT = 5

METRICS_SCHEMA = {
    "cumulative_return": "float",
    "annualized_return": "float",
    "annualized_volatility": "float",
    "sharpe_ratio": "float",
    "sortino_ratio": "float",
    "calmar_ratio": "float",
    "win_rate": "float",
    "profit_loss_ratio": "float",
    "var_95": "float",
    "cvar_95": "float",
    "max_drawdown": "float",
    "information_ratio": "float",
    "tracking_error": "float",
    "beta": "float",
    "excess_return": "float",
    "total_days": "int",
}

METRICS_SCHEMA_KEYS = list(METRICS_SCHEMA.keys())

DRAWDOWN_SCHEMA = {
    "max_drawdown": "float",
    "top_n_drawdowns": "list",
    "underwater_curve": "series",
}

PERIODIC_SCHEMA = {
    "monthly_returns": "dataframe",
    "quarterly_returns": "series",
    "yearly_returns": "series",
}
