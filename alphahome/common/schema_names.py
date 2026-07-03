#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Central database schema names used across PIT and factor production."""

PIT_SCHEMA = "pit"
FACTOR_SCHEMA = "factors"
LEGACY_PGS_FACTORS_SCHEMA = "pgs_factors"
TUSHARE_SCHEMA = "tushare"

PIT_TABLES = (
    "pit_income_quarterly",
    "pit_balance_quarterly",
    "pit_cashflow_quarterly",
    "pit_financial_indicators",
    "pit_industry_classification",
    "pit_income_quarterly_express_backup",
)

FACTOR_TABLES = (
    "p_factor",
    "g_factor",
)
