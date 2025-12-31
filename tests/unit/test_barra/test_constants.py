#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for barra.constants module."""

import pytest

from alphahome.barra.constants import (
    BARRA_SCHEMA,
    STYLE_FACTOR_COLUMNS,
    STYLE_FACTOR_COLUMNS_MVP,
)


class TestBarraConstants:
    """Test Barra constants."""

    def test_barra_schema_is_string(self):
        """BARRA_SCHEMA should be a string."""
        assert isinstance(BARRA_SCHEMA, str)
        assert BARRA_SCHEMA == "barra"

    def test_style_factor_columns_is_tuple(self):
        """STYLE_FACTOR_COLUMNS should be a tuple of strings."""
        assert isinstance(STYLE_FACTOR_COLUMNS, tuple)
        assert len(STYLE_FACTOR_COLUMNS) > 0
        for col in STYLE_FACTOR_COLUMNS:
            assert isinstance(col, str)
            assert col.startswith("style_")

    def test_expected_style_factors(self):
        """Should contain expected core style factors (canonical names)."""
        expected = {
            "style_size",
            "style_beta",
            "style_momentum",
            "style_value",
            "style_liquidity",
            "style_resvol",
        }
        actual = set(STYLE_FACTOR_COLUMNS)
        assert expected == actual, f"Missing: {expected - actual}, Extra: {actual - expected}"

    def test_expected_style_factors_mvp_aliases(self):
        """Legacy MVP column names remain available as aliases for compatibility."""
        expected = {
            "style_size",
            "style_beta",
            "style_mom_12m1m",
            "style_value_bp",
            "style_liquidity",
            "style_resvol",
        }
        actual = set(STYLE_FACTOR_COLUMNS_MVP)
        assert expected == actual, f"Missing: {expected - actual}, Extra: {actual - expected}"
