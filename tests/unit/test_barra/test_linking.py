#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for barra.linking module - multi-period attribution linking algorithms."""

import pytest
import numpy as np
import pandas as pd

from alphahome.barra.linking import (
    link_carino,
    link_menchero,
    link_simple_compound,
    link_attribution_series,
    MultiPeriodLinker,
    _log_ratio,
    _carino_factor,
)


class TestLogRatio:
    """Test _log_ratio helper function."""

    def test_log_ratio_positive(self):
        """log(1+r)/r for positive r."""
        r = 0.1
        expected = np.log(1.1) / 0.1
        assert np.isclose(_log_ratio(r), expected)

    def test_log_ratio_negative(self):
        """log(1+r)/r for negative r."""
        r = -0.05
        expected = np.log(0.95) / (-0.05)
        assert np.isclose(_log_ratio(r), expected)

    def test_log_ratio_near_zero(self):
        """log(1+r)/r approaches 1 as r -> 0."""
        assert _log_ratio(1e-12) == 1.0
        assert _log_ratio(0.0) == 1.0


class TestCarinoFactor:
    """Test _carino_factor calculation."""

    def test_carino_factor_basic(self):
        """Carino factor should be positive for positive returns."""
        total_return = 0.10
        period_return = 0.02
        k = _carino_factor(total_return, period_return)
        assert k > 0

    def test_carino_factor_zero_period(self):
        """When period return is near zero, factor should be computable."""
        total_return = 0.10
        period_return = 0.0
        k = _carino_factor(total_return, period_return)
        # When period return is 0, the factor should still be positive and finite
        assert k > 0
        assert np.isfinite(k)


class TestLinkCarino:
    """Test Carino linking method."""

    def test_empty_input(self):
        """Empty inputs should return empty dict."""
        assert link_carino([], []) == {}

    def test_single_period(self):
        """Single period linking should equal original contributions."""
        returns = [0.02]
        contribs = [{"size": 0.01, "value": 0.01}]
        result = link_carino(returns, contribs)
        # For single period, Carino factor should make contributions sum to return
        assert "size" in result
        assert "value" in result
        assert np.isclose(sum(result.values()), 0.02, atol=1e-10)

    def test_multi_period_geometric_consistency(self):
        """Linked contributions should sum to geometric total return."""
        returns = [0.01, 0.02, -0.005]
        contribs = [
            {"size": 0.005, "value": 0.005},
            {"size": 0.01, "value": 0.01},
            {"size": -0.002, "value": -0.003},
        ]
        result = link_carino(returns, contribs)
        
        # Geometric total return
        total_return = np.prod([1 + r for r in returns]) - 1
        
        # Sum of linked contributions should equal total return
        linked_sum = sum(result.values())
        assert np.isclose(linked_sum, total_return, atol=1e-10)

    def test_length_mismatch_raises(self):
        """Mismatched lengths should raise ValueError."""
        with pytest.raises(ValueError, match="same length"):
            link_carino([0.01, 0.02], [{"a": 0.01}])

    def test_factor_names_subset(self):
        """Should only include specified factor names."""
        returns = [0.01, 0.02]
        contribs = [
            {"size": 0.005, "value": 0.005, "extra": 0.001},
            {"size": 0.01, "value": 0.01, "extra": 0.002},
        ]
        result = link_carino(returns, contribs, factor_names=["size"])
        assert "size" in result
        assert "value" not in result
        assert "extra" not in result


class TestLinkMenchero:
    """Test Menchero linking method."""

    def test_empty_input(self):
        """Empty inputs should return empty dict."""
        assert link_menchero([], []) == {}

    def test_single_period(self):
        """Single period: Menchero should produce valid results."""
        returns = [0.02]
        contribs = [{"size": 0.01, "value": 0.01}]
        result = link_menchero(returns, contribs)
        assert "size" in result
        assert "value" in result

    def test_multi_period_produces_result(self):
        """Multi-period should produce linked contributions."""
        returns = [0.01, 0.02, -0.005]
        contribs = [
            {"size": 0.005, "value": 0.005},
            {"size": 0.01, "value": 0.01},
            {"size": -0.002, "value": -0.003},
        ]
        result = link_menchero(returns, contribs)
        assert "size" in result
        assert "value" in result
        # Menchero may not sum exactly to geometric return
        # but should be close for small returns
        total_return = np.prod([1 + r for r in returns]) - 1
        linked_sum = sum(result.values())
        assert np.isclose(linked_sum, total_return, atol=0.001)

    def test_length_mismatch_raises(self):
        """Mismatched lengths should raise ValueError."""
        with pytest.raises(ValueError, match="same length"):
            link_menchero([0.01, 0.02], [{"a": 0.01}])


class TestLinkSimpleCompound:
    """Test simple additive linking."""

    def test_empty_input(self):
        """Empty inputs should return empty dict."""
        assert link_simple_compound([]) == {}

    def test_simple_sum(self):
        """Should just sum contributions."""
        contribs = [
            {"size": 0.01, "value": 0.02},
            {"size": 0.02, "value": 0.01},
            {"size": -0.01, "value": 0.01},
        ]
        result = link_simple_compound(contribs)
        assert np.isclose(result["size"], 0.02)
        assert np.isclose(result["value"], 0.04)

    def test_factor_names_subset(self):
        """Should only include specified factor names."""
        contribs = [
            {"size": 0.01, "value": 0.02, "extra": 0.001},
            {"size": 0.02, "value": 0.01, "extra": 0.002},
        ]
        result = link_simple_compound(contribs, factor_names=["size"])
        assert "size" in result
        assert "value" not in result


class TestMultiPeriodLinker:
    """Test MultiPeriodLinker class."""

    def test_invalid_method_raises(self):
        """Invalid method should raise ValueError."""
        with pytest.raises(ValueError, match="must be one of"):
            MultiPeriodLinker(method="invalid")

    def test_add_period_and_get_linked(self):
        """Should accumulate periods and compute linked result."""
        linker = MultiPeriodLinker(method="carino")
        linker.add_period(return_=0.01, contributions={"size": 0.005, "value": 0.005})
        linker.add_period(return_=0.02, contributions={"size": 0.01, "value": 0.01})
        
        assert linker.n_periods == 2
        
        result = linker.get_linked()
        assert "total_return" in result
        assert "n_periods" in result
        assert "linked_contributions" in result
        assert "recon_error" in result
        assert result["n_periods"] == 2
        
        # Total return should be geometric
        expected_return = (1.01) * (1.02) - 1
        assert np.isclose(result["total_return"], expected_return)

    def test_clear(self):
        """Clear should reset all state."""
        linker = MultiPeriodLinker(method="carino")
        linker.add_period(return_=0.01, contributions={"size": 0.01})
        linker.clear()
        assert linker.n_periods == 0
        assert linker.total_return == 0.0

    def test_to_dataframe(self):
        """Should convert to DataFrame."""
        linker = MultiPeriodLinker(method="carino")
        linker.add_period(return_=0.01, contributions={"size": 0.005}, date="2025-01-01")
        linker.add_period(return_=0.02, contributions={"size": 0.01}, date="2025-01-02")
        
        df = linker.to_dataframe()
        assert len(df) == 2
        assert "size" in df.columns
        assert "return" in df.columns

    def test_menchero_method(self):
        """Should work with menchero method."""
        linker = MultiPeriodLinker(method="menchero")
        linker.add_period(return_=0.01, contributions={"size": 0.01})
        linker.add_period(return_=0.02, contributions={"size": 0.02})
        result = linker.get_linked()
        assert "linked_contributions" in result

    def test_simple_method(self):
        """Should work with simple method."""
        linker = MultiPeriodLinker(method="simple")
        linker.add_period(return_=0.01, contributions={"size": 0.01})
        linker.add_period(return_=0.02, contributions={"size": 0.02})
        result = linker.get_linked()
        assert np.isclose(result["linked_contributions"]["size"], 0.03)


class TestLinkAttributionSeries:
    """Test link_attribution_series convenience function."""

    def test_empty_dataframe(self):
        """Empty DataFrame should return default result."""
        df = pd.DataFrame()
        result = link_attribution_series(df)
        assert result["total_return"] == 0.0
        assert result["n_periods"] == 0

    def test_basic_usage(self):
        """Should link from DataFrame format."""
        df = pd.DataFrame({
            "trade_date": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            "active_return": [0.01, 0.02, -0.005],
            "contrib_size": [0.005, 0.01, -0.002],
            "contrib_value": [0.005, 0.01, -0.003],
            "specific_contrib": [0.001, 0.002, -0.001],
        })
        
        result = link_attribution_series(df)
        
        assert result["n_periods"] == 3
        assert "linked_contributions" in result
        assert "specific_contrib" in result  # Should be extracted
        assert "size" in result["linked_contributions"]
        assert "value" in result["linked_contributions"]

    def test_specific_contrib_included(self):
        """specific_contrib should be linked and returned separately."""
        df = pd.DataFrame({
            "active_return": [0.01, 0.02],
            "contrib_size": [0.005, 0.01],
            "specific_contrib": [0.005, 0.01],
        })
        
        result = link_attribution_series(
            df, 
            return_col="active_return",
            specific_contrib_col="specific_contrib",
            method="carino"
        )
        
        assert "specific_contrib" in result
        assert result["specific_contrib"] != 0.0

    def test_custom_method(self):
        """Should respect method parameter."""
        df = pd.DataFrame({
            "active_return": [0.01, 0.02],
            "contrib_size": [0.01, 0.02],
        })
        
        # Simple should just sum
        result_simple = link_attribution_series(df, method="simple")
        assert np.isclose(result_simple["linked_contributions"]["size"], 0.03)
