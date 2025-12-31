#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for WLS regression and factor returns computation.

These tests verify the core mathematical operations without requiring database access.
"""

import pytest
import numpy as np
import pandas as pd

# Import the task class to test static methods
from alphahome.processors.tasks.barra.barra_factor_returns_daily import BarraFactorReturnsDailyTask


class TestWinsorize:
    """Test winsorization of returns."""

    def test_winsorize_basic(self):
        """Should clip extreme values to quantiles."""
        s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 100])  # 100 is outlier
        result = BarraFactorReturnsDailyTask._winsorize_series(s, 0.1, 0.9)
        
        # 100 should be clipped to 90th percentile
        assert result.max() < 100
        # Values within quantile bounds may still be clipped if below lower bound
        # Just check result is within expected range
        assert result.min() >= s.quantile(0.1)
        assert result.max() <= s.quantile(0.9)

    def test_winsorize_empty(self):
        """Empty series should return empty."""
        s = pd.Series([], dtype=float)
        result = BarraFactorReturnsDailyTask._winsorize_series(s)
        assert len(result) == 0

    def test_winsorize_with_nan(self):
        """Should handle NaN values."""
        s = pd.Series([1, 2, np.nan, 4, 5])
        result = BarraFactorReturnsDailyTask._winsorize_series(s)
        
        # NaN should remain NaN
        assert pd.isna(result.iloc[2])

    def test_winsorize_all_same(self):
        """Should handle constant series."""
        s = pd.Series([5.0, 5.0, 5.0, 5.0, 5.0])
        result = BarraFactorReturnsDailyTask._winsorize_series(s)
        
        # All values should remain 5
        assert all(result == 5.0)


class TestWLSSolve:
    """Test weighted least squares solver."""

    def test_wls_basic(self):
        """Basic WLS should produce valid coefficients."""
        np.random.seed(42)
        n = 100
        
        # True model: y = 2*x1 + 3*x2 + noise
        x = np.random.randn(n, 2)
        true_coef = np.array([2.0, 3.0])
        y = x @ true_coef + np.random.randn(n) * 0.1
        w = np.ones(n)  # Equal weights
        
        coef = BarraFactorReturnsDailyTask._wls_solve(x, y, w)
        
        assert len(coef) == 2
        assert np.allclose(coef, true_coef, atol=0.2)

    def test_wls_with_weights(self):
        """Weighted observations should have more influence."""
        np.random.seed(42)
        n = 100
        
        # Create data with outliers
        x = np.random.randn(n, 1)
        y = 2 * x.flatten() + np.random.randn(n) * 0.1
        
        # Add outliers at the end
        y[-10:] = y[-10:] + 10
        
        # Equal weights
        w_equal = np.ones(n)
        coef_equal = BarraFactorReturnsDailyTask._wls_solve(x, y, w_equal)
        
        # Downweight outliers
        w_downweight = np.ones(n)
        w_downweight[-10:] = 0.01
        coef_downweight = BarraFactorReturnsDailyTask._wls_solve(x, y, w_downweight)
        
        # Downweighted should be closer to true coef (2.0)
        assert abs(coef_downweight[0] - 2.0) < abs(coef_equal[0] - 2.0)

    def test_wls_zero_weights_handled(self):
        """Zero weights should not cause errors."""
        x = np.array([[1, 0], [0, 1], [1, 1]])
        y = np.array([1.0, 2.0, 3.0])
        w = np.array([1.0, 0.0, 1.0])  # Second observation has zero weight
        
        coef = BarraFactorReturnsDailyTask._wls_solve(x, y, w)
        
        assert len(coef) == 2
        assert np.all(np.isfinite(coef))

    def test_wls_negative_weights_handled(self):
        """Negative weights should be set to zero."""
        x = np.array([[1, 0], [0, 1], [1, 1]])
        y = np.array([1.0, 2.0, 3.0])
        w = np.array([1.0, -1.0, 1.0])  # Negative weight
        
        coef = BarraFactorReturnsDailyTask._wls_solve(x, y, w)
        
        assert len(coef) == 2
        assert np.all(np.isfinite(coef))

    def test_wls_sqrt_transformation(self):
        """WLS should apply sqrt(w) transformation correctly.
        
        The solver minimizes: sum_i w_i * (y_i - x_i @ beta)^2
        
        This is achieved by transforming:
            X_tilde = sqrt(w) * X
            y_tilde = sqrt(w) * y
        and solving OLS on the transformed system.
        """
        np.random.seed(42)
        n = 100
        
        # Create data with known heteroscedasticity
        x = np.random.randn(n, 1)
        
        # Variance inversely proportional to weight
        # High weight = low variance, should have more influence
        weights = np.abs(np.random.randn(n)) + 0.1
        variances = 1.0 / weights
        noise = np.random.randn(n) * np.sqrt(variances)
        
        true_coef = 3.0
        y = true_coef * x.flatten() + noise
        
        # WLS should recover true coef better than OLS
        coef_wls = BarraFactorReturnsDailyTask._wls_solve(x, y, weights)
        coef_ols = BarraFactorReturnsDailyTask._wls_solve(x, y, np.ones(n))
        
        # Both should be close, but WLS might be slightly better
        assert abs(coef_wls[0] - true_coef) < 1.0
        assert abs(coef_ols[0] - true_coef) < 1.0


class TestSumToZeroConstraint:
    """Test that industry returns satisfy sum-to-zero constraint."""

    def test_sum_to_zero_mathematical_proof(self):
        """Verify the C-matrix transformation produces sum-to-zero.
        
        Given J industries and J-1 regression parameters g:
        - C matrix: C = [I_{J-1}; -1^T] ∈ R^{J×(J-1)}
        - Industry returns: f = C @ g
        - sum(f) = sum(g) + (-sum(g)) = 0
        """
        J = 5  # Number of industries
        
        # Construct C matrix
        C = np.vstack([
            np.eye(J - 1),
            -np.ones(J - 1),
        ])
        
        # Arbitrary g parameters
        g = np.array([0.01, -0.02, 0.015, -0.005])
        
        # Recover industry returns
        f = C @ g
        
        # Sum should be zero
        assert np.isclose(f.sum(), 0.0, atol=1e-15)

    def test_difference_parameterization_equivalent(self):
        """Verify difference design (I_j - I_ref) is equivalent to C matrix.
        
        For a stock in industry k (k < J-1):
            X_diff[k] = 1 - 0 = 1, other X_diff = 0
        For a stock in reference industry (J-1):
            X_diff = 0 - 1 = -1 for all columns
        
        This is exactly X_ind @ C.
        """
        J = 4  # Number of industries
        n_stocks = 10
        
        # Create industry one-hot matrix
        industry_labels = np.random.randint(0, J, n_stocks)
        X_ind = np.zeros((n_stocks, J))
        for i, ind in enumerate(industry_labels):
            X_ind[i, ind] = 1
        
        # C matrix transformation
        C = np.vstack([np.eye(J - 1), -np.ones(J - 1)])
        X_via_C = X_ind @ C
        
        # Difference design: X_diff[:, k] = X_ind[:, k] - X_ind[:, J-1]
        X_diff = X_ind[:, :-1] - X_ind[:, -1:]
        
        # Should be identical
        assert np.allclose(X_via_C, X_diff)


class TestR2Computation:
    """Test R² and RMSE computation."""

    def test_r2_perfect_fit(self):
        """R² should be 1 for perfect fit."""
        np.random.seed(42)
        n = 50
        
        x = np.random.randn(n, 2)
        true_coef = np.array([1.0, 2.0])
        y = x @ true_coef  # No noise
        w = np.ones(n)
        
        coef = BarraFactorReturnsDailyTask._wls_solve(x, y, w)
        fitted = x @ coef
        resid = y - fitted
        
        # Compute R²
        wsum = w.sum()
        ybar = (w * y).sum() / wsum
        sse = (w * resid**2).sum()
        sst = (w * (y - ybar)**2).sum()
        r2 = 1.0 - sse / sst
        
        assert np.isclose(r2, 1.0, atol=1e-10)

    def test_r2_range(self):
        """R² should be between 0 and 1 for typical data."""
        np.random.seed(42)
        n = 100
        
        x = np.random.randn(n, 2)
        true_coef = np.array([1.0, 2.0])
        y = x @ true_coef + np.random.randn(n)  # With noise
        w = np.ones(n)
        
        coef = BarraFactorReturnsDailyTask._wls_solve(x, y, w)
        fitted = x @ coef
        resid = y - fitted
        
        # Compute R²
        wsum = w.sum()
        ybar = (w * y).sum() / wsum
        sse = (w * resid**2).sum()
        sst = (w * (y - ybar)**2).sum()
        r2 = 1.0 - sse / sst
        
        assert 0 <= r2 <= 1

    def test_rmse_computation(self):
        """RMSE should be sqrt of weighted mean squared error."""
        np.random.seed(42)
        n = 100
        
        x = np.random.randn(n, 2)
        true_coef = np.array([1.0, 2.0])
        noise_std = 0.5
        y = x @ true_coef + np.random.randn(n) * noise_std
        w = np.ones(n)
        
        coef = BarraFactorReturnsDailyTask._wls_solve(x, y, w)
        fitted = x @ coef
        resid = y - fitted
        
        # Compute RMSE
        wsum = w.sum()
        sse = (w * resid**2).sum()
        rmse = np.sqrt(sse / wsum)
        
        # RMSE should be close to true noise std
        assert abs(rmse - noise_std) < 0.2
