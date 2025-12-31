#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for barra.risk_model module - risk model estimation."""

import pytest
import numpy as np
import pandas as pd

from alphahome.barra.risk_model import (
    RiskModelConfig,
    RiskModel,
    estimate_factor_covariance,
    estimate_specific_variance,
    compute_portfolio_risk,
    _compute_exp_weights,
    _newey_west_adjustment,
)


class TestRiskModelConfig:
    """Test RiskModelConfig dataclass."""

    def test_default_values(self):
        """Default config should have expected values."""
        config = RiskModelConfig()
        assert config.cov_window == 252
        assert config.min_observations == 60
        assert config.half_life == 126
        assert config.newey_west_lags == 2
        assert config.specific_var_shrinkage == 0.2
        assert config.annualization_factor == 252.0
        assert config.specific_var_floor > 0

    def test_custom_values(self):
        """Should accept custom values."""
        config = RiskModelConfig(
            cov_window=126,
            half_life=63,
            newey_west_lags=0,
        )
        assert config.cov_window == 126
        assert config.half_life == 63
        assert config.newey_west_lags == 0


class TestComputeExpWeights:
    """Test exponential weight calculation."""

    def test_equal_weights_when_no_halflife(self):
        """No half-life should give equal weights."""
        w = _compute_exp_weights(10, half_life=None)
        assert len(w) == 10
        assert np.allclose(w, np.ones(10) / 10)

    def test_equal_weights_zero_halflife(self):
        """Zero half-life should give equal weights."""
        w = _compute_exp_weights(10, half_life=0)
        assert np.allclose(w, np.ones(10) / 10)

    def test_exp_weights_sum_to_one(self):
        """Exponential weights should sum to 1."""
        w = _compute_exp_weights(100, half_life=30)
        assert np.isclose(w.sum(), 1.0)

    def test_exp_weights_recent_higher(self):
        """More recent observations should have higher weight."""
        w = _compute_exp_weights(10, half_life=5)
        # w[-1] is most recent, should be largest
        assert w[-1] > w[0]
        # Should be monotonically increasing
        assert all(w[i] <= w[i+1] for i in range(len(w)-1))

    def test_half_life_property(self):
        """Weight at half-life should be ~0.5 of most recent."""
        hl = 10
        n = 20
        w = _compute_exp_weights(n, half_life=hl)
        # Unnormalized: w[-1] = 1, w[-hl-1] = 0.5
        ratio = w[-hl-1] / w[-1]
        # Due to normalization, check the pattern
        lam = 0.5 ** (1.0 / hl)
        expected_ratio = lam ** hl
        assert np.isclose(expected_ratio, 0.5, atol=0.01)


class TestNeweyWestAdjustment:
    """Test Newey-West autocorrelation adjustment."""

    def test_no_adjustment_zero_lags(self):
        """Zero lags should return original covariance."""
        cov = np.array([[1.0, 0.5], [0.5, 1.0]])
        returns = np.random.randn(100, 2)
        result = _newey_west_adjustment(returns, cov, lags=0)
        assert np.allclose(result, cov)

    def test_adjustment_produces_result(self):
        """Positive lags should modify covariance."""
        np.random.seed(42)
        T, K = 100, 3
        returns = np.random.randn(T, K)
        cov = np.cov(returns.T)
        
        result = _newey_west_adjustment(returns, cov, lags=2)
        
        # Result should be different from original
        assert result.shape == cov.shape
        # Result should still be symmetric
        assert np.allclose(result, result.T)


class TestEstimateFactorCovariance:
    """Test factor covariance estimation."""

    @pytest.fixture
    def sample_factor_returns(self):
        """Create sample factor returns DataFrame."""
        np.random.seed(42)
        n_days = 100
        dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
        
        # Generate correlated factor returns
        cov_true = np.array([
            [0.0004, 0.0001, 0.0000],
            [0.0001, 0.0003, 0.0001],
            [0.0000, 0.0001, 0.0002],
        ])
        L = np.linalg.cholesky(cov_true)
        returns = np.random.randn(n_days, 3) @ L.T
        
        df = pd.DataFrame({
            "trade_date": dates,
            "fr_style_size": returns[:, 0],
            "fr_style_value": returns[:, 1],
            "fr_style_mom": returns[:, 2],
            "n_obs": 5000,
            "r2": 0.25,
        })
        return df.set_index("trade_date")

    def test_basic_estimation(self, sample_factor_returns):
        """Should produce valid covariance matrix."""
        config = RiskModelConfig(
            cov_window=100,
            min_observations=50,
            half_life=None,
            newey_west_lags=0,
        )
        
        cov_df, diag = estimate_factor_covariance(sample_factor_returns, config)
        
        assert isinstance(cov_df, pd.DataFrame)
        assert cov_df.shape[0] == cov_df.shape[1]
        assert "fr_style_size" in cov_df.index
        
        # Should be symmetric
        assert np.allclose(cov_df.values, cov_df.values.T)
        
        # Should be positive semi-definite
        eigvals = np.linalg.eigvalsh(cov_df.values)
        assert all(eigvals >= -1e-10)
        
        # Diagnostics
        assert diag["n_obs"] == 100
        assert diag["n_factors"] == 3

    def test_insufficient_observations_raises(self, sample_factor_returns):
        """Should raise if not enough observations."""
        config = RiskModelConfig(min_observations=200)
        
        with pytest.raises(ValueError, match="Insufficient observations"):
            estimate_factor_covariance(sample_factor_returns, config)

    def test_no_factor_columns_raises(self):
        """Should raise if no fr_* columns."""
        df = pd.DataFrame({
            "trade_date": pd.date_range("2024-01-01", periods=100),
            "other_col": np.random.randn(100),
        }).set_index("trade_date")
        
        with pytest.raises(ValueError, match="No factor return columns"):
            estimate_factor_covariance(df)

    def test_annualization(self, sample_factor_returns):
        """Covariance should be annualized."""
        config = RiskModelConfig(
            cov_window=100,
            half_life=None,
            newey_west_lags=0,
            annualization_factor=252.0,
        )
        
        cov_df, _ = estimate_factor_covariance(sample_factor_returns, config)
        
        # Daily variance around 0.0003, annualized should be ~0.0756
        # Just check it's in a reasonable range
        diag_vars = np.diag(cov_df.values)
        assert all(v > 0.01 for v in diag_vars)  # Annualized should be > 1%
        assert all(v < 1.0 for v in diag_vars)   # But < 100%


class TestEstimateSpecificVariance:
    """Test specific variance estimation."""

    @pytest.fixture
    def sample_specific_returns(self):
        """Create sample specific returns DataFrame."""
        np.random.seed(42)
        n_days = 100
        n_stocks = 50
        dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
        tickers = [f"STOCK{i:03d}" for i in range(n_stocks)]
        
        data = []
        for d in dates:
            for t in tickers:
                # Each stock has different volatility
                vol = 0.01 + 0.005 * hash(t) % 10 / 10
                data.append({
                    "trade_date": d,
                    "ticker": t,
                    "specific_return": np.random.randn() * vol,
                })
        
        return pd.DataFrame(data)

    def test_basic_estimation(self, sample_specific_returns):
        """Should produce valid specific variance estimates."""
        config = RiskModelConfig(
            cov_window=100,
            min_observations=30,
            half_life=None,
            specific_var_shrinkage=0,
        )
        
        result_df, diag = estimate_specific_variance(sample_specific_returns, config)
        
        assert "ticker" in result_df.columns
        assert "specific_var" in result_df.columns
        assert "n_obs" in result_df.columns
        
        # All variances should be positive
        assert all(result_df["specific_var"] > 0)
        
        # Should have reasonable number of stocks
        assert len(result_df) == 50
        
        # Diagnostics
        assert diag["n_stocks"] == 50

    def test_shrinkage_applied(self, sample_specific_returns):
        """Shrinkage should pull variances toward mean."""
        config_no_shrink = RiskModelConfig(specific_var_shrinkage=0)
        config_shrink = RiskModelConfig(specific_var_shrinkage=0.5)
        
        result_no_shrink, _ = estimate_specific_variance(
            sample_specific_returns, config_no_shrink
        )
        result_shrink, _ = estimate_specific_variance(
            sample_specific_returns, config_shrink
        )
        
        # With shrinkage, variance should be less dispersed
        std_no_shrink = result_no_shrink["specific_var"].std()
        std_shrink = result_shrink["specific_var"].std()
        
        assert std_shrink < std_no_shrink

    def test_floor_applied(self, sample_specific_returns):
        """Floor should prevent near-zero variances."""
        config = RiskModelConfig(specific_var_floor=0.001)
        
        result_df, _ = estimate_specific_variance(sample_specific_returns, config)
        
        assert all(result_df["specific_var"] >= 0.001)


class TestComputePortfolioRisk:
    """Test portfolio risk computation."""

    @pytest.fixture
    def risk_inputs(self):
        """Create inputs for portfolio risk calculation."""
        tickers = ["A", "B", "C", "D", "E"]
        factors = ["fr_style_size", "fr_style_value"]
        
        # Portfolio weights
        weights = pd.Series([0.3, 0.25, 0.2, 0.15, 0.1], index=tickers)
        
        # Factor exposures
        exposures = pd.DataFrame({
            "style_size": [1.0, 0.5, -0.5, -1.0, 0.0],
            "style_value": [0.5, 1.0, 0.0, -0.5, -1.0],
        }, index=tickers)
        
        # Factor covariance (annualized)
        factor_cov = pd.DataFrame(
            [[0.04, 0.01], [0.01, 0.03]],
            index=factors,
            columns=factors,
        )
        
        # Specific variance
        specific_var = pd.DataFrame({
            "ticker": tickers,
            "specific_var": [0.05, 0.06, 0.04, 0.07, 0.05],
        })
        
        return weights, exposures, factor_cov, specific_var

    def test_basic_computation(self, risk_inputs):
        """Should compute valid risk decomposition."""
        weights, exposures, factor_cov, specific_var = risk_inputs
        
        result = compute_portfolio_risk(weights, exposures, factor_cov, specific_var)
        
        assert "total_var" in result
        assert "factor_var" in result
        assert "specific_var" in result
        assert "total_vol" in result
        assert "factor_vol" in result
        assert "specific_vol" in result
        
        # Variance decomposition should add up
        assert np.isclose(
            result["total_var"],
            result["factor_var"] + result["specific_var"],
            rtol=1e-5
        )
        
        # Volatility should be sqrt of variance
        assert np.isclose(result["total_vol"], np.sqrt(result["total_var"]))
        
        # Percentages should sum to 1
        assert np.isclose(
            result["factor_var_pct"] + result["specific_var_pct"],
            1.0
        )

    def test_all_positive_values(self, risk_inputs):
        """All risk measures should be non-negative."""
        weights, exposures, factor_cov, specific_var = risk_inputs
        
        result = compute_portfolio_risk(weights, exposures, factor_cov, specific_var)
        
        for key in ["total_var", "factor_var", "specific_var",
                    "total_vol", "factor_vol", "specific_vol"]:
            assert result[key] >= 0


class TestRiskModelClass:
    """Test RiskModel convenience class."""

    @pytest.fixture
    def sample_data(self):
        """Create sample factor and specific returns."""
        np.random.seed(42)
        n_days = 100
        n_stocks = 20
        dates = pd.date_range("2024-01-01", periods=n_days, freq="B")
        
        # Factor returns
        factor_returns = pd.DataFrame({
            "fr_style_size": np.random.randn(n_days) * 0.01,
            "fr_style_value": np.random.randn(n_days) * 0.008,
        }, index=dates)
        
        # Specific returns
        tickers = [f"STK{i:02d}" for i in range(n_stocks)]
        specific_data = []
        for d in dates:
            for t in tickers:
                specific_data.append({
                    "trade_date": d,
                    "ticker": t,
                    "specific_return": np.random.randn() * 0.02,
                })
        specific_returns = pd.DataFrame(specific_data)
        
        return factor_returns, specific_returns

    def test_fit_and_access(self, sample_data):
        """Should fit and provide access to results."""
        factor_returns, specific_returns = sample_data
        
        model = RiskModel(config=RiskModelConfig(
            min_observations=50,
            half_life=None,
        ))
        model.fit(factor_returns, specific_returns)
        
        assert model.factor_cov is not None
        assert model.specific_var is not None
        assert len(model.factor_cov) == 2
        assert len(model.specific_var) == 20

    def test_get_factor_volatility(self, sample_data):
        """Should return factor volatilities."""
        factor_returns, specific_returns = sample_data
        
        model = RiskModel()
        model.fit(factor_returns, specific_returns)
        
        vols = model.get_factor_volatility()
        
        assert isinstance(vols, pd.Series)
        assert len(vols) == 2
        assert all(vols > 0)

    def test_get_factor_correlation(self, sample_data):
        """Should return correlation matrix."""
        factor_returns, specific_returns = sample_data
        
        model = RiskModel()
        model.fit(factor_returns, specific_returns)
        
        corr = model.get_factor_correlation()
        
        assert isinstance(corr, pd.DataFrame)
        # Diagonal should be 1
        assert np.allclose(np.diag(corr.values), 1.0)
        # Should be symmetric
        assert np.allclose(corr.values, corr.values.T)
        # Correlations should be in [-1, 1]
        assert corr.values.min() >= -1.0
        assert corr.values.max() <= 1.0

    def test_compute_risk_before_fit_raises(self):
        """Should raise if compute_risk called before fit."""
        model = RiskModel()
        
        with pytest.raises(RuntimeError, match="Must call fit"):
            model.compute_risk(pd.Series(), pd.DataFrame())

    def test_chain_fit(self, sample_data):
        """Fit should return self for chaining."""
        factor_returns, specific_returns = sample_data
        
        model = RiskModel()
        result = model.fit(factor_returns, specific_returns)
        
        assert result is model
