#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Unit tests for Barra factor calculators (Post-MVP).

Tests the factor calculation functions in alphahome.barra.factor_calculators.
"""

import numpy as np
import pandas as pd
import pytest

from alphahome.barra.factor_calculators import (
    winsorize_series,
    weighted_zscore,
    industry_neutralize,
    exponential_weights,
    calculate_size,
    calculate_value,
    calculate_liquidity,
    calculate_growth,
    calculate_leverage,
    calculate_nlsize,
    calculate_momentum,
)


class TestWinsorizeSeries:
    """Tests for winsorize_series function."""
    
    def test_basic_winsorization(self):
        """Test basic quantile winsorization."""
        s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 100])
        result = winsorize_series(s, lower_q=0.1, upper_q=0.9)
        
        # 100 should be clipped to 90th percentile
        assert result.iloc[-1] < 100
        assert result.iloc[0] >= 1  # No clipping at low end for this data
    
    def test_with_absolute_bounds(self):
        """Test winsorization with absolute bounds."""
        s = pd.Series([-200, -50, 0, 50, 200, 600])
        result = winsorize_series(s, lower_bound=-100, upper_bound=500)
        
        assert result.min() >= -100
        assert result.max() <= 500
    
    def test_handles_nan(self):
        """Test that NaN values are preserved."""
        s = pd.Series([1, 2, np.nan, 4, 5])
        result = winsorize_series(s)
        
        assert pd.isna(result.iloc[2])
        assert result.notna().sum() == 4
    
    def test_empty_series(self):
        """Test handling of empty series."""
        s = pd.Series([], dtype=float)
        result = winsorize_series(s)
        assert len(result) == 0


class TestWeightedZscore:
    """Tests for weighted_zscore function."""
    
    def test_basic_zscore(self):
        """Test basic weighted z-score calculation."""
        x = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        w = pd.Series([1.0, 1.0, 1.0, 1.0, 1.0])  # Equal weights
        
        result = weighted_zscore(x, w)
        
        # With equal weights, should be close to standard zscore
        assert abs(result.mean()) < 0.01
        assert abs(result.std() - 1.0) < 0.2
    
    def test_weighted_zscore(self):
        """Test that weights affect the z-score."""
        x = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        w1 = pd.Series([1.0, 1.0, 1.0, 1.0, 1.0])
        w2 = pd.Series([10.0, 1.0, 1.0, 1.0, 1.0])  # Heavy weight on first value
        
        result1 = weighted_zscore(x, w1)
        result2 = weighted_zscore(x, w2)
        
        # Results should differ
        assert not np.allclose(result1.values, result2.values)
    
    def test_handles_nan_values(self):
        """Test handling of NaN values."""
        x = pd.Series([1.0, np.nan, 3.0, 4.0, 5.0])
        w = pd.Series([1.0, 1.0, 1.0, 1.0, 1.0])
        
        result = weighted_zscore(x, w)
        
        assert pd.isna(result.iloc[1])
        assert result.notna().sum() == 4
    
    def test_insufficient_data(self):
        """Test behavior with insufficient data."""
        x = pd.Series([1.0])
        w = pd.Series([1.0])
        
        result = weighted_zscore(x, w)
        assert pd.isna(result.iloc[0])


class TestIndustryNeutralize:
    """Tests for industry_neutralize function."""
    
    def test_removes_industry_effects(self):
        """Test that industry effects are removed."""
        # Create data with clear industry effects (need >= 10 samples)
        x = pd.Series([1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 16])  # Two groups
        industry = pd.Series(["A", "A", "A", "A", "A", "A", 
                              "B", "B", "B", "B", "B", "B"])
        
        result = industry_neutralize(x, industry)
        
        # Residuals should have zero mean per industry
        assert abs(result.iloc[:6].mean()) < 0.01
        assert abs(result.iloc[6:].mean()) < 0.01
    
    def test_preserves_within_industry_variation(self):
        """Test that within-industry variation is preserved."""
        x = pd.Series([1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15, 16])
        industry = pd.Series(["A", "A", "A", "A", "A", "A",
                              "B", "B", "B", "B", "B", "B"])
        
        result = industry_neutralize(x, industry)
        
        # Within-industry spread should be preserved
        assert result.iloc[:6].std() > 0
        assert result.iloc[6:].std() > 0


class TestExponentialWeights:
    """Tests for exponential_weights function."""
    
    def test_weights_sum_to_one(self):
        """Test that weights sum to 1."""
        weights = exponential_weights(window=100, half_life=20)
        assert abs(weights.sum() - 1.0) < 1e-10
    
    def test_decay_pattern(self):
        """Test that weights decay properly."""
        weights = exponential_weights(window=100, half_life=20)
        
        # Most recent weight should be highest
        assert weights[0] > weights[50]
        assert weights[50] > weights[99]
    
    def test_half_life(self):
        """Test that half-life is approximately correct."""
        half_life = 20
        weights = exponential_weights(window=100, half_life=half_life)
        
        # Weight at half_life should be roughly half of weight at 0
        ratio = weights[half_life] / weights[0]
        assert abs(ratio - 0.5) < 0.01


class TestCalculateSize:
    """Tests for calculate_size function."""
    
    def test_log_transform(self):
        """Test that log transform is applied."""
        mcap = pd.Series([100, 1000, 10000])
        
        result = calculate_size(mcap, weights=pd.Series([1, 1, 1]))
        
        # Check relative ordering (log transform preserves order)
        assert result.iloc[0] < result.iloc[1] < result.iloc[2]
    
    def test_handles_zero_and_negative(self):
        """Test handling of zero and negative values."""
        mcap = pd.Series([0, -100, 100, 1000])
        
        result = calculate_size(mcap, weights=pd.Series([1, 1, 1, 1]))
        
        # Zero and negative should result in NaN
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.notna(result.iloc[2])


class TestCalculateValue:
    """Tests for calculate_value function."""
    
    def test_composite_calculation(self):
        """Test that composite value is calculated from multiple indicators."""
        pe = pd.Series([10, 20, 15])
        pb = pd.Series([1, 2, 1.5])
        ps = pd.Series([5, 10, 7.5])
        dv = pd.Series([2, 4, 3])
        w = pd.Series([1, 1, 1])
        
        result = calculate_value(pe, pb, ps, dv, w)
        
        # Should produce valid z-scores
        assert result.notna().all()
        assert abs(result.mean()) < 0.5  # Approximately centered
    
    def test_handles_zero_denominators(self):
        """Test handling of zero PE/PB/PS."""
        pe = pd.Series([0, 10, 20])  # Zero PE
        pb = pd.Series([1, 2, 0])    # Zero PB
        ps = pd.Series([5, 0, 10])   # Zero PS
        dv = pd.Series([2, 3, 4])
        w = pd.Series([1, 1, 1])
        
        result = calculate_value(pe, pb, ps, dv, w)
        
        # Should handle gracefully (some NaNs expected)
        assert result.notna().any()


class TestCalculateLiquidity:
    """Tests for calculate_liquidity function."""
    
    def test_multi_window_composite(self):
        """Test that multi-window turnover is combined."""
        turn_21d = pd.Series([1, 2, 3, 4, 5])
        turn_63d = pd.Series([1.5, 2.5, 3.5, 4.5, 5.5])
        turn_252d = pd.Series([1.2, 2.2, 3.2, 4.2, 5.2])
        w = pd.Series([1, 1, 1, 1, 1])
        
        result = calculate_liquidity(turn_21d, turn_63d, turn_252d, None, w)
        
        assert result.notna().all()
        # Higher turnover should give higher liquidity
        assert result.iloc[0] < result.iloc[4]


class TestCalculateGrowth:
    """Tests for calculate_growth function."""
    
    def test_growth_composite(self):
        """Test growth composite from multiple indicators."""
        np_yoy = pd.Series([10, 20, -5, 30, 15])
        rev_yoy = pd.Series([15, 25, 0, 35, 20])
        ocf_yoy = pd.Series([5, 15, -10, 25, 10])
        w = pd.Series([1, 1, 1, 1, 1])
        
        result = calculate_growth(np_yoy, rev_yoy, ocf_yoy, w)
        
        assert result.notna().all()
    
    def test_extreme_value_handling(self):
        """Test that extreme growth values are handled."""
        np_yoy = pd.Series([-150, 1000, 50, 75, 100])  # Extreme values
        rev_yoy = pd.Series([20, 30, 40, 50, 60])
        ocf_yoy = pd.Series([10, 20, 30, 40, 50])
        w = pd.Series([1, 1, 1, 1, 1])
        
        result = calculate_growth(np_yoy, rev_yoy, ocf_yoy, w)
        
        # Extreme values should be winsorized
        assert result.notna().all()
        assert abs(result.max()) < 5  # z-scores should be bounded


class TestCalculateLeverage:
    """Tests for calculate_leverage function."""
    
    def test_leverage_composite(self):
        """Test leverage composite calculation."""
        da = pd.Series([30, 40, 50, 60, 70])  # Debt/Assets %
        de = pd.Series([0.5, 0.75, 1.0, 1.5, 2.0])  # Debt/Equity
        w = pd.Series([1, 1, 1, 1, 1])
        
        result = calculate_leverage(da, de, w)
        
        assert result.notna().all()
        # Higher leverage should have higher z-score
        assert result.iloc[0] < result.iloc[4]


class TestCalculateNlsize:
    """Tests for calculate_nlsize function."""
    
    def test_orthogonalization(self):
        """Test that NLSize is orthogonal to Size."""
        size = pd.Series([1, 2, 3, 4, 5], dtype=float)
        w = pd.Series([1, 1, 1, 1, 1])
        
        result = calculate_nlsize(size, w)
        
        # Correlation between size and nlsize should be near zero
        valid_mask = size.notna() & result.notna()
        if valid_mask.sum() > 2:
            corr = size[valid_mask].corr(result[valid_mask])
            assert abs(corr) < 0.1


class TestCalculateMomentum:
    """Tests for calculate_momentum function."""
    
    def test_momentum_calculation(self):
        """Test basic momentum calculation."""
        cumret_252_21 = pd.Series([0.1, 0.2, 0.05, 0.3, -0.1])
        cumret_126_21 = pd.Series([0.05, 0.15, 0.02, 0.2, -0.05])
        cumret_21_1 = pd.Series([0.01, 0.02, 0.005, 0.03, -0.01])
        industry = pd.Series(["A", "A", "B", "B", "B"])
        w = pd.Series([1, 1, 1, 1, 1])
        
        result = calculate_momentum(
            cumret_252_21, cumret_126_21, cumret_21_1, 
            industry, w, reversal_adj=0.1, neutralize=False
        )
        
        assert result.notna().all()
    
    def test_reversal_adjustment(self):
        """Test that short-term reversal adjustment works."""
        cumret_252_21 = pd.Series([0.1, 0.1, 0.1, 0.1, 0.1])
        cumret_126_21 = pd.Series([0.1, 0.1, 0.1, 0.1, 0.1])
        cumret_21_1 = pd.Series([0.05, 0.0, -0.05, 0.1, -0.1])  # Varying short-term
        industry = pd.Series(["A", "A", "A", "A", "A"])
        w = pd.Series([1, 1, 1, 1, 1])
        
        # With reversal adjustment
        result_with = calculate_momentum(
            cumret_252_21, cumret_126_21, cumret_21_1,
            industry, w, reversal_adj=0.5, neutralize=False
        )
        
        # Without reversal adjustment
        result_without = calculate_momentum(
            cumret_252_21, cumret_126_21, cumret_21_1,
            industry, w, reversal_adj=0.0, neutralize=False
        )
        
        # Results should differ due to reversal adjustment
        assert not np.allclose(result_with.values, result_without.values, equal_nan=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
