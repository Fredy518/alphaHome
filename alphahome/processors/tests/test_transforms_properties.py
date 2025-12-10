#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for transform functions.

Uses hypothesis library for property-based testing.
"""

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck

from alphahome.processors.operations.transforms import (
    zscore,
    minmax_scale,
    rolling_zscore,
    rolling_percentile,
    winsorize,
    log_return,
    price_acceleration,
    trend_strength_index,
)


# =============================================================================
# Custom Strategies
# =============================================================================

# Use numpy arrays for faster generation, then convert to Series
float_arrays = st.lists(
    st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
    min_size=1,
    max_size=100
).map(lambda x: pd.Series(x))

# For rolling functions that need minimum size
float_arrays_for_rolling = st.lists(
    st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
    min_size=5,
    max_size=100
).map(lambda x: pd.Series(x))

# Strategy for window sizes
window_sizes = st.integers(min_value=2, max_value=20)

# Strategy for constant series (all values equal)
constant_series = st.floats(
    min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False
).flatmap(
    lambda val: st.integers(min_value=1, max_value=100).map(
        lambda size: pd.Series([val] * size)
    )
)

# Strategy for constant series with minimum size for rolling functions
constant_series_for_rolling = st.floats(
    min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False
).flatmap(
    lambda val: st.integers(min_value=5, max_value=100).map(
        lambda size: pd.Series([val] * size)
    )
)


# =============================================================================
# Property 1: Transform output shape preservation
# **Feature: processors-refactoring, Property 1: Transform output shape preservation**
# **Validates: Requirements 1.1, 4.1, 4.2, 4.3, 4.4**
# =============================================================================

class TestProperty1TransformOutputShapePreservation:
    """
    Property 1: Transform output shape preservation
    
    *For any* non-empty input Series, transform functions (zscore, rolling_zscore, 
    rolling_percentile, winsorize) SHALL return a Series with the same length as the input.
    
    **Feature: processors-refactoring, Property 1: Transform output shape preservation**
    **Validates: Requirements 1.1, 4.1, 4.2, 4.3, 4.4**
    """

    @given(float_arrays)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_zscore_preserves_shape(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 1: Transform output shape preservation**
        **Validates: Requirements 1.1, 4.1**
        
        zscore SHALL return a Series with the same length as the input.
        """
        result = zscore(series)
        assert len(result) == len(series), (
            f"zscore changed length: input={len(series)}, output={len(result)}"
        )

    @given(float_arrays)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_minmax_scale_preserves_shape(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 1: Transform output shape preservation**
        **Validates: Requirements 1.1, 4.1**
        
        minmax_scale SHALL return a Series with the same length as the input.
        """
        result = minmax_scale(series)
        assert len(result) == len(series), (
            f"minmax_scale changed length: input={len(series)}, output={len(result)}"
        )

    @given(float_arrays_for_rolling)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_rolling_zscore_preserves_shape(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 1: Transform output shape preservation**
        **Validates: Requirements 4.2**
        
        rolling_zscore SHALL return a Series with the same length as the input.
        """
        # Use a reasonable window size relative to series length
        window = min(5, len(series))
        result = rolling_zscore(series, window=window)
        assert len(result) == len(series), (
            f"rolling_zscore changed length: input={len(series)}, output={len(result)}"
        )

    @given(float_arrays_for_rolling)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_rolling_percentile_preserves_shape(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 1: Transform output shape preservation**
        **Validates: Requirements 4.3**
        
        rolling_percentile SHALL return a Series with the same length as the input.
        """
        # Use a reasonable window size relative to series length
        window = min(5, len(series))
        result = rolling_percentile(series, window=window)
        assert len(result) == len(series), (
            f"rolling_percentile changed length: input={len(series)}, output={len(result)}"
        )

    @given(float_arrays_for_rolling)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_winsorize_preserves_shape(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 1: Transform output shape preservation**
        **Validates: Requirements 4.4**
        
        winsorize SHALL return a Series with the same length as the input.
        """
        # Use a reasonable window size relative to series length
        window = min(5, len(series))
        result = winsorize(series, window=window)
        assert len(result) == len(series), (
            f"winsorize changed length: input={len(series)}, output={len(result)}"
        )


# =============================================================================
# Property 2: Rolling percentile value range
# **Feature: processors-refactoring, Property 2: Rolling percentile value range**
# **Validates: Requirements 4.3**
# =============================================================================

class TestProperty2RollingPercentileValueRange:
    """
    Property 2: Rolling percentile value range
    
    *For any* input Series and any window size, rolling_percentile SHALL return 
    values in the range [0, 1] for all non-NaN outputs.
    
    **Feature: processors-refactoring, Property 2: Rolling percentile value range**
    **Validates: Requirements 4.3**
    """

    @given(float_arrays_for_rolling, window_sizes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_rolling_percentile_values_in_range(self, series: pd.Series, window: int):
        """
        **Feature: processors-refactoring, Property 2: Rolling percentile value range**
        **Validates: Requirements 4.3**
        
        For any input Series and any window size, rolling_percentile SHALL return 
        values in the range [0, 1] for all non-NaN outputs.
        """
        # Ensure window doesn't exceed series length
        window = min(window, len(series))
        
        result = rolling_percentile(series, window=window)
        
        # Get non-NaN values
        valid_values = result.dropna()
        
        if len(valid_values) > 0:
            # All values should be >= 0
            assert (valid_values >= 0).all(), (
                f"rolling_percentile returned values < 0: "
                f"min={valid_values.min()}, values={valid_values[valid_values < 0].tolist()}"
            )
            
            # All values should be <= 1
            assert (valid_values <= 1).all(), (
                f"rolling_percentile returned values > 1: "
                f"max={valid_values.max()}, values={valid_values[valid_values > 1].tolist()}"
            )

    @given(constant_series_for_rolling, window_sizes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_rolling_percentile_constant_series_in_range(self, series: pd.Series, window: int):
        """
        **Feature: processors-refactoring, Property 2: Rolling percentile value range**
        **Validates: Requirements 4.3**
        
        For constant Series (edge case), rolling_percentile SHALL still return 
        values in the range [0, 1] for all non-NaN outputs.
        """
        # Ensure window doesn't exceed series length
        window = min(window, len(series))
        
        result = rolling_percentile(series, window=window)
        
        # Get non-NaN values
        valid_values = result.dropna()
        
        if len(valid_values) > 0:
            # All values should be in [0, 1]
            assert (valid_values >= 0).all() and (valid_values <= 1).all(), (
                f"rolling_percentile returned values outside [0, 1] for constant series: "
                f"min={valid_values.min()}, max={valid_values.max()}"
            )


# =============================================================================
# Property 3: Zscore zero variance handling
# **Feature: processors-refactoring, Property 3: Zscore zero variance handling**
# **Validates: Requirements 1.6, 4.1**
# =============================================================================

class TestProperty3ZscoreZeroVarianceHandling:
    """
    Property 3: Zscore zero variance handling
    
    *For any* constant Series (all values equal), zscore and rolling_zscore 
    SHALL return all zeros instead of NaN or infinity.
    
    **Feature: processors-refactoring, Property 3: Zscore zero variance handling**
    **Validates: Requirements 1.6, 4.1**
    """

    @given(constant_series)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_zscore_returns_zeros_for_constant_series(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 3: Zscore zero variance handling**
        **Validates: Requirements 1.6, 4.1**
        
        For any constant Series (all values equal), zscore SHALL return all zeros
        instead of NaN or infinity.
        """
        result = zscore(series)
        
        # All values should be zero
        assert (result == 0.0).all(), (
            f"zscore did not return all zeros for constant series: {result.tolist()}"
        )
        
        # No NaN values
        assert not result.isna().any(), (
            f"zscore returned NaN for constant series: {result.tolist()}"
        )
        
        # No infinity values
        assert not np.isinf(result).any(), (
            f"zscore returned infinity for constant series: {result.tolist()}"
        )

    @given(constant_series_for_rolling)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_rolling_zscore_returns_zeros_for_constant_series(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 3: Zscore zero variance handling**
        **Validates: Requirements 1.6, 4.1**
        
        For any constant Series (all values equal), rolling_zscore SHALL return 
        all zeros instead of NaN or infinity.
        """
        # Use a reasonable window size relative to series length
        window = min(5, len(series))
        result = rolling_zscore(series, window=window)
        
        # All values should be zero (excluding initial NaN from rolling window warmup)
        # Note: rolling functions may have NaN at the start due to min_periods
        non_nan_result = result.dropna()
        
        if len(non_nan_result) > 0:
            # All non-NaN values should be zero
            assert (non_nan_result == 0.0).all(), (
                f"rolling_zscore did not return all zeros for constant series: "
                f"{non_nan_result.tolist()}"
            )
            
            # No infinity values in non-NaN results
            assert not np.isinf(non_nan_result).any(), (
                f"rolling_zscore returned infinity for constant series: "
                f"{non_nan_result.tolist()}"
            )


# =============================================================================
# Property 4: Winsorize bounds enforcement
# **Feature: processors-refactoring, Property 4: Winsorize bounds enforcement**
# **Validates: Requirements 4.4**
# =============================================================================

class TestProperty4WinsorizeBoundsEnforcement:
    """
    Property 4: Winsorize bounds enforcement
    
    *For any* input Series, winsorize output values SHALL be within n_std 
    standard deviations of the rolling mean.
    
    **Feature: processors-refactoring, Property 4: Winsorize bounds enforcement**
    **Validates: Requirements 4.4**
    """

    @given(float_arrays_for_rolling, window_sizes, st.floats(min_value=1.0, max_value=5.0))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_winsorize_values_within_bounds(
        self, series: pd.Series, window: int, n_std: float
    ):
        """
        **Feature: processors-refactoring, Property 4: Winsorize bounds enforcement**
        **Validates: Requirements 4.4**
        
        For any input Series, winsorize output values SHALL be within n_std 
        standard deviations of the rolling mean.
        """
        # Ensure window doesn't exceed series length
        window = min(window, len(series))
        min_periods = max(1, window // 2)
        
        result = winsorize(series, window=window, n_std=n_std, min_periods=min_periods)
        
        # Compute the bounds that were used
        rolling_mean = series.rolling(window, min_periods=min_periods).mean()
        rolling_std = series.rolling(window, min_periods=min_periods).std()
        
        upper_bound = rolling_mean + n_std * rolling_std
        lower_bound = rolling_mean - n_std * rolling_std
        
        # For each position where we have valid bounds, check that result is within bounds
        for i in range(len(result)):
            if pd.notna(result.iloc[i]) and pd.notna(upper_bound.iloc[i]) and pd.notna(lower_bound.iloc[i]):
                # Use small tolerance for floating point comparison
                tolerance = 1e-10
                assert result.iloc[i] <= upper_bound.iloc[i] + tolerance, (
                    f"winsorize value {result.iloc[i]} exceeds upper bound "
                    f"{upper_bound.iloc[i]} at index {i}"
                )
                assert result.iloc[i] >= lower_bound.iloc[i] - tolerance, (
                    f"winsorize value {result.iloc[i]} below lower bound "
                    f"{lower_bound.iloc[i]} at index {i}"
                )

    @given(float_arrays_for_rolling)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_winsorize_clips_extreme_values(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 4: Winsorize bounds enforcement**
        **Validates: Requirements 4.4**
        
        Winsorize SHALL clip values that exceed n_std standard deviations.
        Values within bounds should remain unchanged.
        """
        window = min(5, len(series))
        n_std = 3.0
        min_periods = max(1, window // 2)
        
        result = winsorize(series, window=window, n_std=n_std, min_periods=min_periods)
        
        # Compute bounds
        rolling_mean = series.rolling(window, min_periods=min_periods).mean()
        rolling_std = series.rolling(window, min_periods=min_periods).std()
        
        upper_bound = rolling_mean + n_std * rolling_std
        lower_bound = rolling_mean - n_std * rolling_std
        
        # Check that values within bounds are unchanged
        for i in range(len(result)):
            if (pd.notna(result.iloc[i]) and pd.notna(upper_bound.iloc[i]) 
                and pd.notna(lower_bound.iloc[i])):
                original = series.iloc[i]
                # If original was within bounds, result should equal original
                if lower_bound.iloc[i] <= original <= upper_bound.iloc[i]:
                    assert np.isclose(result.iloc[i], original, rtol=1e-10), (
                        f"winsorize changed value {original} to {result.iloc[i]} "
                        f"even though it was within bounds [{lower_bound.iloc[i]}, "
                        f"{upper_bound.iloc[i]}] at index {i}"
                    )

    @given(constant_series_for_rolling)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_winsorize_constant_series_unchanged(self, series: pd.Series):
        """
        **Feature: processors-refactoring, Property 4: Winsorize bounds enforcement**
        **Validates: Requirements 4.4**
        
        For constant Series, winsorize SHALL return the original values unchanged
        (since all values are at the mean, they are within any n_std bounds).
        """
        window = min(5, len(series))
        n_std = 3.0
        
        result = winsorize(series, window=window, n_std=n_std)
        
        # For constant series, all values should remain unchanged
        # (they are all at the mean, so within bounds)
        for i in range(len(result)):
            if pd.notna(result.iloc[i]):
                assert np.isclose(result.iloc[i], series.iloc[i], rtol=1e-10), (
                    f"winsorize changed constant value {series.iloc[i]} to "
                    f"{result.iloc[i]} at index {i}"
                )


# =============================================================================
# Property 5: Price acceleration output structure
# **Feature: processors-refactoring, Property 5: Price acceleration output structure**
# **Validates: Requirements 4.6**
# =============================================================================

# Strategy for price series (positive values, simulating stock prices)
price_series = st.lists(
    st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
    min_size=10,
    max_size=100
).map(lambda x: pd.Series(x))


class TestProperty5PriceAccelerationOutputStructure:
    """
    Property 5: Price acceleration output structure
    
    *For any* non-empty price Series, price_acceleration SHALL return a DataFrame 
    containing columns: slope_long, slope_short, acceleration, acceleration_zscore, slope_ratio.
    
    **Feature: processors-refactoring, Property 5: Price acceleration output structure**
    **Validates: Requirements 4.6**
    """

    REQUIRED_COLUMNS = ['slope_long', 'slope_short', 'acceleration', 'acceleration_zscore', 'slope_ratio']

    @given(price_series)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_price_acceleration_returns_dataframe_with_required_columns(self, prices: pd.Series):
        """
        **Feature: processors-refactoring, Property 5: Price acceleration output structure**
        **Validates: Requirements 4.6**
        
        For any non-empty price Series, price_acceleration SHALL return a DataFrame
        containing all required columns.
        """
        # Use smaller windows for shorter test series
        long_window = min(20, len(prices) - 1)
        short_window = min(10, long_window - 1)
        
        # Skip if series is too short for meaningful calculation
        assume(len(prices) >= 10)
        assume(long_window > short_window)
        assume(short_window >= 3)
        
        result = price_acceleration(
            prices, 
            long_window=long_window, 
            short_window=short_window
        )
        
        # Result should be a DataFrame
        assert isinstance(result, pd.DataFrame), (
            f"price_acceleration did not return a DataFrame, got {type(result)}"
        )
        
        # Check all required columns are present
        for col in self.REQUIRED_COLUMNS:
            assert col in result.columns, (
                f"price_acceleration missing required column '{col}'. "
                f"Got columns: {list(result.columns)}"
            )

    @given(price_series)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_price_acceleration_output_length_matches_input(self, prices: pd.Series):
        """
        **Feature: processors-refactoring, Property 5: Price acceleration output structure**
        **Validates: Requirements 4.6**
        
        For any non-empty price Series, price_acceleration output DataFrame
        SHALL have the same length as the input Series.
        """
        # Use smaller windows for shorter test series
        long_window = min(20, len(prices) - 1)
        short_window = min(10, long_window - 1)
        
        # Skip if series is too short for meaningful calculation
        assume(len(prices) >= 10)
        assume(long_window > short_window)
        assume(short_window >= 3)
        
        result = price_acceleration(
            prices, 
            long_window=long_window, 
            short_window=short_window
        )
        
        # Output length should match input length
        assert len(result) == len(prices), (
            f"price_acceleration output length {len(result)} does not match "
            f"input length {len(prices)}"
        )

    @given(price_series)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_price_acceleration_index_preserved(self, prices: pd.Series):
        """
        **Feature: processors-refactoring, Property 5: Price acceleration output structure**
        **Validates: Requirements 4.6**
        
        For any non-empty price Series, price_acceleration output DataFrame
        SHALL preserve the input Series index.
        """
        # Use smaller windows for shorter test series
        long_window = min(20, len(prices) - 1)
        short_window = min(10, long_window - 1)
        
        # Skip if series is too short for meaningful calculation
        assume(len(prices) >= 10)
        assume(long_window > short_window)
        assume(short_window >= 3)
        
        result = price_acceleration(
            prices, 
            long_window=long_window, 
            short_window=short_window
        )
        
        # Index should be preserved
        assert result.index.equals(prices.index), (
            f"price_acceleration did not preserve index. "
            f"Input index: {prices.index.tolist()}, Output index: {result.index.tolist()}"
        )

    def test_price_acceleration_empty_series_returns_empty_dataframe(self):
        """
        **Feature: processors-refactoring, Property 5: Price acceleration output structure**
        **Validates: Requirements 4.6**
        
        For empty price Series, price_acceleration SHALL return an empty DataFrame.
        """
        empty_series = pd.Series(dtype=float)
        result = price_acceleration(empty_series)
        
        assert isinstance(result, pd.DataFrame), (
            f"price_acceleration did not return a DataFrame for empty input, got {type(result)}"
        )
        assert result.empty, (
            f"price_acceleration did not return empty DataFrame for empty input"
        )


# =============================================================================
# Property 6: Trend strength consistency range
# **Feature: processors-refactoring, Property 6: Trend strength consistency range**
# **Validates: Requirements 4.7**
# =============================================================================

class TestProperty6TrendStrengthConsistencyRange:
    """
    Property 6: Trend strength consistency range
    
    *For any* price Series, trend_strength_index output column 'trend_consistency' 
    SHALL have values in range [0, 1] for all non-NaN outputs.
    
    **Feature: processors-refactoring, Property 6: Trend strength consistency range**
    **Validates: Requirements 4.7**
    """

    @given(price_series)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_trend_consistency_values_in_range(self, prices: pd.Series):
        """
        **Feature: processors-refactoring, Property 6: Trend strength consistency range**
        **Validates: Requirements 4.7**
        
        For any price Series, trend_strength_index output column 'trend_consistency'
        SHALL have values in range [0, 1] for all non-NaN outputs.
        """
        # Use smaller windows for shorter test series
        # Default windows are [20, 60, 120, 252], but we need to adapt for test data
        max_window = len(prices) - 1
        windows = [w for w in [5, 10, 15, 20] if w < max_window]
        
        # Skip if series is too short for meaningful calculation
        assume(len(prices) >= 10)
        assume(len(windows) >= 2)
        
        result = trend_strength_index(prices, windows=windows)
        
        # Check that trend_consistency column exists
        assert 'trend_consistency' in result.columns, (
            f"trend_strength_index missing 'trend_consistency' column. "
            f"Got columns: {list(result.columns)}"
        )
        
        # Get non-NaN values from trend_consistency
        consistency_values = result['trend_consistency'].dropna()
        
        if len(consistency_values) > 0:
            # All values should be >= 0
            assert (consistency_values >= 0).all(), (
                f"trend_consistency returned values < 0: "
                f"min={consistency_values.min()}, "
                f"values={consistency_values[consistency_values < 0].tolist()}"
            )
            
            # All values should be <= 1
            assert (consistency_values <= 1).all(), (
                f"trend_consistency returned values > 1: "
                f"max={consistency_values.max()}, "
                f"values={consistency_values[consistency_values > 1].tolist()}"
            )

    @given(price_series)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_trend_strength_index_returns_dataframe_with_required_columns(self, prices: pd.Series):
        """
        **Feature: processors-refactoring, Property 6: Trend strength consistency range**
        **Validates: Requirements 4.7**
        
        For any price Series, trend_strength_index SHALL return a DataFrame
        containing trend_strength and trend_consistency columns.
        """
        # Use smaller windows for shorter test series
        max_window = len(prices) - 1
        windows = [w for w in [5, 10, 15, 20] if w < max_window]
        
        # Skip if series is too short for meaningful calculation
        assume(len(prices) >= 10)
        assume(len(windows) >= 2)
        
        result = trend_strength_index(prices, windows=windows)
        
        # Result should be a DataFrame
        assert isinstance(result, pd.DataFrame), (
            f"trend_strength_index did not return a DataFrame, got {type(result)}"
        )
        
        # Check required columns are present
        assert 'trend_strength' in result.columns, (
            f"trend_strength_index missing 'trend_strength' column. "
            f"Got columns: {list(result.columns)}"
        )
        assert 'trend_consistency' in result.columns, (
            f"trend_strength_index missing 'trend_consistency' column. "
            f"Got columns: {list(result.columns)}"
        )
        
        # Check slope columns for each window
        for w in windows:
            col_name = f'slope_{w}'
            assert col_name in result.columns, (
                f"trend_strength_index missing '{col_name}' column. "
                f"Got columns: {list(result.columns)}"
            )

    @given(price_series)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_trend_strength_index_output_length_matches_input(self, prices: pd.Series):
        """
        **Feature: processors-refactoring, Property 6: Trend strength consistency range**
        **Validates: Requirements 4.7**
        
        For any price Series, trend_strength_index output DataFrame
        SHALL have the same length as the input Series.
        """
        # Use smaller windows for shorter test series
        max_window = len(prices) - 1
        windows = [w for w in [5, 10, 15, 20] if w < max_window]
        
        # Skip if series is too short for meaningful calculation
        assume(len(prices) >= 10)
        assume(len(windows) >= 2)
        
        result = trend_strength_index(prices, windows=windows)
        
        # Output length should match input length
        assert len(result) == len(prices), (
            f"trend_strength_index output length {len(result)} does not match "
            f"input length {len(prices)}"
        )

    def test_trend_strength_index_empty_series_returns_empty_dataframe(self):
        """
        **Feature: processors-refactoring, Property 6: Trend strength consistency range**
        **Validates: Requirements 4.7**
        
        For empty price Series, trend_strength_index SHALL return an empty DataFrame.
        """
        empty_series = pd.Series(dtype=float)
        result = trend_strength_index(empty_series)
        
        assert isinstance(result, pd.DataFrame), (
            f"trend_strength_index did not return a DataFrame for empty input, got {type(result)}"
        )
        assert result.empty, (
            f"trend_strength_index did not return empty DataFrame for empty input"
        )

    @given(st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_trend_consistency_constant_price_series(self, price_value: float):
        """
        **Feature: processors-refactoring, Property 6: Trend strength consistency range**
        **Validates: Requirements 4.7**
        
        For constant price Series (no trend), trend_consistency values
        SHALL still be in range [0, 1].
        """
        # Create a constant price series
        prices = pd.Series([price_value] * 30)
        windows = [5, 10, 15]
        
        result = trend_strength_index(prices, windows=windows)
        
        # Get non-NaN values from trend_consistency
        consistency_values = result['trend_consistency'].dropna()
        
        if len(consistency_values) > 0:
            # All values should be in [0, 1]
            assert (consistency_values >= 0).all() and (consistency_values <= 1).all(), (
                f"trend_consistency returned values outside [0, 1] for constant series: "
                f"min={consistency_values.min()}, max={consistency_values.max()}"
            )


# =============================================================================
# 补充行为测试：窗口不足与对数收益的健壮性
# =============================================================================

def test_rolling_zscore_min_periods_preserves_nan():
    """
    当未满足 min_periods 时应保持 NaN，避免早期窗口产生伪信号。
    """
    series = pd.Series([1, 2, 3, 4, 5], dtype=float)
    window = 3
    result = rolling_zscore(series, window=window)
    # 前 window-1 个位置应为 NaN
    assert result.iloc[0:window-1].isna().all()
    # 之后窗口满足要求，至少有一个非 NaN
    assert result.iloc[window-1:].notna().any()


def test_log_return_handles_zero_denominator_without_inf():
    """
    对数收益在分母为 0 时应返回 NaN 而非 inf/-inf。
    """
    prices = pd.Series([10.0, 0.0, 12.0])
    result = log_return(prices, periods=1)
    assert not np.isinf(result).any(), "log_return 不应产生 inf 值"
    # 第二个位置分母为 10，第三个位置分母为 0，应为 NaN
    assert np.isnan(result.iloc[2]), "分母为 0 时应返回 NaN"