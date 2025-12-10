#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for MarketTechnicalTask.

Uses hypothesis library for property-based testing.

**Feature: processors-refactoring, Property 7: Market technical feature completeness**
**Validates: Requirements 3.1, 3.2, 3.3, 3.4**
"""

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
from unittest.mock import MagicMock, AsyncMock


# =============================================================================
# Custom Strategies for Market Data
# =============================================================================

def generate_market_cross_section_data(num_days: int) -> pd.DataFrame:
    """Generate synthetic market cross-sectional data.
    
    This simulates the output of MarketTechnicalTask.fetch_data(),
    which returns aggregated cross-sectional statistics per day.
    
    Args:
        num_days: Number of trading days to generate
        
    Returns:
        DataFrame with cross-sectional market statistics
    """
    if num_days <= 0:
        return pd.DataFrame()
    
    np.random.seed(None)  # Allow randomness for hypothesis
    
    dates = pd.date_range('2020-01-01', periods=num_days, freq='B')
    
    data = pd.DataFrame(index=dates)
    data.index.name = 'trade_date'
    
    # Total count of stocks
    data['total_count'] = np.random.randint(3000, 5000, num_days)
    
    # Momentum distribution (Requirements 3.1)
    # Median momentum values (in percentage)
    data['mom_5d_median'] = np.random.uniform(-5, 5, num_days)
    data['mom_10d_median'] = np.random.uniform(-8, 8, num_days)
    data['mom_20d_median'] = np.random.uniform(-15, 15, num_days)
    data['mom_60d_median'] = np.random.uniform(-25, 25, num_days)
    data['mom_20d_q25'] = data['mom_20d_median'] - np.random.uniform(5, 15, num_days)
    data['mom_20d_q75'] = data['mom_20d_median'] + np.random.uniform(5, 15, num_days)
    data['mom_20d_std'] = np.random.uniform(5, 20, num_days)
    
    # Momentum strength (positive ratio)
    data['mom_5d_pos_ratio'] = np.random.uniform(0.3, 0.7, num_days)
    data['mom_20d_pos_ratio'] = np.random.uniform(0.3, 0.7, num_days)
    data['mom_60d_pos_ratio'] = np.random.uniform(0.3, 0.7, num_days)
    data['strong_mom_ratio'] = np.random.uniform(0.05, 0.3, num_days)
    data['weak_mom_ratio'] = np.random.uniform(0.05, 0.3, num_days)
    
    # Volatility distribution (Requirements 3.2)
    data['vol_20d_median'] = np.random.uniform(15, 50, num_days)
    data['vol_60d_median'] = np.random.uniform(15, 50, num_days)
    data['vol_20d_q75'] = data['vol_20d_median'] + np.random.uniform(5, 15, num_days)
    data['vol_20d_mean'] = data['vol_20d_median'] + np.random.uniform(-5, 5, num_days)
    data['high_vol_ratio'] = np.random.uniform(0.1, 0.4, num_days)
    data['low_vol_ratio'] = np.random.uniform(0.1, 0.4, num_days)
    
    # Volume activity (Requirements 3.3)
    data['vol_ratio_5d_median'] = np.random.uniform(0.7, 1.5, num_days)
    data['vol_ratio_20d_median'] = np.random.uniform(0.8, 1.3, num_days)
    data['vol_expand_ratio'] = np.random.uniform(0.1, 0.4, num_days)
    data['vol_shrink_ratio'] = np.random.uniform(0.1, 0.4, num_days)
    
    # Price-volume divergence (Requirements 3.4)
    data['price_up_vol_down_ratio'] = np.random.uniform(0.05, 0.25, num_days)
    data['price_down_vol_up_ratio'] = np.random.uniform(0.05, 0.25, num_days)
    data['vol_price_aligned_ratio'] = np.random.uniform(0.4, 0.7, num_days)
    
    return data


# Strategy for number of days
num_days_strategy = st.integers(min_value=10, max_value=300)


# =============================================================================
# Property 7: Market technical feature completeness
# **Feature: processors-refactoring, Property 7: Market technical feature completeness**
# **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
# =============================================================================

class TestProperty7MarketTechnicalFeatureCompleteness:
    """
    Property 7: Market technical feature completeness
    
    *For any* valid market data input, MarketTechnicalTask SHALL produce output 
    containing all required momentum, volatility, and volume features.
    
    **Feature: processors-refactoring, Property 7: Market technical feature completeness**
    **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
    """
    
    # Required momentum features (Requirements 3.1)
    MOMENTUM_FEATURES = [
        'Mom_5D_Median',
        'Mom_10D_Median', 
        'Mom_20D_Median',
        'Mom_60D_Median',
        'Mom_20D_Pos_Ratio',
    ]
    
    # Required volatility features (Requirements 3.2)
    VOLATILITY_FEATURES = [
        'Vol_20D_Median',
        'Vol_60D_Median',
        'High_Vol_Ratio',
    ]
    
    # Required volume activity features (Requirements 3.3)
    VOLUME_FEATURES = [
        'Vol_Ratio_5D_Median',
        'Vol_Expand_Ratio',
        'Vol_Shrink_Ratio',
    ]
    
    # Required price-volume divergence features (Requirements 3.4)
    DIVERGENCE_FEATURES = [
        'Price_Up_Vol_Down_Ratio',
        'Vol_Price_Aligned_Ratio',
    ]
    
    @property
    def all_required_features(self):
        """All required features combined"""
        return (
            self.MOMENTUM_FEATURES + 
            self.VOLATILITY_FEATURES + 
            self.VOLUME_FEATURES + 
            self.DIVERGENCE_FEATURES
        )

    @given(num_days_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    @pytest.mark.asyncio
    async def test_process_data_contains_all_momentum_features(self, num_days: int):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.1**
        
        For any valid market data input, MarketTechnicalTask.process_data SHALL 
        produce output containing all required momentum features:
        - 5/10/20/60-day momentum median
        - 20-day positive momentum ratio
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Generate test data
        input_data = generate_market_cross_section_data(num_days)
        assume(not input_data.empty)
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process data
        result = await task.process_data(input_data)
        
        # Verify all momentum features are present
        for feature in self.MOMENTUM_FEATURES:
            assert feature in result.columns, (
                f"Missing momentum feature '{feature}'. "
                f"Got columns: {list(result.columns)}"
            )

    @given(num_days_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    @pytest.mark.asyncio
    async def test_process_data_contains_all_volatility_features(self, num_days: int):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.2**
        
        For any valid market data input, MarketTechnicalTask.process_data SHALL 
        produce output containing all required volatility features:
        - 20/60-day realized volatility median
        - High volatility ratio
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Generate test data
        input_data = generate_market_cross_section_data(num_days)
        assume(not input_data.empty)
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process data
        result = await task.process_data(input_data)
        
        # Verify all volatility features are present
        for feature in self.VOLATILITY_FEATURES:
            assert feature in result.columns, (
                f"Missing volatility feature '{feature}'. "
                f"Got columns: {list(result.columns)}"
            )

    @given(num_days_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    @pytest.mark.asyncio
    async def test_process_data_contains_all_volume_features(self, num_days: int):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.3**
        
        For any valid market data input, MarketTechnicalTask.process_data SHALL 
        produce output containing all required volume activity features:
        - Volume ratio median
        - Volume expand/shrink ratios
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Generate test data
        input_data = generate_market_cross_section_data(num_days)
        assume(not input_data.empty)
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process data
        result = await task.process_data(input_data)
        
        # Verify all volume features are present
        for feature in self.VOLUME_FEATURES:
            assert feature in result.columns, (
                f"Missing volume feature '{feature}'. "
                f"Got columns: {list(result.columns)}"
            )

    @given(num_days_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    @pytest.mark.asyncio
    async def test_process_data_contains_all_divergence_features(self, num_days: int):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.4**
        
        For any valid market data input, MarketTechnicalTask.process_data SHALL 
        produce output containing all required price-volume divergence features:
        - Price up volume down ratio
        - Volume price aligned ratio
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Generate test data
        input_data = generate_market_cross_section_data(num_days)
        assume(not input_data.empty)
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process data
        result = await task.process_data(input_data)
        
        # Verify all divergence features are present
        for feature in self.DIVERGENCE_FEATURES:
            assert feature in result.columns, (
                f"Missing divergence feature '{feature}'. "
                f"Got columns: {list(result.columns)}"
            )

    @given(num_days_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    @pytest.mark.asyncio
    async def test_process_data_output_length_matches_input(self, num_days: int):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
        
        For any valid market data input, MarketTechnicalTask.process_data output 
        DataFrame SHALL have the same length as the input DataFrame.
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Generate test data
        input_data = generate_market_cross_section_data(num_days)
        assume(not input_data.empty)
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process data
        result = await task.process_data(input_data)
        
        # Output length should match input length
        assert len(result) == len(input_data), (
            f"Output length {len(result)} does not match input length {len(input_data)}"
        )

    @given(num_days_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    @pytest.mark.asyncio
    async def test_process_data_preserves_index(self, num_days: int):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
        
        For any valid market data input, MarketTechnicalTask.process_data output 
        DataFrame SHALL preserve the input DataFrame index (trade_date).
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Generate test data
        input_data = generate_market_cross_section_data(num_days)
        assume(not input_data.empty)
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process data
        result = await task.process_data(input_data)
        
        # Index should be preserved
        assert result.index.equals(input_data.index), (
            f"Output index does not match input index"
        )

    @pytest.mark.asyncio
    async def test_process_data_empty_input_returns_empty_dataframe(self):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
        
        For empty input DataFrame, MarketTechnicalTask.process_data SHALL return 
        an empty DataFrame without raising exceptions.
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process empty data
        empty_df = pd.DataFrame()
        result = await task.process_data(empty_df)
        
        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame), (
            f"Expected DataFrame, got {type(result)}"
        )
        assert result.empty, "Expected empty DataFrame for empty input"

    @pytest.mark.asyncio
    async def test_process_data_none_input_returns_empty_dataframe(self):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
        
        For None input, MarketTechnicalTask.process_data SHALL return 
        an empty DataFrame without raising exceptions.
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process None data
        result = await task.process_data(None)
        
        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame), (
            f"Expected DataFrame, got {type(result)}"
        )
        assert result.empty, "Expected empty DataFrame for None input"

    @given(num_days_strategy)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    @pytest.mark.asyncio
    async def test_process_data_all_features_complete(self, num_days: int):
        """
        **Feature: processors-refactoring, Property 7: Market technical feature completeness**
        **Validates: Requirements 3.1, 3.2, 3.3, 3.4**
        
        For any valid market data input, MarketTechnicalTask.process_data SHALL 
        produce output containing ALL required features from all categories.
        """
        from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask
        
        # Generate test data
        input_data = generate_market_cross_section_data(num_days)
        assume(not input_data.empty)
        
        # Create task with mock db connection
        mock_db = MagicMock()
        task = MarketTechnicalTask(db_connection=mock_db)
        
        # Process data
        result = await task.process_data(input_data)
        
        # Verify ALL required features are present
        missing_features = []
        for feature in self.all_required_features:
            if feature not in result.columns:
                missing_features.append(feature)
        
        assert not missing_features, (
            f"Missing required features: {missing_features}. "
            f"Got columns: {sorted(result.columns)}"
        )
