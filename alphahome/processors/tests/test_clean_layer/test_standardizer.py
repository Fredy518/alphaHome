#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for DataStandardizer.

Tests Properties 10-11 from the design document:
- Property 10: Unit conversion correctness
- Property 11: Unadjusted price preservation

Uses hypothesis library for property-based testing.
"""

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck

from alphahome.processors.clean.standardizer import DataStandardizer, StandardizationError


# =============================================================================
# Custom Strategies
# =============================================================================

# Strategy for valid monetary values (finite, non-NaN floats)
valid_monetary_values = st.floats(
    min_value=-1e15, 
    max_value=1e15, 
    allow_nan=False, 
    allow_infinity=False
)

# Strategy for valid volume values (non-negative, finite floats)
valid_volume_values = st.floats(
    min_value=0, 
    max_value=1e15, 
    allow_nan=False, 
    allow_infinity=False
)

# Strategy for valid price values (positive, finite floats)
valid_price_values = st.floats(
    min_value=0.01, 
    max_value=1e10, 
    allow_nan=False, 
    allow_infinity=False
)


# =============================================================================
# Property 10: Unit conversion correctness
# **Feature: processors-data-layering, Property 10: Unit conversion correctness**
# **Validates: Requirements 3.1, 3.2, 3.5**
# =============================================================================

class TestProperty10UnitConversionCorrectness:
    """
    Property 10: Unit conversion correctness
    
    *For any* monetary value in 万元 or 亿元, the DataStandardizer SHALL convert
    to 元 using the correct conversion factor. *For any* volume in 手, the
    DataStandardizer SHALL convert to 股.
    
    **Feature: processors-data-layering, Property 10: Unit conversion correctness**
    **Validates: Requirements 3.1, 3.2, 3.5**
    """

    @given(valid_monetary_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_wan_yuan_to_yuan_conversion(self, value: float):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        For any monetary value in 万元, convert_monetary SHALL multiply by 10,000.
        """
        df = pd.DataFrame({'amount': [value]})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_monetary(df, 'amount', '万元')
        
        expected = value * 10000
        actual = result['amount'].iloc[0]
        
        # Use relative tolerance for large numbers
        if abs(expected) > 1e-10:
            assert np.isclose(actual, expected, rtol=1e-9), (
                f"Expected {expected}, got {actual} for input {value} 万元"
            )
        else:
            assert np.isclose(actual, expected, atol=1e-10), (
                f"Expected {expected}, got {actual} for input {value} 万元"
            )


    @given(valid_monetary_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_yi_yuan_to_yuan_conversion(self, value: float):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        For any monetary value in 亿元, convert_monetary SHALL multiply by 100,000,000.
        """
        df = pd.DataFrame({'amount': [value]})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_monetary(df, 'amount', '亿元')
        
        expected = value * 100000000
        actual = result['amount'].iloc[0]
        
        # Use relative tolerance for large numbers
        if abs(expected) > 1e-10:
            assert np.isclose(actual, expected, rtol=1e-9), (
                f"Expected {expected}, got {actual} for input {value} 亿元"
            )
        else:
            assert np.isclose(actual, expected, atol=1e-10), (
                f"Expected {expected}, got {actual} for input {value} 亿元"
            )

    @given(valid_volume_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_shou_to_gu_conversion(self, value: float):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        For any volume in 手, convert_volume SHALL multiply by 100.
        """
        df = pd.DataFrame({'vol': [value]})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_volume(df, 'vol', '手')
        
        expected = value * 100
        actual = result['vol'].iloc[0]
        
        # Use relative tolerance for large numbers
        if abs(expected) > 1e-10:
            assert np.isclose(actual, expected, rtol=1e-9), (
                f"Expected {expected}, got {actual} for input {value} 手"
            )
        else:
            assert np.isclose(actual, expected, atol=1e-10), (
                f"Expected {expected}, got {actual} for input {value} 手"
            )

    @given(st.lists(valid_monetary_values, min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_wan_yuan_conversion_batch(self, values: list):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        For any batch of monetary values in 万元, all values SHALL be
        correctly converted to 元.
        """
        df = pd.DataFrame({'amount': values})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_monetary(df, 'amount', '万元')
        
        for i, value in enumerate(values):
            expected = value * 10000
            actual = result['amount'].iloc[i]
            
            if abs(expected) > 1e-10:
                assert np.isclose(actual, expected, rtol=1e-9), (
                    f"Row {i}: Expected {expected}, got {actual}"
                )
            else:
                assert np.isclose(actual, expected, atol=1e-10), (
                    f"Row {i}: Expected {expected}, got {actual}"
                )

    @given(st.lists(valid_volume_values, min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_shou_conversion_batch(self, values: list):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        For any batch of volume values in 手, all values SHALL be
        correctly converted to 股.
        """
        df = pd.DataFrame({'vol': values})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_volume(df, 'vol', '手')
        
        for i, value in enumerate(values):
            expected = value * 100
            actual = result['vol'].iloc[i]
            
            if abs(expected) > 1e-10:
                assert np.isclose(actual, expected, rtol=1e-9), (
                    f"Row {i}: Expected {expected}, got {actual}"
                )
            else:
                assert np.isclose(actual, expected, atol=1e-10), (
                    f"Row {i}: Expected {expected}, got {actual}"
                )

    @given(valid_monetary_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_yuan_no_conversion(self, value: float):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        For any monetary value already in 元, convert_monetary SHALL
        preserve the value unchanged.
        """
        df = pd.DataFrame({'amount': [value]})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_monetary(df, 'amount', '元')
        
        actual = result['amount'].iloc[0]
        assert np.isclose(actual, value, rtol=1e-9), (
            f"Expected {value} (unchanged), got {actual}"
        )

    @given(valid_volume_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_gu_no_conversion(self, value: float):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        For any volume value already in 股, convert_volume SHALL
        preserve the value unchanged.
        """
        df = pd.DataFrame({'vol': [value]})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_volume(df, 'vol', '股')
        
        actual = result['vol'].iloc[0]
        assert np.isclose(actual, value, rtol=1e-9), (
            f"Expected {value} (unchanged), got {actual}"
        )


    @given(valid_monetary_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_original_preserved_monetary(self, value: float):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        When preserve_original=True, convert_monetary SHALL create a column
        with the original unit suffix containing the original value.
        """
        df = pd.DataFrame({'amount': [value]})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_monetary(
            df, 'amount', '万元', preserve_original=True
        )
        
        # Original value should be preserved in amount_万元
        assert 'amount_万元' in result.columns, (
            "Original column with unit suffix should be created"
        )
        original = result['amount_万元'].iloc[0]
        assert np.isclose(original, value, rtol=1e-9), (
            f"Original value {value} should be preserved, got {original}"
        )

    @given(valid_volume_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_original_preserved_volume(self, value: float):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        When preserve_original=True, convert_volume SHALL create a column
        with the original unit suffix containing the original value.
        """
        df = pd.DataFrame({'vol': [value]})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_volume(
            df, 'vol', '手', preserve_original=True
        )
        
        # Original value should be preserved in vol_手
        assert 'vol_手' in result.columns, (
            "Original column with unit suffix should be created"
        )
        original = result['vol_手'].iloc[0]
        assert np.isclose(original, value, rtol=1e-9), (
            f"Original value {value} should be preserved, got {original}"
        )

    def test_missing_column_raises_error_monetary(self):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        convert_monetary SHALL raise StandardizationError when column is missing.
        """
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        standardizer = DataStandardizer()
        
        with pytest.raises(StandardizationError) as exc_info:
            standardizer.convert_monetary(df, 'amount', '万元')
        
        assert 'amount' in str(exc_info.value)

    def test_missing_column_raises_error_volume(self):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        convert_volume SHALL raise StandardizationError when column is missing.
        """
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        standardizer = DataStandardizer()
        
        with pytest.raises(StandardizationError) as exc_info:
            standardizer.convert_volume(df, 'vol', '手')
        
        assert 'vol' in str(exc_info.value)

    def test_empty_dataframe_handled_monetary(self):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        convert_monetary SHALL handle empty DataFrames gracefully.
        """
        df = pd.DataFrame({'amount': []})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_monetary(df, 'amount', '万元')
        
        assert len(result) == 0, "Empty DataFrame should remain empty"

    def test_empty_dataframe_handled_volume(self):
        """
        **Feature: processors-data-layering, Property 10: Unit conversion correctness**
        **Validates: Requirements 3.1, 3.2, 3.5**
        
        convert_volume SHALL handle empty DataFrames gracefully.
        """
        df = pd.DataFrame({'vol': []})
        standardizer = DataStandardizer()
        
        result = standardizer.convert_volume(df, 'vol', '手')
        
        assert len(result) == 0, "Empty DataFrame should remain empty"


# =============================================================================
# Property 11: Unadjusted price preservation
# **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
# **Validates: Requirements 3.4**
# =============================================================================

class TestProperty11UnadjustedPricePreservation:
    """
    Property 11: Unadjusted price preservation
    
    *For any* price column that undergoes adjustment, the DataStandardizer SHALL
    preserve the original value in a column with `_unadj` suffix.
    
    **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
    **Validates: Requirements 3.4**
    """

    @given(valid_price_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_single_price_preserved(self, price: float):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        For any price column, preserve_unadjusted SHALL create a column
        with _unadj suffix containing the original value.
        """
        df = pd.DataFrame({'close': [price]})
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(df, ['close'])
        
        # Verify _unadj column exists
        assert 'close_unadj' in result.columns, (
            "Unadjusted column should be created with _unadj suffix"
        )
        
        # Verify original value is preserved
        unadj_value = result['close_unadj'].iloc[0]
        assert np.isclose(unadj_value, price, rtol=1e-9), (
            f"Original price {price} should be preserved, got {unadj_value}"
        )
        
        # Verify original column still exists
        assert 'close' in result.columns, (
            "Original column should still exist"
        )


    @given(st.lists(valid_price_values, min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_batch_prices_preserved(self, prices: list):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        For any batch of price values, preserve_unadjusted SHALL preserve
        all original values in the _unadj column.
        """
        df = pd.DataFrame({'close': prices})
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(df, ['close'])
        
        for i, price in enumerate(prices):
            unadj_value = result['close_unadj'].iloc[i]
            assert np.isclose(unadj_value, price, rtol=1e-9), (
                f"Row {i}: Original price {price} should be preserved, got {unadj_value}"
            )

    @given(
        valid_price_values,
        valid_price_values,
        valid_price_values,
        valid_price_values
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_multiple_price_columns_preserved(
        self, 
        open_price: float, 
        high_price: float, 
        low_price: float, 
        close_price: float
    ):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        For multiple price columns, preserve_unadjusted SHALL create
        _unadj columns for each specified column.
        """
        df = pd.DataFrame({
            'open': [open_price],
            'high': [high_price],
            'low': [low_price],
            'close': [close_price],
        })
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(
            df, ['open', 'high', 'low', 'close']
        )
        
        # Verify all _unadj columns exist
        for col in ['open', 'high', 'low', 'close']:
            unadj_col = f"{col}_unadj"
            assert unadj_col in result.columns, (
                f"Unadjusted column {unadj_col} should be created"
            )
        
        # Verify all values are preserved
        assert np.isclose(result['open_unadj'].iloc[0], open_price, rtol=1e-9)
        assert np.isclose(result['high_unadj'].iloc[0], high_price, rtol=1e-9)
        assert np.isclose(result['low_unadj'].iloc[0], low_price, rtol=1e-9)
        assert np.isclose(result['close_unadj'].iloc[0], close_price, rtol=1e-9)

    @given(valid_price_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_original_column_unchanged(self, price: float):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        preserve_unadjusted SHALL NOT modify the original column values.
        """
        df = pd.DataFrame({'close': [price]})
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(df, ['close'])
        
        # Original column value should be unchanged
        original_value = result['close'].iloc[0]
        assert np.isclose(original_value, price, rtol=1e-9), (
            f"Original column value {price} should be unchanged, got {original_value}"
        )

    @given(st.lists(valid_price_values, min_size=1, max_size=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_row_count_preserved(self, prices: list):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        preserve_unadjusted SHALL NOT change the number of rows.
        """
        df = pd.DataFrame({'close': prices})
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(df, ['close'])
        
        assert len(result) == len(df), (
            f"Row count should be preserved: expected {len(df)}, got {len(result)}"
        )

    @given(st.lists(valid_price_values, min_size=1, max_size=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_other_columns_preserved(self, prices: list):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        preserve_unadjusted SHALL NOT drop or modify other columns.
        """
        df = pd.DataFrame({
            'close': prices,
            'vol': range(len(prices)),
            'ts_code': ['000001.SZ'] * len(prices),
        })
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(df, ['close'])
        
        # Other columns should be preserved
        assert 'vol' in result.columns, "vol column should be preserved"
        assert 'ts_code' in result.columns, "ts_code column should be preserved"
        
        # Values should be unchanged
        assert list(result['vol']) == list(range(len(prices)))
        assert list(result['ts_code']) == ['000001.SZ'] * len(prices)

    def test_missing_column_raises_error(self):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        preserve_unadjusted SHALL raise StandardizationError when column is missing.
        """
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        standardizer = DataStandardizer()
        
        with pytest.raises(StandardizationError) as exc_info:
            standardizer.preserve_unadjusted(df, ['close'])
        
        assert 'close' in str(exc_info.value)

    def test_empty_dataframe_handled(self):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        preserve_unadjusted SHALL handle empty DataFrames gracefully.
        """
        df = pd.DataFrame({'close': []})
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(df, ['close'])
        
        assert len(result) == 0, "Empty DataFrame should remain empty"
        assert 'close_unadj' in result.columns, (
            "Unadjusted column should still be created for empty DataFrame"
        )

    def test_empty_price_cols_list(self):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        preserve_unadjusted SHALL handle empty price_cols list gracefully.
        """
        df = pd.DataFrame({'close': [100.0, 101.0]})
        standardizer = DataStandardizer()
        
        result = standardizer.preserve_unadjusted(df, [])
        
        # DataFrame should be unchanged
        assert len(result) == len(df)
        assert list(result.columns) == list(df.columns)

    @given(valid_price_values)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_input_dataframe_not_modified(self, price: float):
        """
        **Feature: processors-data-layering, Property 11: Unadjusted price preservation**
        **Validates: Requirements 3.4**
        
        preserve_unadjusted SHALL NOT modify the input DataFrame.
        """
        df = pd.DataFrame({'close': [price]})
        original_columns = list(df.columns)
        original_value = df['close'].iloc[0]
        
        standardizer = DataStandardizer()
        _ = standardizer.preserve_unadjusted(df, ['close'])
        
        # Input DataFrame should be unchanged
        assert list(df.columns) == original_columns, (
            "Input DataFrame columns should not be modified"
        )
        assert df['close'].iloc[0] == original_value, (
            "Input DataFrame values should not be modified"
        )
