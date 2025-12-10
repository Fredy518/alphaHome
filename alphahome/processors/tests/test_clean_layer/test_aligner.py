#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for DataAligner.

Tests Properties 7-9 from the design document:
- Property 7: Date format standardization
- Property 8: Identifier mapping
- Property 9: Primary key uniqueness enforcement

Uses hypothesis library for property-based testing.
"""

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck

from alphahome.processors.clean.aligner import DataAligner, AlignmentError


# =============================================================================
# Custom Strategies
# =============================================================================

# Strategy for valid date integers (YYYYMMDD format)
valid_date_ints = st.integers(min_value=19900101, max_value=20301231).filter(
    lambda x: _is_valid_date_int(x)
)

def _is_valid_date_int(val: int) -> bool:
    """Check if an integer represents a valid YYYYMMDD date."""
    try:
        s = str(val)
        if len(s) != 8:
            return False
        year = int(s[:4])
        month = int(s[4:6])
        day = int(s[6:8])
        if month < 1 or month > 12:
            return False
        if day < 1 or day > 31:
            return False
        # Try to create a datetime to validate
        datetime(year, month, day)
        return True
    except (ValueError, TypeError):
        return False


# Strategy for valid date strings in YYYY-MM-DD format
@st.composite
def valid_date_strings_dash(draw):
    """Generate valid date strings in YYYY-MM-DD format."""
    year = draw(st.integers(min_value=1990, max_value=2030))
    month = draw(st.integers(min_value=1, max_value=12))
    day = draw(st.integers(min_value=1, max_value=28))  # Safe for all months
    return f"{year:04d}-{month:02d}-{day:02d}"


# Strategy for valid date strings in YYYYMMDD format
@st.composite
def valid_date_strings_compact(draw):
    """Generate valid date strings in YYYYMMDD format."""
    year = draw(st.integers(min_value=1990, max_value=2030))
    month = draw(st.integers(min_value=1, max_value=12))
    day = draw(st.integers(min_value=1, max_value=28))
    return f"{year:04d}{month:02d}{day:02d}"


# Strategy for 6-digit stock codes
stock_codes_6digit = st.from_regex(r'^[0-9]{6}$', fullmatch=True)

# Strategy for Shanghai stock codes (6xxxxx)
shanghai_codes = st.from_regex(r'^6[0-9]{5}$', fullmatch=True)

# Strategy for Shenzhen stock codes (0xxxxx, 3xxxxx)
shenzhen_codes = st.one_of(
    st.from_regex(r'^0[0-9]{5}$', fullmatch=True),
    st.from_regex(r'^3[0-9]{5}$', fullmatch=True),
)

# Strategy for prefixed codes (sh600000, sz000001)
@st.composite
def prefixed_codes(draw):
    """Generate prefixed stock codes."""
    prefix = draw(st.sampled_from(['sh', 'sz', 'SH', 'SZ']))
    if prefix.lower() == 'sh':
        code = draw(shanghai_codes)
    else:
        code = draw(shenzhen_codes)
    return f"{prefix}{code}"


# Strategy for ts_code format (000001.SZ)
@st.composite
def ts_code_format(draw):
    """Generate codes already in ts_code format."""
    is_shanghai = draw(st.booleans())
    if is_shanghai:
        code = draw(shanghai_codes)
        return f"{code}.SH"
    else:
        code = draw(shenzhen_codes)
        return f"{code}.SZ"


# =============================================================================
# Property 7: Date format standardization
# **Feature: processors-data-layering, Property 7: Date format standardization**
# **Validates: Requirements 2.1, 2.4**
# =============================================================================

class TestProperty7DateFormatStandardization:
    """
    Property 7: Date format standardization
    
    *For any* date value in supported formats (YYYY-MM-DD, YYYYMMDD, datetime),
    the DataAligner SHALL convert it to the standard trade_date format.
    
    **Feature: processors-data-layering, Property 7: Date format standardization**
    **Validates: Requirements 2.1, 2.4**
    """

    @given(valid_date_strings_dash())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_dash_format_converted_to_int(self, date_str: str):
        """
        **Feature: processors-data-layering, Property 7: Date format standardization**
        **Validates: Requirements 2.1, 2.4**
        
        For any date in YYYY-MM-DD format, align_date SHALL convert it
        to YYYYMMDD integer format.
        """
        df = pd.DataFrame({'date_col': [date_str]})
        aligner = DataAligner()
        
        result = aligner.align_date(df, 'date_col', output_format='int')
        
        # Verify the result is an integer in YYYYMMDD format
        trade_date = result['trade_date'].iloc[0]
        assert pd.notna(trade_date), f"Date conversion failed for {date_str}"
        
        # Verify the date value matches
        expected = int(date_str.replace('-', ''))
        assert trade_date == expected, (
            f"Expected {expected}, got {trade_date} for input {date_str}"
        )

    @given(valid_date_strings_compact())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_compact_format_converted_to_int(self, date_str: str):
        """
        **Feature: processors-data-layering, Property 7: Date format standardization**
        **Validates: Requirements 2.1, 2.4**
        
        For any date in YYYYMMDD string format, align_date SHALL convert it
        to YYYYMMDD integer format.
        """
        df = pd.DataFrame({'date_col': [date_str]})
        aligner = DataAligner()
        
        result = aligner.align_date(df, 'date_col', output_format='int')
        
        trade_date = result['trade_date'].iloc[0]
        assert pd.notna(trade_date), f"Date conversion failed for {date_str}"
        
        expected = int(date_str)
        assert trade_date == expected, (
            f"Expected {expected}, got {trade_date} for input {date_str}"
        )

    @given(st.integers(min_value=19900101, max_value=20301231).filter(_is_valid_date_int))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_int_format_preserved(self, date_int: int):
        """
        **Feature: processors-data-layering, Property 7: Date format standardization**
        **Validates: Requirements 2.1, 2.4**
        
        For any date already in YYYYMMDD integer format, align_date SHALL
        preserve the value.
        """
        df = pd.DataFrame({'date_col': [date_int]})
        aligner = DataAligner()
        
        result = aligner.align_date(df, 'date_col', output_format='int')
        
        trade_date = result['trade_date'].iloc[0]
        assert pd.notna(trade_date), f"Date conversion failed for {date_int}"
        assert trade_date == date_int, (
            f"Expected {date_int}, got {trade_date}"
        )

    @given(st.datetimes(min_value=datetime(1990, 1, 1), max_value=datetime(2030, 12, 31)))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_datetime_converted_to_int(self, dt: datetime):
        """
        **Feature: processors-data-layering, Property 7: Date format standardization**
        **Validates: Requirements 2.1, 2.4**
        
        For any datetime object, align_date SHALL convert it to YYYYMMDD integer.
        """
        df = pd.DataFrame({'date_col': [dt]})
        aligner = DataAligner()
        
        result = aligner.align_date(df, 'date_col', output_format='int')
        
        trade_date = result['trade_date'].iloc[0]
        assert pd.notna(trade_date), f"Date conversion failed for {dt}"
        
        expected = int(dt.strftime('%Y%m%d'))
        assert trade_date == expected, (
            f"Expected {expected}, got {trade_date} for input {dt}"
        )

    @given(valid_date_strings_dash())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_datetime_output_format(self, date_str: str):
        """
        **Feature: processors-data-layering, Property 7: Date format standardization**
        **Validates: Requirements 2.1, 2.4**
        
        When output_format='datetime', align_date SHALL return datetime values.
        """
        df = pd.DataFrame({'date_col': [date_str]})
        aligner = DataAligner()
        
        result = aligner.align_date(df, 'date_col', output_format='datetime')
        
        trade_date = result['trade_date'].iloc[0]
        assert pd.notna(trade_date), f"Date conversion failed for {date_str}"
        assert isinstance(trade_date, pd.Timestamp), (
            f"Expected Timestamp, got {type(trade_date)}"
        )

    @given(st.lists(valid_date_strings_dash(), min_size=1, max_size=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_row_order_preserved(self, date_strs: list):
        """
        **Feature: processors-data-layering, Property 7: Date format standardization**
        **Validates: Requirements 2.1, 2.4**
        
        align_date SHALL NOT change row order.
        """
        df = pd.DataFrame({
            'date_col': date_strs,
            'row_id': range(len(date_strs))
        })
        aligner = DataAligner()
        
        result = aligner.align_date(df, 'date_col', output_format='int')
        
        # Verify row order is preserved
        assert list(result['row_id']) == list(range(len(date_strs))), (
            "Row order was changed during date alignment"
        )

    def test_missing_column_raises_error(self):
        """
        **Feature: processors-data-layering, Property 7: Date format standardization**
        **Validates: Requirements 2.1, 2.4**
        
        align_date SHALL raise AlignmentError when source column is missing.
        """
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        aligner = DataAligner()
        
        with pytest.raises(AlignmentError) as exc_info:
            aligner.align_date(df, 'date_col')
        
        assert 'date_col' in str(exc_info.value)


# =============================================================================
# Property 8: Identifier mapping
# **Feature: processors-data-layering, Property 8: Identifier mapping**
# **Validates: Requirements 2.2, 2.5**
# =============================================================================

class TestProperty8IdentifierMapping:
    """
    Property 8: Identifier mapping
    
    *For any* security identifier in supported formats (000001, sh600000),
    the DataAligner SHALL map it to the correct ts_code format (e.g., 000001.SZ).
    
    **Feature: processors-data-layering, Property 8: Identifier mapping**
    **Validates: Requirements 2.2, 2.5**
    """

    @given(shanghai_codes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_shanghai_6digit_mapped_to_sh(self, code: str):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        For any 6-digit Shanghai code (6xxxxx), align_identifier SHALL
        map it to {code}.SH format.
        """
        df = pd.DataFrame({'code_col': [code]})
        aligner = DataAligner()
        
        result = aligner.align_identifier(df, 'code_col')
        
        ts_code = result['ts_code'].iloc[0]
        expected = f"{code}.SH"
        assert ts_code == expected, (
            f"Expected {expected}, got {ts_code} for input {code}"
        )
        assert result['_mapping_failed'].iloc[0] == False, (
            f"Mapping should not fail for valid Shanghai code {code}"
        )

    @given(shenzhen_codes)
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_shenzhen_6digit_mapped_to_sz(self, code: str):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        For any 6-digit Shenzhen code (0xxxxx, 3xxxxx), align_identifier SHALL
        map it to {code}.SZ format.
        """
        df = pd.DataFrame({'code_col': [code]})
        aligner = DataAligner()
        
        result = aligner.align_identifier(df, 'code_col')
        
        ts_code = result['ts_code'].iloc[0]
        expected = f"{code}.SZ"
        assert ts_code == expected, (
            f"Expected {expected}, got {ts_code} for input {code}"
        )
        assert result['_mapping_failed'].iloc[0] == False, (
            f"Mapping should not fail for valid Shenzhen code {code}"
        )

    @given(prefixed_codes())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_prefixed_codes_mapped_correctly(self, prefixed_code: str):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        For any prefixed code (sh600000, sz000001), align_identifier SHALL
        map it to the correct ts_code format.
        """
        df = pd.DataFrame({'code_col': [prefixed_code]})
        aligner = DataAligner()
        
        result = aligner.align_identifier(df, 'code_col')
        
        ts_code = result['ts_code'].iloc[0]
        
        # Extract the 6-digit code and determine expected suffix
        prefix = prefixed_code[:2].lower()
        code = prefixed_code[2:]
        expected_suffix = '.SH' if prefix == 'sh' else '.SZ'
        expected = f"{code}{expected_suffix}"
        
        assert ts_code == expected, (
            f"Expected {expected}, got {ts_code} for input {prefixed_code}"
        )
        assert result['_mapping_failed'].iloc[0] == False, (
            f"Mapping should not fail for valid prefixed code {prefixed_code}"
        )

    @given(ts_code_format())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_ts_code_format_preserved(self, ts_code: str):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        For any code already in ts_code format, align_identifier SHALL
        preserve the value unchanged.
        """
        df = pd.DataFrame({'code_col': [ts_code]})
        aligner = DataAligner()
        
        result = aligner.align_identifier(df, 'code_col')
        
        result_code = result['ts_code'].iloc[0]
        assert result_code == ts_code, (
            f"Expected {ts_code} to be preserved, got {result_code}"
        )
        assert result['_mapping_failed'].iloc[0] == False, (
            f"Mapping should not fail for valid ts_code {ts_code}"
        )

    @given(st.text(min_size=1, max_size=10).filter(
        lambda x: not x.isdigit() and not x.startswith(('sh', 'sz', 'SH', 'SZ'))
    ))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_unknown_format_marked_as_failed(self, unknown_code: str):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        For any unrecognized identifier format, align_identifier SHALL
        mark _mapping_failed=True and preserve the original value.
        """
        # Filter out codes that might accidentally match valid patterns
        assume(not unknown_code.strip().isdigit())
        assume(len(unknown_code.strip()) != 6 or not unknown_code.strip().isdigit())
        
        df = pd.DataFrame({'code_col': [unknown_code]})
        aligner = DataAligner()
        
        result = aligner.align_identifier(df, 'code_col')
        
        # Original value should be preserved
        assert result['ts_code'].iloc[0] == unknown_code.strip(), (
            f"Original value should be preserved for unknown format"
        )
        # Mapping should be marked as failed
        assert result['_mapping_failed'].iloc[0] == True, (
            f"Mapping should fail for unknown format {unknown_code}"
        )

    def test_strict_mapping_raises_error(self):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        When strict_mapping=True, align_identifier SHALL raise AlignmentError
        for unrecognized identifiers.
        """
        df = pd.DataFrame({'code_col': ['INVALID_CODE']})
        aligner = DataAligner()
        
        with pytest.raises(AlignmentError) as exc_info:
            aligner.align_identifier(df, 'code_col', strict_mapping=True)
        
        assert exc_info.value.failed_rows is not None
        assert len(exc_info.value.failed_rows) == 1

    def test_missing_column_raises_error(self):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        align_identifier SHALL raise AlignmentError when source column is missing.
        """
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        aligner = DataAligner()
        
        with pytest.raises(AlignmentError) as exc_info:
            aligner.align_identifier(df, 'code_col')
        
        assert 'code_col' in str(exc_info.value)

    @given(st.lists(st.one_of(shanghai_codes, shenzhen_codes), min_size=1, max_size=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_mapping_failed_column_added(self, codes: list):
        """
        **Feature: processors-data-layering, Property 8: Identifier mapping**
        **Validates: Requirements 2.2, 2.5**
        
        align_identifier SHALL always add _mapping_failed column.
        """
        df = pd.DataFrame({'code_col': codes})
        aligner = DataAligner()
        
        result = aligner.align_identifier(df, 'code_col')
        
        assert '_mapping_failed' in result.columns, (
            "_mapping_failed column should be added"
        )


# =============================================================================
# Property 9: Primary key uniqueness enforcement
# **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
# **Validates: Requirements 2.6, 5.1**
# =============================================================================

class TestProperty9PrimaryKeyUniquenessEnforcement:
    """
    Property 9: Primary key uniqueness enforcement
    
    *For any* DataFrame written to clean schema, the CleanLayerWriter SHALL
    enforce primary key uniqueness via UPSERT, with no duplicate keys in
    the final table.
    
    Note: This test class focuses on the DataAligner.build_primary_key() method
    which enforces uniqueness at the DataFrame level before writing.
    
    **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
    **Validates: Requirements 2.6, 5.1**
    """

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_unique_keys_after_build(self, key_values: list):
        """
        **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
        **Validates: Requirements 2.6, 5.1**
        
        For any DataFrame with duplicate primary keys, build_primary_key SHALL
        produce a DataFrame with unique keys only.
        """
        df = pd.DataFrame({
            'key_col': key_values,
            'value_col': range(len(key_values)),
        })
        aligner = DataAligner()
        
        result = aligner.build_primary_key(df, keys=['key_col'])
        
        # Verify no duplicates in result
        assert not result.duplicated(subset=['key_col']).any(), (
            "Result should have no duplicate keys"
        )
        
        # Verify result length equals unique key count
        expected_unique = len(set(key_values))
        assert len(result) == expected_unique, (
            f"Expected {expected_unique} unique rows, got {len(result)}"
        )

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_last_occurrence_kept(self, key_values: list):
        """
        **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
        **Validates: Requirements 2.6, 5.1**
        
        build_primary_key SHALL keep the last occurrence of duplicate keys.
        """
        df = pd.DataFrame({
            'key_col': key_values,
            'value_col': range(len(key_values)),  # Sequential values
        })
        aligner = DataAligner()
        
        result = aligner.build_primary_key(df, keys=['key_col'])
        
        # For each unique key, verify the value is from the last occurrence
        for key in set(key_values):
            # Find the last index where this key appears in original
            last_idx = len(key_values) - 1 - key_values[::-1].index(key)
            expected_value = last_idx
            
            actual_value = result[result['key_col'] == key]['value_col'].iloc[0]
            assert actual_value == expected_value, (
                f"For key {key}, expected value {expected_value} (last occurrence), "
                f"got {actual_value}"
            )

    @given(
        st.lists(st.integers(min_value=1, max_value=50), min_size=1, max_size=30),
        st.lists(st.integers(min_value=1, max_value=50), min_size=1, max_size=30)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_composite_key_uniqueness(self, key1_values: list, key2_values: list):
        """
        **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
        **Validates: Requirements 2.6, 5.1**
        
        For composite primary keys, build_primary_key SHALL enforce uniqueness
        across all key columns combined.
        """
        # Ensure same length
        min_len = min(len(key1_values), len(key2_values))
        key1_values = key1_values[:min_len]
        key2_values = key2_values[:min_len]
        
        df = pd.DataFrame({
            'key1': key1_values,
            'key2': key2_values,
            'value_col': range(min_len),
        })
        aligner = DataAligner()
        
        result = aligner.build_primary_key(df, keys=['key1', 'key2'])
        
        # Verify no duplicates on composite key
        assert not result.duplicated(subset=['key1', 'key2']).any(), (
            "Result should have no duplicate composite keys"
        )
        
        # Verify result length equals unique composite key count
        unique_pairs = set(zip(key1_values, key2_values))
        assert len(result) == len(unique_pairs), (
            f"Expected {len(unique_pairs)} unique rows, got {len(result)}"
        )

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=50, unique=True))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_no_duplicates_unchanged(self, unique_keys: list):
        """
        **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
        **Validates: Requirements 2.6, 5.1**
        
        When there are no duplicates, build_primary_key SHALL return
        the DataFrame unchanged (same length).
        """
        df = pd.DataFrame({
            'key_col': unique_keys,
            'value_col': range(len(unique_keys)),
        })
        aligner = DataAligner()
        
        result = aligner.build_primary_key(df, keys=['key_col'])
        
        assert len(result) == len(df), (
            f"Expected {len(df)} rows (no duplicates), got {len(result)}"
        )

    def test_missing_key_column_raises_error(self):
        """
        **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
        **Validates: Requirements 2.6, 5.1**
        
        build_primary_key SHALL raise AlignmentError when key columns are missing.
        """
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        aligner = DataAligner()
        
        with pytest.raises(AlignmentError) as exc_info:
            aligner.build_primary_key(df, keys=['key_col'])
        
        assert 'key_col' in str(exc_info.value)

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_enforce_uniqueness_false_preserves_duplicates(self, key_values: list):
        """
        **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
        **Validates: Requirements 2.6, 5.1**
        
        When enforce_uniqueness=False, build_primary_key SHALL preserve
        all rows including duplicates.
        """
        df = pd.DataFrame({
            'key_col': key_values,
            'value_col': range(len(key_values)),
        })
        aligner = DataAligner()
        
        result = aligner.build_primary_key(df, keys=['key_col'], enforce_uniqueness=False)
        
        # All rows should be preserved
        assert len(result) == len(df), (
            f"Expected {len(df)} rows (duplicates preserved), got {len(result)}"
        )

    def test_empty_dataframe_handled(self):
        """
        **Feature: processors-data-layering, Property 9: Primary key uniqueness enforcement**
        **Validates: Requirements 2.6, 5.1**
        
        build_primary_key SHALL handle empty DataFrames gracefully.
        """
        df = pd.DataFrame({'key_col': [], 'value_col': []})
        aligner = DataAligner()
        
        result = aligner.build_primary_key(df, keys=['key_col'])
        
        assert len(result) == 0, "Empty DataFrame should remain empty"
