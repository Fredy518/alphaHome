#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for DataValidator.

Tests Properties 1-5 from the design document:
- Property 1: Column type validation
- Property 2: Missing column detection
- Property 3: Duplicate key deduplication
- Property 4: Null value rejection
- Property 5: Range validation flagging

Uses hypothesis library for property-based testing.
"""

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck

from alphahome.processors.clean.schema import TableSchema, ValidationResult
from alphahome.processors.clean.validator import DataValidator, ValidationError


# =============================================================================
# Custom Strategies
# =============================================================================

# Strategy for column names (valid Python identifiers)
column_names = st.text(
    alphabet=st.sampled_from('abcdefghijklmnopqrstuvwxyz_'),
    min_size=1,
    max_size=20
).filter(lambda x: x[0] != '_' and x.isidentifier())

# Strategy for generating DataFrames with specific column types
@st.composite
def dataframes_with_types(draw, column_type_map):
    """Generate DataFrames with columns of specified types."""
    n_rows = draw(st.integers(min_value=1, max_value=50))
    
    data = {}
    for col_name, col_type in column_type_map.items():
        if col_type == int:
            data[col_name] = draw(st.lists(
                st.integers(min_value=-1000000, max_value=1000000),
                min_size=n_rows, max_size=n_rows
            ))
        elif col_type == float:
            data[col_name] = draw(st.lists(
                st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
                min_size=n_rows, max_size=n_rows
            ))
        elif col_type == str:
            data[col_name] = draw(st.lists(
                st.text(min_size=1, max_size=20),
                min_size=n_rows, max_size=n_rows
            ))
        elif col_type == bool:
            data[col_name] = draw(st.lists(
                st.booleans(),
                min_size=n_rows, max_size=n_rows
            ))
    
    return pd.DataFrame(data)


# Strategy for generating DataFrames with wrong types
@st.composite
def dataframes_with_wrong_types(draw):
    """Generate DataFrames where column types don't match expected types."""
    n_rows = draw(st.integers(min_value=1, max_value=20))
    
    # Create a DataFrame with string values where we expect int
    df = pd.DataFrame({
        'int_col': ['a', 'b', 'c'][:n_rows] + ['x'] * max(0, n_rows - 3),
        'float_col': ['1.5', '2.5', '3.5'][:n_rows] + ['y'] * max(0, n_rows - 3),
    })
    
    expected_types = {'int_col': int, 'float_col': float}
    
    return df, expected_types


# =============================================================================
# Property 1: Column type validation
# **Feature: processors-data-layering, Property 1: Column type validation**
# **Validates: Requirements 1.1**
# =============================================================================

class TestProperty1ColumnTypeValidation:
    """
    Property 1: Column type validation
    
    *For any* DataFrame with columns that don't match the expected schema types,
    the DataValidator SHALL identify all type mismatches and return them in
    the validation result.
    
    **Feature: processors-data-layering, Property 1: Column type validation**
    **Validates: Requirements 1.1**
    """

    @given(st.integers(min_value=1, max_value=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_correct_types_pass_validation(self, n_rows: int):
        """
        **Feature: processors-data-layering, Property 1: Column type validation**
        **Validates: Requirements 1.1**
        
        For any DataFrame with correct column types, validate_column_types
        SHALL return an empty dictionary (no errors).
        """
        # Create DataFrame with correct types
        df = pd.DataFrame({
            'int_col': np.random.randint(-1000, 1000, n_rows),
            'float_col': np.random.uniform(-100, 100, n_rows),
            'str_col': [f'value_{i}' for i in range(n_rows)],
        })
        
        schema = TableSchema(
            required_columns=['int_col', 'float_col', 'str_col'],
            column_types={'int_col': int, 'float_col': float, 'str_col': str}
        )
        
        validator = DataValidator(schema)
        type_errors = validator.validate_column_types(df)
        
        assert len(type_errors) == 0, (
            f"Expected no type errors for correct types, got: {type_errors}"
        )

    @given(st.integers(min_value=1, max_value=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_wrong_types_detected(self, n_rows: int):
        """
        **Feature: processors-data-layering, Property 1: Column type validation**
        **Validates: Requirements 1.1**
        
        For any DataFrame with wrong column types, validate_column_types
        SHALL identify all type mismatches.
        """
        # Create DataFrame with wrong types (strings where int/float expected)
        df = pd.DataFrame({
            'int_col': [f'str_{i}' for i in range(n_rows)],  # String instead of int
            'float_col': [f'str_{i}' for i in range(n_rows)],  # String instead of float
        })
        
        schema = TableSchema(
            required_columns=['int_col', 'float_col'],
            column_types={'int_col': int, 'float_col': float}
        )
        
        validator = DataValidator(schema)
        type_errors = validator.validate_column_types(df)
        
        # Both columns should have type errors
        assert 'int_col' in type_errors, (
            f"Expected type error for int_col, got: {type_errors}"
        )
        assert 'float_col' in type_errors, (
            f"Expected type error for float_col, got: {type_errors}"
        )

    @given(st.integers(min_value=1, max_value=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_type_error_message_format(self, n_rows: int):
        """
        **Feature: processors-data-layering, Property 1: Column type validation**
        **Validates: Requirements 1.1**
        
        Type error messages SHALL follow the format "expected X, got Y".
        """
        df = pd.DataFrame({
            'int_col': [f'str_{i}' for i in range(n_rows)],
        })
        
        schema = TableSchema(
            required_columns=['int_col'],
            column_types={'int_col': int}
        )
        
        validator = DataValidator(schema)
        type_errors = validator.validate_column_types(df)
        
        assert 'int_col' in type_errors
        error_msg = type_errors['int_col']
        assert 'expected' in error_msg.lower(), (
            f"Error message should contain 'expected': {error_msg}"
        )
        assert 'got' in error_msg.lower(), (
            f"Error message should contain 'got': {error_msg}"
        )

    def test_missing_columns_not_reported_as_type_errors(self):
        """
        **Feature: processors-data-layering, Property 1: Column type validation**
        **Validates: Requirements 1.1**
        
        Missing columns SHALL NOT be reported as type errors
        (they are handled by missing column detection).
        """
        df = pd.DataFrame({
            'existing_col': [1, 2, 3],
        })
        
        schema = TableSchema(
            required_columns=['existing_col', 'missing_col'],
            column_types={'existing_col': int, 'missing_col': int}
        )
        
        validator = DataValidator(schema)
        type_errors = validator.validate_column_types(df)
        
        # missing_col should not appear in type errors
        assert 'missing_col' not in type_errors, (
            f"Missing column should not be in type errors: {type_errors}"
        )


# =============================================================================
# Property 2: Missing column detection
# **Feature: processors-data-layering, Property 2: Missing column detection**
# **Validates: Requirements 1.2**
# =============================================================================

class TestProperty2MissingColumnDetection:
    """
    Property 2: Missing column detection
    
    *For any* DataFrame missing required columns, the DataValidator SHALL
    raise an exception containing the complete list of missing column names.
    
    **Feature: processors-data-layering, Property 2: Missing column detection**
    **Validates: Requirements 1.2**
    """

    @given(st.lists(st.text(min_size=1, max_size=10).filter(str.isidentifier), 
                    min_size=1, max_size=5, unique=True))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_all_missing_columns_detected(self, missing_cols: list):
        """
        **Feature: processors-data-layering, Property 2: Missing column detection**
        **Validates: Requirements 1.2**
        
        For any DataFrame missing required columns, detect_missing_columns
        SHALL return the complete list of missing column names.
        """
        # Create DataFrame without the required columns
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        
        schema = TableSchema(required_columns=missing_cols)
        validator = DataValidator(schema)
        
        detected_missing = validator.detect_missing_columns(df)
        
        # All required columns should be detected as missing
        assert set(detected_missing) == set(missing_cols), (
            f"Expected missing columns {missing_cols}, got {detected_missing}"
        )

    @given(st.integers(min_value=1, max_value=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_present_columns_not_reported_missing(self, n_rows: int):
        """
        **Feature: processors-data-layering, Property 2: Missing column detection**
        **Validates: Requirements 1.2**
        
        Columns that are present SHALL NOT be reported as missing.
        """
        df = pd.DataFrame({
            'col_a': range(n_rows),
            'col_b': range(n_rows),
        })
        
        schema = TableSchema(required_columns=['col_a', 'col_b'])
        validator = DataValidator(schema)
        
        detected_missing = validator.detect_missing_columns(df)
        
        assert len(detected_missing) == 0, (
            f"Expected no missing columns, got: {detected_missing}"
        )

    @given(st.lists(st.text(min_size=1, max_size=10).filter(str.isidentifier),
                    min_size=2, max_size=5, unique=True))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_partial_missing_columns(self, required_cols: list):
        """
        **Feature: processors-data-layering, Property 2: Missing column detection**
        **Validates: Requirements 1.2**
        
        When some required columns are present and some are missing,
        only the missing ones SHALL be reported.
        """
        assume(len(required_cols) >= 2)
        
        # Include only the first column
        present_cols = required_cols[:1]
        expected_missing = required_cols[1:]
        
        df = pd.DataFrame({col: [1, 2, 3] for col in present_cols})
        
        schema = TableSchema(required_columns=required_cols)
        validator = DataValidator(schema)
        
        detected_missing = validator.detect_missing_columns(df)
        
        assert set(detected_missing) == set(expected_missing), (
            f"Expected missing {expected_missing}, got {detected_missing}"
        )

    def test_validation_fails_on_missing_columns(self):
        """
        **Feature: processors-data-layering, Property 2: Missing column detection**
        **Validates: Requirements 1.2**
        
        Validation SHALL fail (is_valid=False) when required columns are missing.
        """
        df = pd.DataFrame({'other_col': [1, 2, 3]})
        
        schema = TableSchema(required_columns=['required_col'])
        validator = DataValidator(schema)
        
        result = validator.validate(df)
        
        assert not result.is_valid, "Validation should fail for missing columns"
        assert 'required_col' in result.missing_columns


# =============================================================================
# Property 3: Duplicate key deduplication
# **Feature: processors-data-layering, Property 3: Duplicate key deduplication**
# **Validates: Requirements 1.3**
# =============================================================================

class TestProperty3DuplicateKeyDeduplication:
    """
    Property 3: Duplicate key deduplication
    
    *For any* DataFrame with duplicate primary keys, the deduplication process
    SHALL keep exactly one record per key (the latest) and the output length
    SHALL equal the number of unique keys.
    
    **Feature: processors-data-layering, Property 3: Duplicate key deduplication**
    **Validates: Requirements 1.3**
    """

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_deduplicate_output_length_equals_unique_keys(self, key_values: list):
        """
        **Feature: processors-data-layering, Property 3: Duplicate key deduplication**
        **Validates: Requirements 1.3**
        
        For any DataFrame with duplicate primary keys, after deduplication
        the output length SHALL equal the number of unique keys.
        """
        df = pd.DataFrame({
            'key_col': key_values,
            'value_col': range(len(key_values)),
        })
        
        schema = TableSchema(required_columns=['key_col', 'value_col'])
        validator = DataValidator(schema)
        
        deduplicated, removed_count = validator.deduplicate(df, keys=['key_col'])
        
        expected_unique = len(set(key_values))
        assert len(deduplicated) == expected_unique, (
            f"Expected {expected_unique} unique rows, got {len(deduplicated)}"
        )

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_deduplicate_keeps_last_by_default(self, key_values: list):
        """
        **Feature: processors-data-layering, Property 3: Duplicate key deduplication**
        **Validates: Requirements 1.3**
        
        Deduplication SHALL keep the latest (last) record by default.
        """
        df = pd.DataFrame({
            'key_col': key_values,
            'value_col': range(len(key_values)),  # Sequential values
        })
        
        schema = TableSchema(required_columns=['key_col', 'value_col'])
        validator = DataValidator(schema)
        
        deduplicated, _ = validator.deduplicate(df, keys=['key_col'], keep='last')
        
        # For each unique key, the value should be the last occurrence
        for key in set(key_values):
            # Find the last index where this key appears
            last_idx = len(key_values) - 1 - key_values[::-1].index(key)
            expected_value = last_idx
            
            actual_value = deduplicated[deduplicated['key_col'] == key]['value_col'].iloc[0]
            assert actual_value == expected_value, (
                f"For key {key}, expected value {expected_value}, got {actual_value}"
            )

    @given(st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_detect_duplicates_returns_duplicate_rows(self, key_values: list):
        """
        **Feature: processors-data-layering, Property 3: Duplicate key deduplication**
        **Validates: Requirements 1.3**
        
        detect_duplicates SHALL return all duplicate rows (excluding first occurrence).
        """
        df = pd.DataFrame({
            'key_col': key_values,
            'value_col': range(len(key_values)),
        })
        
        schema = TableSchema(required_columns=['key_col', 'value_col'])
        validator = DataValidator(schema)
        
        duplicates = validator.detect_duplicates(df, keys=['key_col'])
        
        # Count expected duplicates
        from collections import Counter
        counts = Counter(key_values)
        expected_dup_count = sum(c - 1 for c in counts.values() if c > 1)
        
        assert len(duplicates) == expected_dup_count, (
            f"Expected {expected_dup_count} duplicate rows, got {len(duplicates)}"
        )

    def test_no_duplicates_returns_empty(self):
        """
        **Feature: processors-data-layering, Property 3: Duplicate key deduplication**
        **Validates: Requirements 1.3**
        
        When there are no duplicates, detect_duplicates SHALL return empty DataFrame.
        """
        df = pd.DataFrame({
            'key_col': [1, 2, 3, 4, 5],
            'value_col': ['a', 'b', 'c', 'd', 'e'],
        })
        
        schema = TableSchema(required_columns=['key_col', 'value_col'])
        validator = DataValidator(schema)
        
        duplicates = validator.detect_duplicates(df, keys=['key_col'])
        
        assert len(duplicates) == 0, "Expected no duplicates"


# =============================================================================
# Property 4: Null value rejection
# **Feature: processors-data-layering, Property 4: Null value rejection**
# **Validates: Requirements 1.4**
# =============================================================================

class TestProperty4NullValueRejection:
    """
    Property 4: Null value rejection
    
    *For any* DataFrame with null values in required fields, the DataValidator
    SHALL reject the batch and report all field names containing nulls.
    
    **Feature: processors-data-layering, Property 4: Null value rejection**
    **Validates: Requirements 1.4**
    """

    @given(st.lists(st.text(min_size=1, max_size=10).filter(str.isidentifier),
                    min_size=1, max_size=5, unique=True))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_all_null_fields_detected(self, null_cols: list):
        """
        **Feature: processors-data-layering, Property 4: Null value rejection**
        **Validates: Requirements 1.4**
        
        For any DataFrame with null values in required fields,
        detect_nulls SHALL report all field names containing nulls.
        """
        # Create DataFrame with nulls in specified columns
        data = {col: [1, None, 3] for col in null_cols}
        df = pd.DataFrame(data)
        
        schema = TableSchema(
            required_columns=null_cols,
            nullable_columns=[]  # None are nullable
        )
        validator = DataValidator(schema)
        
        detected_nulls = validator.detect_nulls(df, null_cols)
        
        assert set(detected_nulls) == set(null_cols), (
            f"Expected null fields {null_cols}, got {detected_nulls}"
        )

    @given(st.integers(min_value=1, max_value=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_no_nulls_returns_empty(self, n_rows: int):
        """
        **Feature: processors-data-layering, Property 4: Null value rejection**
        **Validates: Requirements 1.4**
        
        When there are no null values, detect_nulls SHALL return empty list.
        """
        df = pd.DataFrame({
            'col_a': range(n_rows),
            'col_b': [f'val_{i}' for i in range(n_rows)],
        })
        
        schema = TableSchema(required_columns=['col_a', 'col_b'])
        validator = DataValidator(schema)
        
        detected_nulls = validator.detect_nulls(df, ['col_a', 'col_b'])
        
        assert len(detected_nulls) == 0, (
            f"Expected no null fields, got: {detected_nulls}"
        )

    def test_nullable_columns_not_reported(self):
        """
        **Feature: processors-data-layering, Property 4: Null value rejection**
        **Validates: Requirements 1.4**
        
        Nullable columns SHALL NOT be reported even if they contain nulls.
        """
        df = pd.DataFrame({
            'required_col': [1, 2, 3],
            'nullable_col': [1, None, 3],
        })
        
        schema = TableSchema(
            required_columns=['required_col', 'nullable_col'],
            nullable_columns=['nullable_col']
        )
        validator = DataValidator(schema)
        
        # Only check non-nullable required columns
        non_nullable = schema.get_non_nullable_required_columns()
        detected_nulls = validator.detect_nulls(df, non_nullable)
        
        assert 'nullable_col' not in detected_nulls, (
            "Nullable column should not be reported"
        )

    def test_validation_fails_on_null_in_required(self):
        """
        **Feature: processors-data-layering, Property 4: Null value rejection**
        **Validates: Requirements 1.4**
        
        Validation SHALL fail (is_valid=False) when required non-nullable
        fields contain null values.
        """
        df = pd.DataFrame({
            'required_col': [1, None, 3],
        })
        
        schema = TableSchema(
            required_columns=['required_col'],
            nullable_columns=[]
        )
        validator = DataValidator(schema)
        
        result = validator.validate(df)
        
        assert not result.is_valid, "Validation should fail for null in required field"
        assert 'required_col' in result.null_fields


# =============================================================================
# Property 5: Range validation flagging
# **Feature: processors-data-layering, Property 5: Range validation flagging**
# **Validates: Requirements 1.5**
# =============================================================================

class TestProperty5RangeValidationFlagging:
    """
    Property 5: Range validation flagging
    
    *For any* DataFrame with values outside valid ranges, the DataValidator
    SHALL add a `_validation_flag` column marking the out-of-range records.
    
    **Feature: processors-data-layering, Property 5: Range validation flagging**
    **Validates: Requirements 1.5**
    """

    @given(st.lists(st.floats(min_value=-1e6, max_value=1e6, 
                              allow_nan=False, allow_infinity=False),
                    min_size=5, max_size=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_out_of_range_rows_detected(self, values: list):
        """
        **Feature: processors-data-layering, Property 5: Range validation flagging**
        **Validates: Requirements 1.5**
        
        For any DataFrame with values outside valid ranges,
        detect_out_of_range SHALL return the index of out-of-range rows.
        """
        df = pd.DataFrame({'value_col': values})
        
        # Define a range that will likely have some values outside
        min_val, max_val = -100.0, 100.0
        ranges = {'value_col': (min_val, max_val)}
        
        schema = TableSchema(
            required_columns=['value_col'],
            value_ranges=ranges
        )
        validator = DataValidator(schema)
        
        out_of_range = validator.detect_out_of_range(df, ranges)
        
        # Verify each detected row is actually out of range
        for idx in out_of_range:
            val = df.loc[idx, 'value_col']
            assert val < min_val or val > max_val, (
                f"Row {idx} with value {val} is within range [{min_val}, {max_val}]"
            )
        
        # Verify no in-range rows are incorrectly flagged
        in_range_mask = (df['value_col'] >= min_val) & (df['value_col'] <= max_val)
        in_range_indices = df.index[in_range_mask]
        for idx in in_range_indices:
            assert idx not in out_of_range, (
                f"Row {idx} is in range but was flagged as out of range"
            )

    @given(st.integers(min_value=1, max_value=20))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_all_in_range_returns_empty(self, n_rows: int):
        """
        **Feature: processors-data-layering, Property 5: Range validation flagging**
        **Validates: Requirements 1.5**
        
        When all values are within range, detect_out_of_range SHALL return empty index.
        """
        # Create values guaranteed to be in range
        values = np.random.uniform(10, 90, n_rows)
        df = pd.DataFrame({'value_col': values})
        
        ranges = {'value_col': (0.0, 100.0)}
        
        schema = TableSchema(
            required_columns=['value_col'],
            value_ranges=ranges
        )
        validator = DataValidator(schema)
        
        out_of_range = validator.detect_out_of_range(df, ranges)
        
        assert len(out_of_range) == 0, (
            f"Expected no out-of-range rows, got {len(out_of_range)}"
        )

    def test_validation_flag_column_added(self):
        """
        **Feature: processors-data-layering, Property 5: Range validation flagging**
        **Validates: Requirements 1.5**
        
        add_validation_flag SHALL add a _validation_flag column
        marking out-of-range records with 1.
        """
        df = pd.DataFrame({
            'value_col': [50, 150, 75, 200, 25],  # 150 and 200 are out of range
        })
        
        ranges = {'value_col': (0.0, 100.0)}
        
        schema = TableSchema(
            required_columns=['value_col'],
            value_ranges=ranges
        )
        validator = DataValidator(schema)
        
        out_of_range = validator.detect_out_of_range(df, ranges)
        result = validator.add_validation_flag(df, out_of_range)
        
        assert '_validation_flag' in result.columns, (
            "Expected _validation_flag column to be added"
        )
        
        # Check flagged rows
        assert result.loc[1, '_validation_flag'] == 1, "Row 1 should be flagged"
        assert result.loc[3, '_validation_flag'] == 1, "Row 3 should be flagged"
        
        # Check non-flagged rows
        assert result.loc[0, '_validation_flag'] == 0, "Row 0 should not be flagged"
        assert result.loc[2, '_validation_flag'] == 0, "Row 2 should not be flagged"
        assert result.loc[4, '_validation_flag'] == 0, "Row 4 should not be flagged"

    def test_out_of_range_is_warning_not_failure(self):
        """
        **Feature: processors-data-layering, Property 5: Range validation flagging**
        **Validates: Requirements 1.5**
        
        Out-of-range values SHALL be a warning, not a validation failure.
        Validation SHALL still pass (is_valid=True) with out-of-range values.
        """
        df = pd.DataFrame({
            'value_col': [50.0, 150.0, 75.0],  # 150 is out of range, use floats
        })
        
        schema = TableSchema(
            required_columns=['value_col'],
            column_types={'value_col': float},
            value_ranges={'value_col': (0.0, 100.0)}
        )
        validator = DataValidator(schema)
        
        result = validator.validate(df)
        
        # Validation should pass (out of range is warning only)
        assert result.is_valid, "Validation should pass with out-of-range values"
        
        # But out_of_range_rows should be populated
        assert result.has_out_of_range_values(), (
            "Out of range values should be detected"
        )
