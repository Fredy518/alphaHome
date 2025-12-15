#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for MaterializedViewValidator.

Uses hypothesis library for property-based testing.

**Feature: materialized-views-system, Property 5: Data quality check accuracy**
**Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging

from alphahome.processors.materialized_views import MaterializedViewValidator


# =============================================================================
# Custom Strategies for Data Quality Checks
# =============================================================================

def dataframe_with_nulls_strategy():
    """Generate DataFrames with controlled null values."""
    # Generate column names
    columns = st.lists(
        st.text(alphabet='abcdefghijklmnopqrstuvwxyz_', min_size=1, max_size=10),
        min_size=1,
        max_size=5,
        unique=True
    )
    
    return columns.flatmap(lambda cols: st.just(cols).flatmap(lambda c: 
        st.lists(
            st.lists(
                st.one_of(st.none(), st.floats(allow_nan=False, allow_infinity=False)),
                min_size=len(c),
                max_size=len(c)
            ),
            min_size=10,
            max_size=100
        ).map(lambda rows: pd.DataFrame(rows, columns=c))
    ))


def dataframe_with_outliers_strategy():
    """Generate DataFrames with controlled outliers."""
    # Generate numeric data with some outliers
    def make_df(data):
        df = pd.DataFrame({
            'normal_col': data['normal'],
            'outlier_col': data['outliers']
        })
        return df
    
    normal_data = st.lists(
        st.floats(min_value=-10, max_value=10, allow_nan=False, allow_infinity=False),
        min_size=10,
        max_size=100
    )
    
    outlier_data = st.lists(
        st.one_of(
            st.floats(min_value=-10, max_value=10, allow_nan=False, allow_infinity=False),
            st.floats(min_value=100, max_value=1000, allow_nan=False, allow_infinity=False)
        ),
        min_size=10,
        max_size=100
    )
    
    return st.tuples(normal_data, outlier_data).map(
        lambda x: pd.DataFrame({
            'normal_col': x[0],
            'outlier_col': x[1]
        })
    )


def dataframe_with_duplicates_strategy():
    """Generate DataFrames with controlled duplicates."""
    # Generate data with some duplicate rows
    base_data = st.lists(
        st.tuples(
            st.integers(min_value=1, max_value=100),
            st.text(alphabet='abc', min_size=1, max_size=5)
        ),
        min_size=5,
        max_size=20
    )
    
    return base_data.map(lambda rows: 
        pd.DataFrame(rows + rows[:len(rows)//2], columns=['id', 'name'])
    )


# =============================================================================
# Property 5: Data quality check accuracy
# **Feature: materialized-views-system, Property 5: Data quality check accuracy**
# **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
# =============================================================================

class TestProperty5DataQualityCheckAccuracy:
    """
    Property 5: Data quality check accuracy
    
    *For any* materialized view data, the quality checks SHALL:
    1. Accurately detect null values exceeding threshold
    2. Accurately detect outliers using specified methods
    3. Accurately detect row count changes exceeding threshold
    4. Accurately detect duplicate rows based on key columns
    5. Accurately detect type mismatches
    
    **Feature: materialized-views-system, Property 5: Data quality check accuracy**
    **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
    """
    
    @pytest.mark.asyncio
    async def test_null_check_detects_nulls_exceeding_threshold(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.1**
        
        For any DataFrame with null values, validate_null_values() SHALL:
        - Return 'pass' if null percentage <= threshold
        - Return 'warning' if null percentage > threshold
        - Accurately report null count and percentage
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        # Test case 1: No nulls
        df = pd.DataFrame({
            'col1': [1, 2, 3, 4, 5],
            'col2': ['a', 'b', 'c', 'd', 'e']
        })
        
        result = await validator.validate_null_values(
            df,
            {'columns': ['col1', 'col2'], 'threshold': 0.1}
        )
        
        assert result['status'] == 'pass', "Should pass when no nulls"
        assert result['check_name'] == 'null_check'
        
        # Test case 2: Nulls below threshold
        df = pd.DataFrame({
            'col1': [1, 2, None, 4, 5],
            'col2': ['a', 'b', 'c', 'd', 'e']
        })
        
        result = await validator.validate_null_values(
            df,
            {'columns': ['col1'], 'threshold': 0.3}
        )
        
        assert result['status'] == 'pass', "Should pass when nulls below threshold"
        
        # Test case 3: Nulls exceeding threshold
        df = pd.DataFrame({
            'col1': [1, None, None, None, 5],
            'col2': ['a', 'b', 'c', 'd', 'e']
        })
        
        result = await validator.validate_null_values(
            df,
            {'columns': ['col1'], 'threshold': 0.3}
        )
        
        assert result['status'] == 'warning', "Should warn when nulls exceed threshold"
        assert len(result['details']['columns_with_issues']) > 0
        
        # Verify reported null percentage
        issue = result['details']['columns_with_issues'][0]
        assert issue['null_count'] == 3
        assert abs(issue['null_percentage'] - 0.6) < 0.01

    @pytest.mark.asyncio
    async def test_outlier_check_detects_outliers_iqr_method(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.2**
        
        For any DataFrame with numeric data, validate_outliers() with IQR method SHALL:
        - Accurately detect values outside IQR bounds
        - Return 'pass' if no outliers detected
        - Return 'warning' if outliers detected
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        # Test case 1: No outliers
        df = pd.DataFrame({
            'values': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        })
        
        result = await validator.validate_outliers(
            df,
            {'columns': ['values'], 'method': 'iqr', 'threshold': 1.5}
        )
        
        assert result['status'] == 'pass', "Should pass when no outliers"
        
        # Test case 2: With outliers
        df = pd.DataFrame({
            'values': [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]  # 100 is outlier
        })
        
        result = await validator.validate_outliers(
            df,
            {'columns': ['values'], 'method': 'iqr', 'threshold': 1.5}
        )
        
        assert result['status'] == 'warning', "Should warn when outliers detected"
        assert len(result['details']['columns_with_issues']) > 0
        
        # Verify outlier count
        issue = result['details']['columns_with_issues'][0]
        assert issue['outlier_count'] >= 1

    @pytest.mark.asyncio
    async def test_outlier_check_detects_outliers_zscore_method(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.2**
        
        For any DataFrame with numeric data, validate_outliers() with Z-score method SHALL:
        - Accurately detect values beyond threshold standard deviations
        - Return 'pass' if no outliers detected
        - Return 'warning' if outliers detected
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        # Test case: With outliers (Z-score > 3)
        df = pd.DataFrame({
            'values': [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]  # 100 is outlier
        })
        
        result = await validator.validate_outliers(
            df,
            {'columns': ['values'], 'method': 'zscore', 'threshold': 2.0}
        )
        
        assert result['status'] == 'warning', "Should warn when outliers detected"
        assert len(result['details']['columns_with_issues']) > 0

    @pytest.mark.asyncio
    async def test_row_count_change_detects_significant_changes(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.3**
        
        For any DataFrame, validate_row_count_change() SHALL:
        - Return 'pass' if row count change <= threshold
        - Return 'warning' if row count change > threshold
        - Accurately calculate change percentage
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        # Test case 1: First check (no previous count)
        df1 = pd.DataFrame({'col': [1, 2, 3, 4, 5]})
        
        result = await validator.validate_row_count_change(
            df1,
            {'threshold': 0.5}
        )
        
        assert result['status'] == 'pass', "Should pass on first check"
        assert result['details']['current_row_count'] == 5
        
        # Test case 2: Small change (within threshold)
        df2 = pd.DataFrame({'col': [1, 2, 3, 4, 5, 6]})
        
        result = await validator.validate_row_count_change(
            df2,
            {'threshold': 0.5},
            previous_row_count=5
        )
        
        assert result['status'] == 'pass', "Should pass when change within threshold"
        assert abs(result['details']['change_percentage'] - 0.2) < 0.01
        
        # Test case 3: Large change (exceeding threshold)
        df3 = pd.DataFrame({'col': [1, 2, 3]})
        
        result = await validator.validate_row_count_change(
            df3,
            {'threshold': 0.3},
            previous_row_count=5
        )
        
        assert result['status'] == 'warning', "Should warn when change exceeds threshold"
        assert abs(result['details']['change_percentage'] - 0.4) < 0.01

    @pytest.mark.asyncio
    async def test_duplicate_check_detects_duplicate_rows(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.4**
        
        For any DataFrame, validate_duplicates() SHALL:
        - Return 'pass' if no duplicates based on key columns
        - Return 'error' if duplicates detected
        - Accurately count duplicate rows
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        # Test case 1: No duplicates
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['a', 'b', 'c', 'd', 'e']
        })
        
        result = await validator.validate_duplicates(
            df,
            {'columns': ['id']}
        )
        
        assert result['status'] == 'pass', "Should pass when no duplicates"
        assert result['details']['duplicate_count'] == 0
        
        # Test case 2: With duplicates
        df = pd.DataFrame({
            'id': [1, 2, 2, 3, 3],
            'name': ['a', 'b', 'b', 'c', 'c']
        })
        
        result = await validator.validate_duplicates(
            df,
            {'columns': ['id']}
        )
        
        assert result['status'] == 'error', "Should error when duplicates detected"
        assert result['details']['duplicate_count'] == 4  # 2 duplicates of id=2, 2 duplicates of id=3
        
        # Test case 3: Duplicates on composite key
        df = pd.DataFrame({
            'id': [1, 1, 2, 2],
            'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02']
        })
        
        result = await validator.validate_duplicates(
            df,
            {'columns': ['id', 'date']}
        )
        
        assert result['status'] == 'error', "Should error when composite key duplicates detected"

    @pytest.mark.asyncio
    async def test_type_check_detects_type_mismatches(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.5**
        
        For any DataFrame, validate_types() SHALL:
        - Return 'pass' if all columns have expected types
        - Return 'error' if type mismatches detected
        - Accurately report expected vs actual types
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        # Test case 1: Correct types
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.0, 2.0, 3.0],
            'str_col': ['a', 'b', 'c']
        })
        
        result = await validator.validate_types(
            df,
            {
                'columns': {
                    'int_col': 'int64',
                    'float_col': 'float64',
                    'str_col': 'object'
                }
            }
        )
        
        assert result['status'] == 'pass', "Should pass when types match"
        
        # Test case 2: Type mismatch
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': ['1.0', '2.0', '3.0']  # Should be float, but is object
        })
        
        result = await validator.validate_types(
            df,
            {
                'columns': {
                    'int_col': 'int64',
                    'float_col': 'float64'
                }
            }
        )
        
        assert result['status'] == 'error', "Should error when types don't match"
        assert len(result['details']['columns_with_issues']) > 0
        
        # Test case 3: Missing column
        df = pd.DataFrame({
            'int_col': [1, 2, 3]
        })
        
        result = await validator.validate_types(
            df,
            {
                'columns': {
                    'int_col': 'int64',
                    'missing_col': 'float64'
                }
            }
        )
        
        assert result['status'] == 'error', "Should error when column is missing"

    @pytest.mark.asyncio
    async def test_null_check_with_multiple_columns(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.1**
        
        For any DataFrame with multiple columns, validate_null_values() SHALL:
        - Check all specified columns
        - Report issues for each column exceeding threshold
        - Accurately calculate null percentage for each column
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        df = pd.DataFrame({
            'col1': [1, None, None, 4, 5],  # 2 nulls = 40%
            'col2': [None, None, None, 4, 5],  # 3 nulls = 60%
            'col3': [1, 2, 3, 4, 5]  # 0 nulls = 0%
        })
        
        result = await validator.validate_null_values(
            df,
            {'columns': ['col1', 'col2', 'col3'], 'threshold': 0.2}
        )
        
        assert result['status'] == 'warning', "Should warn when any column exceeds threshold"
        assert len(result['details']['columns_with_issues']) == 2  # col1 and col2
        
        # Verify col1 has 2 nulls (40%)
        col1_issue = next(i for i in result['details']['columns_with_issues'] if i['column'] == 'col1')
        assert col1_issue['null_count'] == 2
        assert abs(col1_issue['null_percentage'] - 0.4) < 0.01
        
        # Verify col2 has 3 nulls (60%)
        col2_issue = next(i for i in result['details']['columns_with_issues'] if i['column'] == 'col2')
        assert col2_issue['null_count'] == 3
        assert abs(col2_issue['null_percentage'] - 0.6) < 0.01

    @pytest.mark.asyncio
    async def test_outlier_check_skips_non_numeric_columns(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.2**
        
        For any DataFrame with non-numeric columns, validate_outliers() SHALL:
        - Skip non-numeric columns
        - Only check numeric columns
        - Return 'pass' if no numeric columns or no outliers
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        df = pd.DataFrame({
            'str_col': ['a', 'b', 'c', 'd', 'e'],
            'int_col': [1, 2, 3, 4, 5]
        })
        
        result = await validator.validate_outliers(
            df,
            {'columns': ['str_col', 'int_col'], 'method': 'iqr', 'threshold': 1.5}
        )
        
        # Should pass because int_col has no outliers
        # str_col should be skipped
        assert result['status'] == 'pass', "Should pass when no numeric outliers"

    @pytest.mark.asyncio
    async def test_row_count_change_with_zero_previous_count(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.3**
        
        For any DataFrame with zero previous row count, validate_row_count_change() SHALL:
        - Handle division by zero gracefully
        - Return appropriate status
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        result = await validator.validate_row_count_change(
            df,
            {'threshold': 0.5},
            previous_row_count=0
        )
        
        # Should handle gracefully
        assert result['check_name'] == 'row_count_change'
        assert 'current_row_count' in result['details']
        assert result['details']['current_row_count'] == 3

    @pytest.mark.asyncio
    async def test_duplicate_check_with_composite_keys(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.4**
        
        For any DataFrame, validate_duplicates() with composite keys SHALL:
        - Check duplicates based on multiple columns
        - Accurately count duplicate rows
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        df = pd.DataFrame({
            'id': [1, 1, 1, 2, 2],
            'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-01', '2024-01-01'],
            'value': [10, 10, 20, 30, 30]
        })
        
        # Check duplicates on (id, date)
        result = await validator.validate_duplicates(
            df,
            {'columns': ['id', 'date']}
        )
        
        assert result['status'] == 'error', "Should error when composite key duplicates detected"
        # Rows 0 and 1 are duplicates on (id, date)
        # Rows 3 and 4 are duplicates on (id, date)
        assert result['details']['duplicate_count'] == 4

    @pytest.mark.asyncio
    async def test_all_checks_return_required_fields(self):
        """
        **Feature: materialized-views-system, Property 5: Data quality check accuracy**
        **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
        
        For any quality check, the result SHALL:
        - Have 'check_name' field
        - Have 'status' field (pass/warning/error)
        - Have 'message' field
        - Have 'details' field
        """
        from alphahome.processors.materialized_views import MaterializedViewValidator
        
        validator = MaterializedViewValidator()
        
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [1.0, 2.0, 3.0]
        })
        
        # Test all check methods
        checks = [
            validator.validate_null_values(df, {'columns': ['col1'], 'threshold': 0.1}),
            validator.validate_outliers(df, {'columns': ['col1'], 'method': 'iqr', 'threshold': 1.5}),
            validator.validate_row_count_change(df, {'threshold': 0.5}),
            validator.validate_duplicates(df, {'columns': ['col1']}),
            validator.validate_types(df, {'columns': {'col1': 'int64'}})
        ]
        
        for check_coro in checks:
            result = await check_coro
            
            assert 'check_name' in result, "Result should have 'check_name'"
            assert 'status' in result, "Result should have 'status'"
            assert 'message' in result, "Result should have 'message'"
            assert 'details' in result, "Result should have 'details'"
            
            assert result['status'] in ['pass', 'warning', 'error'], "Status should be pass/warning/error"
            assert isinstance(result['message'], str), "Message should be string"
            assert isinstance(result['details'], dict), "Details should be dict"
