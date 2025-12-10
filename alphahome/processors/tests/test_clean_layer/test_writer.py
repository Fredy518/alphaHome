#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for CleanLayerWriter.

Tests Property 6 from the design document:
- Property 6: Column preservation

Uses hypothesis library for property-based testing.
"""

import asyncio
import numpy as np
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
from typing import List, Tuple, Any
from unittest.mock import MagicMock, AsyncMock

from alphahome.processors.clean.writer import CleanLayerWriter, WriteError
from alphahome.processors.clean.lineage import LineageTracker


# =============================================================================
# Mock Database Connection
# =============================================================================

class MockDBConnection:
    """Mock database connection for testing."""
    
    def __init__(self, should_fail: bool = False, fail_count: int = 0):
        self.should_fail = should_fail
        self.fail_count = fail_count
        self.current_fail_count = 0
        self.executed_sql = []
        self.executed_values = []
    
    async def executemany(self, sql: str, values: List[Tuple]) -> None:
        """Mock executemany that can simulate failures."""
        if self.should_fail and self.current_fail_count < self.fail_count:
            self.current_fail_count += 1
            raise Exception("Simulated database error")
        
        self.executed_sql.append(sql)
        self.executed_values.append(values)
    
    class TransactionContext:
        """Mock transaction context manager."""
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                # Simulate rollback on exception
                pass
            return False
    
    def transaction(self):
        """Return a mock transaction context manager."""
        return self.TransactionContext()


# =============================================================================
# Custom Strategies
# =============================================================================

# Strategy for column names (valid Python identifiers)
column_names = st.text(
    alphabet=st.sampled_from('abcdefghijklmnopqrstuvwxyz'),
    min_size=1,
    max_size=10
).filter(lambda x: x.isidentifier())


@st.composite
def dataframes_with_columns(draw, min_cols=2, max_cols=6):
    """Generate DataFrames with random columns."""
    n_rows = draw(st.integers(min_value=1, max_value=20))
    n_cols = draw(st.integers(min_value=min_cols, max_value=max_cols))
    
    # Generate unique column names
    col_names = []
    for i in range(n_cols):
        col_name = draw(column_names)
        # Ensure uniqueness
        while col_name in col_names:
            col_name = draw(column_names)
        col_names.append(col_name)
    
    # Generate data for each column
    data = {}
    for col_name in col_names:
        col_type = draw(st.sampled_from(['int', 'float', 'str']))
        if col_type == 'int':
            data[col_name] = draw(st.lists(
                st.integers(min_value=-1000, max_value=1000),
                min_size=n_rows, max_size=n_rows
            ))
        elif col_type == 'float':
            data[col_name] = draw(st.lists(
                st.floats(min_value=-100, max_value=100, allow_nan=False, allow_infinity=False),
                min_size=n_rows, max_size=n_rows
            ))
        else:
            data[col_name] = draw(st.lists(
                st.text(min_size=1, max_size=10),
                min_size=n_rows, max_size=n_rows
            ))
    
    return pd.DataFrame(data)


# =============================================================================
# Property 6: Column preservation
# **Feature: processors-data-layering, Property 6: Column preservation**
# **Validates: Requirements 1.6**
# =============================================================================

class TestProperty6ColumnPreservation:
    """
    Property 6: Column preservation
    
    *For any* DataFrame processed by the Clean Layer, the output columns
    SHALL be a superset of input columns (plus lineage columns), with no
    columns silently dropped or renamed.
    
    **Feature: processors-data-layering, Property 6: Column preservation**
    **Validates: Requirements 1.6**
    """

    @given(dataframes_with_columns(min_cols=2, max_cols=5))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow, HealthCheck.large_base_example])
    def test_verify_columns_preserved_detects_no_drops(self, df: pd.DataFrame):
        """
        **Feature: processors-data-layering, Property 6: Column preservation**
        **Validates: Requirements 1.6**
        
        For any DataFrame where no columns are dropped, verify_columns_preserved
        SHALL return (True, []).
        """
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        # Output is same as input (no columns dropped)
        output_df = df.copy()
        
        is_preserved, dropped = writer.verify_columns_preserved(df, output_df)
        
        assert is_preserved is True, (
            f"Expected columns to be preserved, but got dropped: {dropped}"
        )
        assert len(dropped) == 0, (
            f"Expected no dropped columns, got: {dropped}"
        )

    @given(dataframes_with_columns(min_cols=3, max_cols=6))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow, HealthCheck.large_base_example])
    def test_verify_columns_preserved_detects_drops(self, df: pd.DataFrame):
        """
        **Feature: processors-data-layering, Property 6: Column preservation**
        **Validates: Requirements 1.6**
        
        For any DataFrame where columns are dropped, verify_columns_preserved
        SHALL return (False, [list of dropped columns]).
        """
        assume(len(df.columns) >= 2)
        
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        # Drop the first column
        dropped_col = df.columns[0]
        output_df = df.drop(columns=[dropped_col])
        
        is_preserved, dropped = writer.verify_columns_preserved(df, output_df)
        
        assert is_preserved is False, (
            f"Expected columns NOT to be preserved when {dropped_col} was dropped"
        )
        assert dropped_col in dropped, (
            f"Expected {dropped_col} in dropped list, got: {dropped}"
        )

    @given(dataframes_with_columns(min_cols=2, max_cols=5))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow, HealthCheck.large_base_example])
    def test_lineage_columns_allowed_as_new(self, df: pd.DataFrame):
        """
        **Feature: processors-data-layering, Property 6: Column preservation**
        **Validates: Requirements 1.6**
        
        Lineage columns (_source_table, _processed_at, _data_version, _ingest_job_id)
        SHALL be allowed as new columns without being flagged as unexpected.
        """
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        # Add lineage columns using LineageTracker
        tracker = LineageTracker()
        output_df = tracker.add_lineage(
            df,
            source_tables=['test_source'],
            job_id='test_job_123'
        )
        
        is_preserved, dropped = writer.verify_columns_preserved(df, output_df)
        
        assert is_preserved is True, (
            f"Expected columns to be preserved with lineage columns added, "
            f"but got dropped: {dropped}"
        )
        assert len(dropped) == 0, (
            f"Expected no dropped columns, got: {dropped}"
        )
        
        # Verify lineage columns are present
        for lineage_col in LineageTracker.LINEAGE_COLUMNS:
            assert lineage_col in output_df.columns, (
                f"Expected lineage column {lineage_col} to be present"
            )

    @given(dataframes_with_columns(min_cols=2, max_cols=5))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow, HealthCheck.large_base_example])
    def test_validation_flag_allowed_as_new(self, df: pd.DataFrame):
        """
        **Feature: processors-data-layering, Property 6: Column preservation**
        **Validates: Requirements 1.6**
        
        The _validation_flag column SHALL be allowed as a new column
        without being flagged as unexpected.
        """
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        # Add validation flag column
        output_df = df.copy()
        output_df['_validation_flag'] = 0
        
        is_preserved, dropped = writer.verify_columns_preserved(df, output_df)
        
        assert is_preserved is True, (
            f"Expected columns to be preserved with _validation_flag added, "
            f"but got dropped: {dropped}"
        )
        assert len(dropped) == 0, (
            f"Expected no dropped columns, got: {dropped}"
        )

    @given(dataframes_with_columns(min_cols=3, max_cols=6))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow, HealthCheck.large_base_example])
    def test_multiple_columns_dropped_all_detected(self, df: pd.DataFrame):
        """
        **Feature: processors-data-layering, Property 6: Column preservation**
        **Validates: Requirements 1.6**
        
        When multiple columns are dropped, ALL dropped columns SHALL be
        reported in the dropped list.
        """
        assume(len(df.columns) >= 3)
        
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        # Drop multiple columns
        cols_to_drop = list(df.columns[:2])
        output_df = df.drop(columns=cols_to_drop)
        
        is_preserved, dropped = writer.verify_columns_preserved(df, output_df)
        
        assert is_preserved is False, (
            f"Expected columns NOT to be preserved when {cols_to_drop} were dropped"
        )
        for col in cols_to_drop:
            assert col in dropped, (
                f"Expected {col} in dropped list, got: {dropped}"
            )

    def test_empty_dataframe_preserved(self):
        """
        **Feature: processors-data-layering, Property 6: Column preservation**
        **Validates: Requirements 1.6**
        
        Empty DataFrames with matching columns SHALL be considered preserved.
        """
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        # Create empty DataFrames with same columns
        input_df = pd.DataFrame(columns=['col_a', 'col_b', 'col_c'])
        output_df = pd.DataFrame(columns=['col_a', 'col_b', 'col_c'])
        
        is_preserved, dropped = writer.verify_columns_preserved(input_df, output_df)
        
        assert is_preserved is True
        assert len(dropped) == 0


# =============================================================================
# Additional Writer Tests (Unit Tests)
# =============================================================================

class TestCleanLayerWriterUnit:
    """Unit tests for CleanLayerWriter functionality."""

    def test_init_default_values(self):
        """Test default initialization values."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        assert writer.batch_size == 10000
        assert writer.max_retries == 3
        assert writer.retry_delay_base == 2.0

    def test_init_custom_values(self):
        """Test custom initialization values."""
        db = MockDBConnection()
        writer = CleanLayerWriter(
            db,
            batch_size=5000,
            max_retries=5,
            retry_delay_base=1.5
        )
        
        assert writer.batch_size == 5000
        assert writer.max_retries == 5
        assert writer.retry_delay_base == 1.5

    def test_split_into_batches_single_batch(self):
        """Test splitting when data fits in single batch."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db, batch_size=100)
        
        df = pd.DataFrame({'col': range(50)})
        batches = writer._split_into_batches(df)
        
        assert len(batches) == 1
        assert len(batches[0]) == 50

    def test_split_into_batches_multiple_batches(self):
        """Test splitting into multiple batches."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db, batch_size=30)
        
        df = pd.DataFrame({'col': range(100)})
        batches = writer._split_into_batches(df)
        
        assert len(batches) == 4  # 30 + 30 + 30 + 10
        assert len(batches[0]) == 30
        assert len(batches[1]) == 30
        assert len(batches[2]) == 30
        assert len(batches[3]) == 10

    def test_validate_inputs_empty_table_name(self):
        """Test validation rejects empty table name."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="table_name cannot be empty"):
            writer._validate_inputs(df, '', ['col'], 'replace')

    def test_validate_inputs_empty_primary_keys(self):
        """Test validation rejects empty primary keys."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="primary_keys cannot be empty"):
            writer._validate_inputs(df, 'test_table', [], 'replace')

    def test_validate_inputs_merge_strategy_rejected(self):
        """Test validation rejects merge strategy (not implemented)."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="Merge strategy is not implemented"):
            writer._validate_inputs(df, 'test_table', ['col'], 'merge')

    def test_validate_inputs_unsupported_strategy(self):
        """Test validation rejects unsupported strategies."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="Unsupported conflict strategy"):
            writer._validate_inputs(df, 'test_table', ['col'], 'unknown')

    def test_build_upsert_sql_replace_strategy(self):
        """Test UPSERT SQL generation with replace strategy."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        sql = writer._build_upsert_sql(
            'clean.test_table',
            ['trade_date', 'ts_code', 'value'],
            ['trade_date', 'ts_code'],
            'replace'
        )
        
        assert 'INSERT INTO clean.test_table' in sql
        assert 'ON CONFLICT (trade_date, ts_code)' in sql
        assert 'DO UPDATE SET value = EXCLUDED.value' in sql

    def test_build_upsert_sql_all_pk_columns(self):
        """Test UPSERT SQL when all columns are primary keys."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        sql = writer._build_upsert_sql(
            'clean.test_table',
            ['trade_date', 'ts_code'],
            ['trade_date', 'ts_code'],
            'replace'
        )
        
        assert 'INSERT INTO clean.test_table' in sql
        assert 'ON CONFLICT (trade_date, ts_code)' in sql
        assert 'DO NOTHING' in sql

    def test_calculate_retry_delay_exponential(self):
        """Test exponential backoff calculation."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db, retry_delay_base=2.0)
        
        assert writer._calculate_retry_delay(0) == 2.0  # 2^1
        assert writer._calculate_retry_delay(1) == 4.0  # 2^2
        assert writer._calculate_retry_delay(2) == 8.0  # 2^3

    @pytest.mark.asyncio
    async def test_upsert_empty_dataframe(self):
        """Test upsert with empty DataFrame returns 0."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        df = pd.DataFrame()
        rows = await writer.upsert(df, 'test_table', ['pk'])
        
        assert rows == 0

    @pytest.mark.asyncio
    async def test_upsert_missing_primary_key_column(self):
        """Test upsert raises error when primary key column is missing."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db)
        
        df = pd.DataFrame({'col_a': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="Primary key columns not found"):
            await writer.upsert(df, 'test_table', ['missing_pk'])

    @pytest.mark.asyncio
    async def test_upsert_success(self):
        """Test successful upsert execution."""
        db = MockDBConnection()
        writer = CleanLayerWriter(db, batch_size=100)
        
        df = pd.DataFrame({
            'trade_date': [20231001, 20231002, 20231003],
            'ts_code': ['000001.SZ', '000001.SZ', '000001.SZ'],
            'value': [1.0, 2.0, 3.0]
        })
        
        rows = await writer.upsert(
            df,
            'clean.test_table',
            ['trade_date', 'ts_code']
        )
        
        assert rows == 3
        assert len(db.executed_sql) == 1
        assert len(db.executed_values) == 1
        assert len(db.executed_values[0]) == 3

    @pytest.mark.asyncio
    async def test_upsert_retry_on_failure(self):
        """Test upsert retries on transient failures."""
        db = MockDBConnection(should_fail=True, fail_count=2)
        writer = CleanLayerWriter(db, batch_size=100, max_retries=3, retry_delay_base=0.1)
        
        df = pd.DataFrame({
            'trade_date': [20231001],
            'ts_code': ['000001.SZ'],
            'value': [1.0]
        })
        
        # Should succeed after 2 failures
        rows = await writer.upsert(
            df,
            'clean.test_table',
            ['trade_date', 'ts_code']
        )
        
        assert rows == 1
        assert db.current_fail_count == 2

    @pytest.mark.asyncio
    async def test_upsert_fails_after_max_retries(self):
        """Test upsert raises WriteError after max retries."""
        db = MockDBConnection(should_fail=True, fail_count=10)  # Always fail
        writer = CleanLayerWriter(db, batch_size=100, max_retries=3, retry_delay_base=0.1)
        
        df = pd.DataFrame({
            'trade_date': [20231001],
            'ts_code': ['000001.SZ'],
            'value': [1.0]
        })
        
        with pytest.raises(WriteError, match="Failed to write batch"):
            await writer.upsert(
                df,
                'clean.test_table',
                ['trade_date', 'ts_code']
            )
