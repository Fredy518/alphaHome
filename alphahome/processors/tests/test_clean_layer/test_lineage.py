#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for LineageTracker.

Tests Property 12 from the design document:
- Property 12: Lineage metadata completeness

Uses hypothesis library for property-based testing.
"""

from datetime import datetime, timezone
import numpy as np
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck

from alphahome.processors.clean.lineage import LineageTracker, LineageError


# =============================================================================
# Custom Strategies
# =============================================================================

# Strategy for valid table names
table_names = st.text(
    alphabet=st.sampled_from('abcdefghijklmnopqrstuvwxyz_0123456789.'),
    min_size=1,
    max_size=50
).filter(lambda x: x[0].isalpha() and x.strip() == x)

# Strategy for valid job IDs
job_ids = st.text(
    alphabet=st.sampled_from('abcdefghijklmnopqrstuvwxyz0123456789_-'),
    min_size=1,
    max_size=100
).filter(lambda x: x.strip() == x and len(x.strip()) > 0)

# Strategy for data versions
data_versions = st.one_of(
    st.none(),
    st.text(
        alphabet=st.sampled_from('0123456789_'),
        min_size=1,
        max_size=20
    ).filter(lambda x: x.strip() == x and len(x.strip()) > 0)
)


# Strategy for generating simple DataFrames
@st.composite
def simple_dataframes(draw, min_rows=0, max_rows=50):
    """Generate simple DataFrames with random data."""
    n_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
    n_cols = draw(st.integers(min_value=1, max_value=5))
    
    data = {}
    for i in range(n_cols):
        col_name = f'col_{i}'
        col_type = draw(st.sampled_from(['int', 'float', 'str']))
        
        if col_type == 'int':
            data[col_name] = draw(st.lists(
                st.integers(min_value=-1000, max_value=1000),
                min_size=n_rows, max_size=n_rows
            ))
        elif col_type == 'float':
            data[col_name] = draw(st.lists(
                st.floats(min_value=-1000, max_value=1000, 
                         allow_nan=False, allow_infinity=False),
                min_size=n_rows, max_size=n_rows
            ))
        else:
            data[col_name] = draw(st.lists(
                st.text(min_size=1, max_size=10),
                min_size=n_rows, max_size=n_rows
            ))
    
    return pd.DataFrame(data)


# =============================================================================
# Property 12: Lineage metadata completeness
# **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
# **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
# =============================================================================

class TestProperty12LineageMetadataCompleteness:
    """
    Property 12: Lineage metadata completeness
    
    *For any* DataFrame processed by the Clean Layer, the output SHALL contain
    all lineage columns (_source_table, _processed_at, _data_version, 
    _ingest_job_id) with non-null values.
    
    **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
    **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
    """

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids,
        data_version=data_versions
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_all_lineage_columns_present(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str,
        data_version: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.1, 4.2, 4.3, 4.4**
        
        For any DataFrame processed by add_lineage, the output SHALL contain
        all four lineage columns: _source_table, _processed_at, _data_version,
        _ingest_job_id.
        """
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id,
            data_version=data_version
        )
        
        # All lineage columns must be present
        assert LineageTracker.SOURCE_TABLE_COL in result.columns, (
            f"Missing {LineageTracker.SOURCE_TABLE_COL} column"
        )
        assert LineageTracker.PROCESSED_AT_COL in result.columns, (
            f"Missing {LineageTracker.PROCESSED_AT_COL} column"
        )
        assert LineageTracker.DATA_VERSION_COL in result.columns, (
            f"Missing {LineageTracker.DATA_VERSION_COL} column"
        )
        assert LineageTracker.INGEST_JOB_ID_COL in result.columns, (
            f"Missing {LineageTracker.INGEST_JOB_ID_COL} column"
        )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_lineage_columns_have_no_nulls(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
        
        For any DataFrame processed by add_lineage, all lineage columns
        SHALL have non-null values in every row.
        """
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id
        )
        
        # No null values in any lineage column
        for col in LineageTracker.LINEAGE_COLUMNS:
            null_count = result[col].isna().sum()
            assert null_count == 0, (
                f"Column {col} has {null_count} null values"
            )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=5, unique=True),
        job_id=job_ids
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_source_table_contains_all_sources(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.1**
        
        The _source_table column SHALL contain all source table names,
        comma-separated if multiple.
        """
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id
        )
        
        # Get the source table value (should be same for all rows)
        source_value = result[LineageTracker.SOURCE_TABLE_COL].iloc[0]
        
        # All source tables should be in the value
        for table in source_tables:
            assert table.strip() in source_value, (
                f"Source table '{table}' not found in _source_table: {source_value}"
            )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_processed_at_is_utc_timestamp(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.2**
        
        The _processed_at column SHALL contain a UTC timestamp.
        """
        tracker = LineageTracker()
        
        before = datetime.now(timezone.utc)
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id
        )
        after = datetime.now(timezone.utc)
        
        # Get the processed_at value
        processed_at = result[LineageTracker.PROCESSED_AT_COL].iloc[0]
        
        # Should be a datetime
        assert isinstance(processed_at, datetime), (
            f"_processed_at should be datetime, got {type(processed_at)}"
        )
        
        # Should be within the time window
        assert before <= processed_at <= after, (
            f"_processed_at {processed_at} not within expected range [{before}, {after}]"
        )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_job_id_matches_input(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.4**
        
        The _ingest_job_id column SHALL contain the provided job_id.
        """
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id
        )
        
        # All rows should have the same job_id
        for idx in range(len(result)):
            assert result[LineageTracker.INGEST_JOB_ID_COL].iloc[idx] == job_id, (
                f"Row {idx} has wrong job_id"
            )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids,
        data_version=st.text(
            alphabet=st.sampled_from('0123456789_v'),
            min_size=1,
            max_size=20
        ).filter(lambda x: x.strip() == x and len(x.strip()) > 0)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_data_version_matches_input_when_provided(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str,
        data_version: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.3**
        
        When data_version is provided, the _data_version column SHALL
        contain the provided value.
        """
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id,
            data_version=data_version
        )
        
        # All rows should have the provided data_version
        for idx in range(len(result)):
            assert result[LineageTracker.DATA_VERSION_COL].iloc[idx] == data_version, (
                f"Row {idx} has wrong data_version"
            )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_data_version_auto_generated_when_not_provided(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.3**
        
        When data_version is not provided, the _data_version column SHALL
        contain an auto-generated value in format YYYYMMDD_HHMMSS.
        """
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id,
            data_version=None
        )
        
        # Get the data_version value
        data_version = result[LineageTracker.DATA_VERSION_COL].iloc[0]
        
        # Should be a non-empty string
        assert isinstance(data_version, str), (
            f"_data_version should be string, got {type(data_version)}"
        )
        assert len(data_version) > 0, "_data_version should not be empty"
        
        # Should match format YYYYMMDD_HHMMSS (15 characters)
        assert len(data_version) == 15, (
            f"_data_version should be 15 chars (YYYYMMDD_HHMMSS), got {len(data_version)}: {data_version}"
        )
        assert data_version[8] == '_', (
            f"_data_version should have underscore at position 8: {data_version}"
        )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_input_dataframe_not_modified(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.5**
        
        The input DataFrame SHALL NOT be modified by add_lineage.
        """
        original_columns = list(df.columns)
        original_len = len(df)
        
        tracker = LineageTracker()
        _ = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id
        )
        
        # Input DataFrame should be unchanged
        assert list(df.columns) == original_columns, (
            "Input DataFrame columns were modified"
        )
        assert len(df) == original_len, (
            "Input DataFrame length was modified"
        )
        # Lineage columns should NOT be in original
        for col in LineageTracker.LINEAGE_COLUMNS:
            assert col not in df.columns, (
                f"Lineage column {col} was added to input DataFrame"
            )

    @given(
        df=simple_dataframes(min_rows=1, max_rows=50),
        source_tables=st.lists(table_names, min_size=1, max_size=3, unique=True),
        job_id=job_ids
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_validate_lineage_completeness_returns_true(
        self, 
        df: pd.DataFrame, 
        source_tables: list, 
        job_id: str
    ):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
        
        For any DataFrame processed by add_lineage, validate_lineage_completeness
        SHALL return True.
        """
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df, 
            source_tables=source_tables, 
            job_id=job_id
        )
        
        assert LineageTracker.validate_lineage_completeness(result), (
            "validate_lineage_completeness should return True for processed DataFrame"
        )


# =============================================================================
# Additional Unit Tests for Edge Cases
# =============================================================================

class TestLineageTrackerEdgeCases:
    """Unit tests for edge cases and error handling."""

    def test_empty_dataframe_gets_lineage_columns(self):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.1, 4.2, 4.3, 4.4**
        
        Empty DataFrames SHALL still get lineage columns added.
        """
        df = pd.DataFrame({'col1': []})
        tracker = LineageTracker()
        
        result = tracker.add_lineage(
            df,
            source_tables=['test_table'],
            job_id='test_job'
        )
        
        # All lineage columns should be present
        assert LineageTracker.has_lineage_columns(result)
        assert len(result) == 0

    def test_empty_source_tables_raises_error(self):
        """Empty source_tables list SHALL raise LineageError."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        tracker = LineageTracker()
        
        with pytest.raises(LineageError) as exc_info:
            tracker.add_lineage(
                df,
                source_tables=[],
                job_id='test_job'
            )
        
        assert "empty" in str(exc_info.value).lower()

    def test_none_source_tables_raises_error(self):
        """None source_tables SHALL raise LineageError."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        tracker = LineageTracker()
        
        with pytest.raises(LineageError) as exc_info:
            tracker.add_lineage(
                df,
                source_tables=None,
                job_id='test_job'
            )
        
        assert "None" in str(exc_info.value) or "none" in str(exc_info.value).lower()

    def test_empty_job_id_raises_error(self):
        """Empty job_id SHALL raise LineageError."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        tracker = LineageTracker()
        
        with pytest.raises(LineageError) as exc_info:
            tracker.add_lineage(
                df,
                source_tables=['test_table'],
                job_id=''
            )
        
        assert "empty" in str(exc_info.value).lower()

    def test_whitespace_job_id_raises_error(self):
        """Whitespace-only job_id SHALL raise LineageError."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        tracker = LineageTracker()
        
        with pytest.raises(LineageError) as exc_info:
            tracker.add_lineage(
                df,
                source_tables=['test_table'],
                job_id='   '
            )
        
        assert "empty" in str(exc_info.value).lower()

    def test_none_job_id_raises_error(self):
        """None job_id SHALL raise LineageError."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        tracker = LineageTracker()
        
        with pytest.raises(LineageError) as exc_info:
            tracker.add_lineage(
                df,
                source_tables=['test_table'],
                job_id=None
            )
        
        assert "None" in str(exc_info.value) or "none" in str(exc_info.value).lower()

    def test_multiple_source_tables_comma_separated(self):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.1**
        
        Multiple source tables SHALL be comma-separated in _source_table.
        """
        df = pd.DataFrame({'col1': [1, 2, 3]})
        tracker = LineageTracker()
        
        result = tracker.add_lineage(
            df,
            source_tables=['table1', 'table2', 'table3'],
            job_id='test_job'
        )
        
        source_value = result[LineageTracker.SOURCE_TABLE_COL].iloc[0]
        assert source_value == 'table1,table2,table3', (
            f"Expected 'table1,table2,table3', got '{source_value}'"
        )

    def test_generate_job_id_is_unique(self):
        """generate_job_id SHALL produce unique IDs."""
        ids = [LineageTracker.generate_job_id() for _ in range(100)]
        assert len(set(ids)) == 100, "Generated job IDs should be unique"

    def test_generate_job_id_with_prefix(self):
        """generate_job_id SHALL include the provided prefix."""
        job_id = LineageTracker.generate_job_id(prefix='custom')
        assert job_id.startswith('custom_'), (
            f"Job ID should start with 'custom_', got '{job_id}'"
        )

    def test_has_lineage_columns_false_for_missing(self):
        """has_lineage_columns SHALL return False when columns are missing."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        assert not LineageTracker.has_lineage_columns(df)

    def test_get_missing_lineage_columns(self):
        """get_missing_lineage_columns SHALL return all missing column names."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        missing = LineageTracker.get_missing_lineage_columns(df)
        assert set(missing) == set(LineageTracker.LINEAGE_COLUMNS)

    def test_validate_lineage_completeness_false_for_nulls(self):
        """validate_lineage_completeness SHALL return False when nulls exist."""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            '_source_table': ['table1', None, 'table1'],
            '_processed_at': [datetime.now(timezone.utc)] * 3,
            '_data_version': ['v1'] * 3,
            '_ingest_job_id': ['job1'] * 3,
        })
        assert not LineageTracker.validate_lineage_completeness(df)

    def test_existing_lineage_columns_overwritten(self):
        """
        **Feature: processors-data-layering, Property 12: Lineage metadata completeness**
        **Validates: Requirements 4.5**
        
        Existing lineage columns SHALL be overwritten with new values.
        """
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            '_source_table': ['old_table'] * 3,
            '_processed_at': [datetime(2020, 1, 1, tzinfo=timezone.utc)] * 3,
            '_data_version': ['old_version'] * 3,
            '_ingest_job_id': ['old_job'] * 3,
        })
        
        tracker = LineageTracker()
        result = tracker.add_lineage(
            df,
            source_tables=['new_table'],
            job_id='new_job',
            data_version='new_version'
        )
        
        # Values should be updated
        assert result[LineageTracker.SOURCE_TABLE_COL].iloc[0] == 'new_table'
        assert result[LineageTracker.DATA_VERSION_COL].iloc[0] == 'new_version'
        assert result[LineageTracker.INGEST_JOB_ID_COL].iloc[0] == 'new_job'
