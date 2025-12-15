#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for MaterializedViewMonitor.

Uses hypothesis library for property-based testing.

**Feature: materialized-views-system, Property 6: Metadata completeness**
**Validates: Requirements 6.1, 6.2, 6.3**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime
import json
from typing import Dict, Any, List

from alphahome.processors.materialized_views import MaterializedViewMonitor


# =============================================================================
# Custom Strategies for MaterializedViewMonitor
# =============================================================================

def view_name_strategy():
    """Generate valid materialized view names."""
    return st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz_',
        min_size=1,
        max_size=50
    ).filter(lambda x: not x.startswith('_'))


def refresh_result_strategy():
    """Generate valid refresh result dictionaries."""
    return st.fixed_dictionaries({
        'status': st.sampled_from(['success', 'failed']),
        'refresh_time': st.just(datetime.now()),
        'duration_seconds': st.floats(min_value=0.0, max_value=3600.0),
        'row_count': st.integers(min_value=0, max_value=10000000),
        'error_message': st.one_of(st.none(), st.text(max_size=500)),
        'view_schema': st.just('materialized_views'),
        'source_tables': st.lists(
            st.text(alphabet='abcdefghijklmnopqrstuvwxyz_', min_size=1, max_size=30),
            min_size=0,
            max_size=5
        ),
        'refresh_strategy': st.sampled_from(['full', 'concurrent']),
    })


def quality_check_strategy():
    """Generate valid quality check dictionaries."""
    return st.fixed_dictionaries({
        'check_name': st.sampled_from(['null_check', 'outlier_check', 'row_count_change', 'duplicate_check', 'type_check']),
        'check_status': st.sampled_from(['pass', 'warning', 'error']),
        'check_message': st.text(max_size=500),
        'check_details': st.one_of(
            st.none(),
            st.dictionaries(
                keys=st.text(alphabet='abcdefghijklmnopqrstuvwxyz_', min_size=1, max_size=20),
                values=st.one_of(st.text(), st.integers(), st.floats()),
                max_size=5
            )
        ),
    })


# =============================================================================
# Property 6: Metadata completeness
# **Feature: materialized-views-system, Property 6: Metadata completeness**
# **Validates: Requirements 6.1, 6.2, 6.3**
# =============================================================================

class TestProperty6MetadataCompleteness:
    """
    Property 6: Metadata completeness
    
    *For any* materialized view refresh, the refresh metadata should include:
    - view_name
    - refresh_time
    - refresh_status
    - row_count
    - error_message (if failed)
    
    **Feature: materialized-views-system, Property 6: Metadata completeness**
    **Validates: Requirements 6.1, 6.2, 6.3**
    """
    
    @pytest.mark.asyncio
    async def test_record_refresh_metadata_stores_all_required_fields(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1, 6.2**
        
        For any refresh result, record_refresh_metadata() SHALL store all required fields:
        - view_name
        - view_schema
        - refresh_status
        - last_refresh_time
        - row_count
        - refresh_duration_seconds
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        monitor = MaterializedViewMonitor(mock_db)
        
        # Create test refresh result
        refresh_result = {
            'status': 'success',
            'refresh_time': datetime.now(),
            'duration_seconds': 10.5,
            'row_count': 1000,
            'error_message': None,
            'view_schema': 'materialized_views',
            'source_tables': ['rawdata.table1', 'rawdata.table2'],
            'refresh_strategy': 'full',
        }
        
        # Record metadata
        await monitor.record_refresh_metadata('test_mv', refresh_result)
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1:]
        
        # Verify all required fields are in the SQL
        assert 'view_name' in sql, "SQL should include view_name"
        assert 'view_schema' in sql, "SQL should include view_schema"
        assert 'refresh_status' in sql, "SQL should include refresh_status"
        assert 'last_refresh_time' in sql, "SQL should include last_refresh_time"
        assert 'row_count' in sql, "SQL should include row_count"
        assert 'refresh_duration_seconds' in sql, "SQL should include refresh_duration_seconds"
        
        # Verify parameters are passed correctly
        assert params[0] == 'test_mv', "First parameter should be view_name"
        assert params[1] == 'materialized_views', "Second parameter should be view_schema"
        assert params[5] == 'success', "Status parameter should be 'success'"
        assert params[6] == 1000, "Row count parameter should be 1000"

    @pytest.mark.asyncio
    async def test_record_refresh_metadata_handles_failed_status(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.2, 6.3**
        
        For any failed refresh, record_refresh_metadata() SHALL include error_message.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        monitor = MaterializedViewMonitor(mock_db)
        
        # Create test refresh result with failure
        refresh_result = {
            'status': 'failed',
            'refresh_time': datetime.now(),
            'duration_seconds': 5.0,
            'row_count': 0,
            'error_message': 'Connection timeout',
            'view_schema': 'materialized_views',
            'source_tables': [],
            'refresh_strategy': 'full',
        }
        
        # Record metadata
        await monitor.record_refresh_metadata('test_mv_failed', refresh_result)
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify error_message is included
        assert params[8] == 'Connection timeout', "Error message should be included"

    @pytest.mark.asyncio
    async def test_get_refresh_history_returns_list_of_records(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1, 6.2**
        
        For any view_name, get_refresh_history() SHALL return a list of refresh records.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        
        # Mock fetch to return test data
        mock_rows = [
            {
                'view_name': 'test_mv',
                'view_schema': 'materialized_views',
                'source_tables': '["rawdata.table1"]',
                'refresh_strategy': 'full',
                'last_refresh_time': datetime.now(),
                'refresh_status': 'success',
                'row_count': 1000,
                'refresh_duration_seconds': 10.5,
                'error_message': None,
            }
        ]
        mock_db.fetch = AsyncMock(return_value=mock_rows)
        
        monitor = MaterializedViewMonitor(mock_db)
        
        # Get refresh history
        history = await monitor.get_refresh_history('test_mv', limit=10)
        
        # Verify fetch was called
        assert mock_db.fetch.called, "fetch should be called"
        
        # Verify history is a list
        assert isinstance(history, list), "History should be a list"
        assert len(history) == 1, "History should have 1 record"
        
        # Verify record structure
        record = history[0]
        assert 'view_name' in record, "Record should have view_name"
        assert 'refresh_status' in record, "Record should have refresh_status"
        assert 'row_count' in record, "Record should have row_count"
        assert 'refresh_duration_seconds' in record, "Record should have refresh_duration_seconds"

    @pytest.mark.asyncio
    async def test_record_quality_check_stores_check_details(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1, 6.2**
        
        For any quality check, record_quality_check() SHALL store check details.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        monitor = MaterializedViewMonitor(mock_db)
        
        # Create test quality check
        check_details = {
            'null_count': 10,
            'null_percentage': 0.01,
            'threshold': 0.05,
        }
        
        # Record quality check
        await monitor.record_quality_check(
            'test_mv',
            'null_check',
            'pass',
            'Null check passed',
            check_details
        )
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1:]
        
        # Verify all required fields are in the SQL
        assert 'view_name' in sql, "SQL should include view_name"
        assert 'check_name' in sql, "SQL should include check_name"
        assert 'check_status' in sql, "SQL should include check_status"
        assert 'check_message' in sql, "SQL should include check_message"
        assert 'check_details' in sql, "SQL should include check_details"

    @pytest.mark.asyncio
    async def test_get_quality_check_history_returns_list_of_checks(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1, 6.2**
        
        For any view_name, get_quality_check_history() SHALL return a list of quality checks.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        
        # Mock fetch to return test data
        mock_rows = [
            {
                'id': 1,
                'view_name': 'test_mv',
                'check_name': 'null_check',
                'check_status': 'pass',
                'check_message': 'Null check passed',
                'check_details': '{"null_count": 0}',
                'checked_at': datetime.now(),
            }
        ]
        mock_db.fetch = AsyncMock(return_value=mock_rows)
        
        monitor = MaterializedViewMonitor(mock_db)
        
        # Get quality check history
        history = await monitor.get_quality_check_history('test_mv', limit=10)
        
        # Verify fetch was called
        assert mock_db.fetch.called, "fetch should be called"
        
        # Verify history is a list
        assert isinstance(history, list), "History should be a list"
        assert len(history) == 1, "History should have 1 record"
        
        # Verify record structure
        record = history[0]
        assert 'id' in record, "Record should have id"
        assert 'check_name' in record, "Record should have check_name"
        assert 'check_status' in record, "Record should have check_status"
        assert 'check_message' in record, "Record should have check_message"
        assert 'check_details' in record, "Record should have check_details"

    @pytest.mark.asyncio
    async def test_get_latest_refresh_status_returns_complete_status(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1, 6.2, 6.3**
        
        For any view_name, get_latest_refresh_status() SHALL return complete status information.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        
        # Mock fetch_one to return test data
        mock_row = {
            'view_name': 'test_mv',
            'view_schema': 'materialized_views',
            'source_tables': '["rawdata.table1"]',
            'refresh_strategy': 'full',
            'last_refresh_time': datetime.now(),
            'refresh_status': 'success',
            'row_count': 1000,
            'refresh_duration_seconds': 10.5,
            'error_message': None,
        }
        mock_db.fetch_one = AsyncMock(return_value=mock_row)
        
        monitor = MaterializedViewMonitor(mock_db)
        
        # Get latest refresh status
        status = await monitor.get_latest_refresh_status('test_mv')
        
        # Verify fetch_one was called
        assert mock_db.fetch_one.called, "fetch_one should be called"
        
        # Verify status is a dictionary
        assert isinstance(status, dict), "Status should be a dictionary"
        
        # Verify all required fields are present
        assert 'view_name' in status, "Status should have view_name"
        assert 'view_schema' in status, "Status should have view_schema"
        assert 'refresh_status' in status, "Status should have refresh_status"
        assert 'last_refresh_time' in status, "Status should have last_refresh_time"
        assert 'row_count' in status, "Status should have row_count"
        assert 'refresh_duration_seconds' in status, "Status should have refresh_duration_seconds"
        assert 'error_message' in status, "Status should have error_message"

    @pytest.mark.asyncio
    async def test_get_latest_refresh_status_returns_none_if_no_record(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1**
        
        For any view_name with no refresh history, get_latest_refresh_status() SHALL return None.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        mock_db.fetch_one = AsyncMock(return_value=None)
        
        monitor = MaterializedViewMonitor(mock_db)
        
        # Get latest refresh status
        status = await monitor.get_latest_refresh_status('nonexistent_mv')
        
        # Verify fetch_one was called
        assert mock_db.fetch_one.called, "fetch_one should be called"
        
        # Verify status is None
        assert status is None, "Status should be None for nonexistent view"

    @pytest.mark.asyncio
    async def test_record_refresh_metadata_converts_source_tables_to_json(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1, 6.2**
        
        For any refresh result with source_tables list, record_refresh_metadata() 
        SHALL convert it to JSON string.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        monitor = MaterializedViewMonitor(mock_db)
        
        # Create test refresh result
        source_tables = ['rawdata.table1', 'rawdata.table2', 'rawdata.table3']
        refresh_result = {
            'status': 'success',
            'refresh_time': datetime.now(),
            'duration_seconds': 10.5,
            'row_count': 1000,
            'error_message': None,
            'view_schema': 'materialized_views',
            'source_tables': source_tables,
            'refresh_strategy': 'full',
        }
        
        # Record metadata
        await monitor.record_refresh_metadata('test_mv', refresh_result)
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify source_tables is JSON string
        source_tables_json = params[2]
        assert isinstance(source_tables_json, str), "source_tables should be JSON string"
        
        # Verify JSON can be parsed back
        parsed = json.loads(source_tables_json)
        assert parsed == source_tables, "Parsed JSON should match original list"

    @pytest.mark.asyncio
    async def test_record_quality_check_converts_details_to_json(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1, 6.2**
        
        For any quality check with details dict, record_quality_check() 
        SHALL convert it to JSON string.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        monitor = MaterializedViewMonitor(mock_db)
        
        # Create test quality check
        check_details = {
            'null_count': 10,
            'null_percentage': 0.01,
            'threshold': 0.05,
            'columns': ['col1', 'col2'],
        }
        
        # Record quality check
        await monitor.record_quality_check(
            'test_mv',
            'null_check',
            'pass',
            'Null check passed',
            check_details
        )
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify check_details is JSON string
        check_details_json = params[4]
        assert isinstance(check_details_json, str), "check_details should be JSON string"
        
        # Verify JSON can be parsed back
        parsed = json.loads(check_details_json)
        assert parsed == check_details, "Parsed JSON should match original dict"

    @pytest.mark.asyncio
    async def test_monitor_handles_empty_source_tables(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1**
        
        For any refresh result with empty source_tables, record_refresh_metadata() 
        SHALL handle it gracefully.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        monitor = MaterializedViewMonitor(mock_db)
        
        # Create test refresh result with empty source_tables
        refresh_result = {
            'status': 'success',
            'refresh_time': datetime.now(),
            'duration_seconds': 10.5,
            'row_count': 1000,
            'error_message': None,
            'view_schema': 'materialized_views',
            'source_tables': [],
            'refresh_strategy': 'full',
        }
        
        # Record metadata
        await monitor.record_refresh_metadata('test_mv', refresh_result)
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify source_tables is empty JSON array
        source_tables_json = params[2]
        parsed = json.loads(source_tables_json)
        assert parsed == [], "Parsed JSON should be empty list"

    @pytest.mark.asyncio
    async def test_monitor_handles_none_check_details(self):
        """
        **Feature: materialized-views-system, Property 6: Metadata completeness**
        **Validates: Requirements 6.1**
        
        For any quality check with None details, record_quality_check() 
        SHALL handle it gracefully.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        monitor = MaterializedViewMonitor(mock_db)
        
        # Record quality check with None details
        await monitor.record_quality_check(
            'test_mv',
            'null_check',
            'pass',
            'Null check passed',
            None
        )
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify check_details is empty JSON object
        check_details_json = params[4]
        parsed = json.loads(check_details_json)
        assert parsed == {}, "Parsed JSON should be empty dict"

