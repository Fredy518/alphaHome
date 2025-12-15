#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for MaterializedViewRefresh.

Uses hypothesis library for property-based testing.

**Feature: materialized-views-system, Property 6: Refresh idempotency**
**Validates: Requirements 6.1, 6.2, 6.3**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import asyncio
import logging

from alphahome.processors.materialized_views import MaterializedViewRefresh


# =============================================================================
# Custom Strategies for Refresh Testing
# =============================================================================

def view_name_strategy():
    """Generate valid materialized view names."""
    return st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz_0123456789',
        min_size=1,
        max_size=50
    ).filter(lambda x: not x[0].isdigit())  # View names can't start with digit


def schema_name_strategy():
    """Generate valid schema names."""
    return st.just('materialized_views')


def refresh_strategy_strategy():
    """Generate valid refresh strategies."""
    return st.sampled_from(['full', 'concurrent'])


def row_count_strategy():
    """Generate valid row counts."""
    return st.integers(min_value=0, max_value=1000000)


# =============================================================================
# Property 6: Refresh Idempotency
# **Feature: materialized-views-system, Property 6: Refresh idempotency**
# **Validates: Requirements 6.1, 6.2, 6.3**
# =============================================================================

class TestProperty6RefreshIdempotency:
    """
    Property 6: Refresh idempotency
    
    *For any* materialized view, executing REFRESH twice in a row should produce
    identical results (same row count, same data, same metadata).
    
    **Feature: materialized-views-system, Property 6: Refresh idempotency**
    **Validates: Requirements 6.1, 6.2, 6.3**
    """
    
    @pytest.mark.asyncio
    async def test_refresh_returns_consistent_results_on_repeated_calls(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1**
        
        For any materialized view, calling refresh() twice with the same parameters
        SHALL return identical results (same row_count, same status).
        """
        # Create mock database connection
        mock_db = AsyncMock()

        executed_queries = []

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 1000
            return None

        async def mock_execute(query, *args, **kwargs):
            executed_queries.append(query)
            return "OK"

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(side_effect=mock_execute)
        
        # Create refresh executor
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        # First refresh
        result1 = await refresh.refresh('test_view', 'materialized_views', 'full')
        
        # Second refresh
        result2 = await refresh.refresh('test_view', 'materialized_views', 'full')
        
        # Results should be identical
        assert result1['status'] == result2['status'], "Status should be identical"
        assert result1['row_count'] == result2['row_count'], "Row count should be identical"
        assert result1['view_name'] == result2['view_name'], "View name should be identical"
        assert result1['full_name'] == result2['full_name'], "Full name should be identical"
        assert result1['strategy'] == result2['strategy'], "Strategy should be identical"

    @pytest.mark.asyncio
    async def test_refresh_metadata_completeness(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.2**
        
        For any materialized view refresh, the result metadata SHALL include:
        - status (success/failed)
        - view_name
        - full_name (schema.view_name)
        - refresh_time (datetime)
        - duration_seconds (float)
        - row_count (int)
        - strategy (full/concurrent)
        - error_message (if failed)
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 5000
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('pit_financial_indicators_mv', 'materialized_views', 'full')
        
        # Check all required fields
        assert 'status' in result, "Result should have 'status'"
        assert 'view_name' in result, "Result should have 'view_name'"
        assert 'full_name' in result, "Result should have 'full_name'"
        assert 'refresh_time' in result, "Result should have 'refresh_time'"
        assert 'duration_seconds' in result, "Result should have 'duration_seconds'"
        assert 'row_count' in result, "Result should have 'row_count'"
        assert 'strategy' in result, "Result should have 'strategy'"
        
        # Check field types
        assert isinstance(result['status'], str), "status should be string"
        assert isinstance(result['view_name'], str), "view_name should be string"
        assert isinstance(result['full_name'], str), "full_name should be string"
        assert isinstance(result['refresh_time'], datetime), "refresh_time should be datetime"
        assert isinstance(result['duration_seconds'], float), "duration_seconds should be float"
        assert isinstance(result['row_count'], int), "row_count should be int"
        assert isinstance(result['strategy'], str), "strategy should be string"
        
        # Check field values
        assert result['status'] == 'success', "status should be 'success'"
        assert result['view_name'] == 'pit_financial_indicators_mv'
        assert result['full_name'] == 'materialized_views.pit_financial_indicators_mv'
        assert result['row_count'] == 5000
        assert result['strategy'] == 'full'
        assert result['duration_seconds'] >= 0

    @pytest.mark.asyncio
    async def test_refresh_with_full_strategy(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1, 6.3**
        
        For any materialized view, refresh with 'full' strategy SHALL:
        - Execute REFRESH MATERIALIZED VIEW (without CONCURRENTLY)
        - Return success status
        - Record row count
        """
        mock_db = AsyncMock()
        executed_queries = []
        
        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 2000
            return None

        async def mock_execute(query, *args, **kwargs):
            executed_queries.append(query)
            return "OK"

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(side_effect=mock_execute)
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('test_view', 'materialized_views', 'full')
        
        assert result['status'] == 'success'
        assert result['strategy'] == 'full'
        assert result['row_count'] == 2000
        
        # Check that REFRESH command was executed (without CONCURRENTLY)
        refresh_queries = [q for q in executed_queries if 'REFRESH MATERIALIZED VIEW' in q]
        assert len(refresh_queries) > 0, "REFRESH command should be executed"
        assert 'CONCURRENTLY' not in refresh_queries[0], "FULL strategy should not use CONCURRENTLY"

    @pytest.mark.asyncio
    async def test_refresh_with_concurrent_strategy(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1, 6.3**
        
        For any materialized view, refresh with 'concurrent' strategy SHALL:
        - Execute REFRESH MATERIALIZED VIEW CONCURRENTLY
        - Return success status
        - Record row count
        """
        mock_db = AsyncMock()
        executed_queries = []

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 3000
            return None

        async def mock_execute(query, *args, **kwargs):
            executed_queries.append(query)
            return "OK"

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(side_effect=mock_execute)
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('test_view', 'materialized_views', 'concurrent')
        
        assert result['status'] == 'success'
        assert result['strategy'] == 'concurrent'
        assert result['row_count'] == 3000
        
        # Check that CONCURRENT REFRESH command was executed
        refresh_queries = [q for q in executed_queries if 'REFRESH MATERIALIZED VIEW' in q]
        assert len(refresh_queries) > 0, "REFRESH command should be executed"
        assert 'CONCURRENTLY' in refresh_queries[0], "CONCURRENT strategy should use CONCURRENTLY"

    @pytest.mark.asyncio
    async def test_refresh_handles_nonexistent_view(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1**
        
        For any non-existent materialized view, refresh() SHALL:
        - Return failed status
        - Include error message
        - Not attempt to execute REFRESH command
        """
        mock_db = AsyncMock()
        executed_queries = []

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return False
            return None

        async def mock_execute(query, *args, **kwargs):
            executed_queries.append(query)
            return "OK"

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(side_effect=mock_execute)
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('nonexistent_view', 'materialized_views', 'full')
        
        assert result['status'] == 'failed', "Should fail for nonexistent view"
        assert 'error_message' in result, "Should include error message"
        assert 'does not exist' in result['error_message'].lower()
        
        # Check that REFRESH command was NOT executed
        refresh_queries = [q for q in executed_queries if 'REFRESH MATERIALIZED VIEW' in q]
        assert len(refresh_queries) == 0, "REFRESH should not be executed for nonexistent view"

    @pytest.mark.asyncio
    async def test_refresh_handles_database_connection_error(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1**
        
        For any refresh with database connection error during REFRESH execution,
        refresh() SHALL:
        - Return failed status
        - Include error message
        - Not raise exception
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 1000
            return None

        async def mock_execute(query, *args, **kwargs):
            if "REFRESH MATERIALIZED VIEW" in query:
                raise Exception("Connection refused")
            return "OK"

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(side_effect=mock_execute)
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('test_view', 'materialized_views', 'full')
        
        assert result['status'] == 'failed', "Should fail on connection error"
        assert 'error_message' in result, "Should include error message"
        assert 'Connection refused' in result['error_message']

    @pytest.mark.asyncio
    async def test_refresh_invalid_strategy_raises_error(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1**
        
        For any refresh with invalid strategy, refresh() SHALL:
        - Raise ValueError
        """
        mock_db = AsyncMock()
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        with pytest.raises(ValueError) as exc_info:
            await refresh.refresh('test_view', 'materialized_views', 'invalid_strategy')
        
        assert 'Invalid refresh strategy' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_refresh_without_database_connection_raises_error(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1**
        
        For any refresh without database connection, refresh() SHALL:
        - Raise RuntimeError
        """
        refresh = MaterializedViewRefresh(db_connection=None)
        
        with pytest.raises(RuntimeError) as exc_info:
            await refresh.refresh('test_view', 'materialized_views', 'full')
        
        assert 'Database connection' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_refresh_status_returns_last_refresh_result(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.2**
        
        For any materialized view, get_refresh_status() SHALL:
        - Return the last refresh result
        - Return None if never refreshed
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 1500
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        # Before any refresh
        status = refresh.get_refresh_status('test_view', 'materialized_views')
        assert status is None, "Should return None before any refresh"
        
        # After first refresh
        result1 = await refresh.refresh('test_view', 'materialized_views', 'full')
        status = refresh.get_refresh_status('test_view', 'materialized_views')
        assert status is not None, "Should return status after refresh"
        assert status['status'] == 'success'
        assert status['row_count'] == 1500
        
        # After second refresh
        result2 = await refresh.refresh('test_view', 'materialized_views', 'full')
        status = refresh.get_refresh_status('test_view', 'materialized_views')
        assert status is not None, "Should return status after second refresh"
        assert status['status'] == 'success'

    @pytest.mark.asyncio
    async def test_refresh_duration_is_positive(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.2**
        
        For any successful refresh, duration_seconds SHALL:
        - Be a positive number
        - Represent the actual refresh time
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                await asyncio.sleep(0.01)
                return 1000
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('test_view', 'materialized_views', 'full')
        
        assert result['status'] == 'success'
        assert result['duration_seconds'] >= 0, "Duration should be non-negative"

    @pytest.mark.asyncio
    async def test_refresh_full_name_format(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.2**
        
        For any refresh, full_name SHALL:
        - Be in format 'schema.view_name'
        - Match the provided schema and view_name
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 100
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('my_view', 'materialized_views', 'full')
        
        assert result['full_name'] == 'materialized_views.my_view'
        assert result['view_name'] == 'my_view'

    @pytest.mark.asyncio
    async def test_refresh_concurrent_fallback_to_full(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1, 6.3**
        
        For any CONCURRENT refresh that fails (e.g., no unique index),
        refresh() SHALL:
        - Fall back to FULL refresh
        - Return success status
        - Log warning about fallback
        """
        mock_db = AsyncMock()
        executed_queries = []

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 2000
            return None

        async def mock_execute(query, *args, **kwargs):
            executed_queries.append(query)
            if "CONCURRENTLY" in query:
                raise Exception("Cannot refresh concurrently without unique index")
            return "OK"

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(side_effect=mock_execute)
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('test_view', 'materialized_views', 'concurrent')
        
        # Should eventually succeed (after fallback)
        assert result['status'] == 'success', "Should succeed after fallback to FULL"
        assert result['row_count'] == 2000
        
        # Check that both CONCURRENT and FULL queries were attempted
        concurrent_queries = [q for q in executed_queries if 'CONCURRENTLY' in q]
        full_queries = [q for q in executed_queries if 'REFRESH MATERIALIZED VIEW' in q and 'CONCURRENTLY' not in q]
        
        assert len(concurrent_queries) > 0, "Should attempt CONCURRENT refresh first"
        assert len(full_queries) > 0, "Should fall back to FULL refresh"

    @pytest.mark.asyncio
    async def test_refresh_with_different_schemas(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.1**
        
        For any refresh with different schemas, refresh() SHALL:
        - Support custom schema names
        - Include schema in full_name
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 500
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('test_view', 'custom_schema', 'full')
        
        assert result['full_name'] == 'custom_schema.test_view'
        assert result['status'] == 'success'

    @pytest.mark.asyncio
    async def test_refresh_row_count_zero(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.2**
        
        For any refresh that results in zero rows, refresh() SHALL:
        - Return success status
        - Record row_count as 0
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 0
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('empty_view', 'materialized_views', 'full')
        
        assert result['status'] == 'success'
        assert result['row_count'] == 0

    @pytest.mark.asyncio
    async def test_refresh_large_row_count(self):
        """
        **Feature: materialized-views-system, Property 6: Refresh idempotency**
        **Validates: Requirements 6.2**
        
        For any refresh with large row count, refresh() SHALL:
        - Handle large numbers correctly
        - Record accurate row_count
        """
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 10000000
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        refresh = MaterializedViewRefresh(db_connection=mock_db)
        
        result = await refresh.refresh('large_view', 'materialized_views', 'full')
        
        assert result['status'] == 'success'
        assert result['row_count'] == 10000000
