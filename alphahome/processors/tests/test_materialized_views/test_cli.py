#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests for materialized view CLI commands.

Tests the CLI interface for:
- Refreshing single materialized views
- Refreshing all materialized views
- Viewing refresh status and history
- Specifying refresh strategies (full/concurrent)

Requirements: 6.1, 6.2, 6.3
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
import json

from alphahome.processors.materialized_views.cli import (
    refresh_materialized_view,
    refresh_all_materialized_views,
    get_materialized_view_status,
    get_all_materialized_views_status,
    get_materialized_view_history,
    format_result,
)


# =============================================================================
# Test: refresh_materialized_view
# =============================================================================

class TestRefreshMaterializedView:
    """Tests for refresh_materialized_view CLI function"""
    
    @pytest.mark.asyncio
    async def test_refresh_single_view_success(self):
        """
        Test refreshing a single materialized view successfully.
        
        Requirements: 6.1, 6.2, 6.3
        """
        mock_db = AsyncMock()
        
        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 1000
            return None
        
        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        result = await refresh_materialized_view(
            view_name='test_view',
            schema='materialized_views',
            strategy='full',
            db_connection=mock_db,
        )
        
        assert result['status'] == 'success'
        assert result['view_name'] == 'test_view'
        assert result['full_name'] == 'materialized_views.test_view'
        assert result['row_count'] == 1000
        assert result['strategy'] == 'full'
        assert 'refresh_time' in result
        assert 'duration_seconds' in result

    @pytest.mark.asyncio
    async def test_refresh_single_view_with_concurrent_strategy(self):
        """
        Test refreshing a single materialized view with concurrent strategy.
        
        Requirements: 6.1, 6.3
        """
        mock_db = AsyncMock()
        
        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 2000
            return None
        
        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        result = await refresh_materialized_view(
            view_name='test_view',
            schema='materialized_views',
            strategy='concurrent',
            db_connection=mock_db,
        )
        
        assert result['status'] == 'success'
        assert result['strategy'] == 'concurrent'
        assert result['row_count'] == 2000

    @pytest.mark.asyncio
    async def test_refresh_single_view_invalid_strategy(self):
        """
        Test that invalid refresh strategy raises ValueError.
        
        Requirements: 6.1
        """
        mock_db = AsyncMock()
        
        with pytest.raises(ValueError) as exc_info:
            await refresh_materialized_view(
                view_name='test_view',
                schema='materialized_views',
                strategy='invalid',
                db_connection=mock_db,
            )
        
        assert 'Invalid refresh strategy' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_refresh_single_view_no_database_connection(self):
        """
        Test that missing database connection raises ValueError.
        
        Requirements: 6.1
        """
        with pytest.raises(ValueError) as exc_info:
            await refresh_materialized_view(
                view_name='test_view',
                schema='materialized_views',
                strategy='full',
                db_connection=None,
            )
        
        assert 'Database connection' in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_refresh_single_view_failure(self):
        """
        Test refreshing a single materialized view that fails.
        
        Requirements: 6.1, 6.2
        """
        mock_db = AsyncMock()
        
        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return False
            return None
        
        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        result = await refresh_materialized_view(
            view_name='nonexistent_view',
            schema='materialized_views',
            strategy='full',
            db_connection=mock_db,
        )
        
        assert result['status'] == 'failed'
        assert 'error_message' in result
        assert 'does not exist' in result['error_message'].lower()


# =============================================================================
# Test: refresh_all_materialized_views
# =============================================================================

class TestRefreshAllMaterializedViews:
    """Tests for refresh_all_materialized_views CLI function"""
    
    @pytest.mark.asyncio
    async def test_refresh_all_views_success(self):
        """
        Test refreshing all materialized views successfully.
        
        Requirements: 6.1, 6.2, 6.3
        """
        mock_db = AsyncMock()
        
        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 1000
            return None
        
        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        result = await refresh_all_materialized_views(
            strategy='full',
            db_connection=mock_db,
        )
        
        assert result['status'] == 'success'
        assert result['total'] > 0
        assert result['succeeded'] == result['total']
        assert result['failed'] == 0
        assert len(result['results']) == result['total']
        
        # Check that all results are successful
        for r in result['results']:
            assert r['status'] == 'success'
            assert 'view_name' in r
            assert 'row_count' in r
            assert 'duration_seconds' in r

    @pytest.mark.asyncio
    async def test_refresh_all_views_partial_success(self):
        """
        Test refreshing all materialized views with some failures.
        
        Requirements: 6.1, 6.2
        """
        mock_db = AsyncMock()
        call_count = [0]
        
        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                # First view exists, others don't
                call_count[0] += 1
                return call_count[0] == 1
            if "COUNT(*)" in query:
                return 1000
            return None
        
        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        result = await refresh_all_materialized_views(
            strategy='full',
            db_connection=mock_db,
        )
        
        assert result['status'] == 'partial_success'
        assert result['succeeded'] > 0
        assert result['failed'] > 0
        assert result['succeeded'] + result['failed'] == result['total']

    @pytest.mark.asyncio
    async def test_refresh_all_views_all_failures(self):
        """
        Test refreshing all materialized views with all failures.
        
        Requirements: 6.1, 6.2
        """
        mock_db = AsyncMock()
        
        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return False
            return None
        
        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        
        result = await refresh_all_materialized_views(
            strategy='full',
            db_connection=mock_db,
        )
        
        assert result['status'] == 'failed'
        assert result['succeeded'] == 0
        assert result['failed'] == result['total']

    @pytest.mark.asyncio
    async def test_refresh_all_views_no_database_connection(self):
        """
        Test that missing database connection raises ValueError.
        
        Requirements: 6.1
        """
        with pytest.raises(ValueError) as exc_info:
            await refresh_all_materialized_views(
                strategy='full',
                db_connection=None,
            )
        
        assert 'Database connection' in str(exc_info.value)


# =============================================================================
# Test: get_materialized_view_status
# =============================================================================

class TestGetMaterializedViewStatus:
    """Tests for get_materialized_view_status CLI function"""
    
    @pytest.mark.asyncio
    async def test_get_status_with_refresh_history(self):
        """
        Test getting status of a materialized view with refresh history.
        
        Requirements: 6.2
        """
        mock_db = AsyncMock()
        
        # Mock the monitor's get_latest_refresh_status
        with patch(
            'alphahome.processors.materialized_views.cli.MaterializedViewMonitor'
        ) as mock_monitor_class:
            mock_monitor = AsyncMock()
            mock_monitor_class.return_value = mock_monitor
            
            mock_monitor.get_latest_refresh_status = AsyncMock(return_value={
                'last_refresh_time': datetime.now(),
                'refresh_status': 'success',
                'row_count': 5000,
                'refresh_duration_seconds': 10.5,
                'error_message': None,
            })
            
            result = await get_materialized_view_status(
                view_name='test_view',
                db_connection=mock_db,
            )
            
            assert result['view_name'] == 'test_view'
            assert result['refresh_status'] == 'success'
            assert result['row_count'] == 5000
            assert result['refresh_duration_seconds'] == 10.5
            assert result['error_message'] is None

    @pytest.mark.asyncio
    async def test_get_status_never_refreshed(self):
        """
        Test getting status of a materialized view that was never refreshed.
        
        Requirements: 6.2
        """
        mock_db = AsyncMock()
        
        with patch(
            'alphahome.processors.materialized_views.cli.MaterializedViewMonitor'
        ) as mock_monitor_class:
            mock_monitor = AsyncMock()
            mock_monitor_class.return_value = mock_monitor
            
            mock_monitor.get_latest_refresh_status = AsyncMock(return_value=None)
            
            result = await get_materialized_view_status(
                view_name='test_view',
                db_connection=mock_db,
            )
            
            assert result['view_name'] == 'test_view'
            assert result['refresh_status'] == 'never_refreshed'
            assert result['row_count'] == 0
            assert result['last_refresh_time'] is None

    @pytest.mark.asyncio
    async def test_get_status_no_database_connection(self):
        """
        Test that missing database connection raises ValueError.
        
        Requirements: 6.2
        """
        with pytest.raises(ValueError) as exc_info:
            await get_materialized_view_status(
                view_name='test_view',
                db_connection=None,
            )
        
        assert 'Database connection' in str(exc_info.value)


# =============================================================================
# Test: get_all_materialized_views_status
# =============================================================================

class TestGetAllMaterializedViewsStatus:
    """Tests for get_all_materialized_views_status CLI function"""
    
    @pytest.mark.asyncio
    async def test_get_all_status(self):
        """
        Test getting status of all materialized views.
        
        Requirements: 6.2
        """
        mock_db = AsyncMock()
        
        with patch(
            'alphahome.processors.materialized_views.cli.MaterializedViewMonitor'
        ) as mock_monitor_class:
            mock_monitor = AsyncMock()
            mock_monitor_class.return_value = mock_monitor
            
            mock_monitor.get_latest_refresh_status = AsyncMock(return_value={
                'last_refresh_time': datetime.now(),
                'refresh_status': 'success',
                'row_count': 1000,
                'refresh_duration_seconds': 5.0,
                'error_message': None,
            })
            
            result = await get_all_materialized_views_status(
                db_connection=mock_db,
            )
            
            assert 'total' in result
            assert 'views' in result
            assert result['total'] > 0
            assert len(result['views']) == result['total']
            
            # Check that all views have required fields
            for view in result['views']:
                assert 'view_name' in view
                assert 'refresh_status' in view
                assert 'row_count' in view


# =============================================================================
# Test: get_materialized_view_history
# =============================================================================

class TestGetMaterializedViewHistory:
    """Tests for get_materialized_view_history CLI function"""
    
    @pytest.mark.asyncio
    async def test_get_history(self):
        """
        Test getting refresh history of a materialized view.
        
        Requirements: 6.2
        """
        mock_db = AsyncMock()
        
        with patch(
            'alphahome.processors.materialized_views.cli.MaterializedViewMonitor'
        ) as mock_monitor_class:
            mock_monitor = AsyncMock()
            mock_monitor_class.return_value = mock_monitor
            
            now = datetime.now()
            mock_monitor.get_refresh_history = AsyncMock(return_value=[
                {
                    'refresh_time': now,
                    'refresh_status': 'success',
                    'row_count': 1000,
                    'refresh_duration_seconds': 5.0,
                    'error_message': None,
                },
                {
                    'refresh_time': now - timedelta(hours=1),
                    'refresh_status': 'success',
                    'row_count': 1000,
                    'refresh_duration_seconds': 5.0,
                    'error_message': None,
                },
            ])
            
            result = await get_materialized_view_history(
                view_name='test_view',
                limit=10,
                db_connection=mock_db,
            )
            
            assert result['view_name'] == 'test_view'
            assert result['total_records'] == 2
            assert len(result['history']) == 2
            
            # Check that history is in reverse chronological order
            assert result['history'][0]['refresh_time'] >= result['history'][1]['refresh_time']

    @pytest.mark.asyncio
    async def test_get_history_with_limit(self):
        """
        Test getting refresh history with limit parameter.
        
        Requirements: 6.2
        """
        mock_db = AsyncMock()
        
        with patch(
            'alphahome.processors.materialized_views.cli.MaterializedViewMonitor'
        ) as mock_monitor_class:
            mock_monitor = AsyncMock()
            mock_monitor_class.return_value = mock_monitor
            
            mock_monitor.get_refresh_history = AsyncMock(return_value=[])
            
            result = await get_materialized_view_history(
                view_name='test_view',
                limit=5,
                db_connection=mock_db,
            )
            
            # Verify that limit was passed to monitor
            mock_monitor.get_refresh_history.assert_called_once_with('test_view', limit=5)


# =============================================================================
# Test: format_result
# =============================================================================

class TestFormatResult:
    """Tests for format_result function"""
    
    def test_format_single_refresh_result_text(self):
        """Test formatting single refresh result as text"""
        result = {
            'status': 'success',
            'view_name': 'test_view',
            'full_name': 'materialized_views.test_view',
            'refresh_time': datetime.now(),
            'duration_seconds': 5.0,
            'row_count': 1000,
        }
        
        formatted = format_result(result, format='text')
        
        assert 'Refresh successful' in formatted
        assert 'test_view' in formatted
        assert '1000' in formatted
        assert '5.0' in formatted

    def test_format_single_refresh_result_json(self):
        """Test formatting single refresh result as JSON"""
        result = {
            'status': 'success',
            'view_name': 'test_view',
            'full_name': 'materialized_views.test_view',
            'refresh_time': datetime.now(),
            'duration_seconds': 5.0,
            'row_count': 1000,
        }
        
        formatted = format_result(result, format='json')
        parsed = json.loads(formatted)
        
        assert parsed['status'] == 'success'
        assert parsed['view_name'] == 'test_view'
        assert parsed['row_count'] == 1000

    def test_format_batch_refresh_result_text(self):
        """Test formatting batch refresh result as text"""
        result = {
            'status': 'success',
            'total': 2,
            'succeeded': 2,
            'failed': 0,
            'results': [
                {
                    'status': 'success',
                    'view_name': 'view1',
                    'row_count': 1000,
                    'duration_seconds': 5.0,
                },
                {
                    'status': 'success',
                    'view_name': 'view2',
                    'row_count': 2000,
                    'duration_seconds': 10.0,
                },
            ],
        }
        
        formatted = format_result(result, format='text')
        
        assert 'Refresh Results' in formatted
        assert 'Status: success' in formatted
        assert 'Total: 2' in formatted
        assert 'Succeeded: 2' in formatted
        assert 'Failed: 0' in formatted
        assert 'view1' in formatted
        assert 'view2' in formatted

    def test_format_status_result_text(self):
        """Test formatting status result as text"""
        result = {
            'view_name': 'test_view',
            'last_refresh_time': datetime.now(),
            'refresh_status': 'success',
            'row_count': 1000,
            'refresh_duration_seconds': 5.0,
            'error_message': None,
        }
        
        formatted = format_result(result, format='text')
        
        assert 'Materialized Views Status' in formatted or 'test_view' in formatted

    def test_format_history_result_text(self):
        """Test formatting history result as text"""
        now = datetime.now()
        result = {
            'view_name': 'test_view',
            'total_records': 2,
            'history': [
                {
                    'refresh_time': now,
                    'refresh_status': 'success',
                    'row_count': 1000,
                    'refresh_duration_seconds': 5.0,
                },
                {
                    'refresh_time': now - timedelta(hours=1),
                    'refresh_status': 'success',
                    'row_count': 1000,
                    'refresh_duration_seconds': 5.0,
                },
            ],
        }
        
        formatted = format_result(result, format='text')
        
        assert 'Refresh History' in formatted
        assert 'test_view' in formatted
        assert 'Total records: 2' in formatted

    def test_format_datetime_to_json(self):
        """Test that datetime objects are properly serialized to JSON"""
        result = {
            'refresh_time': datetime(2025, 1, 1, 12, 0, 0),
            'status': 'success',
        }
        
        formatted = format_result(result, format='json')
        parsed = json.loads(formatted)
        
        assert isinstance(parsed['refresh_time'], str)
        assert '2025-01-01' in parsed['refresh_time']


class TestMainEntrypoint:
    """Smoke tests for CLI main() wiring."""

    @pytest.mark.asyncio
    async def test_main_refresh_creates_and_closes_db_manager(self):
        from alphahome.processors.materialized_views import cli as mv_cli

        mock_db = AsyncMock()
        mock_db.connect = AsyncMock()
        mock_db.close = AsyncMock()

        refresh_result = {
            "status": "success",
            "view_name": "test_view",
            "full_name": "materialized_views.test_view",
            "refresh_time": datetime.now(),
            "duration_seconds": 0.1,
            "row_count": 1,
            "strategy": "full",
        }

        with (
            patch.object(mv_cli, "DBManager", return_value=mock_db) as mock_db_cls,
            patch.object(mv_cli, "refresh_materialized_view", AsyncMock(return_value=refresh_result)),
            patch("builtins.print"),
        ):
            exit_code = await mv_cli.main(
                ["--db-url", "postgresql://user:pass@localhost:5432/db", "refresh", "test_view"]
            )

        assert exit_code == 0
        mock_db_cls.assert_called_once_with(
            "postgresql://user:pass@localhost:5432/db",
            mode="async",
        )
        mock_db.connect.assert_awaited_once()
        mock_db.close.assert_awaited_once()
