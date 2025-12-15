#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tests for MaterializedViewAlerting.

Tests for refresh failure alerting and data quality alerting.

**Feature: materialized-views-system, Task 18: Monitoring and Alerting**
**Validates: Requirements 6.1, 6.2, 6.3, 4.1, 4.2, 4.3, 4.4, 4.5**
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
import json

from alphahome.processors.materialized_views import (
    MaterializedViewAlerting,
    AlertSeverity,
    AlertType,
)


class TestRefreshFailureAlerting:
    """
    Tests for refresh failure alerting (Task 18.1)
    
    **Validates: Requirements 6.1, 6.2, 6.3**
    """
    
    @pytest.mark.asyncio
    async def test_alert_refresh_failed_records_error_alert(self):
        """
        **Feature: materialized-views-system, Task 18.1**
        **Validates: Requirements 6.1, 6.2, 6.3**
        
        For any refresh failure, alert_refresh_failed() SHALL record an error alert
        with error_message, refresh_strategy, and duration_seconds.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Record refresh failure alert
        await alerting.alert_refresh_failed(
            view_name='test_mv',
            error_message='Connection timeout',
            refresh_strategy='full',
            duration_seconds=30.0
        )
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify alert was recorded with correct parameters
        assert params[0] == 'test_mv', "view_name should be test_mv"
        assert params[2] == AlertSeverity.ERROR.value, "severity should be ERROR"
        assert 'Connection timeout' in params[3], "message should contain error message"
    
    @pytest.mark.asyncio
    async def test_alert_refresh_failed_detects_timeout(self):
        """
        **Feature: materialized-views-system, Task 18.1**
        **Validates: Requirements 6.1, 6.2**
        
        For any timeout error, alert_refresh_failed() SHALL set alert_type to REFRESH_TIMEOUT.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Record timeout alert
        await alerting.alert_refresh_failed(
            view_name='test_mv',
            error_message='Query timeout after 60 seconds',
            refresh_strategy='concurrent',
            duration_seconds=60.0
        )
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify alert_type is REFRESH_TIMEOUT
        assert params[1] == AlertType.REFRESH_TIMEOUT.value, "alert_type should be REFRESH_TIMEOUT"
    
    @pytest.mark.asyncio
    async def test_alert_refresh_failed_includes_additional_details(self):
        """
        **Feature: materialized-views-system, Task 18.1**
        **Validates: Requirements 6.1, 6.2, 6.3**
        
        For any refresh failure with additional_details, alert_refresh_failed() 
        SHALL include them in the alert details.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Record refresh failure with additional details
        additional_details = {
            'error_code': 'PG001',
            'retry_count': 3,
            'last_retry_time': '2025-01-01 12:00:00'
        }
        
        await alerting.alert_refresh_failed(
            view_name='test_mv',
            error_message='Database connection failed',
            refresh_strategy='full',
            duration_seconds=5.0,
            additional_details=additional_details
        )
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify details include additional information
        details_json = params[4]
        details = json.loads(details_json)
        assert 'error_code' in details, "details should include error_code"
        assert details['error_code'] == 'PG001', "error_code should be PG001"
        assert details['retry_count'] == 3, "retry_count should be 3"


class TestDataQualityAlerting:
    """
    Tests for data quality alerting (Task 18.2)
    
    **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**
    """
    
    @pytest.mark.asyncio
    async def test_alert_data_quality_issue_records_warning(self):
        """
        **Feature: materialized-views-system, Task 18.2**
        **Validates: Requirements 4.1, 4.2, 4.3**
        
        For any data quality warning, alert_data_quality_issue() SHALL record a warning alert.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Record data quality warning
        await alerting.alert_data_quality_issue(
            view_name='test_mv',
            check_name='null_check',
            check_status='warning',
            check_message='Null values detected in column ts_code'
        )
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify alert was recorded
        assert params[0] == 'test_mv', "view_name should be test_mv"
        assert params[1] == AlertType.NULL_VALUES_DETECTED.value, "alert_type should be NULL_VALUES_DETECTED"
        assert params[2] == AlertSeverity.WARNING.value, "severity should be WARNING"
    
    @pytest.mark.asyncio
    async def test_alert_data_quality_issue_records_error(self):
        """
        **Feature: materialized-views-system, Task 18.2**
        **Validates: Requirements 4.1, 4.2, 4.4**
        
        For any data quality error, alert_data_quality_issue() SHALL record an error alert.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Record data quality error
        await alerting.alert_data_quality_issue(
            view_name='test_mv',
            check_name='duplicate_check',
            check_status='error',
            check_message='Duplicate keys found'
        )
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify alert_type and severity
        assert params[1] == AlertType.DUPLICATE_KEYS.value, "alert_type should be DUPLICATE_KEYS"
        assert params[2] == AlertSeverity.ERROR.value, "severity should be ERROR"
    
    @pytest.mark.asyncio
    async def test_alert_data_quality_issue_skips_passing_checks(self):
        """
        **Feature: materialized-views-system, Task 18.2**
        **Validates: Requirements 4.1, 4.2**
        
        For any passing data quality check, alert_data_quality_issue() SHALL NOT record an alert.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Record passing check
        await alerting.alert_data_quality_issue(
            view_name='test_mv',
            check_name='null_check',
            check_status='pass',
            check_message='No null values detected'
        )
        
        # Verify execute was NOT called
        assert not mock_db.execute.called, "execute should not be called for passing checks"
    
    @pytest.mark.asyncio
    async def test_alert_data_quality_issue_maps_check_types(self):
        """
        **Feature: materialized-views-system, Task 18.2**
        **Validates: Requirements 4.1, 4.2, 4.3, 4.5**
        
        For any data quality check, alert_data_quality_issue() SHALL map check_name to alert_type.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Test different check types
        check_mappings = [
            ('null_check', AlertType.NULL_VALUES_DETECTED.value),
            ('outlier_check', AlertType.OUTLIERS_DETECTED.value),
            ('row_count_change', AlertType.ROW_COUNT_ANOMALY.value),
            ('duplicate_check', AlertType.DUPLICATE_KEYS.value),
            ('type_check', AlertType.TYPE_MISMATCH.value),
        ]
        
        for check_name, expected_alert_type in check_mappings:
            mock_db.reset_mock()
            
            await alerting.alert_data_quality_issue(
                view_name='test_mv',
                check_name=check_name,
                check_status='warning',
                check_message=f'{check_name} warning'
            )
            
            # Get the SQL and parameters
            call_args = mock_db.execute.call_args
            params = call_args[0][1:]
            
            # Verify alert_type mapping
            assert params[1] == expected_alert_type, f"alert_type for {check_name} should be {expected_alert_type}"
    
    @pytest.mark.asyncio
    async def test_alert_data_quality_issue_includes_check_details(self):
        """
        **Feature: materialized-views-system, Task 18.2**
        **Validates: Requirements 4.1, 4.2, 4.3, 4.5**
        
        For any data quality issue with check_details, alert_data_quality_issue() 
        SHALL include them in the alert details.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Record data quality issue with details
        check_details = {
            'null_count': 100,
            'null_percentage': 0.05,
            'threshold': 0.01,
            'columns': ['col1', 'col2']
        }
        
        await alerting.alert_data_quality_issue(
            view_name='test_mv',
            check_name='null_check',
            check_status='warning',
            check_message='Null values exceed threshold',
            check_details=check_details
        )
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify details include check information
        details_json = params[4]
        details = json.loads(details_json)
        assert 'null_count' in details, "details should include null_count"
        assert details['null_count'] == 100, "null_count should be 100"
        assert details['null_percentage'] == 0.05, "null_percentage should be 0.05"


class TestAlertHistory:
    """
    Tests for alert history and querying
    
    **Validates: Requirements 6.1, 6.2, 6.3, 4.1, 4.2**
    """
    
    @pytest.mark.asyncio
    async def test_get_alert_history_returns_list(self):
        """
        **Feature: materialized-views-system, Task 18**
        **Validates: Requirements 6.1, 6.2**
        
        For any view_name, get_alert_history() SHALL return a list of alerts.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        
        # Mock fetch to return test data
        mock_rows = [
            {
                'id': 1,
                'view_name': 'test_mv',
                'alert_type': AlertType.REFRESH_FAILED.value,
                'severity': AlertSeverity.ERROR.value,
                'message': 'Refresh failed',
                'details': '{"error_message": "Connection timeout"}',
                'created_at': datetime.now(),
            }
        ]
        mock_db.fetch = AsyncMock(return_value=mock_rows)
        
        alerting = MaterializedViewAlerting(mock_db)
        
        # Get alert history
        history = await alerting.get_alert_history(view_name='test_mv', limit=10)
        
        # Verify fetch was called
        assert mock_db.fetch.called, "fetch should be called"
        
        # Verify history is a list
        assert isinstance(history, list), "history should be a list"
        assert len(history) == 1, "history should have 1 record"
        
        # Verify record structure
        record = history[0]
        assert 'id' in record, "record should have id"
        assert 'view_name' in record, "record should have view_name"
        assert 'alert_type' in record, "record should have alert_type"
        assert 'severity' in record, "record should have severity"
        assert 'message' in record, "record should have message"
        assert 'details' in record, "record should have details"
        assert 'created_at' in record, "record should have created_at"
    
    @pytest.mark.asyncio
    async def test_get_alert_summary_returns_statistics(self):
        """
        **Feature: materialized-views-system, Task 18**
        **Validates: Requirements 6.1, 6.2**
        
        For any view_name, get_alert_summary() SHALL return alert statistics.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        
        # Mock fetch to return test data
        mock_rows = [
            {'severity': AlertSeverity.ERROR.value, 'count': 5},
            {'severity': AlertSeverity.WARNING.value, 'count': 10},
        ]
        mock_db.fetch = AsyncMock(return_value=mock_rows)
        
        alerting = MaterializedViewAlerting(mock_db)
        
        # Get alert summary
        summary = await alerting.get_alert_summary(view_name='test_mv', days=7)
        
        # Verify fetch was called
        assert mock_db.fetch.called, "fetch should be called"
        
        # Verify summary structure
        assert 'total' in summary, "summary should have total"
        assert 'by_severity' in summary, "summary should have by_severity"
        assert summary['total'] == 15, "total should be 15"
        assert summary['error'] == 5, "error count should be 5"
        assert summary['warning'] == 10, "warning count should be 10"
    
    @pytest.mark.asyncio
    async def test_get_unacknowledged_alerts_returns_list(self):
        """
        **Feature: materialized-views-system, Task 18**
        **Validates: Requirements 6.1, 6.2, 6.3**
        
        For any view_name, get_unacknowledged_alerts() SHALL return unacknowledged alerts.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        
        # Mock fetch to return test data
        mock_rows = [
            {
                'id': 1,
                'view_name': 'test_mv',
                'alert_type': AlertType.REFRESH_FAILED.value,
                'severity': AlertSeverity.ERROR.value,
                'message': 'Refresh failed',
                'details': '{}',
                'created_at': datetime.now(),
            }
        ]
        mock_db.fetch = AsyncMock(return_value=mock_rows)
        
        alerting = MaterializedViewAlerting(mock_db)
        
        # Get unacknowledged alerts
        alerts = await alerting.get_unacknowledged_alerts(view_name='test_mv', limit=50)
        
        # Verify fetch was called
        assert mock_db.fetch.called, "fetch should be called"
        
        # Verify alerts is a list
        assert isinstance(alerts, list), "alerts should be a list"
        assert len(alerts) == 1, "alerts should have 1 record"
    
    @pytest.mark.asyncio
    async def test_acknowledge_alert_updates_status(self):
        """
        **Feature: materialized-views-system, Task 18**
        **Validates: Requirements 6.1, 6.2**
        
        For any alert_id, acknowledge_alert() SHALL update the alert status.
        """
        # Create mock db_manager
        mock_db = AsyncMock()
        alerting = MaterializedViewAlerting(mock_db)
        
        # Acknowledge alert
        await alerting.acknowledge_alert(
            alert_id=1,
            acknowledged_by='admin',
            notes='Issue resolved'
        )
        
        # Verify execute was called
        assert mock_db.execute.called, "execute should be called"
        
        # Get the SQL and parameters
        call_args = mock_db.execute.call_args
        params = call_args[0][1:]
        
        # Verify parameters
        assert params[0] == 'admin', "acknowledged_by should be admin"
        assert params[1] == 'Issue resolved', "notes should be included"
        assert params[2] == 1, "alert_id should be 1"
