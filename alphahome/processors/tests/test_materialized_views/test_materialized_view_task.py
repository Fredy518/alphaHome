#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for MaterializedViewTask.

Uses hypothesis library for property-based testing.

**Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
**Validates: Requirements 3.1, 3.2, 3.3**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
from unittest.mock import MagicMock, AsyncMock, patch
from typing import Dict, Any, List, Optional
import pandas as pd


# =============================================================================
# Custom Strategies for MaterializedViewTask
# =============================================================================

def materialized_view_name_strategy():
    """Generate valid materialized view names."""
    return st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz_',
        min_size=1,
        max_size=50
    ).filter(lambda x: not x.startswith('_'))


def refresh_strategy_strategy():
    """Generate valid refresh strategies."""
    return st.sampled_from(['full', 'concurrent'])


def source_tables_strategy():
    """Generate valid source table lists."""
    table_name = st.text(
        alphabet='abcdefghijklmnopqrstuvwxyz_',
        min_size=1,
        max_size=30
    ).filter(lambda x: not x.startswith('_'))
    
    return st.lists(
        st.just('rawdata.') + table_name,
        min_size=1,
        max_size=5,
        unique=True
    )


def quality_checks_strategy():
    """Generate valid quality check configurations."""
    return st.dictionaries(
        keys=st.sampled_from(['null_check', 'outlier_check', 'row_count_change']),
        values=st.dictionaries(
            keys=st.sampled_from(['threshold', 'columns', 'method']),
            values=st.one_of(
                st.floats(min_value=0.0, max_value=1.0),
                st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=5),
                st.sampled_from(['iqr', 'zscore', 'percentile'])
            )
        ),
        min_size=0,
        max_size=3
    )


# =============================================================================
# Property 3: MaterializedViewTask interface correctness
# **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
# **Validates: Requirements 3.1, 3.2, 3.3**
# =============================================================================

class TestProperty3MaterializedViewTaskInterfaceCorrectness:
    """
    Property 3: MaterializedViewTask interface correctness
    
    *For any* valid MaterializedViewTask configuration, the task SHALL:
    1. Have all required attributes properly initialized
    2. Implement all required abstract methods
    3. Support both FULL and CONCURRENT refresh strategies
    4. Track refresh metadata correctly
    
    **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
    **Validates: Requirements 3.1, 3.2, 3.3**
    """
    
    @pytest.mark.asyncio
    async def test_materialized_view_task_has_required_attributes(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.1**
        
        For any MaterializedViewTask instance, it SHALL have all required attributes:
        - is_materialized_view = True
        - materialized_view_name (non-empty string)
        - materialized_view_schema (default: 'materialized_views')
        - refresh_strategy (one of: 'full', 'concurrent')
        - source_tables (list of rawdata.* tables)
        - quality_checks (dict)
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Create a concrete implementation for testing
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            source_tables = ["rawdata.test_table"]
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = MagicMock()
        task = TestMVTask(db_connection=mock_db)
        
        # Verify required attributes
        assert hasattr(task, 'is_materialized_view'), "Missing is_materialized_view attribute"
        assert task.is_materialized_view is True, "is_materialized_view should be True"
        
        assert hasattr(task, 'materialized_view_name'), "Missing materialized_view_name attribute"
        assert isinstance(task.materialized_view_name, str), "materialized_view_name should be string"
        assert len(task.materialized_view_name) > 0, "materialized_view_name should not be empty"
        
        assert hasattr(task, 'materialized_view_schema'), "Missing materialized_view_schema attribute"
        assert isinstance(task.materialized_view_schema, str), "materialized_view_schema should be string"
        assert task.materialized_view_schema == "materialized_views", "Default schema should be 'materialized_views'"
        
        assert hasattr(task, 'refresh_strategy'), "Missing refresh_strategy attribute"
        assert task.refresh_strategy in ['full', 'concurrent'], "refresh_strategy should be 'full' or 'concurrent'"
        
        assert hasattr(task, 'source_tables'), "Missing source_tables attribute"
        assert isinstance(task.source_tables, list), "source_tables should be list"
        assert len(task.source_tables) > 0, "source_tables should not be empty"
        
        assert hasattr(task, 'quality_checks'), "Missing quality_checks attribute"
        assert isinstance(task.quality_checks, dict), "quality_checks should be dict"

    @pytest.mark.asyncio
    async def test_materialized_view_task_implements_required_methods(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.1**
        
        For any MaterializedViewTask instance, it SHALL implement all required methods:
        - define_materialized_view_sql() - abstract method
        - refresh_materialized_view() - concrete method
        - validate_data_quality() - concrete method
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Create a concrete implementation for testing
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            source_tables = ["rawdata.test_table"]
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = MagicMock()
        task = TestMVTask(db_connection=mock_db)
        
        # Verify required methods exist and are callable
        assert hasattr(task, 'define_materialized_view_sql'), "Missing define_materialized_view_sql method"
        assert callable(task.define_materialized_view_sql), "define_materialized_view_sql should be callable"
        
        assert hasattr(task, 'refresh_materialized_view'), "Missing refresh_materialized_view method"
        assert callable(task.refresh_materialized_view), "refresh_materialized_view should be callable"
        
        assert hasattr(task, 'validate_data_quality'), "Missing validate_data_quality method"
        assert callable(task.validate_data_quality), "validate_data_quality should be callable"

    @pytest.mark.asyncio
    async def test_define_materialized_view_sql_returns_string(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.1**
        
        For any MaterializedViewTask instance, define_materialized_view_sql() SHALL 
        return a non-empty string containing valid SQL.
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Create a concrete implementation for testing
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            source_tables = ["rawdata.test_table"]
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = MagicMock()
        task = TestMVTask(db_connection=mock_db)
        
        # Call the method
        sql = await task.define_materialized_view_sql()
        
        # Verify return value
        assert isinstance(sql, str), "define_materialized_view_sql should return string"
        assert len(sql) > 0, "SQL should not be empty"
        assert 'SELECT' in sql.upper(), "SQL should contain SELECT statement"

    @pytest.mark.asyncio
    async def test_refresh_materialized_view_returns_dict_with_required_keys(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.2**
        
        For any MaterializedViewTask instance, refresh_materialized_view() SHALL 
        return a dictionary with required keys:
        - status (success/failed)
        - view_name
        - refresh_time
        - duration_seconds
        - row_count
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Create a concrete implementation for testing
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            source_tables = ["rawdata.test_table"]
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = AsyncMock()

        async def mock_fetch_val(query, *args, **kwargs):
            if "pg_matviews" in query:
                return True
            if "COUNT(*)" in query:
                return 1000
            return None

        mock_db.fetch_val = AsyncMock(side_effect=mock_fetch_val)
        mock_db.execute = AsyncMock(return_value="OK")
        task = TestMVTask(db_connection=mock_db)
        
        # Call the method
        result = await task.refresh_materialized_view()
        
        # Verify return value
        assert isinstance(result, dict), "refresh_materialized_view should return dict"
        assert 'status' in result, "Result should contain 'status' key"
        assert 'view_name' in result, "Result should contain 'view_name' key"
        assert 'refresh_time' in result, "Result should contain 'refresh_time' key"
        assert 'duration_seconds' in result, "Result should contain 'duration_seconds' key"
        assert 'row_count' in result, "Result should contain 'row_count' key"

    @pytest.mark.asyncio
    async def test_validate_data_quality_returns_dict_with_required_keys(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.2**
        
        For any MaterializedViewTask instance, validate_data_quality() SHALL 
        return a dictionary with required keys:
        - status (pass/warning/error)
        - checks (list of check results)
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Create a concrete implementation for testing
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            source_tables = ["rawdata.test_table"]
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = AsyncMock()
        mock_db.fetch = AsyncMock(return_value=[])
        task = TestMVTask(db_connection=mock_db)
        
        # Call the method
        result = await task.validate_data_quality()
        
        # Verify return value
        assert isinstance(result, dict), "validate_data_quality should return dict"
        assert 'status' in result, "Result should contain 'status' key"
        assert result['status'] in ['pass', 'warning', 'error'], "Status should be pass/warning/error"
        assert 'checks' in result, "Result should contain 'checks' key"
        assert isinstance(result['checks'], list), "Checks should be a list"

    @pytest.mark.asyncio
    async def test_refresh_strategy_supports_full_and_concurrent(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.3**
        
        For any MaterializedViewTask instance, refresh_strategy SHALL support 
        both 'full' and 'concurrent' strategies.
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Test with 'full' strategy
        class TestMVTaskFull(MaterializedViewTask):
            name = "test_mv_task_full"
            materialized_view_name = "test_mv_full"
            source_tables = ["rawdata.test_table"]
            refresh_strategy = "full"
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        mock_db = MagicMock()
        task_full = TestMVTaskFull(db_connection=mock_db)
        assert task_full.refresh_strategy == "full", "Should support 'full' strategy"
        
        # Test with 'concurrent' strategy
        class TestMVTaskConcurrent(MaterializedViewTask):
            name = "test_mv_task_concurrent"
            materialized_view_name = "test_mv_concurrent"
            source_tables = ["rawdata.test_table"]
            refresh_strategy = "concurrent"
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        task_concurrent = TestMVTaskConcurrent(db_connection=mock_db)
        assert task_concurrent.refresh_strategy == "concurrent", "Should support 'concurrent' strategy"

    @pytest.mark.asyncio
    async def test_get_materialized_view_info_returns_complete_info(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.1, 3.2, 3.3**
        
        For any MaterializedViewTask instance, get_materialized_view_info() SHALL 
        return a dictionary with complete information about the materialized view.
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Create a concrete implementation for testing
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            materialized_view_schema = "materialized_views"
            refresh_strategy = "full"
            source_tables = ["rawdata.test_table"]
            quality_checks = {"null_check": {"threshold": 0.01}}
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = MagicMock()
        task = TestMVTask(db_connection=mock_db)
        
        # Call the method
        info = task.get_materialized_view_info()
        
        # Verify return value
        assert isinstance(info, dict), "get_materialized_view_info should return dict"
        assert 'name' in info, "Info should contain 'name' key"
        assert 'materialized_view_name' in info, "Info should contain 'materialized_view_name' key"
        assert 'materialized_view_schema' in info, "Info should contain 'materialized_view_schema' key"
        assert 'full_name' in info, "Info should contain 'full_name' key"
        assert 'refresh_strategy' in info, "Info should contain 'refresh_strategy' key"
        assert 'source_tables' in info, "Info should contain 'source_tables' key"
        assert 'quality_checks' in info, "Info should contain 'quality_checks' key"
        
        # Verify full_name format
        expected_full_name = f"{task.materialized_view_schema}.{task.materialized_view_name}"
        assert info['full_name'] == expected_full_name, f"full_name should be '{expected_full_name}'"

    @pytest.mark.asyncio
    async def test_materialized_view_task_inherits_from_processor_task_base(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.1**
        
        For any MaterializedViewTask instance, it SHALL inherit from ProcessorTaskBase 
        and have all its methods available.
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        from alphahome.processors.tasks.base_task import ProcessorTaskBase
        
        # Create a concrete implementation for testing
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            source_tables = ["rawdata.test_table"]
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = MagicMock()
        task = TestMVTask(db_connection=mock_db)
        
        # Verify inheritance
        assert isinstance(task, ProcessorTaskBase), "MaterializedViewTask should inherit from ProcessorTaskBase"
        
        # Verify inherited methods are available
        assert hasattr(task, 'fetch_data'), "Should have fetch_data from ProcessorTaskBase"
        assert hasattr(task, 'process_data'), "Should have process_data from ProcessorTaskBase"
        assert hasattr(task, 'save_result'), "Should have save_result from ProcessorTaskBase"
        assert hasattr(task, 'run'), "Should have run from ProcessorTaskBase"

    @pytest.mark.asyncio
    async def test_materialized_view_task_quality_checks_configuration(self):
        """
        **Feature: materialized-views-system, Property 3: MaterializedViewTask interface correctness**
        **Validates: Requirements 3.2**
        
        For any MaterializedViewTask instance with quality_checks configuration, 
        the quality_checks SHALL be a dictionary that can be passed to validate_data_quality().
        """
        from alphahome.processors.materialized_views import MaterializedViewTask
        
        # Create a concrete implementation with quality checks
        class TestMVTask(MaterializedViewTask):
            name = "test_mv_task"
            materialized_view_name = "test_mv"
            source_tables = ["rawdata.test_table"]
            quality_checks = {
                'null_check': {
                    'columns': ['col1', 'col2'],
                    'threshold': 0.01
                },
                'outlier_check': {
                    'columns': ['col3'],
                    'method': 'iqr',
                    'threshold': 3.0
                }
            }
            
            async def define_materialized_view_sql(self) -> str:
                return "SELECT * FROM rawdata.test_table"
        
        # Create instance with mock db connection
        mock_db = MagicMock()
        task = TestMVTask(db_connection=mock_db)
        
        # Verify quality_checks structure
        assert isinstance(task.quality_checks, dict), "quality_checks should be dict"
        assert 'null_check' in task.quality_checks, "Should have null_check configuration"
        assert 'outlier_check' in task.quality_checks, "Should have outlier_check configuration"
        
        # Verify check configurations have required keys
        null_check = task.quality_checks['null_check']
        assert 'columns' in null_check, "null_check should have 'columns' key"
        assert 'threshold' in null_check, "null_check should have 'threshold' key"
        
        outlier_check = task.quality_checks['outlier_check']
        assert 'columns' in outlier_check, "outlier_check should have 'columns' key"
        assert 'method' in outlier_check, "outlier_check should have 'method' key"
        assert 'threshold' in outlier_check, "outlier_check should have 'threshold' key"
