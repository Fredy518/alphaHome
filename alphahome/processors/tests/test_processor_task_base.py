#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ProcessorTaskBase 单元测试

测试处理任务基类的核心功能：
- 执行顺序：fetch_data → process_data → save_result (Requirements 2.3)
- 错误处理和日志记录 (Requirements 2.5, 2.6)
"""

import pytest
import asyncio
import pandas as pd
import logging
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call

from alphahome.processors.tasks.base_task import ProcessorTaskBase


class MockDBManager:
    """模拟数据库管理器"""
    
    def __init__(self):
        self.data_store = {}
    
    async def table_exists(self, task) -> bool:
        return True
    
    async def upsert(self, df, target, conflict_columns, update_columns, timestamp_column=None):
        self.data_store[target.table_name] = df.copy()
        return len(df)
    
    async def copy_from_dataframe(self, df, target):
        self.data_store[target.table_name] = df.copy()
        return len(df)


class ExecutionOrderTrackingTask(ProcessorTaskBase):
    """用于追踪执行顺序的测试任务"""
    name = "execution_order_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    primary_keys = ["id"]
    
    def __init__(self, db_connection=None, **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.execution_order: List[str] = []
        self.fetch_data_called = False
        self.process_data_called = False
        self.save_result_called = False
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        self.execution_order.append("fetch_data")
        self.fetch_data_called = True
        return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        self.execution_order.append("process_data")
        self.process_data_called = True
        # 简单处理：值翻倍
        result = data.copy()
        result["value"] = result["value"] * 2
        return result
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        self.execution_order.append("save_result")
        self.save_result_called = True


class ErrorInFetchTask(ProcessorTaskBase):
    """在 fetch_data 中抛出错误的测试任务"""
    name = "error_in_fetch_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    
    def __init__(self, db_connection=None, error_message: str = "Fetch error", **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.error_message = error_message
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        raise RuntimeError(self.error_message)
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        return data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        pass


class ErrorInProcessTask(ProcessorTaskBase):
    """在 process_data 中抛出错误的测试任务"""
    name = "error_in_process_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    
    def __init__(self, db_connection=None, error_message: str = "Process error", **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.error_message = error_message
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        raise RuntimeError(self.error_message)
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        pass


class ErrorInSaveTask(ProcessorTaskBase):
    """在 save_result 中抛出错误的测试任务"""
    name = "error_in_save_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    primary_keys = ["id"]
    
    def __init__(self, db_connection=None, error_message: str = "Save error", **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.error_message = error_message
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        return pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        return data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        raise RuntimeError(self.error_message)


class EmptyDataTask(ProcessorTaskBase):
    """返回空数据的测试任务"""
    name = "empty_data_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    
    def __init__(self, db_connection=None, **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.save_result_called = False
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        return pd.DataFrame()
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        return data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        self.save_result_called = True


@pytest.fixture
def mock_db_manager():
    """创建模拟数据库管理器"""
    return MockDBManager()


class TestProcessorTaskBaseExecutionOrder:
    """测试 ProcessorTaskBase 执行顺序 (Requirements 2.3)"""
    
    @pytest.mark.asyncio
    async def test_execution_order_fetch_process_save(self, mock_db_manager):
        """
        测试执行顺序：fetch_data → process_data → save_result
        
        **Validates: Requirements 2.3**
        """
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        result = await task.execute()
        
        # 验证执行顺序
        assert task.execution_order == ["fetch_data", "process_data", "save_result"], \
            f"执行顺序应为 fetch_data → process_data → save_result，实际为 {task.execution_order}"
    
    @pytest.mark.asyncio
    async def test_all_methods_called(self, mock_db_manager):
        """测试所有方法都被调用"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        await task.execute()
        
        assert task.fetch_data_called, "fetch_data 应该被调用"
        assert task.process_data_called, "process_data 应该被调用"
        assert task.save_result_called, "save_result 应该被调用"
    
    @pytest.mark.asyncio
    async def test_run_method_calls_execute(self, mock_db_manager):
        """测试 run 方法触发完整执行流程 (Requirements 2.6, 8.1)"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        result = await task.run()
        
        # run 方法应该触发完整的执行流程
        # 新流程：fetch_data → clean_data → compute_features → save_result
        # 注意：ExecutionOrderTrackingTask 没有覆盖 clean_data 和 compute_features，
        # 所以只会记录 fetch_data 和 save_result
        assert "fetch_data" in task.execution_order
        assert "save_result" in task.execution_order
        assert result["status"] == "success"


class TestProcessorTaskBaseErrorHandling:
    """测试 ProcessorTaskBase 错误处理 (Requirements 2.5)"""
    
    @pytest.mark.asyncio
    async def test_error_in_fetch_data_returns_error_status(self, mock_db_manager):
        """测试 fetch_data 错误返回错误状态"""
        task = ErrorInFetchTask(db_connection=mock_db_manager, error_message="Fetch failed")
        
        result = await task.execute()
        
        assert result["status"] == "error"
        assert "Fetch failed" in result["error"]
    
    @pytest.mark.asyncio
    async def test_error_in_process_data_returns_error_status(self, mock_db_manager):
        """测试 process_data 错误返回错误状态"""
        task = ErrorInProcessTask(db_connection=mock_db_manager, error_message="Process failed")
        
        result = await task.execute()
        
        assert result["status"] == "error"
        assert "Process failed" in result["error"]
    
    @pytest.mark.asyncio
    async def test_error_in_save_result_returns_error_status(self, mock_db_manager):
        """测试 save_result 错误返回错误状态"""
        task = ErrorInSaveTask(db_connection=mock_db_manager, error_message="Save failed")
        
        result = await task.execute()
        
        assert result["status"] == "error"
        assert "Save failed" in result["error"]
    
    @pytest.mark.asyncio
    async def test_error_result_contains_task_name(self, mock_db_manager):
        """测试错误结果包含任务名称"""
        task = ErrorInFetchTask(db_connection=mock_db_manager)
        
        result = await task.execute()
        
        assert result["task"] == "error_in_fetch_task"


class TestProcessorTaskBaseLogging:
    """测试 ProcessorTaskBase 日志记录 (Requirements 2.5)"""
    
    @pytest.mark.asyncio
    async def test_error_is_logged(self, mock_db_manager, caplog):
        """测试错误被记录到日志"""
        task = ErrorInFetchTask(db_connection=mock_db_manager, error_message="Test error for logging")
        
        with caplog.at_level(logging.ERROR):
            await task.execute()
        
        # 验证错误被记录
        assert any("Test error for logging" in record.message or "任务执行失败" in record.message 
                   for record in caplog.records)
    
    @pytest.mark.asyncio
    async def test_execution_start_is_logged(self, mock_db_manager, caplog):
        """测试执行开始被记录到日志"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        with caplog.at_level(logging.INFO):
            await task.execute()
        
        # 验证执行开始被记录
        assert any("开始执行任务" in record.message for record in caplog.records)


class TestProcessorTaskBaseEmptyData:
    """测试 ProcessorTaskBase 空数据处理"""
    
    @pytest.mark.asyncio
    async def test_empty_fetch_data_returns_no_data_status(self, mock_db_manager):
        """测试空数据返回 no_data 状态"""
        task = EmptyDataTask(db_connection=mock_db_manager)
        
        result = await task.execute()
        
        assert result["status"] == "no_data"
        assert result["rows"] == 0
    
    @pytest.mark.asyncio
    async def test_empty_data_does_not_call_save(self, mock_db_manager):
        """测试空数据不调用 save_result"""
        task = EmptyDataTask(db_connection=mock_db_manager)
        
        await task.execute()
        
        # 空数据时不应该调用 save_result
        assert not task.save_result_called


class TestProcessorTaskBaseAttributes:
    """测试 ProcessorTaskBase 属性 (Requirements 2.4)"""
    
    def test_source_tables_attribute(self, mock_db_manager):
        """测试 source_tables 属性"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        assert task.source_tables == ["test_input"]
    
    def test_table_name_attribute(self, mock_db_manager):
        """测试 table_name 属性"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        assert task.table_name == "test_output"
    
    def test_task_type_is_processor(self, mock_db_manager):
        """测试 task_type 为 processor"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        assert task.task_type == "processor"
    
    def test_get_task_info(self, mock_db_manager):
        """测试 get_task_info 方法"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        
        info = task.get_task_info()
        
        assert info["name"] == "execution_order_task"
        assert info["type"] == "processor"
        assert info["source_tables"] == ["test_input"]
        assert info["target_table"] == "test_output"


class TestProcessorTaskBaseCancellation:
    """测试 ProcessorTaskBase 取消功能"""
    
    @pytest.mark.asyncio
    async def test_cancellation_before_fetch(self, mock_db_manager):
        """测试在 fetch_data 前取消"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        stop_event = asyncio.Event()
        stop_event.set()  # 立即设置取消
        
        result = await task.execute(stop_event=stop_event)
        
        assert result["status"] == "cancelled"
    
    @pytest.mark.asyncio
    async def test_cancellation_returns_cancelled_status(self, mock_db_manager):
        """测试取消返回 cancelled 状态"""
        task = ExecutionOrderTrackingTask(db_connection=mock_db_manager)
        stop_event = asyncio.Event()
        stop_event.set()
        
        result = await task.execute(stop_event=stop_event)
        
        assert result["status"] == "cancelled"
        assert "task" in result
