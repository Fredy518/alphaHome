#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ProcessorEngine 单元测试

测试处理引擎的核心功能：
- 并发执行 (Requirements 5.1)
- 任务状态追踪 (Requirements 5.2)
- 错误处理 (Requirements 5.3)
"""

import pytest
import asyncio
import pandas as pd
from datetime import datetime
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from alphahome.processors.engine.processor_engine import (
    ProcessorEngine,
    TaskExecutionResult,
    TaskStatus
)
from alphahome.processors.tasks.base_task import ProcessorTaskBase


class MockDBManager:
    """模拟数据库管理器"""
    
    def __init__(self):
        self.data_store = {}
    
    async def connect(self):
        pass
    
    async def close(self):
        pass
    
    async def fetch_data(self, table_name: str, **kwargs) -> pd.DataFrame:
        return self.data_store.get(table_name, pd.DataFrame())
    
    async def save_data(self, data: pd.DataFrame, table_name: str, **kwargs):
        self.data_store[table_name] = data.copy()


class SuccessfulTask(ProcessorTaskBase):
    """成功执行的模拟任务"""
    name = "successful_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    
    def __init__(self, db_connection=None, execution_time: float = 0.1, **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.execution_time = execution_time
        self._executed = False
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        return pd.DataFrame({"value": [1, 2, 3]})
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        await asyncio.sleep(self.execution_time)
        self._executed = True
        return data * 2
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        pass
    
    async def run(self, **kwargs) -> Dict[str, Any]:
        data = await self.fetch_data(**kwargs)
        result = await self.process_data(data, **kwargs)
        await self.save_result(result, **kwargs)
        return {"status": "success", "rows_processed": len(result) if result is not None else 0}


class FailingTask(ProcessorTaskBase):
    """执行失败的模拟任务"""
    name = "failing_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    
    def __init__(self, db_connection=None, error_message: str = "Task failed intentionally", **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.error_message = error_message
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        return pd.DataFrame({"value": [1, 2, 3]})
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        raise RuntimeError(self.error_message)
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        pass
    
    async def run(self, **kwargs) -> Dict[str, Any]:
        data = await self.fetch_data(**kwargs)
        await self.process_data(data, **kwargs)
        return {"status": "success"}


class SlowTask(ProcessorTaskBase):
    """慢速执行的模拟任务"""
    name = "slow_task"
    table_name = "test_output"
    source_tables = ["test_input"]
    
    def __init__(self, db_connection=None, delay: float = 0.5, **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.delay = delay
        self.start_time = None
        self.end_time = None
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        return pd.DataFrame({"value": [1, 2, 3]})
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        self.start_time = datetime.now()
        await asyncio.sleep(self.delay)
        self.end_time = datetime.now()
        return data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        pass
    
    async def run(self, **kwargs) -> Dict[str, Any]:
        data = await self.fetch_data(**kwargs)
        result = await self.process_data(data, **kwargs)
        await self.save_result(result, **kwargs)
        return {"status": "success", "rows_processed": len(result) if result is not None else 0}


@pytest.fixture
def mock_db_manager():
    """创建模拟数据库管理器"""
    return MockDBManager()


@pytest.fixture
def processor_engine(mock_db_manager):
    """创建处理引擎实例"""
    engine = ProcessorEngine(db_manager=mock_db_manager, max_workers=4)
    yield engine
    engine.shutdown()


class TestProcessorEngineInitialization:
    """测试 ProcessorEngine 初始化"""
    
    def test_init_with_valid_db_manager(self, mock_db_manager):
        """测试使用有效的 db_manager 初始化"""
        engine = ProcessorEngine(db_manager=mock_db_manager, max_workers=2)
        assert engine.max_workers == 2
        assert engine.db_manager is mock_db_manager
        engine.shutdown()
    
    def test_init_without_db_manager_raises_error(self):
        """测试没有 db_manager 时抛出错误"""
        with pytest.raises(ValueError, match="必须为ProcessorEngine提供一个DBManager实例"):
            ProcessorEngine(db_manager=None)
    
    def test_init_with_timeout(self, mock_db_manager):
        """测试带超时参数的初始化"""
        engine = ProcessorEngine(db_manager=mock_db_manager, timeout=30.0)
        assert engine.timeout == 30.0
        engine.shutdown()
    
    def test_initial_stats(self, processor_engine):
        """测试初始统计信息"""
        stats = processor_engine.get_stats()
        assert stats["total_tasks"] == 0
        assert stats["successful_tasks"] == 0
        assert stats["failed_tasks"] == 0
        assert stats["success_rate"] == 0.0


class TestProcessorEngineTaskExecution:
    """测试任务执行功能"""
    
    @pytest.mark.asyncio
    async def test_execute_single_task_success(self, processor_engine):
        """测试成功执行单个任务"""
        task = SuccessfulTask(db_connection=processor_engine.db_manager)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            result = await processor_engine.execute_task("successful_task")
        
        assert result["status"] == "success"
        assert "engine_metadata" in result
        assert result["engine_metadata"]["task_name"] == "successful_task"
    
    @pytest.mark.asyncio
    async def test_execute_single_task_failure(self, processor_engine):
        """测试任务执行失败时的处理"""
        task = FailingTask(db_connection=processor_engine.db_manager)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            result = await processor_engine.execute_task("failing_task")
        
        assert result["status"] == "error"
        assert "error" in result
    
    @pytest.mark.asyncio
    async def test_task_execution_updates_stats(self, processor_engine):
        """测试任务执行更新统计信息 (Requirements 5.2)"""
        task = SuccessfulTask(db_connection=processor_engine.db_manager)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            await processor_engine.execute_task("successful_task")
        
        stats = processor_engine.get_stats()
        assert stats["total_tasks"] == 1
        assert stats["successful_tasks"] == 1
        assert stats["failed_tasks"] == 0
    
    @pytest.mark.asyncio
    async def test_failed_task_updates_stats(self, processor_engine):
        """测试失败任务更新统计信息"""
        task = FailingTask(db_connection=processor_engine.db_manager)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            await processor_engine.execute_task("failing_task")
        
        stats = processor_engine.get_stats()
        assert stats["total_tasks"] == 1
        assert stats["failed_tasks"] == 1


class TestProcessorEngineErrorHandling:
    """测试错误处理功能 (Requirements 5.3)"""
    
    @pytest.mark.asyncio
    async def test_continue_on_task_failure(self, processor_engine):
        """测试任务失败后继续执行其他任务 (Requirements 5.3)"""
        successful_task = SuccessfulTask(db_connection=processor_engine.db_manager)
        failing_task = FailingTask(db_connection=processor_engine.db_manager)
        
        async def mock_get_task(name):
            if name == "failing_task":
                return failing_task
            return successful_task
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            results = await processor_engine.execute_tasks(
                ["failing_task", "successful_task"],
                parallel=False
            )
        
        # 验证两个任务都被执行
        assert "failing_task" in results
        assert "successful_task" in results
        
        # 验证失败任务的状态
        assert results["failing_task"]["status"] == "error"
        
        # 验证成功任务仍然执行
        assert results["successful_task"]["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_error_result_contains_error_message(self, processor_engine):
        """测试错误结果包含错误信息"""
        error_msg = "Custom error message"
        task = FailingTask(db_connection=processor_engine.db_manager, error_message=error_msg)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            result = await processor_engine.execute_task("failing_task")
        
        assert result["status"] == "error"
        assert error_msg in result["error"]


class TestProcessorEngineSequentialExecution:
    """测试顺序执行功能"""
    
    @pytest.mark.asyncio
    async def test_sequential_execution(self, processor_engine):
        """测试顺序执行多个任务"""
        task1 = SuccessfulTask(db_connection=processor_engine.db_manager, execution_time=0.05)
        task2 = SuccessfulTask(db_connection=processor_engine.db_manager, execution_time=0.05)
        
        task_map = {"task1": task1, "task2": task2}
        
        async def mock_get_task(name):
            return task_map.get(name, task1)
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            results = await processor_engine.execute_tasks(
                ["task1", "task2"],
                parallel=False
            )
        
        assert len(results) == 2
        assert results["task1"]["status"] == "success"
        assert results["task2"]["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_sequential_execution_order(self, processor_engine):
        """测试顺序执行的顺序正确性"""
        execution_order = []
        
        class OrderTrackingTask(ProcessorTaskBase):
            name = "order_task"
            table_name = "test"
            source_tables = []
            
            def __init__(self, task_id, db_connection=None, **kwargs):
                super().__init__(db_connection=db_connection, **kwargs)
                self.task_id = task_id
            
            async def fetch_data(self, **kwargs):
                return pd.DataFrame()
            
            async def process_data(self, data, stop_event=None, **kwargs):
                execution_order.append(self.task_id)
                return data
            
            async def save_result(self, data, **kwargs):
                pass
            
            async def run(self, **kwargs):
                await self.process_data(pd.DataFrame(), **kwargs)
                return {"status": "success"}
        
        task1 = OrderTrackingTask("first", db_connection=processor_engine.db_manager)
        task2 = OrderTrackingTask("second", db_connection=processor_engine.db_manager)
        task3 = OrderTrackingTask("third", db_connection=processor_engine.db_manager)
        
        task_map = {"task1": task1, "task2": task2, "task3": task3}
        
        async def mock_get_task(name):
            return task_map[name]
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            await processor_engine.execute_tasks(
                ["task1", "task2", "task3"],
                parallel=False
            )
        
        assert execution_order == ["first", "second", "third"]


class TestProcessorEngineStats:
    """测试统计功能 (Requirements 5.2)"""
    
    @pytest.mark.asyncio
    async def test_stats_tracking_multiple_tasks(self, processor_engine):
        """测试多任务执行的统计追踪"""
        successful_task = SuccessfulTask(db_connection=processor_engine.db_manager)
        failing_task = FailingTask(db_connection=processor_engine.db_manager)
        
        async def mock_get_task(name):
            if name == "failing_task":
                return failing_task
            return successful_task
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            await processor_engine.execute_task("successful_task")
            await processor_engine.execute_task("failing_task")
            await processor_engine.execute_task("successful_task_2")
        
        stats = processor_engine.get_stats()
        assert stats["total_tasks"] == 3
        assert stats["successful_tasks"] == 2
        assert stats["failed_tasks"] == 1
        assert stats["success_rate"] == pytest.approx(2/3, rel=0.01)
    
    def test_reset_stats(self, processor_engine):
        """测试重置统计信息"""
        # 手动设置一些统计数据
        processor_engine._execution_stats["total_tasks"] = 10
        processor_engine._execution_stats["successful_tasks"] = 8
        processor_engine._execution_stats["failed_tasks"] = 2
        
        processor_engine.reset_stats()
        
        stats = processor_engine.get_stats()
        assert stats["total_tasks"] == 0
        assert stats["successful_tasks"] == 0
        assert stats["failed_tasks"] == 0
    
    @pytest.mark.asyncio
    async def test_execution_time_tracking(self, processor_engine):
        """测试执行时间追踪"""
        task = SuccessfulTask(db_connection=processor_engine.db_manager, execution_time=0.1)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            result = await processor_engine.execute_task("successful_task")
        
        assert "engine_metadata" in result
        assert "execution_time" in result["engine_metadata"]
        assert result["engine_metadata"]["execution_time"] >= 0.1


class TestProcessorEngineBatchExecution:
    """测试批量执行功能"""
    
    @pytest.mark.asyncio
    async def test_batch_execution_with_different_params(self, processor_engine):
        """测试带不同参数的批量执行"""
        task = SuccessfulTask(db_connection=processor_engine.db_manager)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            results = await processor_engine.execute_batch(
                {
                    "task1": {"param1": "value1"},
                    "task2": {"param2": "value2"}
                },
                parallel=False
            )
        
        assert len(results) == 2
        assert "task1" in results
        assert "task2" in results


class TestProcessorEngineTaskStatusTracking:
    """测试任务状态追踪功能 (Requirements 5.2)"""
    
    @pytest.mark.asyncio
    async def test_get_task_status_after_execution(self, processor_engine):
        """测试执行后获取任务状态"""
        task = SuccessfulTask(db_connection=processor_engine.db_manager)
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            await processor_engine.execute_task("successful_task")
        
        status = processor_engine.get_task_status("successful_task")
        assert status is not None
        assert status.task_name == "successful_task"
        assert status.status == "success"
        assert status.execution_time > 0
    
    @pytest.mark.asyncio
    async def test_get_task_status_for_failed_task(self, processor_engine):
        """测试获取失败任务的状态"""
        task = FailingTask(db_connection=processor_engine.db_manager, error_message="Test error")
        
        with patch.object(processor_engine, '_get_task_instance', return_value=task):
            await processor_engine.execute_task("failing_task")
        
        status = processor_engine.get_task_status("failing_task")
        assert status is not None
        assert status.status == "failed"
        assert "Test error" in status.error_message
    
    @pytest.mark.asyncio
    async def test_get_all_task_status(self, processor_engine):
        """测试获取所有任务状态"""
        successful_task = SuccessfulTask(db_connection=processor_engine.db_manager)
        failing_task = FailingTask(db_connection=processor_engine.db_manager)
        
        async def mock_get_task(name):
            if name == "failing_task":
                return failing_task
            return successful_task
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            await processor_engine.execute_task("successful_task")
            await processor_engine.execute_task("failing_task")
        
        all_status = processor_engine.get_all_task_status()
        assert len(all_status) == 2
        assert "successful_task" in all_status
        assert "failing_task" in all_status
    
    @pytest.mark.asyncio
    async def test_get_failed_tasks(self, processor_engine):
        """测试获取失败任务列表"""
        successful_task = SuccessfulTask(db_connection=processor_engine.db_manager)
        failing_task = FailingTask(db_connection=processor_engine.db_manager)
        
        async def mock_get_task(name):
            if "failing" in name:
                return failing_task
            return successful_task
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            await processor_engine.execute_task("successful_task")
            await processor_engine.execute_task("failing_task_1")
            await processor_engine.execute_task("failing_task_2")
        
        failed_tasks = processor_engine.get_failed_tasks()
        assert len(failed_tasks) == 2
        assert all(t.status == "failed" for t in failed_tasks)
    
    @pytest.mark.asyncio
    async def test_get_successful_tasks(self, processor_engine):
        """测试获取成功任务列表"""
        successful_task = SuccessfulTask(db_connection=processor_engine.db_manager)
        failing_task = FailingTask(db_connection=processor_engine.db_manager)
        
        async def mock_get_task(name):
            if "failing" in name:
                return failing_task
            return successful_task
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            await processor_engine.execute_task("successful_task_1")
            await processor_engine.execute_task("successful_task_2")
            await processor_engine.execute_task("failing_task")
        
        successful_tasks = processor_engine.get_successful_tasks()
        assert len(successful_tasks) == 2
        assert all(t.status == "success" for t in successful_tasks)


class TestProcessorEngineParallelExecution:
    """测试并发执行功能 (Requirements 5.1)"""
    
    @pytest.mark.asyncio
    async def test_parallel_execution_with_semaphore(self, processor_engine):
        """测试使用信号量的并发执行"""
        execution_times = []
        
        class TimedTask(ProcessorTaskBase):
            name = "timed_task"
            table_name = "test"
            source_tables = []
            
            def __init__(self, task_id, db_connection=None, **kwargs):
                super().__init__(db_connection=db_connection, **kwargs)
                self.task_id = task_id
            
            async def fetch_data(self, **kwargs):
                return pd.DataFrame()
            
            async def process_data(self, data, stop_event=None, **kwargs):
                start = datetime.now()
                await asyncio.sleep(0.1)
                end = datetime.now()
                execution_times.append((self.task_id, start, end))
                return data
            
            async def save_result(self, data, **kwargs):
                pass
            
            async def run(self, **kwargs):
                await self.process_data(pd.DataFrame(), **kwargs)
                return {"status": "success"}
        
        tasks = {f"task_{i}": TimedTask(f"task_{i}", db_connection=processor_engine.db_manager) 
                 for i in range(4)}
        
        async def mock_get_task(name):
            return tasks[name]
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            start_time = datetime.now()
            results = await processor_engine.execute_tasks(
                list(tasks.keys()),
                parallel=True
            )
            total_time = (datetime.now() - start_time).total_seconds()
        
        # 验证所有任务都成功
        assert len(results) == 4
        assert all(r["status"] == "success" for r in results.values())
        
        # 并发执行应该比顺序执行快（4个0.1秒任务，并发应该接近0.1秒而非0.4秒）
        # 考虑到开销，设置阈值为0.3秒
        assert total_time < 0.3
    
    @pytest.mark.asyncio
    async def test_parallel_execution_continues_on_failure(self, processor_engine):
        """测试并发执行时失败任务不影响其他任务 (Requirements 5.3)"""
        successful_task = SuccessfulTask(db_connection=processor_engine.db_manager, execution_time=0.05)
        failing_task = FailingTask(db_connection=processor_engine.db_manager)
        
        async def mock_get_task(name):
            if "failing" in name:
                return failing_task
            return successful_task
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            results = await processor_engine.execute_tasks(
                ["successful_task_1", "failing_task", "successful_task_2"],
                parallel=True
            )
        
        # 验证所有任务都有结果
        assert len(results) == 3
        
        # 验证成功任务的状态
        assert results["successful_task_1"]["status"] == "success"
        assert results["successful_task_2"]["status"] == "success"
        
        # 验证失败任务的状态
        assert results["failing_task"]["status"] == "error"


class TestProcessorEngineExecutionReport:
    """测试执行报告功能"""
    
    @pytest.mark.asyncio
    async def test_generate_execution_report(self, processor_engine):
        """测试生成执行报告"""
        successful_task = SuccessfulTask(db_connection=processor_engine.db_manager)
        failing_task = FailingTask(db_connection=processor_engine.db_manager, error_message="Test error")
        
        async def mock_get_task(name):
            if "failing" in name:
                return failing_task
            return successful_task
        
        with patch.object(processor_engine, '_get_task_instance', side_effect=mock_get_task):
            await processor_engine.execute_task("successful_task_1")
            await processor_engine.execute_task("successful_task_2")
            await processor_engine.execute_task("failing_task")
        
        report = processor_engine.generate_execution_report()
        
        # 验证报告结构
        assert "summary" in report
        assert "failed_tasks" in report
        assert "successful_tasks" in report
        
        # 验证摘要
        assert report["summary"]["total_tasks"] == 3
        assert report["summary"]["successful"] == 2
        assert report["summary"]["failed"] == 1
        assert report["summary"]["success_rate"] == pytest.approx(2/3, rel=0.01)
        
        # 验证失败任务列表
        assert len(report["failed_tasks"]) == 1
        assert report["failed_tasks"][0]["task_name"] == "failing_task"
        assert "Test error" in report["failed_tasks"][0]["error"]
        
        # 验证成功任务列表
        assert len(report["successful_tasks"]) == 2


class TestProcessorEngineShutdown:
    """测试引擎关闭功能"""
    
    def test_shutdown(self, mock_db_manager):
        """测试引擎关闭"""
        engine = ProcessorEngine(db_manager=mock_db_manager)
        engine.shutdown()
        # 验证线程池已关闭（不会抛出异常）
        assert True
    
    def test_context_manager(self, mock_db_manager):
        """测试上下文管理器"""
        with ProcessorEngine(db_manager=mock_db_manager) as engine:
            assert engine is not None
        # 退出上下文后引擎应该已关闭
