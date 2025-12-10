#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ProcessorTaskBase 增强功能单元测试

测试数据分层架构的核心功能：
- 执行流程：fetch → clean → feature → save (Requirements 8.1)
- skip_features 行为 (Requirements 8.5)
- feature_dependencies 校验 (Requirements 8.2, 8.3)
- clean_data 方法 (Requirements 8.1)
"""

import pytest
import asyncio
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from alphahome.processors.tasks.base_task import ProcessorTaskBase
from alphahome.processors.clean import (
    TableSchema,
    ValidationResult,
    ValidationError,
    LineageTracker,
)


class MockDBManager:
    """模拟数据库管理器"""
    
    def __init__(self):
        self.data_store = {}
        self.clean_data_store = {}
    
    async def table_exists(self, task) -> bool:
        return True
    
    async def upsert(self, df, target, conflict_columns, update_columns, timestamp_column=None):
        self.data_store[target.table_name] = df.copy()
        return len(df)
    
    async def copy_from_dataframe(self, df, target):
        self.data_store[target.table_name] = df.copy()
        return len(df)


class EnhancedFlowTask(ProcessorTaskBase):
    """用于测试增强执行流程的任务"""
    name = "enhanced_flow_task"
    table_name = "test_feature_output"
    source_tables = ["test_input"]
    clean_table = "clean.test_clean_output"
    primary_keys = ["trade_date", "ts_code"]
    feature_dependencies = []
    skip_features = False
    
    def __init__(self, db_connection=None, **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.execution_order: List[str] = []
        self.clean_data_called = False
        self.compute_features_called = False
        self.save_result_called = False
        self._save_to_clean_called = False
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        self.execution_order.append("fetch_data")
        return pd.DataFrame({
            "trade_date": [20240101, 20240102, 20240103],
            "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
            "close": [10.0, 11.0, 12.0],
            "vol": [1000, 1100, 1200]
        })
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        """实现抽象方法 - 在增强流程中不直接使用"""
        return data
    
    async def clean_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        self.execution_order.append("clean_data")
        self.clean_data_called = True
        # 调用父类的 clean_data 方法
        return await super().clean_data(data, stop_event=stop_event, **kwargs)
    
    async def compute_features(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        self.execution_order.append("compute_features")
        self.compute_features_called = True
        # 简单的特征计算：添加一个移动平均列
        result = data.copy()
        result["close_ma2"] = result["close"].rolling(window=2, min_periods=1).mean()
        return result
    
    async def _save_to_clean(self, data: pd.DataFrame, **kwargs) -> int:
        self.execution_order.append("_save_to_clean")
        self._save_to_clean_called = True
        return len(data)
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        self.execution_order.append("save_result")
        self.save_result_called = True


class SkipFeaturesTask(EnhancedFlowTask):
    """用于测试 skip_features 行为的任务"""
    name = "skip_features_task"
    skip_features = True


class FeatureDependenciesTask(EnhancedFlowTask):
    """用于测试 feature_dependencies 校验的任务"""
    name = "feature_deps_task"
    feature_dependencies = ["rolling_zscore", "rolling_percentile"]


class InvalidFeatureDependenciesTask(EnhancedFlowTask):
    """用于测试无效 feature_dependencies 的任务"""
    name = "invalid_feature_deps_task"
    feature_dependencies = ["non_existent_function", "another_invalid_func"]


class EmptyDataTask(EnhancedFlowTask):
    """返回空数据的测试任务"""
    name = "empty_data_task"
    
    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        self.execution_order.append("fetch_data")
        return pd.DataFrame()
    
    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> pd.DataFrame:
        """实现抽象方法"""
        return data


@pytest.fixture
def mock_db_manager():
    """创建模拟数据库管理器"""
    return MockDBManager()


class TestEnhancedExecutionFlow:
    """测试增强执行流程 (Requirements 8.1)"""
    
    @pytest.mark.asyncio
    async def test_execution_order_fetch_clean_feature_save(self, mock_db_manager):
        """
        测试执行顺序：fetch → clean → feature → save
        
        **Validates: Requirements 8.1**
        """
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        result = await task.run()
        
        # 验证执行顺序
        expected_order = ["fetch_data", "clean_data", "_save_to_clean", "compute_features", "save_result"]
        assert task.execution_order == expected_order, \
            f"执行顺序应为 {expected_order}，实际为 {task.execution_order}"
        
        assert result["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_all_methods_called(self, mock_db_manager):
        """测试所有方法都被调用"""
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        await task.run()
        
        assert task.clean_data_called, "clean_data 应该被调用"
        assert task.compute_features_called, "compute_features 应该被调用"
        assert task.save_result_called, "save_result 应该被调用"
        assert task._save_to_clean_called, "_save_to_clean 应该被调用"
    
    @pytest.mark.asyncio
    async def test_clean_table_in_result(self, mock_db_manager):
        """测试结果包含 clean_table 信息"""
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        result = await task.run()
        
        assert "clean_table" in result
        assert result["clean_table"] == "clean.test_clean_output"


class TestSkipFeaturesFlow:
    """测试 skip_features 行为 (Requirements 8.5)"""
    
    @pytest.mark.asyncio
    async def test_skip_features_class_attribute(self, mock_db_manager):
        """
        测试通过类属性 skip_features=True 跳过特征计算
        
        **Validates: Requirements 8.5**
        """
        task = SkipFeaturesTask(db_connection=mock_db_manager)
        
        result = await task.run()
        
        # 验证特征计算被跳过
        assert not task.compute_features_called, "compute_features 不应该被调用"
        assert not task.save_result_called, "save_result 不应该被调用"
        
        # 验证 clean 数据仍然被处理
        assert task.clean_data_called, "clean_data 应该被调用"
        assert task._save_to_clean_called, "_save_to_clean 应该被调用"
        
        # 验证结果
        assert result["status"] == "success"
        assert result.get("skip_features") == True
    
    @pytest.mark.asyncio
    async def test_skip_features_kwarg_override(self, mock_db_manager):
        """
        测试通过 kwargs 覆盖 skip_features
        
        **Validates: Requirements 8.5**
        """
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        # 通过 kwargs 设置 skip_features=True
        result = await task.run(skip_features=True)
        
        # 验证特征计算被跳过
        assert not task.compute_features_called, "compute_features 不应该被调用"
        assert result.get("skip_features") == True
    
    @pytest.mark.asyncio
    async def test_skip_features_execution_order(self, mock_db_manager):
        """测试 skip_features=True 时的执行顺序"""
        task = SkipFeaturesTask(db_connection=mock_db_manager)
        
        await task.run()
        
        # 验证执行顺序（不包含 compute_features 和 save_result）
        expected_order = ["fetch_data", "clean_data", "_save_to_clean"]
        assert task.execution_order == expected_order, \
            f"执行顺序应为 {expected_order}，实际为 {task.execution_order}"


class TestFeatureDependenciesValidation:
    """测试 feature_dependencies 校验 (Requirements 8.2, 8.3)"""
    
    @pytest.mark.asyncio
    async def test_valid_feature_dependencies(self, mock_db_manager):
        """
        测试有效的 feature_dependencies 校验通过
        
        **Validates: Requirements 8.2, 8.3**
        """
        task = FeatureDependenciesTask(db_connection=mock_db_manager)
        
        # 不应该抛出异常
        result = await task.run()
        assert result["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_invalid_feature_dependencies_raises_error(self, mock_db_manager):
        """
        测试无效的 feature_dependencies 抛出错误
        
        **Validates: Requirements 8.2, 8.3**
        """
        task = InvalidFeatureDependenciesTask(db_connection=mock_db_manager)
        
        result = await task.run()
        
        # 应该返回错误状态
        assert result["status"] == "error"
        assert "Unknown feature dependencies" in result["error"]
    
    def test_validate_feature_dependencies_method(self, mock_db_manager):
        """测试 _validate_feature_dependencies 方法"""
        task = InvalidFeatureDependenciesTask(db_connection=mock_db_manager)
        
        with pytest.raises(ValueError) as exc_info:
            task._validate_feature_dependencies()
        
        assert "Unknown feature dependencies" in str(exc_info.value)
        assert "non_existent_function" in str(exc_info.value)


class TestCleanDataMethod:
    """测试 clean_data 方法 (Requirements 8.1)"""
    
    @pytest.mark.asyncio
    async def test_clean_data_adds_lineage(self, mock_db_manager):
        """
        测试 clean_data 添加血缘元数据
        
        **Validates: Requirements 8.1**
        """
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        input_data = pd.DataFrame({
            "trade_date": [20240101, 20240102],
            "ts_code": ["000001.SZ", "000001.SZ"],
            "close": [10.0, 11.0]
        })
        
        # 直接调用父类的 clean_data（绕过子类的覆盖）
        result = await ProcessorTaskBase.clean_data(task, input_data)
        
        # 验证血缘列被添加
        assert "_source_table" in result.columns
        assert "_processed_at" in result.columns
        assert "_data_version" in result.columns
        assert "_ingest_job_id" in result.columns
    
    @pytest.mark.asyncio
    async def test_clean_data_preserves_original_columns(self, mock_db_manager):
        """测试 clean_data 保留原始列"""
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        input_data = pd.DataFrame({
            "trade_date": [20240101, 20240102],
            "ts_code": ["000001.SZ", "000001.SZ"],
            "close": [10.0, 11.0],
            "custom_col": ["a", "b"]
        })
        
        result = await ProcessorTaskBase.clean_data(task, input_data)
        
        # 验证原始列被保留
        assert "trade_date" in result.columns
        assert "ts_code" in result.columns
        assert "close" in result.columns
        assert "custom_col" in result.columns
    
    @pytest.mark.asyncio
    async def test_clean_data_handles_empty_dataframe(self, mock_db_manager):
        """测试 clean_data 处理空 DataFrame"""
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        input_data = pd.DataFrame()
        
        result = await ProcessorTaskBase.clean_data(task, input_data)
        
        # 空 DataFrame 应该直接返回
        assert result.empty


class TestEmptyDataHandling:
    """测试空数据处理"""
    
    @pytest.mark.asyncio
    async def test_empty_fetch_data_returns_no_data_status(self, mock_db_manager):
        """测试空数据返回 no_data 状态"""
        task = EmptyDataTask(db_connection=mock_db_manager)
        
        result = await task.run()
        
        assert result["status"] == "no_data"
        assert result["rows"] == 0


class TestTaskAttributes:
    """测试任务属性"""
    
    def test_new_attributes_exist(self, mock_db_manager):
        """测试新属性存在"""
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        assert hasattr(task, "clean_table")
        assert hasattr(task, "feature_dependencies")
        assert hasattr(task, "skip_features")
    
    def test_get_task_info_includes_new_attributes(self, mock_db_manager):
        """测试 get_task_info 包含新属性"""
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        
        info = task.get_task_info()
        
        assert "clean_table" in info
        assert "feature_dependencies" in info
        assert "skip_features" in info
        assert info["clean_table"] == "clean.test_clean_output"


class TestCancellation:
    """测试取消功能"""
    
    @pytest.mark.asyncio
    async def test_cancellation_returns_cancelled_status(self, mock_db_manager):
        """测试取消返回 cancelled 状态"""
        task = EnhancedFlowTask(db_connection=mock_db_manager)
        stop_event = asyncio.Event()
        stop_event.set()  # 立即设置取消
        
        result = await task.run(stop_event=stop_event)
        
        assert result["status"] == "cancelled"
        assert result["task"] == "enhanced_flow_task"
