#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pytest配置文件

提供测试所需的fixtures和配置。
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any, List
import tempfile
import os


@pytest.fixture
def sample_stock_data():
    """创建示例股票数据"""
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    np.random.seed(42)  # 确保可重复性
    
    data = pd.DataFrame({
        'ts_code': ['000001.SZ'] * 100,
        'trade_date': [d.strftime('%Y%m%d') for d in dates],
        'open': 10 + np.random.randn(100) * 0.5,
        'high': 11 + np.random.randn(100) * 0.5,
        'low': 9 + np.random.randn(100) * 0.5,
        'close': 10.5 + np.random.randn(100) * 0.5,
        'vol': np.random.randint(1000000, 5000000, 100),
        'amount': np.random.randint(10000000, 50000000, 100)
    })
    
    # 确保价格关系合理
    data['high'] = np.maximum(data['high'], data[['open', 'close']].max(axis=1))
    data['low'] = np.minimum(data['low'], data[['open', 'close']].min(axis=1))
    
    return data


@pytest.fixture
def sample_adj_factor_data():
    """创建示例复权因子数据"""
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    
    data = pd.DataFrame({
        'ts_code': ['000001.SZ'] * 100,
        'trade_date': [d.strftime('%Y%m%d') for d in dates],
        'adj_factor': np.ones(100)  # 简单起见，复权因子都设为1
    })
    
    return data


@pytest.fixture
def sample_merged_data(sample_stock_data, sample_adj_factor_data):
    """创建合并后的股票数据（包含复权因子）"""
    merged = pd.merge(
        sample_stock_data, 
        sample_adj_factor_data,
        on=['ts_code', 'trade_date'],
        how='inner'
    )
    return merged


@pytest.fixture
def sample_dirty_data():
    """创建包含脏数据的示例数据"""
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 1, 2],  # 包含重复
        'price': [10.0, 11.0, None, 12.0, -1.0, 10.0, 11.0],  # 包含缺失值和负数
        'volume': [1000, 1100, 1200, None, 1400, 1000, 1100],  # 包含缺失值
        'date': pd.date_range('2024-01-01', periods=7)
    })
    return data


@pytest.fixture
def mock_db_connection():
    """模拟数据库连接"""
    class MockDBConnection:
        def __init__(self):
            self.data_store = {}
        
        async def fetch_data(self, table_name: str, **kwargs) -> pd.DataFrame:
            """模拟数据获取"""
            return self.data_store.get(table_name, pd.DataFrame())
        
        async def save_data(self, data: pd.DataFrame, table_name: str, **kwargs):
            """模拟数据保存"""
            self.data_store[table_name] = data.copy()
        
        def set_mock_data(self, table_name: str, data: pd.DataFrame):
            """设置模拟数据"""
            self.data_store[table_name] = data.copy()
    
    return MockDBConnection()


@pytest.fixture
def temp_config_dir():
    """创建临时配置目录"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def basic_config():
    """基础配置"""
    return {
        "max_workers": 2,
        "timeout": 30,
        "continue_on_error": False,
        "collect_stats": True
    }


@pytest.fixture
def event_loop():
    """为异步测试提供事件循环"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


class AsyncTestHelper:
    """异步测试辅助类"""
    
    @staticmethod
    async def run_async_test(coro):
        """运行异步测试"""
        return await coro
    
    @staticmethod
    def create_mock_async_func(return_value=None, side_effect=None):
        """创建模拟异步函数"""
        async def mock_func(*args, **kwargs):
            if side_effect:
                if isinstance(side_effect, Exception):
                    raise side_effect
                return side_effect(*args, **kwargs)
            return return_value
        return mock_func


@pytest.fixture
def async_helper():
    """异步测试辅助工具"""
    return AsyncTestHelper()


class TestDataValidator:
    """测试数据验证器"""
    
    @staticmethod
    def validate_dataframe_structure(df: pd.DataFrame, expected_columns: List[str]):
        """验证DataFrame结构"""
        assert isinstance(df, pd.DataFrame), "结果应该是DataFrame"
        assert not df.empty, "DataFrame不应该为空"
        
        missing_cols = set(expected_columns) - set(df.columns)
        assert not missing_cols, f"缺少必要的列: {missing_cols}"
    
    @staticmethod
    def validate_price_data(df: pd.DataFrame):
        """验证价格数据的合理性"""
        price_cols = ['open', 'high', 'low', 'close']
        available_price_cols = [col for col in price_cols if col in df.columns]
        
        for col in available_price_cols:
            assert (df[col] > 0).all(), f"{col}列包含非正数值"
        
        if 'high' in df.columns and 'low' in df.columns:
            assert (df['high'] >= df['low']).all(), "最高价应该大于等于最低价"
    
    @staticmethod
    def validate_execution_result(result: Dict[str, Any]):
        """验证执行结果的结构"""
        assert isinstance(result, dict), "执行结果应该是字典"
        assert "status" in result, "结果应该包含status字段"
        assert result["status"] in ["success", "error"], "status应该是success或error"
        
        if result["status"] == "success":
            assert "data" in result, "成功结果应该包含data字段"
        else:
            assert "error" in result, "错误结果应该包含error字段"


@pytest.fixture
def data_validator():
    """数据验证器"""
    return TestDataValidator()


# 测试标记
pytest_plugins = []

def pytest_configure(config):
    """pytest配置"""
    config.addinivalue_line(
        "markers", "unit: 单元测试"
    )
    config.addinivalue_line(
        "markers", "integration: 集成测试"
    )
    config.addinivalue_line(
        "markers", "slow: 慢速测试"
    )
    config.addinivalue_line(
        "markers", "async_test: 异步测试"
    )
