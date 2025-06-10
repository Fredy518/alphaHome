#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pytest共享配置文件

提供全局fixtures和测试配置
"""

import asyncio
import json
import os

# 添加项目根目录到路径
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ===== 测试配置 =====


@pytest.fixture(scope="session")
def event_loop():
    """创建事件循环，用于异步测试"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_config():
    """测试用配置"""
    return {
        "database": {"url": "postgresql://test:test@localhost:5432/test_db"},
        "tushare": {"token": "test_token_12345"},
        "fetchers": {"default_retry_count": 3, "default_batch_size": 100},
        "processors": {"default_block_size": 50},
    }


@pytest.fixture
def temp_config_file(test_config):
    """创建临时配置文件"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(test_config, f)
        config_path = f.name

    yield config_path

    # 清理
    if os.path.exists(config_path):
        os.unlink(config_path)


# ===== 数据库相关 Fixtures =====


@pytest.fixture
def mock_db_manager():
    """Mock数据库管理器"""
    mock_manager = AsyncMock()
    mock_manager.connect = AsyncMock()
    mock_manager.close = AsyncMock()
    mock_manager.execute = AsyncMock()
    mock_manager.fetch = AsyncMock()
    mock_manager.fetch_one = AsyncMock()
    mock_manager.copy_from_dataframe = AsyncMock()
    mock_manager.table_exists = AsyncMock(return_value=True)
    mock_manager.test_connection = AsyncMock(return_value=True)
    return mock_manager


@pytest.fixture
def sample_stock_data():
    """示例股票数据"""
    return pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ", "000002.SZ"],
            "trade_date": [
                date(2023, 1, 1),
                date(2023, 1, 1),
                date(2023, 1, 2),
                date(2023, 1, 2),
            ],
            "open": [10.0, 20.0, 10.5, 20.2],
            "close": [10.2, 20.1, 10.8, 20.5],
            "high": [10.5, 20.3, 11.0, 20.8],
            "low": [9.8, 19.8, 10.3, 20.0],
            "volume": [1000000, 2000000, 1100000, 2100000],
            "amount": [10200000, 40200000, 11880000, 43050000],
        }
    )


@pytest.fixture
def sample_calendar_data():
    """示例交易日历数据"""
    return pd.DataFrame(
        {
            "cal_date": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
            "is_open": [1, 1, 0],  # 前两天开市，第三天休市
        }
    )


# ===== API相关 Fixtures =====


@pytest.fixture
def mock_tushare_api():
    """Mock Tushare API"""
    mock_api = MagicMock()
    mock_api.daily = MagicMock()
    mock_api.stock_basic = MagicMock()
    mock_api.adj_factor = MagicMock()
    return mock_api


@pytest.fixture
def mock_http_session():
    """Mock HTTP会话"""
    mock_session = AsyncMock()
    mock_session.get = AsyncMock()
    mock_session.post = AsyncMock()
    mock_session.close = AsyncMock()
    return mock_session


# ===== 文件系统相关 Fixtures =====


@pytest.fixture
def temp_data_dir():
    """临时数据目录"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_csv_file(temp_data_dir, sample_stock_data):
    """示例CSV文件"""
    csv_path = os.path.join(temp_data_dir, "sample_stock_data.csv")
    sample_stock_data.to_csv(csv_path, index=False)
    return csv_path


# ===== 测试标记和跳过条件 =====


def pytest_configure(config):
    """配置pytest标记"""
    config.addinivalue_line(
        "markers", "requires_db: mark test as requiring database connection"
    )
    config.addinivalue_line(
        "markers", "requires_api: mark test as requiring external API"
    )
    config.addinivalue_line("markers", "slow: mark test as slow running")


@pytest.fixture
def skip_if_no_db():
    """如果没有数据库连接则跳过测试"""
    # 这里可以添加实际的数据库连接检查逻辑
    # 现在只是示例
    db_available = os.environ.get("TEST_DB_AVAILABLE", "false").lower() == "true"
    if not db_available:
        pytest.skip("Database not available for testing")


# ===== Backtrader相关 Fixtures =====


@pytest.fixture
def mock_backtrader_cerebro():
    """Mock Backtrader Cerebro"""
    mock_cerebro = MagicMock()
    mock_cerebro.addstrategy = MagicMock()
    mock_cerebro.adddata = MagicMock()
    mock_cerebro.run = MagicMock(return_value=[])
    return mock_cerebro


@pytest.fixture
def sample_backtest_result():
    """示例回测结果"""
    return {
        "total_return": 0.15,
        "sharpe_ratio": 1.2,
        "max_drawdown": -0.05,
        "trades_count": 10,
        "win_rate": 0.6,
    }
