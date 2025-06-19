#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库组件单元测试

测试重构后的DBManager Mix-in架构
"""

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from alphahome.common.db_components import (
    DatabaseOperationsMixin,
    DBManagerCore,
    SchemaManagementMixin,
    UtilityMixin,
    TableNameResolver,
)
from alphahome.common.db_manager import (
    DBManager,
    create_async_manager,
    create_sync_manager,
)


@pytest.fixture
def mock_db_manager():
    """模拟数据库连接管理器"""
    mock_pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
    return mock_pool


@pytest.fixture
def sample_stock_data():
    """示例股票数据"""
    data = {
        "ts_code": ["000001.SZ", "000002.SZ"],
        "trade_date": ["2023-01-01", "2023-01-01"],
        "open": [10.0, 20.0],
        "high": [11.0, 21.0],
        "low": [9.0, 19.0],
        "close": [10.5, 20.5],
        "vol": [1000000, 2000000],
    }
    return pd.DataFrame(data)


class TestDatabaseOperationsMixin:
    """测试整合的数据库操作Mix-in"""

    @pytest.mark.asyncio
    async def test_execute_query(self, mock_db_manager):
        """测试SQL执行"""

        # 创建一个包含DatabaseOperationsMixin的测试类
        class TestManager(DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager

        # Mock连接和执行
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.execute.return_value = "INSERT 0 1"

        result = await manager.execute(
            "INSERT INTO test_table VALUES ($1)", "test_value"
        )

        assert result == "INSERT 0 1"
        mock_conn.execute.assert_called_once_with(
            "INSERT INTO test_table VALUES ($1)", "test_value"
        )

    @pytest.mark.asyncio
    async def test_fetch_data(self, mock_db_manager):
        """测试数据获取"""

        class TestManager(DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager

        # Mock数据
        mock_records = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = mock_records

        result = await manager.fetch("SELECT * FROM test_table")

        assert result == mock_records
        mock_conn.fetch.assert_called_once_with("SELECT * FROM test_table")

    @pytest.mark.asyncio
    async def test_copy_from_dataframe(self, mock_db_manager, sample_stock_data):
        """测试DataFrame数据批量导入"""

        class TestManager(DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager
        
        # Mock resolver
        manager.resolver = MagicMock()
        manager.resolver.get_schema_and_table.return_value = ("test", "test_table")
        
        # Mock logger
        manager.logger = MagicMock()

        # Mock连接
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.copy_records_to_table.return_value = "COPY 2"

        result = await manager.copy_from_dataframe(sample_stock_data, "test_table")

        # 验证copy操作被调用
        assert result == 2  # 2 rows copied

    @pytest.mark.asyncio
    async def test_upsert_operation(self, mock_db_manager, sample_stock_data):
        """测试upsert操作"""

        class TestManager(DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager
        
        # Mock resolver
        manager.resolver = MagicMock()
        manager.resolver.get_schema_and_table.return_value = ("test", "test_table")
        
        # Mock logger
        manager.logger = MagicMock()

        # Mock连接
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.copy_records_to_table.return_value = "COPY 2"

        result = await manager.upsert(
            sample_stock_data, "test_table", conflict_columns=["ts_code", "trade_date"]
        )

        assert result == 2


class TestSchemaManagementMixin:
    """测试表结构管理Mix-in"""

    @pytest.mark.asyncio
    async def test_table_exists(self, mock_db_manager):
        """测试表存在性检查"""

        class TestManager(SchemaManagementMixin, DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager
        
        # Mock resolver
        manager.resolver = MagicMock()
        manager.resolver.get_schema_and_table.return_value = ("test", "test_table")

        # Mock连接
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetchval.return_value = True

        result = await manager.table_exists("test_table")

        assert result is True
        mock_conn.fetchval.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_table(self, mock_db_manager):
        """测试表创建"""

        class TestManager(SchemaManagementMixin, DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager
        
        # Mock resolver
        manager.resolver = MagicMock()
        manager.resolver.get_schema_and_table.return_value = ("test", "test_table")

        # Mock target with schema_def
        target = MagicMock()
        target.schema_def = {
            "id": "SERIAL PRIMARY KEY",
            "name": "VARCHAR(50)",
            "value": "DECIMAL(10,2)"
        }

        # Mock连接
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn

        await manager.create_table_from_schema(target)

        mock_conn.execute.assert_called_once()


class TestUtilityMixin:
    """测试实用工具Mix-in"""

    @pytest.mark.asyncio
    async def test_get_latest_date(self, mock_db_manager):
        """测试获取最新日期"""

        class TestManager(UtilityMixin, DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager
        
        # Mock resolver
        manager.resolver = MagicMock()
        manager.resolver.get_schema_and_table.return_value = ("test", "test_table")

        # Mock连接
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetchval.return_value = "2023-01-01"

        result = await manager.get_latest_date("test_table", "trade_date")

        assert result == "2023-01-01"

    @pytest.mark.asyncio
    async def test_get_distinct_values(self, mock_db_manager):
        """测试获取唯一值"""

        class TestManager(UtilityMixin, DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager
        
        # Mock resolver
        manager.resolver = MagicMock()
        manager.resolver.get_schema_and_table.return_value = ("test", "test_table")

        # Mock连接和数据
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = [{"distinct_col": "value1"}, {"distinct_col": "value2"}]

        result = await manager.get_distinct_values("test_table", "distinct_col")

        assert len(result) == 2


class TestDBManager:
    """测试完整的DBManager组合"""

    def test_manager_inheritance(self):
        """测试管理器继承关系"""
        from alphahome.common.db_manager import DBManager

        manager = DBManager("postgresql://test:test@localhost/test", mode="async")

        # 验证管理器包含所有必要的Mixin
        assert isinstance(manager, DatabaseOperationsMixin)
        assert isinstance(manager, SchemaManagementMixin)
        assert isinstance(manager, UtilityMixin)
        assert isinstance(manager, DBManagerCore)

    @pytest.mark.asyncio
    async def test_combined_operations(self, mock_db_manager):
        """测试组合操作"""

        class TestManager(DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager

        # Mock连接
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn

        # 可以执行SQL和数据操作
        assert hasattr(manager, "execute")
        assert hasattr(manager, "fetch")
        assert hasattr(manager, "copy_from_dataframe")
        assert hasattr(manager, "upsert")


class TestTableNameResolver:
    """测试表名解析器"""

    def test_resolver_initialization(self):
        """测试解析器初始化"""
        resolver = TableNameResolver()

        assert resolver is not None
        assert hasattr(resolver, "get_schema_and_table")


@pytest.mark.unit
class TestPerformanceAndEdgeCases:
    """测试性能和边界情况"""

    @pytest.mark.asyncio
    async def test_large_dataframe_handling(self, mock_db_manager):
        """测试大数据框处理"""
        # 创建大数据框
        large_df = pd.DataFrame(
            {"id": range(10000), "value": [f"value_{i}" for i in range(10000)]}
        )

        class TestManager(DatabaseOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager

        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn

        # 测试批量处理不会抛出异常
        await manager.copy_from_dataframe(large_df, "large_table", batch_size=1000)

        # 验证分批处理
        assert mock_conn.copy_records_to_table.call_count >= 1

    def test_invalid_connection_string(self):
        """测试无效连接字符串"""
        with pytest.raises(ValueError):
            DBManager("")  # 空连接字符串应该抛出异常

    @pytest.mark.asyncio
    async def test_connection_failure_handling(self):
        """测试连接失败处理"""
        with patch(
            "alphahome.common.db_components.db_manager_core.asyncpg.create_pool"
        ) as mock_create_pool:
            mock_create_pool.side_effect = Exception("Connection failed")

            manager = DBManagerCore(
                "postgresql://invalid:invalid@localhost/invalid", mode="async"
            )

            with pytest.raises(Exception, match="Connection failed"):
                await manager.connect()
