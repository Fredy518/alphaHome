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
    DataOperationsMixin,
    DBManagerCore,
    SchemaManagementMixin,
    SQLOperationsMixin,
    UtilityMixin,
)
from alphahome.common.db_manager import (
    DBManager,
    create_async_manager,
    create_sync_manager,
)


class TestDBManagerCore:
    """测试数据库管理器核心功能"""

    def test_init_async_mode(self):
        """测试异步模式初始化"""
        manager = DBManagerCore("postgresql://test:test@localhost/test", mode="async")
        assert manager.mode == "async"
        assert manager.connection_string == "postgresql://test:test@localhost/test"

    def test_init_sync_mode(self):
        """测试同步模式初始化"""
        manager = DBManagerCore("postgresql://test:test@localhost/test", mode="sync")
        assert manager.mode == "sync"
        assert manager.connection_string == "postgresql://test:test@localhost/test"

    @pytest.mark.asyncio
    async def test_async_connection_lifecycle(self):
        """测试异步连接生命周期"""
        with patch(
            "alphahome.common.db_components.db_manager_core.asyncpg.create_pool"
        ) as mock_create_pool:
            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool

            manager = DBManagerCore(
                "postgresql://test:test@localhost/test", mode="async"
            )

            # 测试连接
            await manager.connect()
            mock_create_pool.assert_called_once()
            assert manager.pool == mock_pool

            # 测试关闭
            await manager.close()
            mock_pool.close.assert_called_once()


class TestSQLOperationsMixin:
    """测试SQL操作Mix-in"""

    @pytest.mark.asyncio
    async def test_execute_query(self, mock_db_manager):
        """测试SQL执行"""

        # 创建一个包含SQLOperationsMixin的测试类
        class TestManager(SQLOperationsMixin, DBManagerCore):
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

        class TestManager(SQLOperationsMixin, DBManagerCore):
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


class TestDataOperationsMixin:
    """测试数据操作Mix-in"""

    @pytest.mark.asyncio
    async def test_copy_from_dataframe(self, mock_db_manager, sample_stock_data):
        """测试DataFrame数据批量导入"""

        class TestManager(DataOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager

        # Mock连接
        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn

        await manager.copy_from_dataframe(sample_stock_data, "test_table")

        # 验证copy_to_table被调用
        mock_conn.copy_to_table.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_operation(self, mock_db_manager, sample_stock_data):
        """测试upsert操作"""

        class TestManager(DataOperationsMixin, SQLOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager

        # Mock execute方法
        manager.execute = AsyncMock(return_value="INSERT 0 2 ON CONFLICT UPDATE 2")

        result = await manager.upsert(
            sample_stock_data, "test_table", conflict_columns=["ts_code", "trade_date"]
        )

        assert "INSERT" in result or "UPDATE" in result
        manager.execute.assert_called()


class TestSchemaManagementMixin:
    """测试表结构管理Mix-in"""

    @pytest.mark.asyncio
    async def test_table_exists(self, mock_db_manager):
        """测试表存在性检查"""

        class TestManager(SchemaManagementMixin, SQLOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.fetch_one = AsyncMock(return_value={"exists": True})

        exists = await manager.table_exists("test_table")

        assert exists is True
        manager.fetch_one.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_table_schema(self, mock_db_manager):
        """测试获取表结构"""

        class TestManager(SchemaManagementMixin, SQLOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")

        mock_schema = [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "name", "data_type": "varchar"},
        ]
        manager.fetch = AsyncMock(return_value=mock_schema)

        schema = await manager.get_table_schema("test_table")

        assert len(schema) == 2
        assert schema[0]["column_name"] == "id"
        manager.fetch.assert_called_once()


class TestUtilityMixin:
    """测试实用工具Mix-in"""

    @pytest.mark.asyncio
    async def test_get_latest_date(self, mock_db_manager):
        """测试获取最新日期"""

        class TestManager(UtilityMixin, SQLOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.fetch_one = AsyncMock(return_value={"max_date": date(2023, 12, 31)})

        latest_date = await manager.get_latest_date("test_table", "date_column")

        assert latest_date == date(2023, 12, 31)
        manager.fetch_one.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_connection(self, mock_db_manager):
        """测试连接测试功能"""

        class TestManager(UtilityMixin, SQLOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.fetch_one = AsyncMock(return_value={"result": 1})

        is_connected = await manager.test_connection()

        assert is_connected is True
        manager.fetch_one.assert_called_once()


class TestDBManager:
    """测试完整的DBManager类"""

    def test_inheritance_chain(self):
        """测试继承链"""
        manager = DBManager("postgresql://test:test@localhost/test")

        # 验证继承链
        assert isinstance(manager, DataOperationsMixin)
        assert isinstance(manager, SchemaManagementMixin)
        assert isinstance(manager, UtilityMixin)
        assert isinstance(manager, SQLOperationsMixin)
        assert isinstance(manager, DBManagerCore)

    def test_factory_functions(self):
        """测试工厂函数"""
        # 测试异步管理器工厂
        async_manager = create_async_manager("postgresql://test:test@localhost/test")
        assert async_manager.mode == "async"
        assert isinstance(async_manager, DBManager)

        # 测试同步管理器工厂
        sync_manager = create_sync_manager("postgresql://test:test@localhost/test")
        assert sync_manager.mode == "sync"
        assert isinstance(sync_manager, DBManager)

    @pytest.mark.asyncio
    async def test_combined_functionality(self, mock_db_manager, sample_stock_data):
        """测试组合功能"""
        manager = create_async_manager("postgresql://test:test@localhost/test")
        manager.pool = mock_db_manager

        # Mock各种方法
        manager.table_exists = AsyncMock(return_value=False)
        manager.execute = AsyncMock(return_value="CREATE TABLE")
        manager.copy_from_dataframe = AsyncMock(return_value=4)

        # 测试完整工作流
        exists = await manager.table_exists("test_table")
        assert not exists

        # 创建表
        await manager.execute("CREATE TABLE test_table (...)")

        # 插入数据
        result = await manager.copy_from_dataframe(sample_stock_data, "test_table")
        assert result == 4


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

        class TestManager(DataOperationsMixin, DBManagerCore):
            pass

        manager = TestManager("postgresql://test:test@localhost/test", mode="async")
        manager.pool = mock_db_manager

        mock_conn = AsyncMock()
        mock_db_manager.acquire.return_value.__aenter__.return_value = mock_conn

        # 测试批量处理不会抛出异常
        await manager.copy_from_dataframe(large_df, "large_table", batch_size=1000)

        # 验证分批处理
        assert mock_conn.copy_to_table.call_count >= 1

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
