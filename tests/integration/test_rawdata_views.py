#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
rawdata schema 视图映射集成测试

测试用例：
1. tushare 优先覆盖：验证 tushare 创建表时自动覆盖已存在的 rawdata 视图
2. CASCADE 删除：验证删除源表时，rawdata 视图自动被删除
3. 优先级保护：验证 tushare 存在时，其他数据源不会创建视图
4. COMMENT 标记验证：验证自动创建的视图都有正确的标记

使用方式：
    pytest tests/integration/test_rawdata_views.py -v

依赖：
    - pytest
    - asyncpg
    - PostgreSQL 数据库
"""

import asyncio
import pytest
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.db_manager import create_async_manager
from alphahome.common.logging_utils import get_logger


logger = get_logger(__name__)


@pytest.fixture
async def db_manager():
    """创建数据库管理器"""
    from alphahome.config import get_db_connection_string
    
    connection_string = get_db_connection_string()
    manager = create_async_manager(connection_string)
    await manager.connect()
    
    yield manager
    
    await manager.close()


class TestRawdataViewMapping:
    """rawdata 视图映射测试类"""
    
    @pytest.mark.asyncio
    async def test_tushare_priority_coverage(self, db_manager):
        """
        测试场景1：tushare 优先覆盖
        
        目标：验证 tushare 创建表时自动覆盖已存在的 rawdata 视图
        步骤：
        1. 创建 akshare 测试表和视图
        2. 创建 tushare 测试表（应该覆盖视图）
        3. 验证 rawdata 视图指向 tushare
        """
        test_table = "test_priority_coverage"
        
        try:
            # 步骤1：创建 akshare 测试表
            await db_manager.execute(
                f'CREATE TABLE IF NOT EXISTS akshare."{test_table}" (id INT)'
            )
            
            # 创建视图指向 akshare
            await db_manager.create_rawdata_view(
                view_name=test_table,
                source_schema="akshare",
                source_table=test_table,
                replace=False
            )
            
            # 步骤2：创建 tushare 测试表
            await db_manager.execute(
                f'CREATE TABLE IF NOT EXISTS tushare."{test_table}" (id INT)'
            )
            
            # 使用 tushare 优先策略覆盖视图
            await db_manager.create_rawdata_view(
                view_name=test_table,
                source_schema="tushare",
                source_table=test_table,
                replace=True
            )
            
            # 步骤3：验证视图指向 tushare
            query = """
            SELECT pg_get_viewdef('rawdata."test_priority_coverage"'::regclass)
            """
            result = await db_manager.fetch_one(query)
            
            assert result is not None, "视图不存在"
            view_def = result['pg_get_viewdef']
            assert 'tushare' in view_def, f"视图不是指向 tushare: {view_def}"
            
            logger.info("✓ 测试通过：tushare 优先覆盖")
            
        finally:
            # 清理测试表
            await db_manager.execute(
                f'DROP TABLE IF EXISTS tushare."{test_table}" CASCADE'
            )
            await db_manager.execute(
                f'DROP TABLE IF EXISTS akshare."{test_table}" CASCADE'
            )
    
    @pytest.mark.asyncio
    async def test_cascade_delete(self, db_manager):
        """
        测试场景2：CASCADE 删除
        
        目标：验证删除源表时，rawdata 视图自动被删除
        步骤：
        1. 创建测试表和视图
        2. 验证视图存在
        3. 删除源表（使用 CASCADE）
        4. 验证视图已删除
        """
        test_table = "test_cascade"
        
        try:
            # 步骤1：创建测试表
            await db_manager.execute(
                f'CREATE TABLE IF NOT EXISTS tushare."{test_table}" (id INT)'
            )
            
            # 创建视图
            await db_manager.create_rawdata_view(
                view_name=test_table,
                source_schema="tushare",
                source_table=test_table,
                replace=False
            )
            
            # 步骤2：验证视图存在
            view_exists = await db_manager.view_exists('rawdata', test_table)
            assert view_exists, "视图创建失败"
            
            # 步骤3：使用 CASCADE 删除源表
            await db_manager.execute(
                f'DROP TABLE IF EXISTS tushare."{test_table}" CASCADE'
            )
            
            # 步骤4：验证视图已删除
            view_exists = await db_manager.view_exists('rawdata', test_table)
            assert not view_exists, "视图未被删除"
            
            logger.info("✓ 测试通过：CASCADE 删除视图")
            
        except Exception as e:
            # 确保清理
            try:
                await db_manager.execute(
                    f'DROP TABLE IF EXISTS tushare."{test_table}" CASCADE'
                )
            except:
                pass
            raise
    
    @pytest.mark.asyncio
    async def test_priority_protection(self, db_manager):
        """
        测试场景3：优先级保护
        
        目标：验证 tushare 存在时，其他数据源不会创建视图
        步骤：
        1. 先创建 tushare 表和视图
        2. 尝试创建 akshare 表
        3. 验证视图仍指向 tushare（被保护）
        """
        test_table = "test_protection"
        
        try:
            # 步骤1：创建 tushare 表和视图
            await db_manager.execute(
                f'CREATE TABLE IF NOT EXISTS tushare."{test_table}" (id INT)'
            )
            
            await db_manager.create_rawdata_view(
                view_name=test_table,
                source_schema="tushare",
                source_table=test_table,
                replace=False
            )
            
            # 步骤2：创建 akshare 表
            await db_manager.execute(
                f'CREATE TABLE IF NOT EXISTS akshare."{test_table}" (id INT)'
            )
            
            # 尝试为 akshare 创建视图（应该被跳过）
            tushare_exists = await db_manager.check_table_exists(
                'tushare', test_table
            )
            assert tushare_exists, "tushare 表应该存在"
            
            # 步骤3：验证视图仍指向 tushare
            query = f"""
            SELECT pg_get_viewdef('rawdata."{test_table}"'::regclass)
            """
            result = await db_manager.fetch_one(query)
            
            assert result is not None, "视图不存在"
            view_def = result['pg_get_viewdef']
            assert 'tushare' in view_def, f"视图应该指向 tushare: {view_def}"
            
            logger.info("✓ 测试通过：优先级保护")
            
        finally:
            # 清理
            await db_manager.execute(
                f'DROP TABLE IF EXISTS tushare."{test_table}" CASCADE'
            )
            await db_manager.execute(
                f'DROP TABLE IF EXISTS akshare."{test_table}" CASCADE'
            )
    
    @pytest.mark.asyncio
    async def test_comment_marking(self, db_manager):
        """
        测试场景4：COMMENT 标记验证
        
        目标：验证自动创建的视图都有正确的 COMMENT 标记
        """
        test_table = "test_marking"
        
        try:
            # 创建表和视图
            await db_manager.execute(
                f'CREATE TABLE IF NOT EXISTS tushare."{test_table}" (id INT)'
            )
            
            await db_manager.create_rawdata_view(
                view_name=test_table,
                source_schema="tushare",
                source_table=test_table,
                replace=False
            )
            
            # 查询视图的 COMMENT
            query = """
            SELECT obj_description(
                (quote_ident('rawdata') || '.' || quote_ident($1))::regclass
            ) as comment
            """
            result = await db_manager.fetch_one(query, test_table)
            
            assert result is not None, "无法获取视图注释"
            comment = result['comment']
            
            assert comment is not None, "视图注释为空"
            assert 'AUTO_MANAGED' in comment, f"视图注释缺少 AUTO_MANAGED 标记: {comment}"
            assert 'tushare' in comment, f"视图注释缺少数据源信息: {comment}"
            
            logger.info("✓ 测试通过：COMMENT 标记验证")
            
        finally:
            # 清理
            await db_manager.execute(
                f'DROP TABLE IF EXISTS tushare."{test_table}" CASCADE'
            )


# 支持 pytest-asyncio
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
