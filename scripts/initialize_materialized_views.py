#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
初始化物化视图系统

创建 materialized_views schema、物化视图、索引和元数据表。

使用方法：
    python scripts/initialize_materialized_views.py

或在 Python 代码中：
    from scripts.initialize_materialized_views import initialize_materialized_views
    initialize_materialized_views()
"""

import sys
import os
import logging
from typing import Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import ConfigManager
from alphahome.processors.materialized_views.database_init import MaterializedViewDatabaseInit


def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def get_db_connection_string() -> str:
    """Get database connection string from config or environment."""
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        return db_url

    try:
        config_manager = ConfigManager()
        db_url = config_manager.get_database_url()
        if db_url:
            return db_url
    except Exception:
        pass

    raise RuntimeError(
        "No database URL configured. Set DATABASE_URL or configure ~/.alphahome/config.json (database.url)."
    )


def initialize_materialized_views(db_manager: Optional[DBManager] = None) -> bool:
    """
    初始化物化视图系统
    
    Args:
        db_manager: 数据库管理器（可选，如果为 None 则创建新实例）
    
    Returns:
        bool: 初始化是否成功
    """
    logger = logging.getLogger(__name__)
    
    try:
        # 创建数据库管理器
        if db_manager is None:
            logger.info("创建数据库管理器...")
            db_url = get_db_connection_string()
            db_manager = DBManager(db_url, mode="sync")
        
        # 获取所有初始化 SQL
        init_sqls = MaterializedViewDatabaseInit.get_all_init_sqls()
        
        logger.info(f"开始初始化物化视图系统，共 {len(init_sqls)} 个 SQL 语句")
        
        # 执行每个 SQL 语句
        for i, sql in enumerate(init_sqls, 1):
            if not sql.strip():
                continue
            
            logger.info(f"执行 SQL {i}/{len(init_sqls)}...")
            logger.debug(f"SQL: {sql[:100]}...")
            
            try:
                db_manager.execute_sync(sql)
                logger.info(f"SQL {i} 执行成功")
            except Exception as e:
                logger.error(f"SQL {i} 执行失败: {e}")
                # 继续执行其他 SQL，不中断
                continue
        
        logger.info("物化视图系统初始化完成")
        return True
        
    except Exception as e:
        logger.error(f"初始化物化视图系统失败: {e}", exc_info=True)
        return False


def main():
    """主函数"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("物化视图系统初始化")
    logger.info("=" * 60)
    
    success = initialize_materialized_views()
    
    if success:
        logger.info("✅ 初始化成功")
        return 0
    else:
        logger.error("❌ 初始化失败")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
