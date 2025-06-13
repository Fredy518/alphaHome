"""
数据库 Schema 自动迁移工具

在应用启动时运行，检查 `public` schema 中是否存在应属于其他数据源 schema 的表，
并自动将它们迁移到正确的位置。
"""

import logging
from typing import List, Type

from ..common.db_manager import DBManager
from ..common.task_system.base_task import BaseTask

logger = logging.getLogger(__name__)

# 定义不应被迁移的全局表
EXCLUDED_TABLES = ['task_status']


async def run_migration_check(db_manager: DBManager, task_registry: List[Type[BaseTask]]):
    """
    执行 schema 迁移检查和操作。

    Args:
        db_manager (DBManager): 数据库管理器实例。
        task_registry (List[Type[BaseTask]]): 所有已注册的任务类列表。
    """
    logger.info("开始执行数据库 schema 自动迁移检查...")
    migrated_count = 0

    for task_class in task_registry:
        table_name = getattr(task_class, 'table_name', None)
        data_source = getattr(task_class, 'data_source', None)

        if not table_name:
            logger.debug(f"任务类 {task_class.__name__} 没有 table_name 属性，跳过。")
            continue

        if table_name in EXCLUDED_TABLES:
            logger.debug(f"表 '{table_name}' 在排除列表中，跳过迁移。")
            continue

        if not data_source:
            logger.debug(
                f"任务 {task_class.name} ({table_name}) 未定义 data_source，"
                "假定其属于 public schema，跳过迁移。"
            )
            continue
            
        try:
            # 1. 检查表是否存在于 public schema
            public_table_identifier = f"public.{table_name}"
            table_in_public = await db_manager.table_exists(public_table_identifier)

            if table_in_public:
                logger.info(f"发现表 '{table_name}' 存在于 public schema 中，准备迁移...")

                # 2. 确保目标 schema 存在
                target_schema = data_source
                await db_manager.ensure_schema_exists(target_schema)

                # 3. 执行迁移
                alter_query = f'ALTER TABLE "public"."{table_name}" SET SCHEMA "{target_schema}"'
                await db_manager.execute(alter_query)
                
                migrated_count += 1
                logger.info(
                    f"✅ 成功将表 'public.{table_name}' "
                    f"迁移到 '{target_schema}.{table_name}'"
                )

        except Exception as e:
            logger.error(
                f"处理表 '{table_name}' 的迁移时发生严重错误: {e}", exc_info=True
            )
            # 不重新抛出异常，以确保一个表的失败不会中断整个应用的启动

    if migrated_count > 0:
        logger.info(f"数据库 schema 自动迁移完成，共迁移了 {migrated_count} 个表。")
    else:
        logger.info("数据库 schema 自动迁移检查完成，无需迁移任何表。") 