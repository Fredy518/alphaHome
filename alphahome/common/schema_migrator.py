"""
数据库 Schema 自动迁移工具

在应用启动时运行，检查 `public` schema 中是否存在应属于其他数据源 schema 的表，
并自动将它们迁移到正确的位置。
"""

import logging
from typing import List, Type, Dict, Set, Tuple, Any, TYPE_CHECKING

from ..common.db_manager import DBManager
from ..common.task_system.base_task import BaseTask

logger = logging.getLogger(__name__)

# 定义不应被迁移的全局表
EXCLUDED_TABLES = ['task_status']

if TYPE_CHECKING:
    from ..common.db_manager import DBManager

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


async def run_refactoring_check(db_manager: "DBManager", all_tasks: List[Type[BaseTask]]):
    """
    检查并执行数据库表的重构，将旧的、带前缀的物理表重命名为新的、无前缀的名称，
    并为旧的任务名称创建向后兼容的视图。
    
    Args:
        db_manager (DBManager): 数据库管理器实例。
        all_tasks (List[Type[BaseTask]]): 所有已注册的任务类列表。
    """
    logger.info("开始执行数据库表名重构检查...")

    # 1. 从所有任务定义中收集唯一的目标表
    target_tables: Set[Tuple[str, str]] = set()
    for task_def in all_tasks:
        schema = getattr(task_def, "data_source", "public")
        table_name = getattr(task_def, "table_name", None)
        if table_name:
            target_tables.add((schema, table_name))

    if not target_tables:
        logger.info("未找到任何定义了 table_name 的任务，无需重构。")
        return

    logger.info(f"收集到 {len(target_tables)} 个唯一的目标表定义。")

    # --- PASS 1: 重命名物理表 ---
    # 基于一个约定：旧的物理表名是 schema 和 new_name 的组合，例如 "tushare_stock_daily"
    logger.info("PASS 1: 开始检查并重命名旧的物理表...")
    renamed_count = 0
    for schema, new_name in target_tables:
        old_physical_name = f"{schema}_{new_name}"
        try:
            # 构建完整的表标识符
            old_table_identifier = f'"{schema}"."{old_physical_name}"'
            old_table_exists = await db_manager.table_exists(old_table_identifier)
            
            if not old_table_exists:
                continue

            new_table_identifier = f'"{schema}"."{new_name}"'
            new_table_exists = await db_manager.table_exists(new_table_identifier)
            
            if new_table_exists:
                logger.debug(f"新表 '{schema}.{new_name}' 已存在，无需从 '{old_physical_name}' 重命名。")
                continue

            logger.info(f"检测到旧物理表 '{schema}.{old_physical_name}'，将重命名为 '{schema}.{new_name}'...")
            await db_manager.rename_table(old_physical_name, new_name, schema)
            renamed_count += 1
            logger.info(f"成功将表 '{schema}.{old_physical_name}' 重命名为 '{schema}.{new_name}'。")

        except Exception as e:
            logger.error(f"处理表 '{schema}.{old_physical_name}' -> '{schema}.{new_name}' 时发生错误: {e}", exc_info=True)
    
    if renamed_count > 0:
        logger.info(f"PASS 1: 完成，共重命名了 {renamed_count} 个物理表。")
    else:
        logger.info("PASS 1: 未发现需要重命名的物理表。")

    # --- PASS 2: 创建向后兼容的视图 ---
    # 视图的名称是任务的 `name` 属性
    logger.info("PASS 2: 开始检查并创建向后兼容的视图...")
    view_created_count = 0
    for task_def in all_tasks:
        schema = getattr(task_def, "data_source", "public")
        new_name = getattr(task_def, "table_name", None)
        view_name = getattr(task_def, "name", None)  # 旧的任务名，作为视图名

        if not new_name or not view_name or view_name == new_name:
            continue
            
        try:
            view_exists = await db_manager.view_exists(view_name, schema)
            if view_exists:
                continue
            
            # 构建完整的表标识符
            target_table_identifier = f'"{schema}"."{new_name}"'
            target_table_exists = await db_manager.table_exists(target_table_identifier)
            
            if not target_table_exists:
                logger.warning(f"无法为 '{view_name}' 创建视图，因为目标表 '{schema}.{new_name}' 不存在。")
                continue

            logger.info(f"为旧任务名 '{schema}.{view_name}' 创建指向 '{schema}.{new_name}' 的视图...")
            await db_manager.create_view(view_name, new_name, schema)
            view_created_count += 1
            logger.info(f"成功创建视图 '{schema}.{view_name}'。")

        except Exception as e:
            logger.error(f"为 '{schema}.{view_name}' 创建视图时发生错误: {e}", exc_info=True)

    if view_created_count > 0:
        logger.info(f"PASS 2: 完成，共创建了 {view_created_count} 个兼容性视图。")
    else:
        logger.info("PASS 2: 未发现需要创建的新视图。")

    logger.info("数据库表名重构检查完成。") 