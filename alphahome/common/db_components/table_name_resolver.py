"""
表名解析器

负责将任务对象或表名字符串解析为数据库中完整的、带schema的表名。
"""

import logging
from typing import Any, Union

logger = logging.getLogger(__name__)


class TableNameResolver:
    """
    一个用于解析数据库表名的类。

    它可以根据输入的类型（任务对象或字符串）来确定最终的、
    包含schema的完整表名，如 'tushare.stock_basic' 或 'public.legacy_table'。
    """

    def get_schema_and_table(self, target: Any) -> tuple[str, str]:
        """
        解析目标，返回一个包含schema和表名的元组。

        Args:
            target (Any):
                可以是任务对象（任何拥有 table_name 和 data_source 属性的对象）
                或表名字符串。

        Returns:
            tuple[str, str]: (schema_name, table_name)
        """
        if isinstance(target, str):
            if "." in target:
                parts = target.split('.', 1)
                schema = parts[0].strip().strip('"')
                table = parts[1].strip().strip('"')
                return schema, table
            else:
                return 'public', target.strip().strip('"')
        
        elif hasattr(target, 'table_name'):
            table = target.table_name
            # 确保data_source存在且不为空，否则默认为public
            schema = getattr(target, 'data_source', 'public') or 'public' 
            return schema, table
            
        else:
            raise TypeError(
                f"不支持的解析目标类型: {type(target)}。必须是 str 或拥有 'table_name' 属性的对象。"
            )

    def get_full_name(self, target: Any) -> str:
        """
        获取包含schema的完整表名。

        Args:
            target (Any):
                可以是任务对象（任何拥有 table_name 和 data_source 属性的对象）
                或表名字符串。

        Returns:
            str: 格式为 'schema_name.table_name' 的完整表名。
        """
        schema, table = self.get_schema_and_table(target)
        # 返回不带引号的版本，因为调用者会处理引号
        return f"{schema}.{table}"

    def get_full_name_old(self, target: Any) -> str:
        """
        获取包含schema的完整表名。

        Args:
            target (Any):
                可以是任务对象（任何拥有 table_name 和 data_source 属性的对象）
                或表名字符串。

        Returns:
            str: 格式为 'schema_name.table_name' 的完整表名。
        """
        if isinstance(target, str):
            # --- 输入是字符串 ---
            if "." in target:
                # 字符串中已包含schema，直接返回，确保格式正确
                parts = target.split('.')
                schema_name = parts[0].strip('"')
                table_name = parts[1].strip('"')
                return f'{schema_name}.{table_name}'
            else:
                # 字符串中不包含schema，默认使用public
                full_name = f'public.{target.strip()}'
                logger.debug(
                    f"解析字符串 '{target}' (不含schema)，默认指向 -> {full_name}"
                )
                return full_name
        
        # --- 输入是类任务对象 (Duck Typing) ---
        elif hasattr(target, 'table_name') and hasattr(target, 'data_source'):
            schema_name = getattr(target, 'data_source', None)
            table_name = target.table_name

            if schema_name:
                # 如果任务定义了data_source，则使用它作为schema
                full_name = f'{schema_name}.{table_name}'
                task_name_attr = getattr(target, 'name', '未知任务')
                logger.debug(f"解析任务对象 '{task_name_attr}' -> {full_name}")
                return full_name
            else:
                # 任务未定义data_source，默认使用public schema
                full_name = f'public.{table_name}'
                task_name_attr = getattr(target, 'name', '未知任务')
                logger.debug(
                    f"任务对象 '{task_name_attr}' 未定义data_source，默认指向 -> {full_name}"
                )
                return full_name
        
        else:
            # --- 输入类型不支持 ---
            raise TypeError(
                f"不支持的解析目标类型: {type(target)}。必须是 str 或拥有 "
                "'table_name' 和 'data_source' 属性的对象。"
            ) 