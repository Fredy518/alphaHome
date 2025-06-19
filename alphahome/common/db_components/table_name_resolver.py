"""
表名解析器

负责将任务对象或表名字符串解析为数据库中完整的、带schema的表名。
"""

import logging
from typing import Any, Union

logger = logging.getLogger(__name__)


class TableNameResolver:
    """数据库表名解析器
    
    职责：
    ----
    专门负责将任务对象或表名字符串解析为数据库中完整的、带schema的表名。
    这是数据库操作中的关键组件，确保所有表操作都能正确定位到目标表。
    
    主要功能：
    1. 任务对象到表名的转换
    2. Schema和表名的智能分离
    3. 默认schema的自动应用
    4. 表名格式的标准化
    5. 跨数据源的表名映射
    
    解析规则：
    --------
    **字符串输入**:
    - 包含"."：解析为 schema.table_name
    - 不包含"."：默认为 public.table_name
    
    **任务对象输入**:
    - 使用 data_source 属性作为 schema
    - 使用 table_name 属性作为表名
    - data_source为空时默认使用 public schema
    
    设计特点：
    --------
    1. **Duck Typing**: 支持任何具有table_name属性的对象
    2. **向后兼容**: 支持传统的字符串表名
    3. **Schema隔离**: 基于data_source实现多数据源隔离
    4. **默认处理**: 智能应用默认schema，减少配置复杂度
    5. **格式统一**: 确保所有表名输出格式一致
    
    Schema映射策略：
    --------------
    - tushare数据源 → tushare schema
    - wind数据源 → wind schema  
    - 自定义数据源 → 对应schema
    - 未指定/空值 → public schema
    
    使用示例：
    --------
    ```python
    resolver = TableNameResolver()
    
    # 任务对象解析
    task = TushareStockBasic()  # data_source='tushare', table_name='stock_basic'
    full_name = resolver.get_full_name(task)  # 'tushare.stock_basic'
    
    # 字符串解析
    full_name = resolver.get_full_name('public.user_data')  # 'public.user_data'
    full_name = resolver.get_full_name('temp_table')  # 'public.temp_table'
    ```
    
    与其他组件关系：
    -------------
    - 被所有数据库Mixin组件使用
    - 为SQL构建提供标准化表名
    - 支持多数据源的schema隔离策略
    - 确保表操作的准确性和一致性
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
            
        elif isinstance(target, dict) and "table_name" in target:
            # 支持字典格式的任务定义，如GUI服务中使用的格式
            table = target["table_name"]
            # 使用get方法获取data_source，如果不存在或为None则使用"public"
            schema = target.get("data_source", "public") or "public"
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

        # --- 输入是字典 ---
        elif isinstance(target, dict) and "table_name" in target:
            schema_name = target.get('data_source', 'public') or 'public'
            table_name = target["table_name"]
            full_name = f'{schema_name}.{table_name}'
            
            task_name = target.get('name', '未知任务')
            logger.debug(f"解析任务字典 '{task_name}' -> {full_name}")
            return full_name
        
        else:
            # --- 输入类型不支持 ---
            raise TypeError(
                f"不支持的解析目标类型: {type(target)}。必须是 str 或拥有 "
                "'table_name' 和 'data_source' 属性的对象，或包含这些键的字典。"
            ) 