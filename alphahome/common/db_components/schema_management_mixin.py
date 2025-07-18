from typing import Any, Dict, List, Optional, Union

import asyncpg


class SchemaManagementMixin:
    """表结构管理Mixin
    
    职责：
    ----
    专门负责数据库表结构的管理和操作，提供完整的DDL操作支持。主要负责：
    1. 表存在性检查和结构查询
    2. 基于任务定义的自动建表
    3. 索引的创建和管理
    4. 表结构元数据的获取
    5. Schema命名空间的管理
    
    核心功能：
    --------
    - **表存在性检查**: 高效查询表是否存在，支持跨schema查询
    - **结构查询**: 获取表的完整结构信息（列、类型、约束等）
    - **自动建表**: 根据任务对象的schema_def自动创建表和索引
    - **索引管理**: 智能创建性能优化索引（日期列、复合索引等）
    - **Schema管理**: 自动创建和管理数据库schema
    
    设计特点：
    --------
    1. **声明式设计**: 基于任务对象的schema_def声明式创建表结构
    2. **智能索引**: 自动为日期列和关键字段创建优化索引
    3. **元数据驱动**: 充分利用PostgreSQL的information_schema
    4. **事务安全**: 所有DDL操作都在事务中执行，确保原子性
    5. **注释支持**: 自动添加列注释，提高数据库可维护性
    
    Schema定义格式：
    --------------
    支持的任务对象属性：
    - schema_def: 表结构定义字典
    - primary_keys: 主键列列表
    - date_column: 日期列名（用于自动索引）
    - indexes: 额外索引定义列表
    - auto_add_update_time: 是否自动添加更新时间列
    
    索引策略：
    --------
    - 日期列自动索引：提高时间范围查询性能
    - 主键自动约束：确保数据唯一性
    - 自定义索引：支持复合索引和唯一索引
    - 命名规范：遵循统一的索引命名规则
    
    适用场景：
    --------
    - 新任务的首次表创建
    - 数据库迁移和结构升级
    - 表结构的运行时查询
    - 多数据源的schema隔离
    
    与其他组件关系：
    -------------
    - 使用SQLOperationsMixin执行DDL语句
    - 配合TableNameResolver进行表名解析
    - 为DataOperationsMixin提供表结构信息
    """

    async def table_exists(self, target: Any) -> bool:
        """检查表是否存在

        Args:
            target (Any): 表名字符串或任务对象

        Returns:
            bool: 如果表存在则返回True，否则返回False
        """
        if self.pool is None: # type: ignore
            await self.connect() # type: ignore

        # 解析表名以支持schema
        resolved_table_name = self.resolver.get_full_name(target) # type: ignore
        schema, simple_name = resolved_table_name.split('.')
        schema = schema.strip('"')
        simple_name = simple_name.strip('"')

        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
        );
        """
        async with self.pool.acquire() as conn: # type: ignore
            result = await conn.fetchval(query, schema, simple_name)
        return result if result is not None else False

    async def is_physical_table(self, schema: str, table_name: str) -> bool:
        """检查一个给定的名称是否是物理表（而不是视图）。

        Args:
            schema (str): Schema名称。
            table_name (str): 表名。

        Returns:
            bool: 如果是物理表则返回True，否则返回False。
        """
        if self.pool is None: # type: ignore
            await self.connect() # type: ignore

        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE'
        );
        """
        async with self.pool.acquire() as conn: # type: ignore
            result = await conn.fetchval(query, schema, table_name)
        return result if result is not None else False

    async def get_table_schema(self, target: Any) -> List[Dict[str, Any]]:
        """获取表结构

        Args:
            target (Any): 表名字符串或任务对象

        Returns:
            List[Dict[str, Any]]: 表结构信息列表，每个元素是一个字典，
                                 包含列名、数据类型、是否可为空、默认值等信息。
        """
        resolved_table_name = self.resolver.get_full_name(target) # type: ignore
        schema, simple_name = resolved_table_name.split('.')
        schema = schema.strip('"')
        simple_name = simple_name.strip('"')
        
        query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable, 
            column_default
        FROM 
            information_schema.columns
        WHERE 
            table_schema = $1 AND table_name = $2
        ORDER BY 
            ordinal_position;
        """
        result = await self.fetch(query, schema, simple_name) # type: ignore

        # 转换为字典列表
        schema_info = []
        for row in result:
            schema_info.append(
                {
                    "column_name": row["column_name"],
                    "data_type": row["data_type"],
                    "is_nullable": row["is_nullable"]
                    == "YES",  # 将 'YES'/'NO' 转换为布尔值
                    "default": row["column_default"],
                }
            )

        return schema_info

    async def create_table_from_schema(self, target: Any):
        """根据任务对象定义的 schema (结构) 创建数据库表和相关索引。"""
        if self.pool is None: # type: ignore
            await self.connect() # type: ignore

        # 从任务对象中提取属性
        schema_def = getattr(target, "schema_def", None)
        primary_keys = getattr(target, "primary_keys", None)
        date_column = getattr(target, "date_column", None)
        indexes = getattr(target, "indexes", None)
        auto_add_update_time = getattr(target, "auto_add_update_time", True)

        resolved_table_name = self.resolver.get_full_name(target) # type: ignore

        if not schema_def:  # schema 定义不能为空
            raise ValueError(
                f"无法创建表 '{resolved_table_name}'，未提供 schema_def (表结构定义)。"
            )
        
        schema, simple_name = resolved_table_name.split('.')
        schema = schema.strip('"')

        async with self.pool.acquire() as conn: # type: ignore
            async with conn.transaction():  # 为DDL（数据定义语言）操作使用事务
                try:
                    # --- 0. 确保 schema 存在 ---
                    await self.ensure_schema_exists(schema)
                    
                    # --- 1. 构建 CREATE TABLE 语句 ---
                    columns = []
                    for col_name, col_def_item in schema_def.items():
                        if isinstance(
                            col_def_item, dict
                        ):  # 如果列定义是字典 (包含类型和约束)
                            col_type = col_def_item.get("type", "TEXT")  # 默认类型为TEXT
                            constraints_val = col_def_item.get(
                                "constraints"
                            )  # 获取原始约束值
                            constraints_str = (
                                str(constraints_val).strip()
                                if constraints_val is not None
                                else ""
                            )
                            columns.append(
                                f'"{col_name}" {col_type} {constraints_str}'.strip()
                            )
                        else:  # 如果列定义只是字符串 (类型)
                            columns.append(f'"{col_name}" {col_def_item}')

                    # 添加 update_time 列（如果配置需要且Schema中不存在）
                    if auto_add_update_time and "update_time" not in schema_def:
                        columns.append(
                            '"update_time" TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP'
                        )

                    # 添加主键约束
                    if (
                        primary_keys
                        and isinstance(primary_keys, list)
                        and len(primary_keys) > 0
                    ):
                        pk_cols_str = ", ".join([f'"{pk}"' for pk in primary_keys])
                        columns.append(f"PRIMARY KEY ({pk_cols_str})")

                    columns_str = ", ".join(columns)
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {resolved_table_name} (
                        {",\n            ".join(columns)}
                    );
                    """

                    self.logger.info( # type: ignore
                        f"准备为表 '{resolved_table_name}' 执行建表语句:\n{create_table_sql}"
                    )
                    await conn.execute(create_table_sql) # type: ignore
                    self.logger.info(f"表 '{resolved_table_name}' 创建成功或已存在。") # type: ignore

                    # --- 1.1 添加列注释 ---
                    for col_name, col_def_item in schema_def.items():
                        if isinstance(col_def_item, dict) and "comment" in col_def_item:
                            comment_text = col_def_item["comment"]
                            if comment_text is not None:
                                # 转义 comment_text 中的单引号，防止SQL注入或语法错误
                                escaped_comment_text = str(comment_text).replace(
                                    "'", "''"
                                )
                                comment_sql = f"COMMENT ON COLUMN {resolved_table_name}.\"{col_name}\" IS '{escaped_comment_text}';"
                                self.logger.info( # type: ignore
                                    f"准备为列 '{resolved_table_name}.{col_name}' 添加注释: {comment_sql}"
                                )
                                await conn.execute(comment_sql) # type: ignore
                                self.logger.debug( # type: ignore
                                    f"为列 '{resolved_table_name}.{col_name}' 添加注释成功。"
                                )

                    # --- 2. 构建并执行 CREATE INDEX 语句 ---
                    # 为 date_column 创建索引 (如果需要且不是主键的一部分)
                    if date_column and date_column not in (primary_keys or []):
                        index_name_date = f"idx_{simple_name}_{date_column}"
                        create_index_sql_date = f'CREATE INDEX IF NOT EXISTS "{index_name_date}" ON {resolved_table_name} ("{date_column}");'
                        self.logger.info( # type: ignore
                            f"准备为 '{resolved_table_name}.{date_column}' 创建索引: {index_name_date}"
                        )
                        await conn.execute(create_index_sql_date) # type: ignore
                        self.logger.info(f"索引 '{index_name_date}' 创建成功或已存在。") # type: ignore

                    # 创建 schema 中定义的其他索引
                    if indexes and isinstance(indexes, list):
                        for index_def in indexes:
                            index_name = None
                            index_columns_str = None
                            unique = False

                            if isinstance(index_def, dict):  # 索引定义是字典
                                index_columns_list = index_def.get("columns")
                                if not index_columns_list:
                                    self.logger.warning( # type: ignore
                                        f"跳过无效的索引定义 (缺少 columns): {index_def}"
                                    )
                                    continue
                                # 将列名或列名列表转换为SQL字符串
                                if isinstance(index_columns_list, str):
                                    index_columns_str = f'"{index_columns_list}"'
                                elif isinstance(index_columns_list, list):
                                    index_columns_str = ", ".join(
                                        [f'"{col}"' for col in index_columns_list]
                                    )
                                else:
                                    self.logger.warning( # type: ignore 
                                        f"索引定义中的 'columns' 类型无效: {index_columns_list}"
                                    )
                                    continue

                                # 规范化索引名称中列名的部分，移除特殊字符
                                safe_cols_for_name = (
                                    str(index_columns_list)
                                    .replace(" ", "")
                                    .replace('"', "")
                                    .replace("[", "")
                                    .replace("]", "")
                                    .replace("'", "")
                                    .replace(",", "_")
                                )
                                index_name = index_def.get(
                                    "name", f"idx_{simple_name}_{safe_cols_for_name}"
                                )
                                unique = index_def.get("unique", False)

                            elif isinstance(index_def, str):  # 索引定义是单个列名字符串
                                index_columns_str = f'"{index_def}"'
                                index_name = f"idx_{simple_name}_{index_def}"
                            else:  # 未知格式
                                self.logger.warning( # type: ignore
                                    f"跳过未知格式的索引定义: {index_def}"
                                )
                                continue

                            unique_str = "UNIQUE " if unique else ""
                            create_index_sql = f'CREATE {unique_str}INDEX IF NOT EXISTS "{index_name}" ON {resolved_table_name} ({index_columns_str});'
                            self.logger.info( # type: ignore
                                f"准备创建索引 '{index_name}' 于 '{resolved_table_name}({index_columns_str})': {unique_str.strip()}"
                            )
                            await conn.execute(create_index_sql) # type: ignore
                            self.logger.info(f"索引 '{index_name}' 创建成功或已存在。") # type: ignore

                except Exception as e:
                    self.logger.error( # type: ignore
                        f"创建表或索引 '{resolved_table_name}' 时失败: {e}", exc_info=True
                    )
                    raise

    async def ensure_schema_exists(self, schema_name: str):
        """确保指定的 schema 存在，如果不存在则创建。

        Args:
            schema_name (str): 需要确保其存在的 schema 的名称。
        """
        if not schema_name or not isinstance(schema_name, str) or "." in schema_name:
            self.logger.error(f"无效的 schema 名称: '{schema_name}'") # type: ignore
            return

        try:
            # 使用参数化查询来防止SQL注入，尽管这里风险较低
            await self.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"') # type: ignore
            self.logger.info(f"Schema '{schema_name}' 创建成功或已存在。") # type: ignore
        except Exception as e:
            self.logger.error(f"创建 schema '{schema_name}' 时失败: {e}", exc_info=True) # type: ignore
            raise

    async def rename_table(self, old_table_name: str, new_table_name: str, schema: str = "public"):
        """重命名数据库中的表。"""
        query = f'ALTER TABLE "{schema}"."{old_table_name}" RENAME TO "{new_table_name}";'
        try:
            await self.execute(query) # type: ignore
            self.logger.info(f"成功将表 '{schema}.{old_table_name}' 重命名为 '{new_table_name}'。") # type: ignore
        except Exception as e:
            self.logger.error(f"重命名表 '{schema}.{old_table_name}' 失败: {e}", exc_info=True) # type: ignore
            raise

    async def view_exists(self, view_name: str, schema: str = "public") -> bool:
        """检查指定的视图是否存在于数据库中。"""
        query = """
        SELECT 1 FROM information_schema.views 
        WHERE table_schema = $1 AND table_name = $2;
        """
        try:
            result = await self.fetch_one(query, schema, view_name) # type: ignore
            return result is not None
        except Exception as e:
            self.logger.error(f"检查视图 '{schema}.{view_name}' 是否存在时失败: {e}", exc_info=True) # type: ignore
            return False

    async def create_view(self, view_name: str, target_table_name: str, schema: str = "public"):
        """创建一个指向目标表的视图。"""
        query = f'CREATE OR REPLACE VIEW "{schema}"."{view_name}" AS SELECT * FROM "{schema}"."{target_table_name}";'
        try:
            await self.execute(query) # type: ignore
            self.logger.info(f"成功创建或更新视图 '{schema}.{view_name}' 以指向 '{target_table_name}'。") # type: ignore
        except Exception as e:
            self.logger.error(f"创建视图 '{schema}.{view_name}' 失败: {e}", exc_info=True) # type: ignore
            raise
