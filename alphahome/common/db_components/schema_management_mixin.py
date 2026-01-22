import re
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
            udt_name,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position;
        """
        result = await self.fetch(query, schema, simple_name) # type: ignore

        # 转换为字典列表
        schema_info = []
        for row in result:
            schema_info.append(
                {
                    "column_name": row["column_name"],
                    "data_type": row["data_type"],
                    "udt_name": row["udt_name"],
                    "is_nullable": row["is_nullable"]
                    == "YES",  # 将 'YES'/'NO' 转换为布尔值
                    "default": row["column_default"],
                    "character_maximum_length": row["character_maximum_length"],
                    "numeric_precision": row["numeric_precision"],
                    "numeric_scale": row["numeric_scale"],
                    "datetime_precision": row["datetime_precision"],
                }
            )

        return schema_info

    _VARCHAR_LEN_RE = re.compile(
        r"^\s*(?:character\s+varying|varchar)\s*\(\s*(\d+)\s*\)\s*$",
        flags=re.IGNORECASE,
    )

    def _quote_ident(self, name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    def _parse_varchar_length(self, type_str: str) -> Optional[int]:
        match = self._VARCHAR_LEN_RE.match(type_str or "")
        if not match:
            return None
        try:
            return int(match.group(1))
        except Exception:
            return None

    def _build_safe_text_to_timestamp_using_expr(self, col_name: str) -> str:
        col = f'"{col_name}"'
        # 兼容常见格式：
        # - YYYYMMDD
        # - YYYY-MM-DD
        # - YYYY-MM-DD HH:MM:SS[.MS]
        # 解析失败则置 NULL，避免迁移被异常值阻断。
        return f"""
        CASE
            WHEN {col} IS NULL OR btrim({col}) = '' THEN NULL
            WHEN {col} ~ '^[0-9]{{8}}$' THEN to_date({col}, 'YYYYMMDD')::timestamp
            WHEN {col} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN to_date({col}, 'YYYY-MM-DD')::timestamp
            WHEN {col} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}[ T][0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}(\\.[0-9]{{1,6}})?$' THEN {col}::timestamp
            ELSE NULL
        END
        """.strip()

    async def _get_relation_oid(self, conn: asyncpg.Connection, schema: str, name: str) -> Optional[int]:
        sql = """
            SELECT c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
        """
        return await conn.fetchval(sql, schema, name)

    async def _get_direct_dependent_relations(self, conn: asyncpg.Connection, parent_oid: int):
        sql = """
            SELECT DISTINCT
                v.oid AS oid,
                nv.nspname AS schema,
                v.relname AS name,
                v.relkind AS kind
            FROM pg_depend d
            JOIN pg_rewrite r ON r.oid = d.objid
            JOIN pg_class v ON v.oid = r.ev_class
            JOIN pg_namespace nv ON nv.oid = v.relnamespace
            WHERE d.refobjid = $1
              AND v.relkind IN ('v','m')
        """
        return await conn.fetch(sql, parent_oid)

    async def _collect_dependent_views_closure(
        self, conn: asyncpg.Connection, base_schema: str, base_table: str
    ):
        base_oid = await self._get_relation_oid(conn, base_schema, base_table)
        if base_oid is None:
            return base_oid, {}, [], {}

        edges: Dict[int, set[int]] = {base_oid: set()}
        rels: Dict[int, Dict[str, Any]] = {}
        queue = [base_oid]
        visited = {base_oid}

        while queue:
            current = queue.pop(0)
            edges.setdefault(current, set())
            for row in await self._get_direct_dependent_relations(conn, current):
                oid = row["oid"]
                kind = row["kind"]
                if isinstance(kind, (bytes, bytearray)):
                    kind = kind.decode("utf-8")
                kind = str(kind)

                edges[current].add(oid)
                rels[oid] = {
                    "oid": oid,
                    "schema": row["schema"],
                    "name": row["name"],
                    "kind": kind,
                }

                edges.setdefault(oid, set())
                if oid not in visited:
                    visited.add(oid)
                    queue.append(oid)

        matviews = [r for r in rels.values() if r["kind"] == "m"]
        if matviews:
            return base_oid, rels, [], {}

        drop_order: List[int] = []
        seen: set[int] = set()

        def dfs(node: int):
            if node in seen:
                return
            seen.add(node)
            for child in edges.get(node, set()):
                dfs(child)
            if node != base_oid and rels.get(node, {}).get("kind") == "v":
                drop_order.append(node)

        dfs(base_oid)

        view_defs: Dict[int, str] = {}
        for oid in drop_order:
            view_defs[oid] = await conn.fetchval(
                "SELECT pg_get_viewdef($1::oid, true)", oid
            )

        return base_oid, rels, drop_order, view_defs

    async def ensure_table_schema_compatible(self, target: Any) -> Dict[str, Any]:
        """
        对已存在的表执行“安全范围内”的结构兼容处理：
        1) 自动确保 update_time 为 timestamp（历史版本可能是 varchar(10)）
        2) 仅对 VARCHAR 长度执行“增大”调整（不会缩小）

        该方法不会尝试进行有风险的通用迁移（例如 NUMERIC 精度变更、NOT NULL 强化等）。
        """
        if self.pool is None:  # type: ignore
            await self.connect()  # type: ignore

        schema_def = getattr(target, "schema_def", None)
        if not schema_def:
            return {"status": "skipped", "reason": "no_schema_def", "actions": []}

        schema, table = self.resolver.get_schema_and_table(target)  # type: ignore
        resolved_table_name = f'"{schema}"."{table}"'

        existing_cols = await self.get_table_schema(target)
        existing_by_name = {c["column_name"]: c for c in existing_cols}

        actions: List[str] = []

        auto_add_update_time = getattr(target, "auto_add_update_time", True)
        desired_types: Dict[str, str] = {}
        for col_name, col_def_item in schema_def.items():
            if isinstance(col_def_item, dict):
                desired_types[col_name] = str(col_def_item.get("type", "TEXT"))
            else:
                desired_types[col_name] = str(col_def_item)

        if auto_add_update_time and "update_time" not in desired_types:
            desired_types["update_time"] = "TIMESTAMP WITHOUT TIME ZONE"

        # 0) Add missing columns (safe: only add with type, no NOT NULL enforcement here)
        missing_cols = [c for c in desired_types.keys() if c not in existing_by_name]
        if missing_cols:
            async with self.pool.acquire() as conn:  # type: ignore
                async with conn.transaction():
                    for col_name in missing_cols:
                        col_type = desired_types[col_name]
                        # update_time handled later as well; ADD COLUMN here is safe and idempotent
                        sql = (
                            f'ALTER TABLE {resolved_table_name} '
                            f'ADD COLUMN IF NOT EXISTS "{col_name}" {col_type};'
                        )
                        await conn.execute(sql)
                        actions.append(f"add_column:{col_name}")

            # refresh schema snapshot for subsequent type/length adjustments
            existing_cols = await self.get_table_schema(target)
            existing_by_name = {c["column_name"]: c for c in existing_cols}

        pending_ddls: List[tuple[str, str]] = []

        # 1) Ensure update_time exists + correct type
        if "update_time" in desired_types:
            existing = existing_by_name.get("update_time")
            if not existing:
                sql = (
                    f'ALTER TABLE {resolved_table_name} '
                    f'ADD COLUMN IF NOT EXISTS "update_time" '
                    f'TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP;'
                )
                pending_ddls.append(("add_column:update_time", sql))
            else:
                existing_type = (existing.get("data_type") or "").lower()
                desired_type = desired_types["update_time"].strip().lower()
                if desired_type.startswith("timestamp") and existing_type != "timestamp without time zone":
                    if existing_type in ("character varying", "text", "character"):
                        using_expr = self._build_safe_text_to_timestamp_using_expr("update_time")
                        sql = (
                            f'ALTER TABLE {resolved_table_name} '
                            f'ALTER COLUMN "update_time" TYPE TIMESTAMP WITHOUT TIME ZONE '
                            f'USING ({using_expr});'
                        )
                    elif existing_type == "date":
                        sql = (
                            f'ALTER TABLE {resolved_table_name} '
                            f'ALTER COLUMN "update_time" TYPE TIMESTAMP WITHOUT TIME ZONE '
                            f'USING ("update_time"::timestamp);'
                        )
                    else:
                        sql = (
                            f'ALTER TABLE {resolved_table_name} '
                            f'ALTER COLUMN "update_time" TYPE TIMESTAMP WITHOUT TIME ZONE '
                            f'USING ("update_time"::timestamp);'
                        )

                    pending_ddls.append(
                        (f"alter_type:update_time:{existing_type}->timestamp", sql)
                    )

        # 2) Widen VARCHAR columns when schema_def requires larger size
        for col_name, desired_type in desired_types.items():
            desired_len = self._parse_varchar_length(desired_type)
            if desired_len is None:
                continue

            existing = existing_by_name.get(col_name)
            if not existing:
                continue

            existing_type = (existing.get("data_type") or "").lower()
            if existing_type != "character varying":
                continue

            existing_len = existing.get("character_maximum_length")
            if isinstance(existing_len, int) and existing_len < desired_len:
                sql = (
                    f'ALTER TABLE {resolved_table_name} '
                    f'ALTER COLUMN "{col_name}" TYPE VARCHAR({desired_len});'
                )
                pending_ddls.append(
                    (f"widen_varchar:{col_name}:{existing_len}->{desired_len}", sql)
                )

        if not pending_ddls:
            return {"status": "ok", "actions": []}

        async with self.pool.acquire() as conn:  # type: ignore
            async with conn.transaction():
                base_oid, rels, drop_order, view_defs = await self._collect_dependent_views_closure(
                    conn, schema, table
                )

                if base_oid is None:
                    return {"status": "skipped", "reason": "table_not_found", "actions": []}

                if not drop_order and any(r.get("kind") == "m" for r in rels.values()):
                    return {
                        "status": "skipped",
                        "reason": "dependent_materialized_views",
                        "actions": [],
                    }

                for oid in drop_order:
                    r = rels[oid]
                    fq = f'{self._quote_ident(r["schema"])}.{self._quote_ident(r["name"])}'
                    await conn.execute(f"DROP VIEW IF EXISTS {fq};")

                for action, sql in pending_ddls:
                    await conn.execute(sql)
                    actions.append(action)

                for oid in reversed(drop_order):
                    r = rels[oid]
                    fq = f'{self._quote_ident(r["schema"])}.{self._quote_ident(r["name"])}'
                    view_def = view_defs.get(oid)
                    if view_def:
                        await conn.execute(f"CREATE OR REPLACE VIEW {fq} AS {view_def};")

        return {"status": "ok", "actions": actions}

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
            result = await self.fetch(query, schema, view_name) # type: ignore
            return len(result) > 0
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

    async def create_rawdata_view(
        self,
        view_name: str,
        source_schema: str,
        source_table: str,
        replace: bool = False
    ) -> None:
        """
        在 rawdata schema 创建跨 schema 的映射视图
        
        此方法支持从任意源 schema（tushare, akshare, ifind等）创建映射到 rawdata 的视图。
        PostgreSQL 会自动在 pg_depend 中记录视图对源表的依赖关系，
        使用 DROP TABLE ... CASCADE 时会自动删除相应的视图。
        
        Args:
            view_name: 视图名称（通常与源表同名）
            source_schema: 源表所在 schema（tushare/akshare/ifind等）
            source_table: 源表名称
            replace: 是否使用 OR REPLACE（仅 tushare 数据源使用）
        """
        if self.pool is None: # type: ignore
            await self.connect() # type: ignore

        # 确保 rawdata schema 存在
        await self.ensure_schema_exists('rawdata')
        
        replace_clause = "OR REPLACE " if replace else ""
        
        # 构建跨 schema 的视图创建语句
        create_view_sql = f"""
        CREATE {replace_clause}VIEW rawdata."{view_name}" AS
        SELECT * FROM {source_schema}."{source_table}";
        """
        
        try:
            async with self.pool.acquire() as conn: # type: ignore
                await conn.execute(create_view_sql) # type: ignore
            
            self.logger.info( # type: ignore
                f"成功创建 rawdata 视图 rawdata.{view_name} "
                f"指向 {source_schema}.{source_table}"
            )
            
            # 添加 COMMENT 标记此视图为自动管理
            comment_sql = f"""
            COMMENT ON VIEW rawdata."{view_name}" IS 
            'AUTO_MANAGED: source={source_schema}.{source_table}';
            """
            
            async with self.pool.acquire() as conn: # type: ignore
                await conn.execute(comment_sql) # type: ignore
            
            self.logger.debug( # type: ignore
                f"已为视图 rawdata.{view_name} 添加 AUTO_MANAGED 标记"
            )
        except Exception as e:
            self.logger.error( # type: ignore
                f"创建 rawdata 视图 rawdata.{view_name} 失败: {e}", 
                exc_info=True
            )
            raise

    async def check_table_exists(
        self,
        schema: str,
        table_name: str
    ) -> bool:
        """
        检查指定 schema 中的表是否存在
        
        Args:
            schema: Schema 名称
            table_name: 表名称
            
        Returns:
            bool: 表存在返回 True，否则返回 False
        """
        if self.pool is None: # type: ignore
            await self.connect() # type: ignore

        query = """
        SELECT EXISTS (
            SELECT 1 FROM pg_tables 
            WHERE schemaname = $1 AND tablename = $2
        )
        """
        
        try:
            async with self.pool.acquire() as conn: # type: ignore
                result = await conn.fetchval(query, schema, table_name) # type: ignore
            
            return result if result is not None else False
        except Exception as e:
            self.logger.error( # type: ignore
                f"检查表 {schema}.{table_name} 存在性失败: {e}",
                exc_info=True
            )
            return False

    async def get_tables_in_schema(self, schema: str) -> List[str]:
        """
        获取指定 schema 中的所有表名列表
        
        Args:
            schema: Schema 名称
            
        Returns:
            List[str]: 表名列表（仅包含物理表，不包含视图）
        """
        if self.pool is None: # type: ignore
            await self.connect() # type: ignore

        query = """
        SELECT tablename FROM pg_tables 
        WHERE schemaname = $1
        ORDER BY tablename
        """
        
        try:
            async with self.pool.acquire() as conn: # type: ignore
                rows = await conn.fetch(query, schema) # type: ignore
            
            return [row['tablename'] for row in rows]
        except Exception as e:
            self.logger.error( # type: ignore
                f"获取 schema {schema} 中的表列表失败: {e}",
                exc_info=True
            )
            return []
