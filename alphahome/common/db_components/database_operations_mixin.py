"""
整合数据库操作Mixin - 提供完整的数据库操作接口

该模块整合了原本的SQLOperationsMixin和DataOperationsMixin的所有功能，
提供统一的数据库操作接口，简化架构并提高维护效率。

保留了所有原有功能，确保向后兼容性。
"""

import asyncio
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union

import asyncpg
import pandas as pd
import psycopg2.extras


class DatabaseOperationsMixin:
    """整合数据库操作Mixin
    
    职责：
    ----
    整合了基础SQL操作和高级数据操作功能，提供完整的数据库操作接口。
    这是原SQLOperationsMixin和DataOperationsMixin的整合版本，保留了所有原有功能。
    
    功能分层：
    --------
    **基础SQL操作层**（原SQLOperationsMixin功能）：
    - execute系列: 执行SQL语句，返回影响行数或执行状态
    - fetch系列: 执行查询，返回结果集（全部/单行/单值）
    - executemany: 批量执行相同SQL的多组参数
    - 同步模式接口: 支持同步环境的操作
    
    **高级数据操作层**（原DataOperationsMixin功能）：
    - copy_from_dataframe: 利用PostgreSQL COPY命令实现高速数据导入
    - upsert: 基于冲突检测的智能插入或更新操作
    - 数据预处理: 自动处理空值、特殊字符、日期格式转换
    - 临时表策略: 使用临时表提高批量操作的安全性和性能
    
    设计特点：
    --------
    1. **功能完整**: 包含所有原有功能，确保向后兼容
    2. **性能优先**: 使用PostgreSQL原生COPY命令，比INSERT快10-100倍
    3. **数据安全**: 通过临时表和事务确保数据一致性
    4. **模式适配**: 同时支持异步asyncpg和同步psycopg2操作
    5. **类型智能**: 自动识别和转换日期、数值等特殊类型
    
    迁移说明：
    --------
    该组件整合了以下原组件的功能：
    - SQLOperationsMixin: 所有基础SQL操作方法
    - DataOperationsMixin: 所有高级数据操作方法
    
    原组件仍然可用以确保向后兼容，但建议新代码使用此整合组件。
    """

    # ============================================================================
    # 基础SQL操作部分（原SQLOperationsMixin的所有功能）
    # ============================================================================

    async def execute(self, query: str, *args, **kwargs):
        """执行SQL语句

        Args:
            query (str): SQL语句
            *args: SQL位置参数
            **kwargs: SQL关键字参数

        Returns:
            Any: 执行结果
        """
        # Note: This method is low-level and does not automatically resolve table names.
        # It's intended for raw SQL execution. Higher-level methods should handle resolution.
        if self.pool is None:  # type: ignore
            await self.connect()  # type: ignore

        async with self.pool.acquire() as conn:  # type: ignore
            try:
                result = await conn.execute(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(  # type: ignore
                    f"SQL执行失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}"
                )
                raise

    async def fetch(self, query: str, *args, **kwargs):
        """执行查询并返回所有结果

        Args:
            query (str): SQL查询语句
            *args: SQL位置参数
            **kwargs: SQL关键字参数

        Returns:
            List[asyncpg.Record]: 查询结果记录列表
        """
        if self.pool is None:  # type: ignore
            await self.connect()  # type: ignore

        async with self.pool.acquire() as conn:  # type: ignore
            try:
                result = await conn.fetch(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(  # type: ignore
                    f"SQL查询失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}"
                )
                raise

    async def fetch_one(self, query: str, *args, **kwargs):
        """执行查询并返回第一行结果

        Args:
            query (str): SQL查询语句
            *args: SQL位置参数
            **kwargs: SQL关键字参数

        Returns:
            Optional[asyncpg.Record]: 查询结果的第一行记录，如果没有结果则返回None
        """
        if self.pool is None:  # type: ignore
            await self.connect()  # type: ignore

        async with self.pool.acquire() as conn:  # type: ignore
            try:
                result = await conn.fetchrow(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(  # type: ignore
                    f"SQL查询失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}"
                )
                raise

    async def fetch_val(self, query: str, *args, **kwargs):
        """执行查询并返回第一行第一列的值

        Args:
            query (str): SQL查询语句
            *args: SQL位置参数
            **kwargs: SQL关键字参数

        Returns:
            Any: 查询结果的第一行第一列的值，如果没有结果则返回None
        """
        if self.pool is None:  # type: ignore
            await self.connect()  # type: ignore

        async with self.pool.acquire() as conn:  # type: ignore
            try:
                result = await conn.fetchval(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(  # type: ignore
                    f"SQL查询失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}"
                )
                raise

    # === 统一同步方法接口 ===

    def execute_sync(self, query: str, params: Optional[tuple] = None):
        """同步执行SQL语句"""
        # Note: Low-level method, no table name resolution.
        if self.mode == "async":  # type: ignore
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.execute(query, *params))  # type: ignore
            else:
                return self._run_sync(self.execute(query))  # type: ignore
        elif self.mode == "sync":  # type: ignore
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()  # type: ignore
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    connection.commit()
                    return cursor.rowcount
            except Exception as e:
                self.logger.error(f"同步SQL执行失败: {e}\nSQL: {query}\n参数: {params}")  # type: ignore
                connection.rollback()
                raise

    def fetch_sync(self, query: str, params: Optional[tuple] = None):
        """同步执行查询并返回所有结果"""
        if self.mode == "async":  # type: ignore
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch(query, *params))  # type: ignore
            else:
                return self._run_sync(self.fetch(query))  # type: ignore
        elif self.mode == "sync":  # type: ignore
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()  # type: ignore
            try:
                with connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    return [dict(row) for row in rows]
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")  # type: ignore
                connection.rollback()
                raise

    def fetch_one_sync(self, query: str, params: Optional[tuple] = None):
        """同步执行查询并返回第一行结果"""
        if self.mode == "async":  # type: ignore
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch_one(query, *params))  # type: ignore
            else:
                return self._run_sync(self.fetch_one(query))  # type: ignore
        elif self.mode == "sync":  # type: ignore
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()  # type: ignore
            try:
                with connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    cursor.execute(query, params)
                    row = cursor.fetchone()
                    return dict(row) if row else None
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")  # type: ignore
                connection.rollback()
                raise

    def fetch_val_sync(self, query: str, params: Optional[tuple] = None):
        """同步执行查询并返回第一行第一列的值"""
        if self.mode == "async":  # type: ignore
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch_val(query, *params))  # type: ignore
            else:
                return self._run_sync(self.fetch_val(query))  # type: ignore
        elif self.mode == "sync":  # type: ignore
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()  # type: ignore
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    row = cursor.fetchone()
                    return row[0] if row else None
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")  # type: ignore
                connection.rollback()
                raise

    async def executemany(
        self,
        query: str,
        args_list: List[tuple],
        stop_event: Optional[asyncio.Event] = None,
    ):
        """批量执行相同SQL的多组参数

        Args:
            query (str): SQL语句
            args_list (List[tuple]): 参数列表
            stop_event (Optional[asyncio.Event]): 停止事件

        Returns:
            Any: 执行结果
        """
        if self.pool is None:  # type: ignore
            await self.connect()  # type: ignore

        async with self.pool.acquire() as conn:  # type: ignore
            try:
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError("批量执行在开始前被取消")

                result = await conn.executemany(query, args_list)
                return result
            except Exception as e:
                self.logger.error(  # type: ignore
                    f"批量SQL执行失败: {str(e)}\nSQL: {query}\n参数列表数量: {len(args_list)}"
                )
                raise

    # ============================================================================
    # 高级数据操作部分（原DataOperationsMixin的所有功能）
    # ============================================================================

    def _parse_date_string(self, date_str: str) -> Optional[date]:
        """解析日期字符串为Python date对象
        
        支持的格式：
        - YYYYMMDD (如: 20240101)
        - YYYY-MM-DD (如: 2024-01-01)
        - 其他pandas能识别的格式
        
        Args:
            date_str: 日期字符串
            
        Returns:
            date对象或None（如果解析失败）
        """
        if not date_str or pd.isna(date_str):
            return None
            
        try:
            # 尝试YYYYMMDD格式
            if len(date_str) == 8 and date_str.isdigit():
                return datetime.strptime(date_str, "%Y%m%d").date()
            
            # 尝试YYYY-MM-DD格式
            if len(date_str) == 10 and date_str.count('-') == 2:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # 使用pandas的通用日期解析
            parsed_date = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(parsed_date):
                return parsed_date.date()
                
        except Exception as e:
            self.logger.warning(f"无法解析日期字符串 '{date_str}': {e}") # type: ignore
            
        return None

    def _get_date_and_timestamp_columns_from_target(self, target: Any) -> tuple[set, set]:
        """从目标对象获取日期和时间戳列名集合
        
        Args:
            target: 目标表对象或表名
            
        Returns:
            一个元组，包含两个集合: (日期列名, 时间戳列名)
        """
        date_columns = set()
        timestamp_columns = set()
        
        # 如果target有schema_def属性，从中提取日期列
        if hasattr(target, 'schema_def') and target.schema_def:
            for col_name, col_def in target.schema_def.items():
                col_type = (
                    col_def.get("type", "").upper()
                    if isinstance(col_def, dict)
                    else str(col_def).upper()
                )
                if "TIMESTAMP" in col_type:
                    timestamp_columns.add(col_name)
                elif "DATE" in col_type:
                    date_columns.add(col_name)
        
        return date_columns, timestamp_columns

    async def copy_from_dataframe(
        self,
        df: pd.DataFrame,
        target: Any,
        conflict_columns: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
    ):
        """将DataFrame数据高效复制并可选地UPSERT到数据库表中。

        利用 PostgreSQL 的 COPY 命令和临时表实现高效数据加载。
        如果指定了 conflict_columns，则执行 UPSERT (插入或更新) 操作。

        Args:
            df (pd.DataFrame): 要复制的DataFrame。
            target (Any): 目标表，可以是表名字符串或任务对象。
            conflict_columns (Optional[List[str]]): 用于检测冲突的列名列表。如果为None，则执行简单插入。
            update_columns (Optional[List[str]]): 发生冲突时要更新的列名列表。
                                                如果为None且conflict_columns已指定，则更新所有非冲突列。
            timestamp_column (Optional[str]): 时间戳列名。如果指定并在冲突时更新，
                                             如果其他数据列发生变化或特定条件下，该列将自动更新为当前时间。

        Returns:
            int: 影响的总行数 (指通过COPY命令加载到临时表的行数)。

        Raises:
            ValueError: 如果参数无效或DataFrame为空。
            Exception: 如果发生数据库操作错误。
        """
        if self.pool is None: # type: ignore
            await self.connect() # type: ignore

        schema, table_name = self.resolver.get_schema_and_table(target) # type: ignore
        resolved_table_name = f'"{schema}"."{table_name}"'

        if df.empty:
            self.logger.info( # type: ignore
                f"COPY_FROM_DATAFRAME (表: {resolved_table_name}): DataFrame为空，跳过操作。"
            )
            return 0

        df_columns = list(df.columns)

        # 检查时间戳列是否存在于DataFrame中
        if timestamp_column and timestamp_column not in df_columns:
            self.logger.info( # type: ignore
                f"COPY_FROM_DATAFRAME (表: {resolved_table_name}): 时间戳列 '{timestamp_column}' 未在DataFrame列中找到，自动添加当前时间。"
            )
            df = df.copy()
            df[timestamp_column] = datetime.now()
            df_columns = list(df.columns)

        # 获取目标表的日期列信息
        date_columns, timestamp_columns = self._get_date_and_timestamp_columns_from_target(target)

        # 创建一个唯一的临时表名
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        # 从解析后的名称中获取不带schema的表名用于临时表
        simple_table_name = table_name.strip('"')
        temp_table = f"temp_{simple_table_name}_{timestamp_ms}_{id(df)}"

        create_temp_table_sql = f'''
        CREATE TEMPORARY TABLE "{temp_table}" (LIKE {resolved_table_name} INCLUDING DEFAULTS) ON COMMIT DROP;
        '''

        # --- 使用生成器准备记录以减少内存占用 ---
        async def _df_to_records_generator(df_internal: pd.DataFrame):
            for row_tuple in df_internal.itertuples(index=False, name=None):
                processed_values = []
                for i, val in enumerate(row_tuple):
                    col_name = df_columns[i]
                    
                    if pd.isna(val):
                        processed_values.append(None)
                    elif isinstance(val, str):
                        # 检查是否是日期列
                        if col_name in date_columns.union(timestamp_columns):
                            # 尝试解析日期字符串
                            parsed_date = self._parse_date_string(val)
                            processed_values.append(parsed_date)
                        else:
                            # 仅清理真正有问题的字符，保留正常的空格
                            cleaned_val = val.replace('\x00', '')  # 移除NULL字符
                            cleaned_val = cleaned_val.replace('\r', '')  # 移除回车符
                            cleaned_val = cleaned_val.replace('\n', '')  # 移除换行符
                            cleaned_val = cleaned_val.replace('\t', ' ')  # 制表符替换为空格
                            # 不要替换双引号，让asyncpg自己处理
                            processed_values.append(cleaned_val if cleaned_val else None)
                    elif pd.api.types.is_datetime64_any_dtype(pd.Series([val])):
                        # 处理pandas datetime对象
                        if pd.isnull(val): # type: ignore
                            processed_values.append(None)
                        else:
                            # 如果是纯日期列，则截断时间
                            if col_name in date_columns:
                                processed_values.append(val.date() if hasattr(val, 'date') else val)
                            else:
                                # 对于时间戳列，保留完整的 datetime 对象 (asyncpg会处理)
                                processed_values.append(val)
                    else:
                        processed_values.append(val)
                
                record = tuple(processed_values)
                yield record

        records_iterable = _df_to_records_generator(df)

        async with self.pool.acquire() as conn: # type: ignore
            async with conn.transaction():
                try:
                    # 1. 创建临时表
                    await conn.execute(create_temp_table_sql)
                    self.logger.debug(f"已创建临时表 {temp_table}") # type: ignore

                    # 2. 使用 COPY 高效加载数据到临时表
                    copy_result = await conn.copy_records_to_table(
                        temp_table,
                        records=records_iterable,
                        columns=df_columns,
                        timeout=600, # 将超时时间增加到 600 秒 (10 分钟)
                    )
                    # 解析 COPY 命令的返回值 (格式: "COPY 123")
                    if isinstance(copy_result, str) and copy_result.startswith('COPY '):
                        copy_count = int(copy_result.split()[1])
                    else:
                        copy_count = copy_result if isinstance(copy_result, int) else len(df)
                    self.logger.debug(f"已复制 {copy_count} 条记录到 {temp_table}") # type: ignore

                    # 3. 从临时表插入/更新到目标表
                    target_col_str = ", ".join([f'"{col}"' for col in df_columns])

                    if conflict_columns:
                        # --- UPSERT 逻辑 ---
                        conflict_col_str = ", ".join([f'"{col}"' for col in conflict_columns])

                        # 确定要更新的列
                        if update_columns is None:
                            # 如果未指定更新列，则更新所有非冲突列
                            update_columns = [col for col in df_columns if col not in conflict_columns]

                        if update_columns:
                            # 构建更新子句，处理时间戳列的特殊逻辑
                            update_clauses = []
                            for col in update_columns:
                                if col == timestamp_column:
                                    # 时间戳列：仅当数据变化时更新
                                    non_ts_columns = [c for c in update_columns if c != timestamp_column]
                                    if non_ts_columns:
                                        # 检查非时间戳列是否有变化
                                        change_conditions = [
                                            f'{resolved_table_name}."{col}" IS DISTINCT FROM EXCLUDED."{col}"'
                                            for col in non_ts_columns
                                        ]
                                        change_condition = " OR ".join(change_conditions)
                                        update_clauses.append(
                                            f'"{col}" = CASE WHEN ({change_condition}) THEN CURRENT_TIMESTAMP ELSE {resolved_table_name}."{col}" END'
                                        )
                                    else:
                                        # 只有时间戳列需要更新，保持原值
                                        update_clauses.append(f'"{col}" = {resolved_table_name}."{col}"')
                                else:
                                    # 普通列：直接更新
                                    update_clauses.append(f'"{col}" = EXCLUDED."{col}"')

                            update_clause_str = ", ".join(update_clauses)

                            upsert_sql = f'''
                            INSERT INTO {resolved_table_name} ({target_col_str})
                            SELECT {target_col_str} FROM "{temp_table}"
                            ON CONFLICT ({conflict_col_str}) DO UPDATE SET
                                {update_clause_str};
                            '''
                        else:
                            # 没有要更新的列，只执行插入（忽略冲突）
                            upsert_sql = f'''
                            INSERT INTO {resolved_table_name} ({target_col_str})
                            SELECT {target_col_str} FROM "{temp_table}"
                            ON CONFLICT ({conflict_col_str}) DO NOTHING;
                            '''

                        self.logger.debug(f"执行UPSERT: {upsert_sql[:200]}...") # type: ignore
                        await conn.execute(upsert_sql)
                    else:
                        # --- 简单插入 ---
                        insert_sql = f'''
                        INSERT INTO {resolved_table_name} ({target_col_str})
                        SELECT {target_col_str} FROM "{temp_table}";
                        '''
                        self.logger.debug(f"执行INSERT: {insert_sql[:200]}...") # type: ignore
                        await conn.execute(insert_sql)

                    self.logger.info( # type: ignore
                        f"COPY_FROM_DATAFRAME (表: {resolved_table_name}): 成功处理 {copy_count} 条记录"
                    )
                    return copy_count

                except Exception as e:
                    self.logger.error( # type: ignore
                        f"COPY_FROM_DATAFRAME (表: {resolved_table_name}) 失败: {str(e)}"
                    )
                    raise



    async def upsert(
        self,
        df: pd.DataFrame,
        target: Any,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
    ):
        """高效的UPSERT操作（插入或更新）

        这是copy_from_dataframe的便捷包装，专门用于UPSERT操作。

        Args:
            df (pd.DataFrame): 要处理的DataFrame
            target (Any): 目标表，可以是表名字符串或任务对象
            conflict_columns (List[str]): 用于检测冲突的列名列表（必须）
            update_columns (Optional[List[str]]): 发生冲突时要更新的列名列表。
                                                如果为None，则更新所有非冲突列。
            timestamp_column (Optional[str]): 时间戳列名，用于智能更新时间戳

        Returns:
            int: 影响的行数

        Raises:
            ValueError: 如果conflict_columns为空
            Exception: 如果发生数据库操作错误
        """
        if not conflict_columns:
            raise ValueError("UPSERT操作必须指定conflict_columns")

        return await self.copy_from_dataframe(
            df=df,
            target=target,
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            timestamp_column=timestamp_column,
        ) 

    async def get_all_physical_tables(self, schema_name: str) -> List[str]:
        """获取指定 schema 下的所有物理表名称。"""
        query = """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = $1
        """
        try:
            records = await self.fetch(query, schema_name)
            return [record['tablename'] for record in records] if records else []
        except Exception as e:
            self.logger.error(f"获取 schema '{schema_name}' 的所有表时出错: {e}", exc_info=True)
            return [] 