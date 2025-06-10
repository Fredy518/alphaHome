import asyncio
import threading
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from urllib.parse import urlparse
from .logging_utils import get_logger

import asyncpg
import psycopg2
import psycopg2.extras
import pandas as pd

class DBManager:
    """数据库连接管理器 - 支持异步和同步双模式操作
    
    支持两种工作模式：
    - async: 使用 asyncpg，适用于异步环境（如 fetchers）
    - sync: 使用 psycopg2，适用于同步环境（如 Backtrader）
    """
    
    def __init__(self, connection_string: str, mode: str = 'async'):
        """初始化数据库连接管理器
        
        Args:
            connection_string (str): 数据库连接字符串，格式为：
                `postgresql://username:password@host:port/database`
            mode (str): 工作模式，'async' 或 'sync'
        """
        self.connection_string = connection_string
        self.mode = mode.lower()
        self.logger = get_logger(f"db_manager_{self.mode}")
        
        if self.mode not in ['async', 'sync']:
            raise ValueError(f"不支持的模式: {mode}，只支持 'async' 或 'sync'")
            
        if self.mode == 'async':
            # 异步模式：使用 asyncpg
            self.pool = None
            self._sync_lock = threading.Lock()
            self._loop = None
            self._executor = None
        elif self.mode == 'sync':
            # 同步模式：使用 psycopg2
            self._parse_connection_string()
            self._local = threading.local()
            self.pool = None  # 兼容性属性
    
    def _parse_connection_string(self):
        """解析连接字符串为psycopg2连接参数（仅同步模式）"""
        parsed = urlparse(self.connection_string)
        self._conn_params = {
            'host': parsed.hostname or 'localhost',
            'port': parsed.port or 5432,
            'user': parsed.username or 'postgres',
            'password': parsed.password or '',
            'database': parsed.path.lstrip('/') if parsed.path else 'postgres'
        }
    
    def _get_sync_connection(self):
        """获取线程本地的数据库连接（仅同步模式）"""
        if self.mode != 'sync':
            raise RuntimeError("_get_sync_connection 只能在同步模式下使用")
            
        if not hasattr(self._local, 'connection') or self._local.connection.closed:
            try:
                self._local.connection = psycopg2.connect(**self._conn_params)
                self.logger.debug("创建新的同步数据库连接")
            except Exception as e:
                self.logger.error(f"创建同步数据库连接失败: {e}")
                raise
        return self._local.connection
    
    async def connect(self):
        """创建数据库连接池（仅异步模式）"""
        if self.mode != 'async':
            raise RuntimeError("connect 方法只能在异步模式下使用")
            
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(self.connection_string)
                self.logger.info("异步数据库连接池创建成功")
            except Exception as e:
                self.logger.error(f"异步数据库连接池创建失败: {str(e)}")
                raise
    
    async def close(self):
        """关闭数据库连接池（仅异步模式）"""
        if self.mode != 'async':
            raise RuntimeError("close 方法只能在异步模式下使用")
            
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            self.logger.info("异步数据库连接池已关闭")
    
    def close_sync(self):
        """关闭同步数据库连接"""
        if self.mode == 'async':
            return self._run_sync(self.close())
        elif self.mode == 'sync':
            if hasattr(self._local, 'connection') and not self._local.connection.closed:
                self._local.connection.close()
                self.logger.debug("同步数据库连接已关闭")
            
    def _run_sync(self, coro):
        """在同步上下文中运行异步代码"""
        with self._sync_lock:
            try:
                # 尝试获取当前事件循环
                loop = asyncio.get_running_loop()
                # 如果有运行中的循环，在新线程中运行
                if self._executor is None:
                    self._executor = ThreadPoolExecutor(max_workers=1)
                future = self._executor.submit(asyncio.run, coro)
                return future.result()
            except RuntimeError:
                # 没有运行中的循环，直接运行
                return asyncio.run(coro)
    
    # === 同步方法接口 ===
    
    def connect_sync(self):
        """同步创建数据库连接池"""
        return self._run_sync(self.connect())
    
    def close_sync(self):
        """同步关闭数据库连接池"""
        return self._run_sync(self.close())
    
    async def execute(self, query: str, *args, **kwargs):
        """执行SQL语句
        
        Args:
            query (str): SQL语句
            *args: SQL位置参数
            **kwargs: SQL关键字参数
            
        Returns:
            Any: 执行结果
        """
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.execute(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f"SQL执行失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}")
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
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetch(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f"SQL查询失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}")
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
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchrow(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f"SQL查询失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}")
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
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f"SQL查询失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}")
                raise
    
    # === 统一同步方法接口 ===
    
    def execute_sync(self, query: str, params: tuple = None):
        """同步执行SQL语句"""
        if self.mode == 'async':
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.execute(query, *params))
            else:
                return self._run_sync(self.execute(query))
        elif self.mode == 'sync':
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    connection.commit()
                    return cursor.rowcount
            except Exception as e:
                self.logger.error(f"同步SQL执行失败: {e}\nSQL: {query}\n参数: {params}")
                connection.rollback()
                raise
    
    def fetch_sync(self, query: str, params: tuple = None):
        """同步执行查询并返回所有结果"""
        if self.mode == 'async':
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch(query, *params))
            else:
                return self._run_sync(self.fetch(query))
        elif self.mode == 'sync':
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()
            try:
                with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    return [dict(row) for row in rows]
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")
                connection.rollback()
                raise
    
    def fetch_one_sync(self, query: str, params: tuple = None):
        """同步执行查询并返回第一行结果"""
        if self.mode == 'async':
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch_one(query, *params))
            else:
                return self._run_sync(self.fetch_one(query))
        elif self.mode == 'sync':
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()
            try:
                with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    row = cursor.fetchone()
                    return dict(row) if row else None
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")
                connection.rollback()
                raise
    
    def fetch_val_sync(self, query: str, params: tuple = None):
        """同步执行查询并返回第一行第一列的值"""
        if self.mode == 'async':
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch_val(query, *params))
            else:
                return self._run_sync(self.fetch_val(query))
        elif self.mode == 'sync':
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, params)
                    row = cursor.fetchone()
                    return row[0] if row else None
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")
                connection.rollback()
                raise
    
    def test_connection(self):
        """测试数据库连接"""
        try:
            if self.mode == 'sync':
                result = self.fetch_val_sync("SELECT 1")
            else:
                # 异步模式下不能在此方法中直接测试，需要异步调用
                self.logger.warning("异步模式下test_connection方法不可用，请使用异步测试方法")
                return False
            return result == 1
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {e}")
            return False
    
    async def executemany(self, query: str, args_list: List[tuple], stop_event: Optional[asyncio.Event] = None): # 添加 stop_event 参数
        """批量执行SQL语句
        
        Args:
            query (str): SQL语句
            args_list (List[tuple]): 参数元组的列表
            stop_event (Optional[asyncio.Event]): 用于中途中断操作的异步事件。
            
        Returns:
            int: 影响的行数
        """
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                # 开始事务
                async with conn.transaction():
                    # 创建预编译语句
                    stmt = await conn.prepare(query)
                    
                    # 批量执行
                    count = 0
                    for args in args_list:
                        # 在执行批处理中的每个语句之前检查停止事件
                        if stop_event and stop_event.is_set():
                            # 移除了用于取消点的DEBUG日志
                            # 选项1：抛出 CancelledError 以传播取消信号
                            raise asyncio.CancelledError("批量执行被用户取消")
                            # 选项2：返回部分计数（对取消的表达不那么明确）
                            # return count 
                        
                        # 使用fetchrow代替execute，因为PreparedStatement没有execute方法
                        # （译者注：PreparedStatement 有 statuses = await stmt.executemany(args_list) 的用法，但这里是单条执行循环）
                        await stmt.fetchrow(*args) # fetchrow 适合预期单行返回或无返回的情况
                        count += 1
                    
                    return count # 如果循环完成，返回完整计数
            except Exception as e:
                self.logger.error(f"批量SQL执行失败: {str(e)}\nSQL: {query}")
                raise
    
    async def copy_from_dataframe(self, 
                                  df: pd.DataFrame, 
                                  table_name: str,
                                  conflict_columns: Optional[List[str]] = None,
                                  update_columns: Optional[List[str]] = None,
                                  timestamp_column: Optional[str] = None):
        """将DataFrame数据高效复制并可选地UPSERT到数据库表中。

        利用 PostgreSQL 的 COPY 命令和临时表实现高效数据加载。
        如果指定了 conflict_columns，则执行 UPSERT (插入或更新) 操作。
        
        Args:
            df (pd.DataFrame): 要复制的DataFrame。
            table_name (str): 目标表名。
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
        # self.logger.info(f"COPY_FROM_DATAFRAME_ENTRY ({table_name}): Received timestamp_column = {repr(timestamp_column)}, type = {type(timestamp_column)}") # 已移除调试日志
        
        if self.pool is None:
            await self.connect()
        
        if df.empty:
            self.logger.info(f"copy_from_dataframe: 表 '{table_name}' 的DataFrame为空，跳过操作。")
            return 0
            
        if conflict_columns and not isinstance(conflict_columns, list): # 确保 conflict_columns 是列表或 None
            raise ValueError("conflict_columns 必须是一个列表或None。")
        if conflict_columns is not None and not conflict_columns: # 检查是否为空列表
            raise ValueError("如果提供了 conflict_columns，则它不能为空列表。") # 原为 "conflict_columns cannot be an empty list if provided."
            
        # 获取DataFrame的列名 - 这些将用于COPY命令
        df_columns = list(df.columns)

        # --- 针对timestamp_column的防御性检查和添加 ---
        if timestamp_column and timestamp_column not in df.columns:
            self.logger.warning(f"COPY_FROM_DATAFRAME (表: {table_name}): 时间戳列 '{timestamp_column}' 未在DataFrame列中找到，现用None值添加。")
            df[timestamp_column] = None # 使用None，假设数据库默认值或UPDATE时的NOW()会处理它
            df_columns = list(df.columns) # 关键：修改后重新评估df_columns
            # self.logger.info(f"COPY_FROM_DATAFRAME (表: {table_name}): 添加 '{timestamp_column}' 后, 新 df_columns: {df_columns}") # 移除此INFO日志
        # --- 防御性检查结束 ---

        # --- 验证列名 ---
        if conflict_columns:
            for col in conflict_columns:
                if col not in df_columns:
                    raise ValueError(f"冲突列 '{col}' 在DataFrame的列中未找到。")
        if update_columns:
            # 如果设置了conflict_columns，允许update_columns为空列表以实现DO NOTHING
            if not isinstance(update_columns, list):
                raise ValueError("update_columns必须是一个列表或None。")
            for col in update_columns:
                if col not in df_columns:
                    raise ValueError(f"更新列 '{col}' 在DataFrame的列中未找到。")
        if timestamp_column and timestamp_column not in df_columns: # 此条件理论上在上面的防御性代码后不应为真
            self.logger.warning(f"时间戳列 '{timestamp_column}' 未在DataFrame中找到，"
                            f"如果在UPDATE时需要会被添加，但如果表结构要求此列，初始INSERT可能会失败。")

        # 创建一个唯一的临时表名
        # 使用毫秒以增加并发场景下的唯一性机会
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        temp_table = f"temp_{table_name}_{timestamp_ms}_{id(df)}" # id(df) 增加唯一性
        
        # ON COMMIT DROP 确保即使在提交开始后但在删除操作前发生错误，也能清理临时表
        create_temp_table_sql = f"""
        CREATE TEMPORARY TABLE "{temp_table}" (LIKE "{table_name}" INCLUDING DEFAULTS) ON COMMIT DROP;
        """
        
        # --- 使用生成器准备记录以减少内存占用 ---
        async def _df_to_records_generator(df_internal: pd.DataFrame):
            # 此生成器为 copy_records_to_table 生成元组。
            # copy_from_dataframe 开头的 df.empty 检查处理了初始为空的DataFrame。
            for row_tuple in df_internal.itertuples(index=False, name=None):
                processed_values = []
                for val in row_tuple:
                    if pd.isna(val):
                        processed_values.append(None)
                    else:
                        processed_values.append(val)
                yield tuple(processed_values)

        records_iterable = _df_to_records_generator(df)
        # 注意：原始的 'if not data_records:' 检查已被移除，因为它不直接适用于生成器（除非消耗它）。
        # 父函数开头的 df.empty 检查是主要的防护。

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # 1. 创建临时表
                    await conn.execute(create_temp_table_sql)
                    self.logger.debug(f"已创建临时表 {temp_table}")

                    # 2. 使用 COPY 高效加载数据到临时表
                    # copy_records_to_table 需要与 *表* 定义匹配的列名
                    # 由于临时表是 LIKE 目标表，df_columns 应与目标表列匹配
                    copy_count = await conn.copy_records_to_table(
                        temp_table, 
                        records=records_iterable, # 使用生成器迭代器
                        columns=df_columns,
                        timeout=300 # 为大型复制增加超时时间
                    )
                    self.logger.debug(f"已复制 {copy_count} 条记录到 {temp_table}")

                    # 3. 从临时表插入/更新到目标表
                    target_col_str = ', '.join([f'"{col}"' for col in df_columns]) # 来自DataFrame的列
                    
                    if conflict_columns:
                        # --- UPSERT 逻辑 ---
                        conflict_col_str = ', '.join([f'"{col}"' for col in conflict_columns])
                        
                        # 确定要更新的列
                        actual_update_columns = []
                        if update_columns is None:
                            # 默认：更新DataFrame中所有非冲突列
                            actual_update_columns = [col for col in df_columns if col not in conflict_columns]
                        else:
                            # 使用明确提供的update_columns
                            actual_update_columns = update_columns
                            
                        # 构建SET子句
                        if actual_update_columns:
                            set_clauses = [f'"{col}" = EXCLUDED."{col}"' for col in actual_update_columns]
                            
                            # self.logger.info(f"UPSERT (表: {table_name}): 时间戳逻辑前: df_columns = {df_columns}, timestamp_column = "{timestamp_column}"') # 已移除调试日志
                            
                            if timestamp_column and timestamp_column in df_columns:
                                if timestamp_column not in conflict_columns:
                                    # 从set_clauses中移除任何"EXCLUDED"版本的时间戳列
                                    excluded_timestamp_clause = f'"{timestamp_column}" = EXCLUDED."{timestamp_column}"'
                                    if excluded_timestamp_clause in set_clauses:
                                        set_clauses.remove(excluded_timestamp_clause)

                                    # 确定用于比较的内容列（排除冲突列和时间戳列）
                                    content_columns_to_compare = [\
                                        col for col in df_columns \
                                        if col not in conflict_columns and col != timestamp_column\
                                    ]
                                    
                                    if not content_columns_to_compare:
                                        # 没有其他内容列可比较，因此如果发生冲突，则更新时间戳
                                        set_clauses.append(f'"{timestamp_column}" = NOW()')
                                        # self.logger.info(f"UPSERT (表: {table_name}): 无内容列可比较。'{timestamp_column}' 将设为 NOW(). Set clauses: {set_clauses}") # 移除调试日志
                                    else:
                                        # 构建时间戳列的条件更新
                                        conditions = [\
                                            f'"{table_name}". "{col}" IS DISTINCT FROM EXCLUDED."{col}"' \
                                            for col in content_columns_to_compare\
                                        ]
                                        condition_str = " OR ".join(conditions)
                                        timestamp_update_clause = (\
                                            f'"{timestamp_column}" = CASE '\
                                            f'WHEN {condition_str} THEN NOW() '\
                                            f'ELSE "{table_name}". "{timestamp_column}" END'\
                                        )
                                        set_clauses.append(timestamp_update_clause)
                                        # self.logger.info(f"UPSERT (表: {table_name}): 条件时间戳更新。比较的内容列: {content_columns_to_compare}. Set clauses: {set_clauses}") # 移除调试日志

                            if set_clauses:
                                update_clause = ', '.join(set_clauses)
                                action_sql = f"DO UPDATE SET {update_clause}"
                            else:
                                # 此情况意味着actual_update_columns为空，并且时间戳逻辑也未产生子句（例如，timestamp_column是conflict_column）
                                self.logger.warning(f"UPSERT (表: {table_name}): set_clauses变为空。默认为DO NOTHING。")
                                action_sql = "DO NOTHING"
                        else: # actual_update_columns 为空
                            # 如果只涉及主键和可选的时间戳列
                            if timestamp_column and timestamp_column in df_columns and timestamp_column not in conflict_columns:
                                # 即使没有其他列，主键冲突也意味着活动，因此更新时间戳。
                                # 这维持了此特定情况的先前行为。
                                # 或者，如果业务规则不同，这里可以是 target_table.timestamp_column
                                set_clauses = ['"{timestamp_column}" = NOW()']
                                update_clause = ', '.join(set_clauses)
                                action_sql = f"DO UPDATE SET {update_clause}"
                                # self.logger.info(f"UPSERT (表: {table_name}): actual_update_columns 为空。'{timestamp_column}' 将设为 NOW().") # 移除调试日志
                            else:
                                action_sql = "DO NOTHING"

                        insert_sql = f"""
                        INSERT INTO "{table_name}" ({target_col_str})
                        SELECT {target_col_str}
                        FROM "{temp_table}"
                        ON CONFLICT ({conflict_col_str}) {action_sql};
                        """

                        self.logger.debug(f"UPSERT (表: {table_name}): DataFrame头部 (前3行, 用于EXCLUDED上下文):\n{df.head(3).to_string()}")
                        self.logger.debug(f"为表 {table_name} 执行最终UPSERT SQL:\n{insert_sql}") # 保留此重要日志
                        
                    else:
                        # --- 简单 INSERT 逻辑 ---
                        insert_sql = f"""
                        INSERT INTO "{table_name}" ({target_col_str})
                        SELECT {target_col_str} 
                        FROM "{temp_table}";
                        """
                        
                    # 执行最终传输
                    status = await conn.execute(insert_sql)
                    # 如果所有行都冲突并且执行了DO NOTHING/UPDATE，状态可能报告INSERT 0
                    # copy_count 反映加载到临时表的行数，这是衡量已完成工作的更好指标。
                    self.logger.debug(f"最终插入/更新命令状态: {status}")
                    
                    # --- !! 修正：从状态字符串中解析整数 !! ---
                    rows_copied = 0
                    try:
                        # copy_count 是类似 'COPY 12345' 的字符串
                        if isinstance(copy_count, str) and copy_count.startswith('COPY '):
                            rows_copied = int(copy_count.split()[-1])
                        else:
                            self.logger.warning(f"copy_records_to_table 返回了意外状态: {copy_count}。假设复制了0行。")
                    except (ValueError, IndexError) as parse_err:
                        self.logger.error(f"从copy_count '{copy_count}' 解析行数失败: {parse_err}。假设复制了0行。")
                    
                    return rows_copied # 返回解析后的整数计数

                except asyncpg.exceptions.UndefinedTableError as e:
                    # 表在LIKE期间不存在的特定错误
                    self.logger.error(f"创建临时表失败 (LIKE '{table_name}')。目标表可能不存在。错误: {e}")
                    raise ValueError(f"目标表 '{table_name}' 未找到。") from e
                except asyncpg.exceptions.UndefinedColumnError as e:
                    # DataFrame和表之间的列不匹配
                    self.logger.error(f"表 '{table_name}' 的复制/插入过程中发生列不匹配错误。错误: {e}")
                    self.logger.error(f"DataFrame 列: {df_columns}")
                    # 如果需要，可以考虑在此处获取实际表列进行比较
                    raise ValueError(f"DataFrame和表 '{table_name}' 之间列不匹配。请检查DataFrame列和表结构。") from e
                except Exception as e:
                    self.logger.error(f"高效复制/UPSERT操作失败 (表: {table_name}): {str(e)}")
                    # 记录临时表名，以便在ON COMMIT DROP意外失败时进行手动检查
                    self.logger.error(f"临时表曾是: {temp_table}") 
                    raise
                # 临时表在成功提交或回滚时会因ON COMMIT DROP子句自动删除。
    
    async def table_exists(self, table_name: str) -> bool:
        """检查表是否存在
        
        Args:
            table_name (str): 表名
            
        Returns:
            bool: 如果表存在则返回True，否则返回False
        """
        if self.pool is None:
            await self.connect()
        
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
        );
        """
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(query, table_name)
        return result if result is not None else False # 确保返回布尔值
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表结构
        
        Args:
            table_name (str): 表名
            
        Returns:
            List[Dict[str, Any]]: 表结构信息列表，每个元素是一个字典，
                                 包含列名、数据类型、是否可为空、默认值等信息。
        """
        query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable, 
            column_default
        FROM 
            information_schema.columns
        WHERE 
            table_name = $1
        ORDER BY 
            ordinal_position;
        """
        result = await self.fetch(query, table_name)
        
        # 转换为字典列表
        schema = []
        for row in result:
            schema.append({
                "column_name": row["column_name"],
                "data_type": row["data_type"],
                "is_nullable": row["is_nullable"] == "YES", # 将 'YES'/'NO' 转换为布尔值
                "default": row["column_default"]
            })
        
        return schema
    
    async def get_latest_date(self, table_name: str, date_column: str, return_raw_object: bool = False) -> Optional[Union[str, datetime]]:
        """获取表中指定日期/时间戳列的最大值。
        
        Args:
            table_name (str): 表名。
            date_column (str): 日期或时间戳列名。
            return_raw_object (bool): 如果为 True，返回原始的 datetime 对象 (如果适用)；
                                     否则，尝试返回 YYYYMMDD 格式的字符串。默认为 False。
            
        Returns:
            Optional[Union[str, datetime]]: 该列的最大值 (datetime 对象或 YYYYMMDD 字符串)，
                                           如果列为空或查询失败，则返回 None。
        """
        if self.mode == 'sync':
            # 同步模式：使用同步方法
            query = f"""
            SELECT MAX("{date_column}") FROM "{table_name}";
            """
            try:
                result = self.fetch_val_sync(query)
            except Exception as e:
                self.logger.warning(f"查询表 {table_name} 列 {date_column} 的最大值时出错: {e}")
                return None
        else:
            # 异步模式：确保连接池存在并使用原生异步方法
            if self.pool is None:
                await self.connect()
            
            query = f"""
            SELECT MAX("{date_column}") FROM "{table_name}";
            """
            try:
                async with self.pool.acquire() as conn:
                    result = await conn.fetchval(query)
            except Exception as e:
                self.logger.warning(f"查询表 {table_name} 列 {date_column} 的最大值时出错: {e}")
                return None
        
        if result is not None:
            self.logger.debug(f"get_latest_date (表: {table_name}, 列: {date_column}) 返回类型: {type(result)}")
        else:
            self.logger.debug(f"get_latest_date (表: {table_name}, 列: {date_column}) 返回 None")

        # --- 为了向后兼容性的条件格式化 --- 
        if result is not None and not return_raw_object:
            # 默认行为：格式化为 YYYYMMDD 字符串
            formatted_result = None
            if isinstance(result, str):
                # 处理来自数据库的潜在字符串输入 (尽管对于asyncpg不太可能)
                if '-' in result: # 尝试 YYYY-MM-DD
                    try: formatted_result = datetime.strptime(result, '%Y-%m-%d').strftime('%Y%m%d')
                    except ValueError: self.logger.warning(f"无法将日期字符串 '{result}' 解析为 YYYY-MM-DD 格式。")
                elif len(result) == 8 and result.isdigit(): # 已经是 YYYYMMDD
                    formatted_result = result 
                else: # 其他未知字符串格式
                    self.logger.warning(f"接收到意外的日期字符串格式 '{result}'。")
            elif isinstance(result, datetime): # 包括 datetime.date (因为它是datetime的子类)
                # 将 datetime/date 对象转换为 YYYYMMDD 字符串
                formatted_result = result.strftime('%Y%m%d')
            else: # 其他未知类型
                self.logger.warning(f"接收到意外的日期类型 '{type(result)}'。")
            
            return formatted_result # 返回格式化后的字符串，如果格式化失败则为 None
        else:
            # return_raw_object 为 True 或 result 为 None
            return result # 返回原始 datetime 对象或 None

    async def upsert(self,
                    table_name: str,
                    data: Union[pd.DataFrame, List[Dict[str, Any]]],
                    conflict_columns: List[str],
                    update_columns: Optional[List[str]] = None,
                    timestamp_column: Optional[str] = None,
                    stop_event: Optional[asyncio.Event] = None) -> int:
        """通用UPSERT操作：插入数据，如果发生冲突则根据条件更新。

        利用 COPY + 临时表 + INSERT ON CONFLICT 策略以提高效率。
        
        Args:
            table_name (str): 目标表名。
            data (Union[pd.DataFrame, List[Dict[str, Any]]]): 要插入的数据，可以是DataFrame或字典列表。
            conflict_columns (List[str]): 用于检测冲突的列名列表（通常是主键或唯一索引的列）。
            update_columns (Optional[List[str]]): 发生冲突时要更新的列名列表。
                                                如果为None，则更新所有非冲突列。
            timestamp_column (Optional[str]): 时间戳列名。如果指定，在冲突更新时，
                                             若其他数据列发生变化，此列将更新为NOW()。
                                             插入时，如果DataFrame中此列无值，会尝试添加。
            stop_event (Optional[asyncio.Event]): 用于取消操作的异步事件。
                                                 (注意: COPY操作通常不能优雅地中途中断，
                                                  此事件主要用于在调用前检查，或在未来的批处理中。)
            
        Returns:
            int: 影响的行数 (基于COPY到临时表的行数)。
            
        Raises:
            ValueError: 当数据为空或参数无效时。
            Exception: 如果发生数据库操作错误。
        """
        # self.logger.info(f"UPSERT_ENTRY ({table_name}): Received timestamp_column = {repr(timestamp_column)}, type = {type(timestamp_column)}") # 已移除调试日志
        
        if self.pool is None:
            await self.connect()
        
        # --- 参数检查 ---
        if not table_name:
            raise ValueError("表名不能为空")
        if not conflict_columns: # 进一步检查 conflict_columns 是否有效
            raise ValueError("必须指定冲突检测列 (conflict_columns)。")
        if not isinstance(conflict_columns, list) or len(conflict_columns) == 0:
            raise ValueError("conflict_columns 必须是一个非空的字符串列表。")
            
        # --- 数据准备 ---
        df_to_process = None
        if isinstance(data, pd.DataFrame):
            if data.empty:
                self.logger.info(f"UPSERT ({table_name}): 输入的DataFrame为空。")
                return 0
            df_to_process = data.copy() # 使用副本以避免修改原始DataFrame
        elif isinstance(data, list):
            if not data: # 检查列表是否为空
                self.logger.info(f"UPSERT ({table_name}): 输入的列表为空。")
                return 0
            # 将字典列表转换为DataFrame以进行一致处理
            try:
                df_to_process = pd.DataFrame(data)
            except Exception as e:
                self.logger.error(f"UPSERT ({table_name}): 无法将字典列表转换为DataFrame: {e}")
                raise ValueError("无法将输入的字典列表转换为DataFrame。") from e
            if df_to_process.empty: # 再次检查转换后的DataFrame是否为空
                self.logger.info(f"UPSERT ({table_name}): 从列表转换的DataFrame为空。")
                return 0
        else:
            raise ValueError("参数 data 必须是DataFrame或字典列表。")
            
        # --- 预处理 ---
        
        # 1. 检查 Stop Event (在耗时操作前)
        if stop_event and stop_event.is_set():
            self.logger.warning(f"UPSERT 操作 ({table_name}) 在开始前被取消。")
            raise asyncio.CancelledError("UPSERT操作在开始前被取消")
        
        # 2. 时间戳处理 (确保列存在于 DataFrame)
        current_time = datetime.now() # 如果需要在INSERT时填充，但主要依赖于UPDATE时的NOW()
        if timestamp_column: # 此处的 timestamp_column 已被 UPSERT_FIX 保证为 'update_time' (如果原为None)
            if timestamp_column not in df_to_process.columns:
                # 如果目标表需要此列，添加该列并用当前时间初始化
                # 对于UPDATE情况，此值会被NOW()覆盖（如果其他列变化）
                self.logger.info(f"UPSERT ({table_name}): 未检测到 {timestamp_column} 列，已自动为所有行添加当前时间戳。")
                df_to_process[timestamp_column] = current_time
            else:
                # 可选：如果需要，填充现有时间戳列中的NaN值
                # 否则，保留原样以供COPY处理
                # df_to_process[timestamp_column] = df_to_process[timestamp_column].fillna(current_time)
                pass # 让 copy_from_dataframe 处理NaN到NULL的转换

        # 3. 移除冲突列为 NULL 的行 (重要! ON CONFLICT 不适用于 NULL 值)
        initial_rows = len(df_to_process)
        # 创建一个布尔掩码，用于标记任何一个冲突列为NaN/None的行
        null_mask = df_to_process[conflict_columns].isnull().any(axis=1)
        if null_mask.any():
            df_to_process_before_filter = df_to_process # 用于日志记录
            df_to_process = df_to_process[~null_mask]
            removed_count = initial_rows - len(df_to_process)
            self.logger.warning(
                f"UPSERT ({table_name}): 移除了 {removed_count} 行，因为其冲突列 "
                f"({', '.join(conflict_columns)}) 中包含NULL值。"
            )
            if df_to_process.empty:
                self.logger.info(f"UPSERT ({table_name}): 移除包含NULL主键的行后，没有有效数据可执行。")
                return 0
                
        # --- 执行 --- 
        try:
            # 使用重构后的 copy_from_dataframe 执行 UPSERT
            # DataFrame中的NaN值将在 copy_from_dataframe 内部处理
            # self.logger.info(f"UPSERT_BEFORE_COPY ({table_name}): Preparing to call copy_from_dataframe with timestamp_column = {repr(timestamp_column)}, type = {type(timestamp_column)}") # 已移除调试日志
            affected_rows = await self.copy_from_dataframe(
                df=df_to_process,
                table_name=table_name,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                timestamp_column=timestamp_column # 此处的 timestamp_column 已被 UPSERT_FIX 修正
            )
            self.logger.info(f"UPSERT ({table_name}) 完成，通过 COPY 处理了 {affected_rows} 行数据到临时表。")
            return affected_rows
        except asyncio.CancelledError: 
            # 理论上应在调用 copy_from_dataframe 前捕获，但以防万一再次捕获
            self.logger.warning(f"UPSERT 操作 ({table_name}) 被取消。")
            raise 
        except Exception as e:
            self.logger.error(f"UPSERT操作失败 (使用COPY策略): {str(e)}\n表: {table_name}")
            # 此处避免记录可能很大的DataFrame
            raise

    async def create_table_from_schema(self,
                                       table_name: str,
                                       schema_def: Dict[str, Union[str, Dict[str, str]]],
                                       primary_keys: Optional[List[str]] = None,
                                       date_column: Optional[str] = None,
                                       indexes: Optional[List[Union[str, Dict[str, Any]]]] = None,
                                       auto_add_update_time: bool = True): # 添加了 auto_add_update_time 参数
        """根据任务定义的 schema (结构) 创建数据库表和相关索引。"""
        if self.pool is None:
            await self.connect()

        if not schema_def: # schema 定义不能为空
            raise ValueError(f"无法创建表 '{table_name}'，未提供 schema_def (表结构定义)。")

        async with self.pool.acquire() as conn:
            async with conn.transaction(): # 为DDL（数据定义语言）操作使用事务
                try:
                    # --- 1. 构建 CREATE TABLE 语句 --- 
                    columns = []
                    for col_name, col_def in schema_def.items():
                        if isinstance(col_def, dict): # 如果列定义是字典 (包含类型和约束)
                            col_type = col_def.get('type', 'TEXT') # 默认类型为TEXT
                            constraints_val = col_def.get('constraints') # 获取原始约束值
                            constraints_str = str(constraints_val).strip() if constraints_val is not None else "" #转换为字符串
                            columns.append(f'"{col_name}" {col_type} {constraints_str}'.strip()) # 移除末尾多余空格
                        else: # 如果列定义只是字符串 (类型)
                            columns.append(f'"{col_name}" {col_def}')
                    
                    # 添加 update_time 列（如果配置需要且Schema中不存在）
                    if auto_add_update_time and 'update_time' not in schema_def:
                        # 确保 TIMESTAMP WITHOUT TIME ZONE 的默认精度
                        columns.append('"update_time" TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP')

                    # 添加主键约束
                    if primary_keys and isinstance(primary_keys, list) and len(primary_keys) > 0:
                        pk_cols_str = ', '.join([f'"{pk}"' for pk in primary_keys]) # 格式化主键列
                        columns.append(f"PRIMARY KEY ({pk_cols_str})")
                        
                    columns_str = ',\n            '.join(columns) # 用逗号和换行连接所有列定义
                    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS "{table_name}" (
                        {columns_str}
                    );
                    """
                    
                    self.logger.info(f"准备为表 '{table_name}' 执行建表语句:\n{create_table_sql}")
                    await conn.execute(create_table_sql)
                    self.logger.info(f"表 '{table_name}' 创建成功或已存在。")

                    # --- 1.1 添加列注释 ---
                    for col_name, col_def in schema_def.items():
                        if isinstance(col_def, dict) and 'comment' in col_def:
                            comment_text = col_def['comment'] # 获取注释文本
                            if comment_text is not None:
                                # 转义 comment_text 中的单引号，防止SQL注入或语法错误
                                escaped_comment_text = str(comment_text).replace("'", "''")
                                comment_sql = f'COMMENT ON COLUMN "{table_name}"."{col_name}" IS \'{escaped_comment_text}\';'
                                self.logger.info(f"准备为列 '{table_name}.{col_name}' 添加注释: {comment_sql}")
                                await conn.execute(comment_sql)
                                self.logger.debug(f"为列 '{table_name}.{col_name}' 添加注释成功。")

                    # --- 2. 构建并执行 CREATE INDEX 语句 --- 
                    # 为 date_column 创建索引 (如果需要且不是主键的一部分)
                    if date_column and date_column not in (primary_keys or []): # primary_keys可能为None
                        index_name_date = f"idx_{table_name}_{date_column}" # 标准索引命名
                        create_index_sql_date = f'CREATE INDEX IF NOT EXISTS "{index_name_date}" ON "{table_name}" ("{date_column}");'
                        self.logger.info(f"准备为 '{table_name}.{date_column}' 创建索引: {index_name_date}")
                        await conn.execute(create_index_sql_date)
                        self.logger.info(f"索引 '{index_name_date}' 创建成功或已存在。")

                    # 创建 schema 中定义的其他索引
                    if indexes and isinstance(indexes, list):
                        for index_def in indexes:
                            index_name = None
                            index_columns_str = None # 用于SQL语句的列字符串
                            unique = False
                            
                            if isinstance(index_def, dict): # 索引定义是字典
                                index_columns_list = index_def.get('columns') # 获取列名或列名列表
                                if not index_columns_list:
                                    self.logger.warning(f"跳过无效的索引定义 (缺少 columns): {index_def}")
                                    continue
                                # 将列名或列名列表转换为SQL字符串
                                if isinstance(index_columns_list, str):
                                    index_columns_str = f'"{index_columns_list}"'
                                elif isinstance(index_columns_list, list):
                                    index_columns_str = ', '.join([f'"{col}"' for col in index_columns_list])
                                else:
                                    self.logger.warning(f"索引定义中的 'columns' 类型无效: {index_columns_list}")
                                    continue
                                
                                # 规范化索引名称中列名的部分，移除特殊字符
                                safe_cols_for_name = str(index_columns_list).replace(' ', '').replace('"','').replace('[','').replace(']','').replace("'",'').replace(',','_')
                                index_name = index_def.get('name', f"idx_{table_name}_{safe_cols_for_name}")
                                unique = index_def.get('unique', False) # 是否是唯一索引

                            elif isinstance(index_def, str): # 索引定义是单个列名字符串
                                index_columns_str = f'"{index_def}"'
                                index_name = f"idx_{table_name}_{index_def}" # 标准索引命名
                            else: # 未知格式
                                self.logger.warning(f"跳过未知格式的索引定义: {index_def}")
                                continue
                            
                            unique_str = "UNIQUE " if unique else "" # UNIQUE关键字
                            create_index_sql = f'CREATE {unique_str}INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ({index_columns_str});'
                            self.logger.info(f"准备创建索引 '{index_name}' 于 '{table_name}({index_columns_str})': {unique_str.strip()}")
                            await conn.execute(create_index_sql)
                            self.logger.info(f"索引 '{index_name}' 创建成功或已存在。")
                            
                except Exception as e:
                    self.logger.error(f"创建表或索引 '{table_name}' 时失败: {e}", exc_info=True) # 记录详细异常信息
                    raise # 重新抛出异常以通知调用者失败
    
# === 工厂函数 ===

def create_async_manager(connection_string: str) -> DBManager:
    """创建异步模式的数据库管理器
    
    Args:
        connection_string (str): 数据库连接字符串
        
    Returns:
        DBManager: 异步模式的数据库管理器实例
    """
    return DBManager(connection_string, mode='async')

def create_sync_manager(connection_string: str) -> DBManager:
    """创建同步模式的数据库管理器
    
    专为 Backtrader 等同步环境设计，使用 psycopg2 提供真正的同步操作
    
    Args:
        connection_string (str): 数据库连接字符串
        
    Returns:
        DBManager: 同步模式的数据库管理器实例
    """
    return DBManager(connection_string, mode='sync')

# === 向后兼容别名 ===

# 为了保持向后兼容性，提供旧名称的别名
def SyncDBManager(connection_string: str):
    """向后兼容别名，推荐使用 create_sync_manager"""
    return create_sync_manager(connection_string)
    
