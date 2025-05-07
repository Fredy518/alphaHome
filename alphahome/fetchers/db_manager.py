import asyncio
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

import asyncpg
import pandas as pd

class DBManager:
    """数据库连接管理器"""
    
    def __init__(self, connection_string: str):
        """初始化数据库连接管理器
        
        Args:
            connection_string: 数据库连接字符串，格式为：
                postgresql://username:password@host:port/database
        """
        self.connection_string = connection_string
        self.pool = None
        self.logger = logging.getLogger("db_manager")
    
    async def connect(self):
        """创建数据库连接池"""
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(self.connection_string)
                self.logger.info("数据库连接池创建成功")
            except Exception as e:
                self.logger.error(f"数据库连接池创建失败: {str(e)}")
                raise
    
    async def close(self):
        """关闭数据库连接池"""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            self.logger.info("数据库连接池已关闭")
    
    async def execute(self, query: str, *args):
        """执行SQL语句
        
        Args:
            query: SQL语句
            *args: SQL参数
            
        Returns:
            执行结果
        """
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.execute(query, *args)
                return result
            except Exception as e:
                self.logger.error(f"SQL执行失败: {str(e)}\nSQL: {query}\n参数: {args}")
                raise
    
    async def fetch(self, query: str, *args):
        """执行查询并返回所有结果
        
        Args:
            query: SQL查询语句
            *args: SQL参数
            
        Returns:
            查询结果列表
        """
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetch(query, *args)
                return result
            except Exception as e:
                self.logger.error(f"SQL查询失败: {str(e)}\nSQL: {query}\n参数: {args}")
                raise
    
    async def fetch_one(self, query: str, *args):
        """执行查询并返回第一行结果
        
        Args:
            query: SQL查询语句
            *args: SQL参数
            
        Returns:
            查询结果的第一行，如果没有结果则返回None
        """
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchrow(query, *args)
                return result
            except Exception as e:
                self.logger.error(f"SQL查询失败: {str(e)}\nSQL: {query}\n参数: {args}")
                raise
    
    async def fetch_val(self, query: str, *args):
        """执行查询并返回第一行第一列的值
        
        Args:
            query: SQL查询语句
            *args: SQL参数
            
        Returns:
            查询结果的第一行第一列的值，如果没有结果则返回None
        """
        if self.pool is None:
            await self.connect()
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval(query, *args)
                return result
            except Exception as e:
                self.logger.error(f"SQL查询失败: {str(e)}\nSQL: {query}\n参数: {args}")
                raise
    
    async def executemany(self, query: str, args_list, stop_event: Optional[asyncio.Event] = None): # Add stop_event
        """批量执行SQL语句
        
        Args:
            query: SQL语句
            args_list: 参数列表的列表
            
        Returns:
            影响的行数
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
                        # Check stop event before executing each statement in the batch
                        if stop_event and stop_event.is_set():
                            # Removed DEBUG log for cancellation point
                            # Option 1: Raise CancelledError to propagate cancellation
                            raise asyncio.CancelledError("批量执行被用户取消")
                            # Option 2: Return partial count (less explicit about cancellation)
                            # return count
                        
                        # 使用fetchrow代替execute，因为PreparedStatement没有execute方法
                        await stmt.fetchrow(*args)
                        count += 1
                    
                    return count # Return full count if loop completes
            except Exception as e:
                self.logger.error(f"批量SQL执行失败: {str(e)}\nSQL: {query}")
                raise
    
    async def copy_from_dataframe(self, 
                                  df: pd.DataFrame, 
                                  table_name: str,
                                  conflict_columns: Optional[List[str]] = None,
                                  update_columns: Optional[List[str]] = None,
                                  timestamp_column: Optional[str] = None):
        """将DataFrame数据高效复制并可选地UPSERT到数据库表中
        
        利用 PostgreSQL 的 COPY 命令和临时表实现高效数据加载。
        如果指定了 conflict_columns，则执行 UPSERT 操作。
        
        Args:
            df: 要复制的DataFrame
            table_name: 目标表名
            conflict_columns: 用于检测冲突的列名列表。如果为None，则执行简单插入。
            update_columns: 发生冲突时要更新的列名列表。如果为None且conflict_columns指定，则更新所有非冲突列。
            timestamp_column: 时间戳列名，如果指定并在冲突时更新，该列将自动更新为当前时间。
            
        Returns:
            影响的总行数 (COPY到临时表的行数)
            
        Raises:
            ValueError: 参数无效或DataFrame为空
            Exception: 数据库操作错误
        """
        if self.pool is None:
            await self.connect()
        
        if df.empty:
            self.logger.info(f"copy_from_dataframe: DataFrame for table '{table_name}' is empty. Skipping.")
            return 0
            
        if conflict_columns and not conflict_columns: # Check for empty list
             raise ValueError("conflict_columns cannot be an empty list if provided.")
             
        # 获取DataFrame的列名 - 这些将用于COPY
        df_columns = list(df.columns)
        
        # --- 验证列名 ---
        if conflict_columns:
            for col in conflict_columns:
                if col not in df_columns:
                    raise ValueError(f"Conflict column '{col}' not found in DataFrame columns.")
        if update_columns:
            # Allow update_columns to be empty list for DO NOTHING case if conflict_columns is set
            if not isinstance(update_columns, list):
                raise ValueError("update_columns must be a list or None.")
            for col in update_columns:
                if col not in df_columns:
                    raise ValueError(f"Update column '{col}' not found in DataFrame columns.")
        if timestamp_column and timestamp_column not in df_columns:
            # Timestamp column *must* exist in the DataFrame if provided, 
            # even if we overwrite it during UPDATE
            self.logger.warning(f"Timestamp column '{timestamp_column}' not found in DataFrame, "
                            f"it will be added if needed for UPDATE, but initial INSERT might fail if it's required by the table.")
            # Consider adding it if missing? For now, assume it exists or DB handles it.

        # 创建一个唯一的临时表名
        # Using milliseconds for higher chance of uniqueness in concurrent scenarios
        timestamp_ms = int(datetime.now().timestamp() * 1000)
        temp_table = f"temp_{table_name}_{timestamp_ms}_{id(df)}"
        
        # ON COMMIT DROP ensures cleanup even if errors occur after commit starts but before drop
        create_temp_table_sql = f"""
        CREATE TEMPORARY TABLE "{temp_table}" (LIKE "{table_name}" INCLUDING DEFAULTS) ON COMMIT DROP;
        """
        
        # 准备数据: asyncpg's copy_records_to_table handles type conversion
        # Important: Ensure DataFrame dtypes align reasonably with DB types.
        # Convert NaN to None before passing to copy_records_to_table
        data_records = [
            tuple(None if pd.isna(val) else val for val in row) 
            for row in df.itertuples(index=False, name=None)
        ]

        if not data_records:
            self.logger.info(f"copy_from_dataframe: No data records to copy for table '{table_name}' after processing DataFrame.")
            return 0
            
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # 1. 创建临时表
                    await conn.execute(create_temp_table_sql)
                    self.logger.debug(f"Created temporary table {temp_table}")

                    # 2. 使用 COPY 高效加载数据到临时表
                    # copy_records_to_table needs column names matching the *table* definition
                    # Since temp table is LIKE target_table, df_columns should match target table columns
                    copy_count = await conn.copy_records_to_table(
                        temp_table, 
                        records=data_records,
                        columns=df_columns,
                        timeout=300 # Increase timeout for large copies
                    )
                    self.logger.debug(f"Copied {copy_count} records to {temp_table}")

                    # 3. 从临时表插入/更新到目标表
                    target_col_str = ', '.join([f'"{col}"' for col in df_columns]) # Columns from DataFrame
                    
                    if conflict_columns:
                        # --- UPSERT Logic ---
                        conflict_col_str = ', '.join([f'"{col}"' for col in conflict_columns])
                        
                        # Determine columns to update
                        actual_update_columns = []
                        if update_columns is None:
                            # Default: update all non-conflict columns present in the DataFrame
                            actual_update_columns = [col for col in df_columns if col not in conflict_columns]
                        else:
                            # Use explicitly provided update_columns
                            actual_update_columns = update_columns
                            
                        # Build SET clause
                        if actual_update_columns:
                            set_clauses = [f'"{col}" = EXCLUDED."{col}"' for col in actual_update_columns]
                            # Handle timestamp update
                            if timestamp_column and timestamp_column in df_columns: 
                                # Ensure timestamp column is updated if specified, even if not in update_columns list explicitly
                                # But only if it's not a conflict column itself
                                if timestamp_column not in conflict_columns:
                                    # Avoid duplicate SET if already included
                                    if f'"{timestamp_column}" = EXCLUDED."{timestamp_column}"' not in set_clauses:
                                        # Prefer NOW() for consistency in bulk upsert unless specific timestamp needed
                                        set_clauses.append(f'"{timestamp_column}" = NOW()') 
                                        # If you need the exact timestamp from the file:
                                        # set_clauses.append(f'"{timestamp_column}" = EXCLUDED."{timestamp_column}"') 
                            
                            update_clause = ', '.join(set_clauses)
                            action_sql = f"DO UPDATE SET {update_clause}"
                        else:
                            action_sql = "DO NOTHING" # No columns to update or explicitly told not to update

                        insert_sql = f"""
                        INSERT INTO "{table_name}" ({target_col_str})
                        SELECT {target_col_str} 
                        FROM "{temp_table}"
                        ON CONFLICT ({conflict_col_str}) {action_sql};
                        """
                    else:
                        # --- Simple INSERT Logic ---
                        insert_sql = f"""
                        INSERT INTO "{table_name}" ({target_col_str})
                        SELECT {target_col_str} 
                        FROM "{temp_table}";
                        """
                        
                    self.logger.debug(f"Executing final insert/upsert from temp table:\n{insert_sql}")
                    # Execute the final transfer
                    status = await conn.execute(insert_sql)
                    # The status might report INSERT 0 if all rows conflicted and did DO NOTHING/UPDATE
                    # copy_count reflects rows loaded to temp table, which is a better measure of work done.
                    self.logger.debug(f"Final insert/upsert command status: {status}")
                    
                    # --- !! FIX: Parse integer from status string !! ---
                    rows_copied = 0
                    try:
                        # copy_count is a string like 'COPY 12345'
                        if isinstance(copy_count, str) and copy_count.startswith('COPY '):
                            rows_copied = int(copy_count.split()[-1])
                        else:
                            self.logger.warning(f"copy_records_to_table returned unexpected status: {copy_count}. Assuming 0 rows copied.")
                    except (ValueError, IndexError) as parse_err:
                        self.logger.error(f"Failed to parse row count from copy_count '{copy_count}': {parse_err}. Assuming 0 rows copied.")
                    
                    return rows_copied # Return the parsed integer count

                except asyncpg.exceptions.UndefinedTableError as e:
                    # Specific error for table not existing during LIKE
                    self.logger.error(f"Failed to create temp table like '{table_name}'. Target table might not exist. Error: {e}")
                    raise ValueError(f"Target table '{table_name}' not found.") from e
                except asyncpg.exceptions.UndefinedColumnError as e:
                    # Column mismatch between DataFrame and Table
                    self.logger.error(f"Column mismatch error during copy/insert for table '{table_name}'. Error: {e}")
                    self.logger.error(f"DataFrame columns: {df_columns}")
                    # Consider fetching actual table columns here for comparison if needed
                    raise ValueError(f"Column mismatch between DataFrame and table '{table_name}'. Check DataFrame columns and table schema.") from e
                except Exception as e:
                    self.logger.error(f"高效复制/UPSERT操作失败 (Table: {table_name}): {str(e)}")
                    # Log temp table name for potential manual inspection if ON COMMIT DROP fails somehow
                    self.logger.error(f"Temporary table was: {temp_table}") 
                    raise
                # Temporary table is dropped automatically due to ON COMMIT DROP clause upon successful commit or rollback.
    
    async def table_exists(self, table_name: str) -> bool:
        """检查表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            如果表存在则返回True，否则返回False
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
        );
        """
        result = await self.fetch_val(query, table_name)
        return result
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表结构
        
        Args:
            table_name: 表名
            
        Returns:
            表结构信息列表，每个元素是一个字典，包含列名、类型等信息
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
                "is_nullable": row["is_nullable"] == "YES",
                "default": row["column_default"]
            })
        
        return schema
    
    async def get_latest_date(self, table_name: str, date_column: str, return_raw_object: bool = False) -> Optional[Union[str, datetime]]:
        """获取表中指定列的最大值（通常是日期或时间戳）
        
        Args:
            table_name: 表名
            date_column: 列名 (例如日期或时间戳列)
            return_raw_object: 如果为 True，返回原始 datetime 对象；否则返回 YYYYMMDD 格式字符串。
            
        Returns:
            该列的最大值 (datetime 或 YYYYMMDD str) 或 None
        """
        query = f"""
        SELECT MAX(\"{date_column}\") FROM \"{table_name}\";
        """
        try:
            # fetch_val should return the raw value from the DB, 
            # which asyncpg maps to python types (e.g., timestamp -> datetime)
            result = await self.fetch_val(query)
            
            # Log the result type before returning
            if result is not None:
                self.logger.debug(f"get_latest_date for {table_name}.{date_column} returning type: {type(result)}")
            else:
                self.logger.debug(f"get_latest_date for {table_name}.{date_column} returning None")

            # --- Conditional formatting for backward compatibility --- 
            if result is not None and not return_raw_object:
                # Default behavior: format to YYYYMMDD string
                formatted_result = None
                if isinstance(result, str):
                    # Handle potential string input from DB (though less likely with asyncpg)
                    if '-' in result:
                        try: formatted_result = datetime.strptime(result, '%Y-%m-%d').strftime('%Y%m%d');
                        except ValueError: self.logger.warning(f"Could not parse date string '{result}' as YYYY-MM-DD.")
                    elif len(result) == 8 and result.isdigit():
                        formatted_result = result # Assume it's already YYYYMMDD
                    else:
                        self.logger.warning(f"Received unexpected string format '{result}'.")
                elif isinstance(result, datetime):
                    # Convert datetime/date object to YYYYMMDD string
                    formatted_result = result.strftime('%Y%m%d')
                else:
                    self.logger.warning(f"Received unexpected type '{type(result)}'.")
                
                return formatted_result # Return formatted string or None if formatting failed
            else:
                # return_raw_object is True OR result is None
                return result # Return raw datetime object or None

        except Exception as e:
            self.logger.warning(f"查询表 {table_name} 列 {date_column} 的最大值时出错: {e}")
            return None # Return None on error

    async def upsert(self,
                    table_name: str,
                    data: Union[pd.DataFrame, List[Dict[str, Any]]],
                    conflict_columns: List[str],
                    update_columns: Optional[List[str]] = None,
                    timestamp_column: Optional[str] = None,
                    stop_event: Optional[asyncio.Event] = None) -> int: # Add stop_event
        """通用UPSERT操作：插入数据，如有冲突则更新
        
        利用 COPY + 临时表 + INSERT ON CONFLICT 策略提高效率。
        
        Args:
            table_name: 目标表名
            data: 要插入的数据，可以是DataFrame或字典列表
            conflict_columns: 用于检测冲突的列名列表（通常是主键或唯一索引）
            update_columns: 发生冲突时要更新的列名列表。如果为None，则更新所有非冲突列。
            timestamp_column: 时间戳列名。如果指定，在冲突更新时该列会更新为NOW()，
                              插入时需要确保DataFrame中此列有值（或表有默认值）。
            stop_event: 用于取消操作的异步事件 (注意: COPY操作通常不能优雅地中途中断，
                         此事件主要用于在调用前检查，或在未来可能的批处理中)
            
        Returns:
            影响的行数 (基于COPY到临时表的行数)
            
        Raises:
            ValueError: 当数据为空或参数无效时
            Exception: 数据库操作错误
        """
        if self.pool is None:
            await self.connect()
        
        # --- 参数检查 ---
        if not table_name:
            raise ValueError("表名不能为空")
        if not conflict_columns:
            raise ValueError("必须指定冲突检测列")
        if not isinstance(conflict_columns, list) or len(conflict_columns) == 0:
            raise ValueError("conflict_columns 必须是一个非空列表")
            
        # --- 数据准备 ---
        df_to_process = None
        if isinstance(data, pd.DataFrame):
            if data.empty:
                self.logger.info(f"UPSERT ({table_name}): 输入的DataFrame为空.")
                return 0
            df_to_process = data.copy() # Use a copy to avoid modifying original
        elif isinstance(data, list):
            if not data:
                self.logger.info(f"UPSERT ({table_name}): 输入的列表为空.")
                return 0
            # Convert list of dicts to DataFrame for consistent processing
            try:
                df_to_process = pd.DataFrame(data)
            except Exception as e:
                self.logger.error(f"UPSERT ({table_name}): 无法将字典列表转换为DataFrame: {e}")
                raise ValueError("无法将输入的字典列表转换为DataFrame") from e
            if df_to_process.empty:
                self.logger.info(f"UPSERT ({table_name}): 从列表转换的DataFrame为空.")
                return 0
        else:
            raise ValueError("data参数必须是DataFrame或字典列表")
            
        # --- 预处理 (移到 copy_from_dataframe 内部处理更优，但保留部分检查) ---
        
        # 1. 检查 Stop Event (在耗时操作前)
        if stop_event and stop_event.is_set():
            self.logger.warning(f"UPSERT 操作 ({table_name}) 在开始前被取消。")
            raise asyncio.CancelledError("UPSERT操作在开始前被取消")
        
        # 2. 时间戳处理 (确保列存在于 DataFrame)
        current_time = datetime.now() # Not strictly needed here anymore if NOW() used in SQL
        if timestamp_column:
            if timestamp_column not in df_to_process.columns:
                # Add the column if missing, initialize with current time
                # This helps if the target table requires it, but it will be NOW() on update anyway
                self.logger.warning(f"UPSERT ({table_name}): Timestamp column '{timestamp_column}' not found in input data, adding it with current time.")
                df_to_process[timestamp_column] = current_time
            else:
                # Optional: Fill NaN in existing timestamp column if desired, 
                # otherwise leave as is for COPY
                # df_to_process[timestamp_column] = df_to_process[timestamp_column].fillna(current_time)
                pass # Let copy_from_dataframe handle NaNs -> NULLs

        # 3. 移除冲突列为 NULL 的行 (重要! ON CONFLICT 不适用于 NULL)
        initial_rows = len(df_to_process)
        # Create boolean mask for rows where *any* conflict column is NaN/None
        null_mask = df_to_process[conflict_columns].isnull().any(axis=1)
        if null_mask.any():
            df_to_process = df_to_process[~null_mask]
            removed_count = initial_rows - len(df_to_process)
            self.logger.warning(
                f"UPSERT ({table_name}): 移除了 {removed_count} 行，因为其冲突列 "
                f"({', '.join(conflict_columns)}) 包含NULL值。"
            )
            if df_to_process.empty:
                self.logger.info(f"UPSERT ({table_name}): 移除包含NULL的主键行后，没有有效数据可执行。")
                return 0
                
        # --- 执行 --- 
        try:
            # 使用重构后的 copy_from_dataframe 执行 UPSERT
            # NaN values in the DataFrame will be handled inside copy_from_dataframe
            affected_rows = await self.copy_from_dataframe(
                df=df_to_process,
                table_name=table_name,
                conflict_columns=conflict_columns,
                update_columns=update_columns,
                timestamp_column=timestamp_column
                # stop_event is checked above, COPY itself is harder to interrupt
            )
            self.logger.info(f"UPSERT ({table_name}) 完成，通过 COPY 处理了 {affected_rows} 行数据到临时表。")
            return affected_rows
        except asyncio.CancelledError: 
            # Should be caught before calling copy_from_dataframe, but catch just in case
            self.logger.warning(f"UPSERT 操作 ({table_name}) 被取消。")
            raise 
        except Exception as e:
            self.logger.error(f"UPSERT操作失败 (使用COPY策略): {str(e)}\n表: {table_name}")
            # Avoid logging potentially large dataframes here
            raise

    # ===============================================
    # == Deprecated/Alternative Upsert Implementation (using executemany) ==
    # You might keep this around for comparison or specific small-batch scenarios
    # ===============================================
    # async def upsert_executemany(self,
    #                 table_name: str,
    #                 data: Union[pd.DataFrame, List[Dict[str, Any]]],
    #                 conflict_columns: List[str],
    #                 update_columns: Optional[List[str]] = None,
    #                 timestamp_column: Optional[str] = None,
    #                 stop_event: Optional[asyncio.Event] = None) -> int:
    #     """ [Original docstring] """
    #     if self.pool is None:
    #         await self.connect()
        
    #     # ... [Original parameter checks] ...
        
    #     records = []
    #     # ... [Original data conversion logic] ...
            
    #     current_time = datetime.now()
    #     # ... [Original timestamp logic] ...
        
    #     columns = list(records[0].keys())
    #     # ... [Original update_columns logic] ...
        
    #     # ... [Original SQL build logic] ...
        
    #     values = []
    #     # ... [Original value preparation with NaN -> None] ...

    #     # ... [Original NULL conflict key filtering logic] ...

    #     try:
    #         affected_rows = await self.executemany(upsert_sql, values, stop_event=stop_event)
    #         return affected_rows
    #     # ... [Original exception handling] ...