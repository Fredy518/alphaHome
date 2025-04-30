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
    
    async def copy_from_dataframe(self, df: pd.DataFrame, table_name: str):
        """将DataFrame数据复制到数据库表中
        
        Args:
            df: 要复制的DataFrame
            table_name: 目标表名
            
        Returns:
            复制的行数
        """
        if self.pool is None:
            await self.connect()
        
        if df.empty:
            return 0
        
        # 获取列名
        columns = list(df.columns)
        
        # 创建临时表
        temp_table = f"temp_{table_name}_{id(df)}"
        create_temp_table_sql = f"""
        CREATE TEMPORARY TABLE {temp_table} (
            {', '.join([f'"{col}" TEXT' for col in columns])}
        ) ON COMMIT DROP;
        """
        
        # 准备数据
        data = [[str(val) if val is not None else None for val in row] for row in df.values]
        
        async with self.pool.acquire() as conn:
            try:
                # 开始事务
                async with conn.transaction():
                    # 创建临时表
                    await conn.execute(create_temp_table_sql)
                    
                    # 复制数据到临时表
                    count = await conn.copy_records_to_table(
                        temp_table, 
                        records=data,
                        columns=columns
                    )
                    
                    # 从临时表插入到目标表
                    insert_sql = f"""
                    INSERT INTO {table_name} (
                        {', '.join([f'"{col}"' for col in columns])}
                    )
                    SELECT {', '.join([f'"{col}"' for col in columns])} 
                    FROM {temp_table};
                    """
                    await conn.execute(insert_sql)
                    
                    return count
            except Exception as e:
                self.logger.error(f"从DataFrame复制数据失败: {str(e)}")
                raise
    
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

        PostgreSQL的UPSERT实现：当插入的记录与已有记录在指定的列上发生冲突时，
        可以选择更新部分列或者忽略此次插入。
        
        Args:
            table_name: 目标表名
            data: 要插入的数据，可以是DataFrame或字典列表
            conflict_columns: 用于检测冲突的列名列表（通常是主键或唯一索引）
            update_columns: 发生冲突时要更新的列名列表，如果为None，则更新所有非冲突列
            timestamp_column: 时间戳列名，如果指定，该列将自动更新为当前时间
            
        Returns:
            影响的行数
            
        Raises:
            ValueError: 当数据为空或参数无效时
            Exception: 数据库操作错误
        """
        if self.pool is None:
            await self.connect()
        
        # 检查参数有效性
        if not table_name:
            raise ValueError("表名不能为空")
        
        if not conflict_columns:
            raise ValueError("必须指定冲突检测列")
            
        # 将DataFrame转换为字典列表
        records = []
        if isinstance(data, pd.DataFrame):
            if data.empty:
                return 0
            # 使用 fillna(pd.NA).to_dict('records') 可能更优雅地处理 NaN 转 None
            # 但为了明确处理逻辑，我们先手动转换
            # Important: Convert NaN to None *before* converting to dict might lose distinction
            # Better to handle it during value extraction below.
            records = data.to_dict('records') 
        elif isinstance(data, list):
            if not data:
                return 0
            records = data
        else:
            raise ValueError("data参数必须是DataFrame或字典列表")
        
        # 准备时间戳
        current_time = datetime.now()
        
        # 添加时间戳列
        if timestamp_column:
            for record in records:
                record[timestamp_column] = current_time
        
        # 获取列名
        columns = list(records[0].keys())
        
        # 如果未指定更新列，则更新所有非冲突列
        if update_columns is None:
            update_columns = [col for col in columns if col not in conflict_columns]
        
        # 检查更新列的有效性
        for col in update_columns:
            if col not in columns:
                raise ValueError(f"更新列 '{col}' 不在数据列中")
        
        # 构建UPSERT SQL语句
        placeholders = ', '.join([f'${i+1}' for i in range(len(columns))])
        column_str = ', '.join([f'"{col}"' for col in columns]) # Ensure columns are quoted
        conflict_col_str = ', '.join([f'"{col}"' for col in conflict_columns]) # Quote conflict columns
        
        if update_columns:
            # 构建更新子句
            update_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns]) # Quote update columns
            upsert_sql = f"""
            INSERT INTO {table_name} ({column_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_col_str}) 
            DO UPDATE SET {update_clause};
            """
        else:
            # 如果没有需要更新的列，则冲突时不做任何操作
            upsert_sql = f"""
            INSERT INTO {table_name} ({column_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_col_str}) 
            DO NOTHING;
            """
        
        # 准备数据，并将 NaN 替换为 None
        values = []
        for record in records:
            row_values = []
            for col in columns:
                val = record.get(col)
                # 检查是否为 NaN (使用 pandas.isna)
                if pd.isna(val):
                    row_values.append(None) # 将 NaN 替换为 None
                else:
                    row_values.append(val)
            values.append(row_values)
        
        # Filter rows where any conflict column value is None
        if conflict_columns:
            try:
                conflict_indices = [columns.index(col) for col in conflict_columns]
            except ValueError as e:
                self.logger.error(f"UPSERT 失败：冲突列 '{str(e).split()[-1]}' 不在数据列中。")
                raise ValueError(f"Conflict column {str(e).split()[-1]} not found in data columns")
            
            filtered_values = []
            invalid_rows_count = 0
            for row_values in values:
                has_null_in_conflict = False
                for index in conflict_indices:
                    if row_values[index] is None:
                        has_null_in_conflict = True
                        break
                if not has_null_in_conflict:
                    filtered_values.append(row_values)
                else:
                    invalid_rows_count += 1
            
            if invalid_rows_count > 0:
                self.logger.warning(
                    f"UPSERT ({table_name}): 发现 {invalid_rows_count} 行的冲突列 "
                    f"({', '.join(conflict_columns)}) 包含NULL值，将移除这些行。"
                )
            
            if not filtered_values:
                self.logger.info(f"UPSERT ({table_name}): 移除包含NULL的主键行后，没有有效数据可执行。")
                return 0 # No valid rows left to upsert
            
            values = filtered_values # Use the filtered list for execution

        try:
            # 执行UPSERT, pass stop_event to executemany
            affected_rows = await self.executemany(upsert_sql, values, stop_event=stop_event)
            return affected_rows
        except asyncio.CancelledError: # Propagate cancellation if executemany raises it
             self.logger.warning(f"UPSERT 操作 ({table_name}) 被取消。")
             raise # Re-raise CancelledError
        except Exception as e:
            # Log the problematic SQL and values for easier debugging
            self.logger.error(f"UPSERT操作失败: {str(e)}\n表: {table_name}\nSQL: {upsert_sql}\n第一行数据示例: {values[0] if values else '无数据'}")
            raise