import asyncio
import logging
from typing import Dict, List, Any, Optional, Union

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
    
    async def executemany(self, query: str, args_list):
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
                        # 使用fetchrow代替execute，因为PreparedStatement没有execute方法
                        await stmt.fetchrow(*args)
                        count += 1
                    
                    return count
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
    
    async def get_latest_date(self, table_name: str, date_column: str) -> Optional[str]:
        """获取表中最新日期
        
        Args:
            table_name: 表名
            date_column: 日期列名
            
        Returns:
            最新日期，如果表为空则返回None
        """
        query = f"""
        SELECT MAX({date_column}) FROM {table_name};
        """
        result = await self.fetch_val(query)
        
        if result is not None:
            # 格式化日期为YYYYMMDD格式
            if isinstance(result, str):
                # 如果已经是字符串，尝试标准化格式
                if '-' in result:
                    # 假设格式为YYYY-MM-DD
                    from datetime import datetime
                    result = datetime.strptime(result, '%Y-%m-%d').strftime('%Y%m%d')
            else:
                # 如果是日期对象，转换为字符串
                result = result.strftime('%Y%m%d')
        
        return result
