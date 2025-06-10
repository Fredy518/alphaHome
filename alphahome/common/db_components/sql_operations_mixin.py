import asyncio
from typing import Any, List, Optional

import asyncpg
import psycopg2.extras


class SQLOperationsMixin:
    """基础SQL操作Mixin - 提供execute, fetch等基础数据库操作方法"""

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
                self.logger.error(
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
        if self.pool is None:
            await self.connect()

        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetch(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(
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
        if self.pool is None:
            await self.connect()

        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchrow(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(
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
        if self.pool is None:
            await self.connect()

        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval(query, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(
                    f"SQL查询失败: {str(e)}\nSQL: {query}\n位置参数: {args}\n关键字参数: {kwargs}"
                )
                raise

    # === 统一同步方法接口 ===

    def execute_sync(self, query: str, params: tuple = None):
        """同步执行SQL语句"""
        if self.mode == "async":
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.execute(query, *params))
            else:
                return self._run_sync(self.execute(query))
        elif self.mode == "sync":
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
        if self.mode == "async":
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch(query, *params))
            else:
                return self._run_sync(self.fetch(query))
        elif self.mode == "sync":
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()
            try:
                with connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    return [dict(row) for row in rows]
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")
                connection.rollback()
                raise

    def fetch_one_sync(self, query: str, params: tuple = None):
        """同步执行查询并返回第一行结果"""
        if self.mode == "async":
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch_one(query, *params))
            else:
                return self._run_sync(self.fetch_one(query))
        elif self.mode == "sync":
            # 同步模式：直接使用 psycopg2
            connection = self._get_sync_connection()
            try:
                with connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    cursor.execute(query, params)
                    row = cursor.fetchone()
                    return dict(row) if row else None
            except Exception as e:
                self.logger.error(f"同步SQL查询失败: {e}\nSQL: {query}\n参数: {params}")
                connection.rollback()
                raise

    def fetch_val_sync(self, query: str, params: tuple = None):
        """同步执行查询并返回第一行第一列的值"""
        if self.mode == "async":
            # 异步模式：包装异步方法
            if params:
                return self._run_sync(self.fetch_val(query, *params))
            else:
                return self._run_sync(self.fetch_val(query))
        elif self.mode == "sync":
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

    async def executemany(
        self,
        query: str,
        args_list: List[tuple],
        stop_event: Optional[asyncio.Event] = None,
    ):
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
                            # 抛出 CancelledError 以传播取消信号
                            raise asyncio.CancelledError("批量执行被用户取消")

                        # 使用fetchrow代替execute，因为PreparedStatement没有execute方法
                        await stmt.fetchrow(
                            *args
                        )  # fetchrow 适合预期单行返回或无返回的情况
                        count += 1

                    return count  # 如果循环完成，返回完整计数
            except Exception as e:
                self.logger.error(f"批量SQL执行失败: {str(e)}\nSQL: {query}")
                raise
