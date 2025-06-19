"""
数据库实用工具Mixin

该模块提供数据库的实用工具功能，为上层应用提供便捷的查询和分析接口。
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import asyncpg


class UtilityMixin:
    """数据库实用工具Mixin
    
    职责：
    ----
    提供数据库操作的实用工具方法，为应用层提供便捷的数据查询和分析功能。
    主要包括：
    1. 数据查询工具（最新日期、唯一值、列信息等）
    2. 连接测试和状态检查
    3. 数据统计和分析工具
    4. 便捷的数据访问接口
    
    核心方法：
    --------
    - **get_latest_date**: 获取指定表和列的最新日期
    - **get_distinct_values**: 获取指定列的所有唯一值
    - **get_column_names**: 获取表的所有列名
    - **test_connection**: 测试数据库连接状态
    - **get_database_stats**: 获取数据库统计信息
    
    设计特点：
    --------
    1. **高级抽象**: 封装常用的查询模式，简化业务代码
    2. **性能优化**: 使用适当的查询策略，避免全表扫描
    3. **表名解析**: 自动处理schema和表名的解析
    4. **错误处理**: 提供友好的错误信息和异常处理
    5. **类型安全**: 适当的类型提示和数据验证
    
    使用场景：
    --------
    - 获取数据更新状态和统计信息
    - 数据质量检查和验证
    - 应用健康状态监控
    - 数据探索和分析支持
    
    依赖关系：
    --------
    - 使用DatabaseOperationsMixin进行底层查询
    - 使用TableNameResolver进行表名解析
    - 与其他Mixin协同提供完整的数据库功能
    """

    async def get_latest_date(self, target: Any, date_column: str) -> Optional[datetime]:
        """获取指定表中日期列的最新日期

        Args:
            target (Any): 目标表，可以是表名字符串或任务对象
            date_column (str): 日期列名

        Returns:
            Optional[datetime]: 最新日期的datetime对象，如果表为空则返回None
        """
        schema, table_name = self.resolver.get_schema_and_table(target)  # type: ignore
        resolved_table_name = f'"{schema}"."{table_name}"'

        query = f'''
        SELECT MAX("{date_column}") 
        FROM {resolved_table_name}
        '''

        try:
            result = await self.fetch_val(query)  # type: ignore
            return result if result is not None else None
        except Exception as e:
            self.logger.error(  # type: ignore
                f"获取最新日期失败 (表: {resolved_table_name}, 列: {date_column}): {e}"
            )
            raise

    async def get_latest_update_time(self, target: Any) -> Optional[datetime]:
        """获取指定表中'update_time'列的最新时间，是get_latest_date的特例。

        Args:
            target (Any): 目标表，可以是表名字符串或任务对象

        Returns:
            Optional[datetime]: 最新时间的datetime对象，如果表为空则返回None
        """
        try:
            # 直接调用 get_latest_date，并硬编码列名为 "update_time"
            return await self.get_latest_date(target, "update_time")
        except asyncpg.exceptions.UndefinedColumnError:
            # 如果 'update_time' 列不存在，则返回 None 而不是抛出异常
            self.logger.warning(f"表 '{target}' 中没有找到 'update_time' 列。")
            return None
        except Exception as e:
            # 捕获其他可能的异常，如表不存在
            self.logger.error(f"为表 '{target}' 获取最新更新时间时失败: {e}")
            return None

    async def get_distinct_values(
        self, target: Any, column_name: str, limit: Optional[int] = None
    ) -> List[Any]:
        """获取指定列的所有唯一值

        Args:
            target (Any): 目标表，可以是表名字符串或任务对象
            column_name (str): 列名
            limit (Optional[int]): 限制返回的记录数，默认无限制

        Returns:
            List[Any]: 唯一值列表
        """
        schema, table_name = self.resolver.get_schema_and_table(target)  # type: ignore
        resolved_table_name = f'"{schema}"."{table_name}"'

        limit_clause = f"LIMIT {limit}" if limit else ""
        query = f'''
        SELECT DISTINCT "{column_name}"
        FROM {resolved_table_name}
        ORDER BY "{column_name}"
        {limit_clause}
        '''

        try:
            records = await self.fetch(query)  # type: ignore
            return [record[column_name] for record in records]
        except Exception as e:
            self.logger.error(  # type: ignore
                f"获取唯一值失败 (表: {resolved_table_name}, 列: {column_name}): {e}"
            )
            raise

    async def get_column_names(self, target: Any) -> List[str]:
        """获取表的所有列名

        Args:
            target (Any): 目标表，可以是表名字符串或任务对象

        Returns:
            List[str]: 列名列表
        """
        schema, table_name = self.resolver.get_schema_and_table(target)  # type: ignore

        query = f'''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        '''

        try:
            records = await self.fetch(query, schema, table_name)  # type: ignore
            return [record["column_name"] for record in records]
        except Exception as e:
            self.logger.error(  # type: ignore
                f"获取列名失败 (schema: {schema}, table: {table_name}): {e}"
            )
            raise

    async def test_connection(self) -> bool:
        """测试数据库连接

        Returns:
            bool: 连接状态，True表示连接正常
        """
        try:
            result = await self.fetch_val("SELECT 1")  # type: ignore
            return result == 1
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {e}")  # type: ignore
            return False

    async def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息

        Returns:
            Dict[str, Any]: 包含数据库统计信息的字典
        """
        stats = {}

        try:
            # 获取数据库版本
            version = await self.fetch_val("SELECT version()")  # type: ignore
            stats["version"] = str(version) if version else "Unknown"

            # 获取当前时间
            current_time = await self.fetch_val("SELECT NOW()")  # type: ignore
            stats["current_time"] = str(current_time) if current_time else "Unknown"

            # 获取活动连接数
            connections = await self.fetch_val(  # type: ignore
                "SELECT count(*) FROM pg_stat_activity"
            )
            stats["active_connections"] = connections if connections else 0

            return stats

        except Exception as e:
            self.logger.error(f"获取数据库统计信息失败: {e}")  # type: ignore
            return {"error": str(e)}

    async def analyze_table_performance(self, target: Any) -> Dict[str, Any]:
        """分析表的性能统计信息

        Args:
            target (Any): 目标表，可以是表名字符串或任务对象

        Returns:
            Dict[str, Any]: 包含表性能统计的字典
        """
        schema, table_name = self.resolver.get_schema_and_table(target)  # type: ignore
        stats = {}

        try:
            # 获取表大小
            size_query = '''
            SELECT pg_size_pretty(pg_total_relation_size($1::regclass)) as table_size,
                   pg_size_pretty(pg_relation_size($1::regclass)) as data_size
            '''
            size_result = await self.fetch_one(  # type: ignore
                size_query, f'"{schema}"."{table_name}"'
            )
            if size_result:
                stats.update(dict(size_result))

            # 获取行数估计
            count_query = f'''
            SELECT reltuples::bigint as estimated_rows
            FROM pg_class
            WHERE oid = '"{schema}"."{table_name}"'::regclass
            '''
            count_result = await self.fetch_val(count_query)  # type: ignore
            stats["estimated_rows"] = count_result if count_result else 0

            return stats

        except Exception as e:
            self.logger.error(  # type: ignore
                f"分析表性能失败 (schema: {schema}, table: {table_name}): {e}"
            )
            return {"error": str(e)}
