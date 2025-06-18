from datetime import datetime, date
from typing import Optional, Union, List, Any


class UtilityMixin:
    """实用工具Mixin - 提供各种实用工具方法"""

    def test_connection(self):
        """测试数据库连接"""
        try:
            if self.mode == "sync": # type: ignore
                result = self.fetch_val_sync("SELECT 1") # type: ignore
            else:
                # 异步模式下不能在此方法中直接测试，需要异步调用
                self.logger.warning( # type: ignore
                    "异步模式下test_connection方法不可用，请使用异步测试方法"
                )
                return False
            return result == 1
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {e}") # type: ignore
            return False

    async def get_latest_date(self, target: Any, date_column: str) -> Optional[date]:
        """获取指定表中指定日期列的最新日期

        Args:
            target (Any): 表名字符串或任务对象 (任何拥有 table_name 和 data_source 属性的对象)
            date_column (str): 日期列名

        Returns:
            Optional[datetime.date]: 最新日期，如果表为空或不存在则返回None
        """
        resolved_table_name = self.resolver.get_full_name(target) # type: ignore
        
        # 首先检查表是否存在，如果不存在则直接返回None
        exists = await self.table_exists(resolved_table_name) # type: ignore
        if not exists:
            self.logger.warning( # type: ignore
                f"尝试从不存在的表 '{resolved_table_name}' 获取最新日期，返回 None。"
            )
            return None

        # 构建查询语句
        query = f'SELECT MAX("{date_column}") FROM {resolved_table_name};'

        try:
            latest_date_val = await self.fetch_val(query) # type: ignore
            if latest_date_val is None:
                self.logger.info(f"表 '{resolved_table_name}' 为空，无最新日期。") # type: ignore
                return None
            
            # 确保返回的是 date 对象
            if isinstance(latest_date_val, datetime):
                return latest_date_val.date()
            if isinstance(latest_date_val, date):
                return latest_date_val
            # 如果是其他类型，尝试解析
            if isinstance(latest_date_val, str):
                try:
                    return datetime.strptime(latest_date_val, "%Y-%m-%d").date()
                except ValueError:
                    self.logger.error(f"无法将字符串 '{latest_date_val}' 解析为日期。") # type: ignore
                    return None
            
            self.logger.warning(f"获取的最新日期类型未知: {type(latest_date_val)}") # type: ignore
            return latest_date_val

        except Exception as e:
            self.logger.error(f"获取表 '{resolved_table_name}' 最新日期时出错: {e}", exc_info=True) # type: ignore
            return None

    async def get_column_names(self, target: Any) -> List[str]:
        """获取数据表的所有列名"""
        resolved_table_name = self.resolver.get_full_name(target) # type: ignore
        schema, simple_name = resolved_table_name.split('.')
        schema = schema.strip('"')
        simple_name = simple_name.strip('"')

        query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position;
        """
        rows = await self.fetch(query, schema, simple_name) # type: ignore
        return [row['column_name'] for row in rows] if rows else []

    async def get_distinct_values(self, target: Any, column_name: str) -> List[Any]:
        """获取指定表中指定列的唯一值列表"""
        resolved_table_name = self.resolver.get_full_name(target) # type: ignore
        
        # 检查表是否存在
        if not await self.table_exists(resolved_table_name): # type: ignore
            self.logger.warning(f"表 '{resolved_table_name}' 不存在，无法获取唯一值。") # type: ignore
            return []

        query = f'SELECT DISTINCT "{column_name}" FROM {resolved_table_name};'
        try:
            rows = await self.fetch(query) # type: ignore
            return [row[0] for row in rows] if rows else []
        except Exception as e:
            self.logger.error(f"从表 '{resolved_table_name}' 获取列 '{column_name}' 唯一值时出错: {e}", exc_info=True) # type: ignore
            return []
