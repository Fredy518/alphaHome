from datetime import datetime
from typing import Optional, Union


class UtilityMixin:
    """实用工具Mixin - 提供各种实用工具方法"""

    def test_connection(self):
        """测试数据库连接"""
        try:
            if self.mode == "sync":
                result = self.fetch_val_sync("SELECT 1")
            else:
                # 异步模式下不能在此方法中直接测试，需要异步调用
                self.logger.warning(
                    "异步模式下test_connection方法不可用，请使用异步测试方法"
                )
                return False
            return result == 1
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {e}")
            return False

    async def get_latest_date(
        self, table_name: str, date_column: str, return_raw_object: bool = False
    ) -> Optional[Union[str, datetime]]:
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
        if self.mode == "sync":
            # 同步模式：使用同步方法
            query = f"""
            SELECT MAX("{date_column}") FROM "{table_name}";
            """
            try:
                result = self.fetch_val_sync(query)
            except Exception as e:
                self.logger.warning(
                    f"查询表 {table_name} 列 {date_column} 的最大值时出错: {e}"
                )
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
                self.logger.warning(
                    f"查询表 {table_name} 列 {date_column} 的最大值时出错: {e}"
                )
                return None

        if result is not None:
            self.logger.debug(
                f"get_latest_date (表: {table_name}, 列: {date_column}) 返回类型: {type(result)}"
            )
        else:
            self.logger.debug(
                f"get_latest_date (表: {table_name}, 列: {date_column}) 返回 None"
            )

        # --- 为了向后兼容性的条件格式化 ---
        if result is not None and not return_raw_object:
            # 默认行为：格式化为 YYYYMMDD 字符串
            formatted_result = None
            if isinstance(result, str):
                # 处理来自数据库的潜在字符串输入 (尽管对于asyncpg不太可能)
                if "-" in result:  # 尝试 YYYY-MM-DD
                    try:
                        formatted_result = datetime.strptime(
                            result, "%Y-%m-%d"
                        ).strftime("%Y%m%d")
                    except ValueError:
                        self.logger.warning(
                            f"无法将日期字符串 '{result}' 解析为 YYYY-MM-DD 格式。"
                        )
                elif len(result) == 8 and result.isdigit():  # 已经是 YYYYMMDD
                    formatted_result = result
                else:  # 其他未知字符串格式
                    self.logger.warning(f"接收到意外的日期字符串格式 '{result}'。")
            elif isinstance(
                result, datetime
            ):  # 包括 datetime.date (因为它是datetime的子类)
                # 将 datetime/date 对象转换为 YYYYMMDD 字符串
                formatted_result = result.strftime("%Y%m%d")
            else:  # 其他未知类型
                self.logger.warning(f"接收到意外的日期类型 '{type(result)}'。")

            return formatted_result  # 返回格式化后的字符串，如果格式化失败则为 None
        else:
            # return_raw_object 为 True 或 result 为 None
            return result  # 返回原始 datetime 对象或 None
