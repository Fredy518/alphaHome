#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL查询构建器

用于简化SQL查询构建的工具类，支持条件、排序、分页等。
支持参数化查询，防止SQL注入。
"""

from typing import Any, Dict, List, Optional, Tuple, Union


class QueryBuilder:
    """SQL查询构建器

    用于构建参数化SQL查询的工具类，支持链式调用。

    示例:
    ```python
    # 创建查询构建器
    query_builder = QueryBuilder('stock_daily_bar')

    # 添加条件和排序
    query_builder.add_condition("trade_date >= $start_date")
                 .add_condition("trade_date <= $end_date")
                 .add_in_condition("ts_code", "$codes")
                 .add_order_by("trade_date")
                 .limit(1000)

    # 构建查询和参数
    query, params = query_builder.build({
        'start_date': '20230101',
        'end_date': '20230131',
        'codes': ['000001.SZ', '000002.SZ']
    })

    # 执行查询
    rows = await db.fetch_all(query, params)
    ```
    """

    def __init__(self, table_name: str, select_columns: List[str] = None):
        """初始化查询构建器

        Args:
            table_name: 表名
            select_columns: 查询列，默认为 '*'
        """
        self.table_name = table_name
        self.select_columns = select_columns or ["*"]
        self.conditions = []
        self.order_clauses = []
        self.group_clauses = []
        self.having_conditions = []
        self.limit_value = None
        self.offset_value = None

    def select(self, columns: List[str]) -> "QueryBuilder":
        """设置SELECT子句的列

        Args:
            columns: 列名列表

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        self.select_columns = columns
        return self

    def add_condition(self, condition: str) -> "QueryBuilder":
        """添加WHERE条件

        条件中可以包含参数占位符，如 $param_name。

        Args:
            condition: 条件表达式，如 "column_name > $param_name"

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        self.conditions.append(condition)
        return self

    def add_in_condition(self, column: str, param_name: str) -> "QueryBuilder":
        """添加IN条件

        用于构建 "column IN ($param)" 形式的条件。
        参数应该是一个数组。

        Args:
            column: 列名
            param_name: 参数名，格式为 "$param_name"

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        # 确保参数名以$开头
        if not param_name.startswith("$"):
            param_name = f"${param_name}"

        self.conditions.append(f"{column} = ANY({param_name})")
        return self

    def add_order_by(self, column: str, direction: str = "ASC") -> "QueryBuilder":
        """添加排序

        Args:
            column: 列名
            direction: 排序方向，"ASC" 或 "DESC"，默认为 "ASC"

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        self.order_clauses.append(f"{column} {direction}")
        return self

    def add_group_by(self, column: str) -> "QueryBuilder":
        """添加GROUP BY子句

        Args:
            column: 列名

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        self.group_clauses.append(column)
        return self

    def add_having(self, condition: str) -> "QueryBuilder":
        """添加HAVING条件

        Args:
            condition: 条件表达式

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        self.having_conditions.append(condition)
        return self

    def limit(self, limit: int) -> "QueryBuilder":
        """添加LIMIT子句

        Args:
            limit: 限制返回的记录数

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        self.limit_value = limit
        return self

    def offset(self, offset: int) -> "QueryBuilder":
        """添加OFFSET子句

        Args:
            offset: 跳过的记录数

        Returns:
            QueryBuilder: 返回自身，支持链式调用
        """
        self.offset_value = offset
        return self

    def build(self, param_values: Dict[str, Any] = None) -> Tuple[str, Dict[str, Any]]:
        """构建SQL查询和参数

        Args:
            param_values: 参数值的字典，键应该与条件中的参数名匹配（不含$前缀）

        Returns:
            Tuple[str, Dict[str, Any]]: (SQL查询字符串, 参数字典)
        """
        # 构建SELECT子句
        query = f"SELECT {', '.join(self.select_columns)} FROM {self.table_name}"

        # 添加WHERE子句
        if self.conditions:
            query += " WHERE " + " AND ".join(self.conditions)

        # 添加GROUP BY子句
        if self.group_clauses:
            query += " GROUP BY " + ", ".join(self.group_clauses)

        # 添加HAVING子句
        if self.having_conditions:
            query += " HAVING " + " AND ".join(self.having_conditions)

        # 添加ORDER BY子句
        if self.order_clauses:
            query += " ORDER BY " + ", ".join(self.order_clauses)

        # 添加LIMIT子句
        if self.limit_value is not None:
            query += f" LIMIT {self.limit_value}"

        # 添加OFFSET子句
        if self.offset_value is not None:
            query += f" OFFSET {self.offset_value}"

        # 准备参数
        params = {}
        if param_values:
            # 移除参数名中的$前缀，以便与asyncpg兼容
            for key, value in param_values.items():
                params[key.lstrip("$")] = value

        return query, params

    def count(self) -> Tuple[str, Dict[str, Any]]:
        """构建COUNT查询

        构建一个计算查询结果总数的SQL查询。

        Returns:
            Tuple[str, Dict[str, Any]]: (COUNT查询字符串, 参数字典)
        """
        original_select = self.select_columns
        self.select_columns = ["COUNT(*) as total"]

        # 构建COUNT查询
        query, params = self.build()

        # 恢复原始SELECT
        self.select_columns = original_select

        return query, params
