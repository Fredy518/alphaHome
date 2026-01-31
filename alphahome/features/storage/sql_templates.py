"""
features.storage SQL 模板生成器

提供 SQL 模板和生成器，用于创建不同类型的物化视图。
支持三种主要模式：
1. PIT（Point-in-Time）物化视图 - 时间序列展开
2. 聚合物化视图 - 横截面统计
3. JOIN 物化视图 - 多表关联

迁移自: 旧 processors.materialized_views.sql_templates（已删除）
"""

from typing import List, Dict, Any, Optional
from textwrap import dedent


class MaterializedViewSQL:
    """
    SQL 模板和生成器
    
    提供静态方法生成不同类型的物化视图 SQL 语句。
    每个模板都包含以下步骤：
    1. 数据对齐（格式标准化）
    2. 数据标准化（单位转换）
    3. 业务逻辑（如果适合 SQL）
    4. 血缘元数据
    5. 数据校验（缺失值、异常值）
    """

    # Phase1 强制迁移：仅允许 features schema
    DEFAULT_SCHEMA = "features"
    
    @staticmethod
    def pit_template(
        view_name: str,
        source_table: str,
        key_columns: List[str],
        time_columns: Dict[str, str],
        value_columns: List[str],
        quality_checks: Optional[Dict[str, Any]] = None,
        schema: str = "features"
    ) -> str:
        """
        PIT（Point-in-Time）物化视图 SQL 模板
        
        用于处理 PIT 数据的时间序列展开。PIT 数据通常包含公告日期（ann_date）
        和数据日期（end_date），需要展开为查询开始日期和查询结束日期。
        
        参数：
        - view_name: 物化视图名称（不含 schema）
        - source_table: 源表（rawdata.* 表）
        - key_columns: 主键列（例如 ['ts_code']）
        - time_columns: 时间列映射（例如 {'ann_date': 'announcement_date', 'end_date': 'data_date'}）
        - value_columns: 数值列（例如 ['pe_ttm', 'pb', 'ps']）
        - quality_checks: 数据质量检查条件（可选）
        - schema: 物化视图所在的 schema（默认 'features'，强制）
        
        返回：
        - str: 物化视图的 CREATE MATERIALIZED VIEW SQL 语句
        """
        # Phase1 强制迁移
        if schema != "features":
            raise ValueError(
                f"Phase1 强制迁移：仅允许 schema='features'，收到: {schema!r}"
            )

        if not key_columns:
            raise ValueError("key_columns cannot be empty")
        if not time_columns:
            raise ValueError("time_columns cannot be empty")
        if not value_columns:
            raise ValueError("value_columns cannot be empty")
        
        # 构建 SELECT 子句
        select_parts = []
        
        # 1. 主键列（数据对齐）
        for col in key_columns:
            select_parts.append(f"    {col}")
        
        # 2. 时间序列展开
        ann_date_col = list(time_columns.keys())[0]  # 第一个时间列作为公告日期
        select_parts.append(f"    {ann_date_col} as query_start_date")
        select_parts.append(
            f"    COALESCE(\n"
            f"        LEAD({ann_date_col}) OVER (PARTITION BY {', '.join(key_columns)} ORDER BY {ann_date_col}) - INTERVAL '1 day',\n"
            f"        '2099-12-31'::date\n"
            f"    ) as query_end_date"
        )
        
        # 3. 其他时间列
        for col_name, col_alias in list(time_columns.items())[1:]:
            select_parts.append(f"    {col_name} as {col_alias}")
        
        # 4. 数值列（数据标准化）
        for col in value_columns:
            select_parts.append(f"    CAST({col} AS DECIMAL(15,4)) as {col}")
        
        # 5. 血缘元数据
        select_parts.append(f"    '{source_table}' as _source_table")
        select_parts.append(f"    NOW() as _processed_at")
        select_parts.append(f"    CURRENT_DATE as _data_version")
        
        select_clause = ",\n".join(select_parts)
        
        # 构建 WHERE 子句（数据校验）
        where_conditions = []
        
        # 检查主键和时间列不为空
        for col in key_columns:
            where_conditions.append(f"    {col} IS NOT NULL")
        for col in time_columns.keys():
            where_conditions.append(f"    {col} IS NOT NULL")
        
        # 检查数值列不为空
        for col in value_columns:
            where_conditions.append(f"    {col} IS NOT NULL")
        
        # 添加质量检查条件
        if quality_checks and 'outlier_check' in quality_checks:
            outlier_check = quality_checks['outlier_check']
            if 'columns' in outlier_check and 'threshold' in outlier_check:
                # 简单的范围检查（可根据需要扩展）
                for col in outlier_check.get('columns', []):
                    if col in value_columns:
                        where_conditions.append(f"    {col} BETWEEN -1000000 AND 1000000")
        
        where_clause = " AND\n".join(where_conditions)
        
        # 构建完整的 SQL
        sql = dedent(f"""
            CREATE MATERIALIZED VIEW {schema}.{view_name} AS
            SELECT
            {select_clause}
            FROM {source_table}
            WHERE
            {where_clause}
            ORDER BY {', '.join(key_columns)}, {ann_date_col} DESC;
        """).strip()
        
        return sql
    
    @staticmethod
    def aggregation_template(
        view_name: str,
        source_table: str,
        group_by_columns: List[str],
        aggregate_functions: Dict[str, List[str]],
        quality_checks: Optional[Dict[str, Any]] = None,
        schema: str = "features"
    ) -> str:
        """
        聚合物化视图 SQL 模板
        
        用于处理横截面统计和聚合计算。支持多种聚合函数：
        - sum: 求和
        - avg: 平均值
        - count: 计数
        - min: 最小值
        - max: 最大值
        - stddev: 标准差
        
        参数：
        - view_name: 物化视图名称（不含 schema）
        - source_table: 源表（rawdata.* 表）
        - group_by_columns: 分组列（例如 ['trade_date', 'industry']）
        - aggregate_functions: 聚合函数映射（例如 {'sum': ['amount'], 'avg': ['price']}）
        - quality_checks: 数据质量检查条件（可选）
        - schema: 物化视图所在的 schema（默认 'features'，强制）
        
        返回：
        - str: 物化视图的 CREATE MATERIALIZED VIEW SQL 语句
        """
        # Phase1 强制迁移
        if schema != "features":
            raise ValueError(
                f"Phase1 强制迁移：仅允许 schema='features'，收到: {schema!r}"
            )

        if not group_by_columns:
            raise ValueError("group_by_columns cannot be empty")
        if not aggregate_functions:
            raise ValueError("aggregate_functions cannot be empty")
        
        # 构建 SELECT 子句
        select_parts = []
        
        # 1. 分组列
        for col in group_by_columns:
            select_parts.append(f"    {col}")
        
        # 2. 聚合函数
        for func_name, columns in aggregate_functions.items():
            func_upper = func_name.upper()
            for col in columns:
                if func_upper == "COUNT":
                    select_parts.append(f"    COUNT(DISTINCT {col}) as {func_name}_{col}")
                elif func_upper in ["SUM", "AVG", "MIN", "MAX", "STDDEV"]:
                    select_parts.append(f"    {func_upper}({col}) as {func_name}_{col}")
                else:
                    # 默认使用 SUM
                    select_parts.append(f"    SUM({col}) as {func_name}_{col}")
        
        # 3. 血缘元数据
        select_parts.append(f"    '{source_table}' as _source_table")
        select_parts.append(f"    NOW() as _processed_at")
        select_parts.append(f"    CURRENT_DATE as _data_version")
        
        select_clause = ",\n".join(select_parts)
        
        # 构建 WHERE 子句（数据校验）
        where_conditions = []
        
        # 检查分组列不为空
        for col in group_by_columns:
            where_conditions.append(f"    {col} IS NOT NULL")
        
        # 检查聚合列不为空
        for columns in aggregate_functions.values():
            for col in columns:
                where_conditions.append(f"    {col} IS NOT NULL")
        
        where_clause = " AND\n".join(where_conditions)
        
        # 构建 GROUP BY 子句
        group_by_clause = ", ".join(group_by_columns)
        
        # 构建完整的 SQL
        sql = dedent(f"""
            CREATE MATERIALIZED VIEW {schema}.{view_name} AS
            SELECT
            {select_clause}
            FROM {source_table}
            WHERE
            {where_clause}
            GROUP BY {group_by_clause}
            ORDER BY {group_by_clause};
        """).strip()
        
        return sql
    
    @staticmethod
    def join_template(
        view_name: str,
        source_tables: List[str],
        join_conditions: List[str],
        select_columns: List[str],
        quality_checks: Optional[Dict[str, Any]] = None,
        schema: str = "features"
    ) -> str:
        """
        JOIN 物化视图 SQL 模板
        
        用于处理多表关联。支持 INNER JOIN、LEFT JOIN 等。
        
        参数：
        - view_name: 物化视图名称（不含 schema）
        - source_tables: 源表列表（例如 ['rawdata.stock_daily', 'rawdata.industry_classification']）
        - join_conditions: JOIN 条件列表（例如 ['t1.ts_code = t2.ts_code', 't1.trade_date = t2.trade_date']）
        - select_columns: SELECT 列列表（例如 ['t1.ts_code', 't1.trade_date', 't1.close', 't2.industry']）
        - quality_checks: 数据质量检查条件（可选）
        - schema: 物化视图所在的 schema（默认 'features'，强制）
        
        返回：
        - str: 物化视图的 CREATE MATERIALIZED VIEW SQL 语句
        """
        # Phase1 强制迁移
        if schema != "features":
            raise ValueError(
                f"Phase1 强制迁移：仅允许 schema='features'，收到: {schema!r}"
            )

        if len(source_tables) < 2:
            raise ValueError("source_tables must have at least 2 tables")
        if not join_conditions:
            raise ValueError("join_conditions cannot be empty")
        if not select_columns:
            raise ValueError("select_columns cannot be empty")
        
        # 构建 FROM 和 JOIN 子句
        from_clause = f"{source_tables[0]} t1"
        
        join_clauses = []
        for i, table in enumerate(source_tables[1:], start=2):
            join_clauses.append(f"INNER JOIN {table} t{i} ON {join_conditions[i-2]}")
        
        join_clause = "\n".join(join_clauses)
        
        # 构建 SELECT 子句
        select_parts = []
        for col in select_columns:
            select_parts.append(f"    {col}")
        
        # 血缘元数据
        select_parts.append(f"    '{', '.join(source_tables)}' as _source_table")
        select_parts.append(f"    NOW() as _processed_at")
        select_parts.append(f"    CURRENT_DATE as _data_version")
        
        select_clause = ",\n".join(select_parts)
        
        # 构建 WHERE 子句（数据校验）
        where_conditions = []
        for col in select_columns:
            # 简单的非空检查
            where_conditions.append(f"    {col} IS NOT NULL")
        
        where_clause = " AND\n".join(where_conditions)
        
        # 构建完整的 SQL
        sql = dedent(f"""
            CREATE MATERIALIZED VIEW {schema}.{view_name} AS
            SELECT
            {select_clause}
            FROM {from_clause}
            {join_clause}
            WHERE
            {where_clause}
            ORDER BY {select_columns[0]};
        """).strip()
        
        return sql
