#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DataFrame 序列化工具

提供 DataFrame 与数据库表之间的序列化和反序列化功能。
保持索引和列类型的一致性。

Requirements: 6.1, 6.2, 6.3
"""

from datetime import datetime, date
from typing import Any, Dict, List, Optional, Type, Union

import numpy as np
import pandas as pd

from ...common.logging_utils import get_logger


# 类型映射：Python/Pandas 类型 -> PostgreSQL 类型
DTYPE_TO_PG_TYPE = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "int16": "SMALLINT",
    "Int64": "BIGINT",  # nullable integer
    "Int32": "INTEGER",
    "Int16": "SMALLINT",
    "float64": "DOUBLE PRECISION",
    "float32": "REAL",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "object": "TEXT",
    "string": "TEXT",
    "datetime64[ns]": "TIMESTAMP",
    "datetime64[ns, UTC]": "TIMESTAMPTZ",
    "date": "DATE",
}

# PostgreSQL 类型 -> Pandas dtype
PG_TYPE_TO_DTYPE = {
    "bigint": "Int64",
    "integer": "Int32",
    "smallint": "Int16",
    "double precision": "float64",
    "real": "float32",
    "boolean": "boolean",
    "text": "string",
    "varchar": "string",
    "character varying": "string",
    "timestamp": "datetime64[ns]",
    "timestamp without time zone": "datetime64[ns]",
    "timestamp with time zone": "datetime64[ns]",
    "date": "object",  # Keep as object, convert later
}


class DataFrameSerializer:
    """DataFrame 序列化器
    
    提供 DataFrame 与数据库表之间的序列化和反序列化功能。
    
    主要功能：
    1. 保存 DataFrame 到数据库表 (Requirements 6.1)
    2. 从数据库表加载数据到 DataFrame (Requirements 6.2)
    3. 保持索引和列类型 (Requirements 6.3)
    
    示例:
    ```python
    serializer = DataFrameSerializer(db_connection)
    
    # 保存 DataFrame
    await serializer.save_dataframe(
        df, 
        table_name="my_table",
        index_columns=["trade_date"],
        primary_keys=["trade_date"]
    )
    
    # 加载 DataFrame
    df = await serializer.load_dataframe(
        table_name="my_table",
        index_columns=["trade_date"]
    )
    ```
    """
    
    def __init__(self, db_connection=None):
        """初始化序列化器
        
        Args:
            db_connection: 数据库连接实例（DBManager）
        """
        self.db = db_connection
        self.logger = get_logger("DataFrameSerializer")
    
    def set_connection(self, db_connection):
        """设置数据库连接
        
        Args:
            db_connection: 数据库连接实例
        """
        self.db = db_connection

    def _infer_pg_type(self, dtype: str) -> str:
        """推断 PostgreSQL 类型
        
        Args:
            dtype: Pandas dtype 字符串
            
        Returns:
            PostgreSQL 类型字符串
        """
        dtype_str = str(dtype).lower()
        
        # 直接匹配
        if dtype_str in DTYPE_TO_PG_TYPE:
            return DTYPE_TO_PG_TYPE[dtype_str]
        
        # 模糊匹配
        if "int" in dtype_str:
            return "BIGINT"
        if "float" in dtype_str:
            return "DOUBLE PRECISION"
        if "datetime" in dtype_str:
            return "TIMESTAMP"
        if "bool" in dtype_str:
            return "BOOLEAN"
        
        return "TEXT"
    
    def _infer_pandas_dtype(self, pg_type: str) -> str:
        """推断 Pandas dtype
        
        Args:
            pg_type: PostgreSQL 类型字符串
            
        Returns:
            Pandas dtype 字符串
        """
        pg_type_lower = pg_type.lower()
        
        # 直接匹配
        if pg_type_lower in PG_TYPE_TO_DTYPE:
            return PG_TYPE_TO_DTYPE[pg_type_lower]
        
        # 模糊匹配
        if "int" in pg_type_lower:
            return "Int64"
        if "float" in pg_type_lower or "double" in pg_type_lower or "numeric" in pg_type_lower:
            return "float64"
        if "timestamp" in pg_type_lower:
            return "datetime64[ns]"
        if "bool" in pg_type_lower:
            return "boolean"
        
        return "object"
    
    def _prepare_dataframe_for_save(
        self,
        df: pd.DataFrame,
        index_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """准备 DataFrame 用于保存
        
        将索引转换为列，处理特殊类型。
        
        Args:
            df: 原始 DataFrame
            index_columns: 索引列名列表
            
        Returns:
            准备好的 DataFrame
        """
        result = df.copy()
        
        # 如果有索引，将其转换为列
        if index_columns and df.index.name is not None:
            result = result.reset_index()
        elif df.index.name is not None:
            result = result.reset_index()
        elif not isinstance(df.index, pd.RangeIndex):
            # 非默认索引，保存为列
            result = result.reset_index()
        
        # 处理 datetime 索引/列 - 使用 ISO 8601 格式
        for col in result.columns:
            if pd.api.types.is_datetime64_any_dtype(result[col]):
                # 保持 datetime 类型，让数据库驱动处理
                pass
            elif result[col].dtype == "object":
                # 检查是否是 date 对象
                sample = result[col].dropna().head(1)
                if len(sample) > 0 and isinstance(sample.iloc[0], date):
                    # 转换为字符串格式 (ISO 8601)
                    result[col] = result[col].apply(
                        lambda x: x.isoformat() if isinstance(x, date) else x
                    )
        
        return result
    
    def _restore_dataframe_types(
        self,
        df: pd.DataFrame,
        index_columns: Optional[List[str]] = None,
        column_types: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """恢复 DataFrame 类型
        
        将列转换回索引，恢复数据类型。
        
        Args:
            df: 从数据库加载的 DataFrame
            index_columns: 索引列名列表
            column_types: 列类型映射
            
        Returns:
            恢复类型后的 DataFrame
        """
        result = df.copy()
        
        # 恢复列类型
        if column_types:
            for col, dtype in column_types.items():
                if col in result.columns:
                    try:
                        if dtype == "datetime64[ns]":
                            result[col] = pd.to_datetime(result[col])
                        elif dtype in ("Int64", "Int32", "Int16"):
                            result[col] = pd.to_numeric(result[col], errors="coerce").astype(dtype)
                        elif dtype in ("float64", "float32"):
                            result[col] = pd.to_numeric(result[col], errors="coerce")
                        elif dtype == "boolean":
                            result[col] = result[col].astype("boolean")
                        elif dtype == "string":
                            result[col] = result[col].astype("string")
                    except Exception as e:
                        self.logger.warning(f"无法转换列 '{col}' 到类型 {dtype}: {e}")
        
        # 恢复索引
        if index_columns:
            existing_index_cols = [c for c in index_columns if c in result.columns]
            if existing_index_cols:
                result = result.set_index(existing_index_cols)
                if len(existing_index_cols) == 1:
                    result.index.name = existing_index_cols[0]
        
        return result

    async def save_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        index_columns: Optional[List[str]] = None,
        primary_keys: Optional[List[str]] = None,
        schema: str = "public",
        if_exists: str = "upsert",
        timestamp_column: Optional[str] = None,
    ) -> int:
        """保存 DataFrame 到数据库表
        
        将 DataFrame 序列化并保存到指定的数据库表。
        支持 upsert 模式处理重复数据。
        
        Args:
            df: 要保存的 DataFrame
            table_name: 目标表名
            index_columns: 索引列名列表（将被保存为表列）
            primary_keys: 主键列名列表（用于 upsert 冲突检测）
            schema: 数据库 schema，默认 "public"
            if_exists: 存在时的处理方式:
                - "upsert": 插入或更新（默认）
                - "append": 追加
                - "replace": 替换（先删除再插入）
            timestamp_column: 时间戳列名（用于智能更新）
            
        Returns:
            影响的行数
            
        Raises:
            ValueError: 如果 DataFrame 为空或参数无效
            Exception: 如果数据库操作失败
            
        Requirements: 6.1, 6.3
        """
        if self.db is None:
            raise ValueError("数据库连接未设置")
        
        if df is None or df.empty:
            self.logger.warning(f"DataFrame 为空，跳过保存到 {table_name}")
            return 0
        
        # 准备数据
        save_df = self._prepare_dataframe_for_save(df, index_columns)
        
        self.logger.info(
            f"保存 DataFrame 到 {schema}.{table_name}，"
            f"行数: {len(save_df)}，列数: {len(save_df.columns)}"
        )
        
        try:
            if if_exists == "upsert" and primary_keys:
                # 使用 upsert 模式
                rows_affected = await self.db.copy_from_dataframe(
                    df=save_df,
                    target=f"{schema}.{table_name}",
                    conflict_columns=primary_keys,
                    timestamp_column=timestamp_column,
                )
            elif if_exists == "append":
                # 追加模式
                rows_affected = await self.db.copy_from_dataframe(
                    df=save_df,
                    target=f"{schema}.{table_name}",
                )
            elif if_exists == "replace":
                # 替换模式：先删除再插入
                await self.db.execute(f'DELETE FROM "{schema}"."{table_name}"')
                rows_affected = await self.db.copy_from_dataframe(
                    df=save_df,
                    target=f"{schema}.{table_name}",
                )
            else:
                # 默认追加
                rows_affected = await self.db.copy_from_dataframe(
                    df=save_df,
                    target=f"{schema}.{table_name}",
                )
            
            self.logger.info(f"成功保存 {rows_affected} 行到 {schema}.{table_name}")
            return rows_affected
            
        except Exception as e:
            self.logger.error(f"保存 DataFrame 到 {schema}.{table_name} 失败: {e}")
            raise
    
    async def load_dataframe(
        self,
        table_name: str,
        index_columns: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        schema: str = "public",
    ) -> pd.DataFrame:
        """从数据库表加载 DataFrame
        
        从指定的数据库表反序列化数据到 DataFrame。
        自动恢复索引和列类型。
        
        Args:
            table_name: 源表名
            index_columns: 索引列名列表（将被设置为 DataFrame 索引）
            columns: 要加载的列名列表，None 表示全部
            where_clause: WHERE 条件（不含 WHERE 关键字）
            order_by: ORDER BY 子句（不含 ORDER BY 关键字）
            limit: 限制返回行数
            schema: 数据库 schema，默认 "public"
            
        Returns:
            加载的 DataFrame
            
        Raises:
            ValueError: 如果参数无效
            Exception: 如果数据库操作失败
            
        Requirements: 6.2, 6.3
        """
        if self.db is None:
            raise ValueError("数据库连接未设置")
        
        # 构建查询
        select_cols = ", ".join(columns) if columns else "*"
        query = f'SELECT {select_cols} FROM "{schema}"."{table_name}"'
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        self.logger.debug(f"执行查询: {query}")
        
        try:
            rows = await self.db.fetch(query)
            
            if not rows:
                self.logger.info(f"从 {schema}.{table_name} 加载到 0 行数据")
                return pd.DataFrame()
            
            # 转换为 DataFrame
            df = pd.DataFrame([dict(row) for row in rows])
            
            # 获取列类型信息
            column_types = await self._get_column_types(table_name, schema)
            
            # 恢复类型和索引
            df = self._restore_dataframe_types(df, index_columns, column_types)
            
            self.logger.info(
                f"从 {schema}.{table_name} 加载 {len(df)} 行数据，"
                f"列数: {len(df.columns)}"
            )
            
            return df
            
        except Exception as e:
            self.logger.error(f"从 {schema}.{table_name} 加载数据失败: {e}")
            raise
    
    async def _get_column_types(
        self,
        table_name: str,
        schema: str = "public",
    ) -> Dict[str, str]:
        """获取表的列类型信息
        
        Args:
            table_name: 表名
            schema: schema 名
            
        Returns:
            列名到 Pandas dtype 的映射
        """
        query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        """
        
        try:
            rows = await self.db.fetch(query, schema, table_name)
            
            column_types = {}
            for row in rows:
                col_name = row["column_name"]
                pg_type = row["data_type"]
                column_types[col_name] = self._infer_pandas_dtype(pg_type)
            
            return column_types
            
        except Exception as e:
            self.logger.warning(f"获取表 {schema}.{table_name} 的列类型失败: {e}")
            return {}

    def serialize_to_dict(
        self,
        df: pd.DataFrame,
        index_columns: Optional[List[str]] = None,
        date_format: str = "iso",
    ) -> Dict[str, Any]:
        """将 DataFrame 序列化为字典
        
        用于 JSON 序列化或其他用途。
        
        Args:
            df: 要序列化的 DataFrame
            index_columns: 索引列名列表
            date_format: 日期格式，"iso" 使用 ISO 8601
            
        Returns:
            包含数据和元信息的字典
        """
        if df is None or df.empty:
            return {
                "data": [],
                "columns": [],
                "index_columns": index_columns or [],
                "dtypes": {},
            }
        
        # 准备数据
        save_df = self._prepare_dataframe_for_save(df, index_columns)
        
        # 收集类型信息
        dtypes = {col: str(dtype) for col, dtype in save_df.dtypes.items()}
        
        # 转换为记录列表
        records = save_df.to_dict(orient="records")
        
        # 处理日期格式
        if date_format == "iso":
            for record in records:
                for key, value in record.items():
                    if isinstance(value, (datetime, date)):
                        record[key] = value.isoformat()
                    elif pd.isna(value):
                        record[key] = None
        
        return {
            "data": records,
            "columns": list(save_df.columns),
            "index_columns": index_columns or [],
            "dtypes": dtypes,
        }
    
    def deserialize_from_dict(
        self,
        data_dict: Dict[str, Any],
    ) -> pd.DataFrame:
        """从字典反序列化为 DataFrame
        
        Args:
            data_dict: serialize_to_dict 返回的字典
            
        Returns:
            反序列化的 DataFrame
        """
        if not data_dict or not data_dict.get("data"):
            return pd.DataFrame()
        
        # 创建 DataFrame
        df = pd.DataFrame(data_dict["data"])
        
        # 恢复类型
        dtypes = data_dict.get("dtypes", {})
        column_types = {}
        for col, dtype_str in dtypes.items():
            column_types[col] = self._infer_pandas_dtype(dtype_str)
        
        # 恢复索引
        index_columns = data_dict.get("index_columns", [])
        
        return self._restore_dataframe_types(df, index_columns, column_types)


# 便捷函数

def create_serializer(db_connection=None) -> DataFrameSerializer:
    """创建 DataFrame 序列化器
    
    Args:
        db_connection: 数据库连接实例
        
    Returns:
        DataFrameSerializer 实例
    """
    return DataFrameSerializer(db_connection)


async def save_dataframe(
    df: pd.DataFrame,
    table_name: str,
    db_connection,
    index_columns: Optional[List[str]] = None,
    primary_keys: Optional[List[str]] = None,
    schema: str = "public",
    if_exists: str = "upsert",
) -> int:
    """便捷函数：保存 DataFrame 到数据库
    
    Args:
        df: 要保存的 DataFrame
        table_name: 目标表名
        db_connection: 数据库连接
        index_columns: 索引列名列表
        primary_keys: 主键列名列表
        schema: 数据库 schema
        if_exists: 存在时的处理方式
        
    Returns:
        影响的行数
    """
    serializer = DataFrameSerializer(db_connection)
    return await serializer.save_dataframe(
        df=df,
        table_name=table_name,
        index_columns=index_columns,
        primary_keys=primary_keys,
        schema=schema,
        if_exists=if_exists,
    )


async def load_dataframe(
    table_name: str,
    db_connection,
    index_columns: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    where_clause: Optional[str] = None,
    schema: str = "public",
) -> pd.DataFrame:
    """便捷函数：从数据库加载 DataFrame
    
    Args:
        table_name: 源表名
        db_connection: 数据库连接
        index_columns: 索引列名列表
        columns: 要加载的列名列表
        where_clause: WHERE 条件
        schema: 数据库 schema
        
    Returns:
        加载的 DataFrame
    """
    serializer = DataFrameSerializer(db_connection)
    return await serializer.load_dataframe(
        table_name=table_name,
        index_columns=index_columns,
        columns=columns,
        where_clause=where_clause,
        schema=schema,
    )
