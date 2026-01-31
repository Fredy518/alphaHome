#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Barra DataFrame 序列化工具。

提供 save_dataframe 函数用于将 DataFrame 保存到数据库。
此模块独立于 processors，避免循环依赖。
"""

from datetime import datetime, date
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


async def save_dataframe(
    df: pd.DataFrame,
    table_name: str,
    db_connection,
    primary_keys: List[str],
    schema: str = "public",
    if_exists: str = "upsert",
) -> None:
    """Save DataFrame to database table.
    
    Args:
        df: DataFrame to save
        table_name: Target table name
        db_connection: Database connection (DBManager)
        primary_keys: List of primary key columns
        schema: Database schema
        if_exists: How to handle existing data ('upsert', 'replace', 'append')
    """
    if df is None or df.empty:
        return

    full_table = f"{schema}.{table_name}"
    columns = list(df.columns)
    
    # Build INSERT statement
    col_list = ", ".join(columns)
    placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
    
    if if_exists == "upsert" and primary_keys:
        # ON CONFLICT DO UPDATE
        pk_list = ", ".join(primary_keys)
        update_cols = [c for c in columns if c not in primary_keys]
        if update_cols:
            update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
            sql = f"""
                INSERT INTO {full_table} ({col_list})
                VALUES ({placeholders})
                ON CONFLICT ({pk_list}) DO UPDATE SET {update_clause}
            """
        else:
            sql = f"""
                INSERT INTO {full_table} ({col_list})
                VALUES ({placeholders})
                ON CONFLICT ({pk_list}) DO NOTHING
            """
    else:
        sql = f"""
            INSERT INTO {full_table} ({col_list})
            VALUES ({placeholders})
        """

    # Convert DataFrame to list of tuples
    def convert_value(val):
        """Convert pandas/numpy types to Python native types."""
        if pd.isna(val):
            return None
        if isinstance(val, (np.integer,)):
            return int(val)
        if isinstance(val, (np.floating,)):
            return float(val)
        if isinstance(val, (np.bool_,)):
            return bool(val)
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime()
        if isinstance(val, np.datetime64):
            return pd.Timestamp(val).to_pydatetime()
        return val

    records = []
    for _, row in df.iterrows():
        record = tuple(convert_value(row[c]) for c in columns)
        records.append(record)

    # Execute in batches
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        await db_connection.executemany(sql, batch)
