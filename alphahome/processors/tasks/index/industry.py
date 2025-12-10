#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
申万一级行业：收益与宽度任务
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register


@task_register()
class IndustryReturnTask(ProcessorTaskBase):
    """申万一级行业收益"""

    name = "industry_return"
    table_name = "processor_industry_return"
    description = "申万一级行业收益"
    source_tables = ["tushare.index_swdaily", "tushare.index_swmember"]
    primary_keys = ["trade_date"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        self.result_table = (config or {}).get("result_table", self.table_name)
        self.table_name = self.result_table

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")

        query = f"""
        SELECT trade_date, ts_code, close
        FROM tushare.index_swdaily
        WHERE trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
          AND ts_code IN (SELECT DISTINCT l1_code FROM tushare.index_swmember)
        ORDER BY trade_date, ts_code
        """
        rows = await self.db.fetch(query)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()
        pivot = data.pivot(index="trade_date", columns="ts_code", values="close").sort_index()
        returns = pivot.pct_change()
        returns.columns = [f"SW_{c.replace('.SI','')}" for c in returns.columns]
        return returns

    async def save_result(self, data: pd.DataFrame, **kwargs):
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return
        save_df = data.reset_index().rename(columns={"index": "trade_date"})
        if pd.api.types.is_datetime64_any_dtype(save_df["trade_date"]):
            save_df["trade_date"] = save_df["trade_date"].dt.strftime("%Y%m%d")
        await self.db.save_dataframe(
            save_df,
            self.table_name,
            primary_keys=["trade_date"],
            use_insert_mode=False,
        )


@task_register()
class IndustryBreadthTask(ProcessorTaskBase):
    """申万行业宽度"""

    name = "industry_breadth"
    table_name = "processor_industry_breadth"
    description = "申万一级行业宽度与分散度"
    source_tables = ["tushare.index_swdaily", "tushare.index_swmember"]
    primary_keys = ["trade_date"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        self.result_table = (config or {}).get("result_table", self.table_name)
        self.table_name = self.result_table

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")

        query = f"""
        SELECT trade_date, ts_code, close
        FROM tushare.index_swdaily
        WHERE trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
          AND ts_code IN (SELECT DISTINCT l1_code FROM tushare.index_swmember)
        ORDER BY trade_date, ts_code
        """
        rows = await self.db.fetch(query)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()
        pivot = data.pivot(index="trade_date", columns="ts_code", values="close").sort_index()
        returns = pivot.pct_change()

        result = pd.DataFrame(index=returns.index)
        denom = returns.notna().sum(axis=1)
        result["Industry_Up_Ratio"] = (returns > 0).sum(axis=1) / denom
        result["Industry_Strong_Ratio"] = (returns > 0.01).sum(axis=1) / denom
        result["Industry_Weak_Ratio"] = (returns < -0.01).sum(axis=1) / denom
        result["Industry_Return_Std"] = returns.std(axis=1)
        result["Industry_Return_Skew"] = returns.apply(
            lambda x: x.dropna().skew() if x.notna().sum() >= 5 else np.nan, axis=1
        )
        result["Industry_Up_Ratio_5D"] = result["Industry_Up_Ratio"].rolling(5, min_periods=3).mean()
        return result

    async def save_result(self, data: pd.DataFrame, **kwargs):
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return
        save_df = data.reset_index().rename(columns={"index": "trade_date"})
        if pd.api.types.is_datetime64_any_dtype(save_df["trade_date"]):
            save_df["trade_date"] = save_df["trade_date"].dt.strftime("%Y%m%d")
        await self.db.save_dataframe(
            save_df,
            self.table_name,
            primary_keys=["trade_date"],
            use_insert_mode=False,
        )

