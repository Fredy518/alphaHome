#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
风格指数收益任务
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register

STYLE_INDEXES = {
    "000016.SH": "SZ50",
    "000300.SH": "HS300",
    "000905.SH": "ZZ500",
    "000852.SH": "ZZ1000",
    "399006.SZ": "CYB",
    "000922.CSI": "DIV",
    "000919.CSI": "HS300_VAL",
    "000918.CSI": "HS300_GRO",
}


@task_register()
class StyleIndexReturnTask(ProcessorTaskBase):
    """风格指数收益"""

    name = "style_index_return"
    table_name = "processor_style_index_return"
    description = "风格指数收益与多窗口收益率"
    source_tables = ["tushare.index_factor_pro"]
    primary_keys = ["trade_date"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.index_map = cfg.get("index_map", STYLE_INDEXES)
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")
        codes = list(self.index_map.keys())
        if not codes:
            return pd.DataFrame()
        codes_str = ",".join(f"'{c}'" for c in codes)
        query = f"""
        SELECT trade_date, ts_code, close
        FROM tushare.index_factor_pro
        WHERE ts_code IN ({codes_str})
          AND trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
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

        pivot = data.pivot(index="trade_date", columns="ts_code", values="close")
        pivot = pivot.sort_index()
        pivot.columns = [self.index_map.get(c, c) for c in pivot.columns]

        result = pd.DataFrame(index=pivot.index)
        for col in pivot.columns:
            prices = pivot[col]
            result[f"{col}_Return"] = prices.pct_change()
            result[f"{col}_Return_5D"] = prices.pct_change(5)
            result[f"{col}_Return_20D"] = prices.pct_change(20)
            result[f"{col}_Return_60D"] = prices.pct_change(60)

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

