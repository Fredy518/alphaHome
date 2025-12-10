#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
指数波动率任务

基于 tushare.index_factor_pro 的收盘价计算 20/60/252 日实现波动率及分位、短长比。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ...operations.transforms import rolling_percentile
from ....common.logging_utils import get_logger


CORE_INDEXES = {
    "000300.SH": "HS300",
    "000905.SH": "ZZ500",
    "000985.CSI": "ZZQZ",
    "399006.SZ": "CYB",
    "000016.SH": "SZ50",
    "000001.SH": "SZZZ",
    "000852.SH": "ZZ1000",
}


@task_register()
class IndexVolatilityTask(ProcessorTaskBase):
    """指数波动率处理任务"""

    name = "index_volatility"
    table_name = "processor_index_volatility"
    description = "指数实现波动率与分位"
    source_tables = ["tushare.index_factor_pro"]
    primary_keys = ["trade_date"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.index_map = cfg.get("index_map", CORE_INDEXES)
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table
        self.logger = get_logger("processors.index_volatility")

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")
        codes = list(self.index_map.keys())
        if not codes:
            self.logger.warning("未配置指数代码")
            return pd.DataFrame()

        codes_str = ",".join(f"'{c}'" for c in codes)
        query = f"""
        SELECT trade_date, ts_code, close
        FROM tushare.index_factor_pro
        WHERE ts_code IN ({codes_str})
          AND trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
        ORDER BY ts_code, trade_date
        """
        rows = await self.db.fetch(query)
        if not rows:
            self.logger.warning("指数波动率数据为空")
            return pd.DataFrame()
        df = pd.DataFrame([dict(r) for r in rows])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        return df

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()

        result = pd.DataFrame()
        ann_factor = np.sqrt(252)

        for ts_code, alias in self.index_map.items():
            idx_df = data[data["ts_code"] == ts_code].copy()
            if idx_df.empty:
                continue

            idx_df = idx_df.set_index("trade_date").sort_index()
            returns = idx_df["close"].pct_change()

            rv20 = returns.rolling(20, min_periods=10).std() * ann_factor
            rv60 = returns.rolling(60, min_periods=30).std() * ann_factor
            rv252 = returns.rolling(252, min_periods=120).std() * ann_factor

            result[f"{alias}_RV_20D"] = rv20
            result[f"{alias}_RV_60D"] = rv60
            result[f"{alias}_RV_252D"] = rv252
            result[f"{alias}_RV_20D_Pctl"] = rolling_percentile(rv20, window=252, min_periods=60)
            result[f"{alias}_RV_Ratio_20_60"] = rv20 / rv60.replace(0, np.nan)

        result = result.sort_index()
        return result

    async def save_result(self, data: pd.DataFrame, **kwargs):
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return

        save_df = data.copy()
        save_df = save_df.reset_index().rename(columns={"index": "trade_date"})
        if pd.api.types.is_datetime64_any_dtype(save_df["trade_date"]):
            save_df["trade_date"] = save_df["trade_date"].dt.strftime("%Y%m%d")

        await self.db.save_dataframe(
            save_df,
            self.table_name,
            primary_keys=["trade_date"],
            use_insert_mode=False,
        )

