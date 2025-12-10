#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
市场资金流任务

从 tushare.stock_moneyflow 汇总主力净流入，结合 stock_dailybasic 的流通市值
计算净流入率及其滚动分位/标准化。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ...operations.transforms import rolling_zscore, rolling_percentile


@task_register()
class MoneyFlowTask(ProcessorTaskBase):
    """市场资金流任务"""

    name = "market_money_flow"
    table_name = "processor_market_money_flow"
    description = "主力净流入及标准化指标"
    source_tables = ["tushare.stock_moneyflow", "tushare.stock_dailybasic"]
    primary_keys = ["trade_date"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.window = cfg.get("window", 252)
        self.min_periods = cfg.get("min_periods", 60)
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")

        query = f"""
        SELECT 
            trade_date,
            SUM(net_mf_amount) AS total_net_mf_amount
        FROM tushare.stock_moneyflow
        WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
        GROUP BY trade_date
        ORDER BY trade_date
        """

        mv_query = f"""
        SELECT 
            trade_date,
            SUM(circ_mv) AS total_circ_mv
        FROM tushare.stock_dailybasic
        WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
        GROUP BY trade_date
        ORDER BY trade_date
        """

        rows = await self.db.fetch(query)
        mv_rows = await self.db.fetch(mv_query)

        if not rows:
            self.logger.warning("资金流数据为空")
            return pd.DataFrame()

        df = pd.DataFrame([dict(r) for r in rows])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.set_index("trade_date").sort_index()
        df["total_net_mf_amount"] = pd.to_numeric(df["total_net_mf_amount"], errors="coerce")

        if mv_rows:
            mv = pd.DataFrame([dict(r) for r in mv_rows])
            mv["trade_date"] = pd.to_datetime(mv["trade_date"])
            mv = mv.set_index("trade_date").sort_index()
            mv["total_circ_mv"] = pd.to_numeric(mv["total_circ_mv"], errors="coerce")
            df = df.join(mv, how="left")
        else:
            df["total_circ_mv"] = np.nan

        return df

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()
        circ_mv_yuan = df["total_circ_mv"] * 10000
        circ_mv_yuan = circ_mv_yuan.replace(0, np.nan)

        result = pd.DataFrame(index=df.index)
        result["Total_Net_MF"] = df["total_net_mf_amount"]
        result["Net_MF_Rate"] = df["total_net_mf_amount"] / circ_mv_yuan

        # 滚动标准化/分位
        result["Net_MF_Rate_ZScore"] = rolling_zscore(
            result["Net_MF_Rate"], window=self.window, min_periods=self.min_periods
        )
        result["Net_MF_Rate_Pctl"] = rolling_percentile(
            result["Net_MF_Rate"], window=self.window, min_periods=self.min_periods
        )

        # 对主力净流入绝对值做标准化
        result["Net_MF_ZScore"] = rolling_zscore(
            result["Total_Net_MF"], window=self.window, min_periods=self.min_periods
        )
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

