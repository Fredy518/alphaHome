#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
指数估值处理任务

从 tushare.index_dailybasic 获取核心指数 PE/PB，并计算滚动分位与 ERP。
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List

import pandas as pd

from ...operations.transforms import rolling_percentile
from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register


@task_register()
class IndexValuationTask(ProcessorTaskBase):
    """指数估值处理任务

    - 计算核心指数的 PE/PB、10年/12个月滚动分位
    - 计算 ERP = 1/PE - 国债收益率
    """

    name = "index_valuation"
    table_name = "processor_index_valuation"
    description = "核心指数估值与ERP"
    # 数据血缘
    source_tables = ["tushare.index_dailybasic", "akshare.macro_bond_rate"]
    primary_keys = ["trade_date"]

    # 默认指数映射（ts_code -> 别名）
    INDEXES: Dict[str, str] = {
        "000300.SH": "HS300",
        "000905.SH": "ZZ500",
        "399006.SZ": "CYB",
    }

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.index_map = cfg.get("index_map", self.INDEXES)
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """获取指数估值原始数据与国债收益率"""
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")
        codes = list(self.index_map.keys())
        if not codes:
            self.logger.warning("未配置指数代码，跳过获取")
            return pd.DataFrame()

        codes_sql = ",".join(f"'{c}'" for c in codes)
        query_pe_pb = f"""
        SELECT trade_date, ts_code, pe_ttm, pb
        FROM tushare.index_dailybasic
        WHERE ts_code IN ({codes_sql})
          AND trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
        ORDER BY trade_date, ts_code
        """

        query_yield = f"""
        SELECT date AS trade_date, yield
        FROM akshare.macro_bond_rate
        WHERE country = 'CN'
          AND term = '10y'
          AND date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY trade_date
        """

        rows = await self.db.fetch(query_pe_pb)
        yield_rows = await self.db.fetch(query_yield)

        if not rows:
            self.logger.warning("指数估值数据为空")
            return pd.DataFrame()

        df = pd.DataFrame([dict(r) for r in rows])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values(["trade_date", "ts_code"])

        yield_df = pd.DataFrame([dict(r) for r in yield_rows]) if yield_rows else pd.DataFrame()
        if not yield_df.empty:
            yield_df["trade_date"] = pd.to_datetime(yield_df["trade_date"])
            yield_df = yield_df.set_index("trade_date").sort_index()

        df["_yield_df"] = [yield_df]  # 暂存，process_data 使用
        return df

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        """计算滚动分位与 ERP"""
        if data is None or data.empty:
            return pd.DataFrame()

        # 取出收益率表
        yield_df = None
        if "_yield_df" in data.columns:
            yield_df = data["_yield_df"].iloc[0]
            data = data.drop(columns=["_yield_df"])

        data["trade_date"] = pd.to_datetime(data["trade_date"])
        data = data.set_index("trade_date")

        results = pd.DataFrame(index=data.index.unique().sort_values())

        # 国债收益率对齐
        if yield_df is not None and not yield_df.empty:
            yield_df = yield_df.reindex(results.index).ffill()
            results["China_10Y_Yield"] = yield_df["yield"].astype(float)
        else:
            results["China_10Y_Yield"] = pd.NA

        # 逐指数计算
        for ts_code, alias in self.index_map.items():
            df_code = data[data["ts_code"] == ts_code][["pe_ttm", "pb"]].copy()
            if df_code.empty:
                self.logger.warning(f"{ts_code} 无估值数据，跳过")
                continue

            df_code = df_code.astype(float)
            df_code = df_code.reindex(results.index)

            pe_col = f"{alias}_PE"
            pb_col = f"{alias}_PB"
            results[pe_col] = df_code["pe_ttm"]
            results[pb_col] = df_code["pb"]

            # 分位：10年窗口（2520，最小252），12个月窗口（252，最小60）
            results[f"{alias}_PE_Pctl_10Y"] = rolling_percentile(
                results[pe_col], window=2520, min_periods=252
            )
            results[f"{alias}_PB_Pctl_10Y"] = rolling_percentile(
                results[pb_col], window=2520, min_periods=252
            )
            results[f"{alias}_PE_Pctl_12M"] = rolling_percentile(
                results[pe_col], window=252, min_periods=60
            )
            results[f"{alias}_PB_Pctl_12M"] = rolling_percentile(
                results[pb_col], window=252, min_periods=60
            )

            # ERP = 1/PE - yield/100
            if pe_col in results.columns and "China_10Y_Yield" in results.columns:
                yield_fraction = pd.to_numeric(results["China_10Y_Yield"], errors="coerce") / 100
                results[f"{alias}_ERP"] = (1 / results[pe_col]) - yield_fraction

        # 清理
        results = results.sort_index()
        return results

    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存结果"""
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

