#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货相关任务：股指期货基差、会员持仓
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ....common.logging_utils import get_logger
from ...operations.transforms import rolling_zscore, rolling_percentile


@task_register()
class FuturesBasisTask(ProcessorTaskBase):
    """股指期货基差"""

    name = "futures_basis"
    table_name = "processor_futures_basis"
    description = "IF/IC/IM 基差与基差率"
    source_tables = ["tushare.future_daily", "tushare.index_factor_pro"]
    primary_keys = ["trade_date"]

    INDEX_MAP = {
        "IF": "000300.SH",  # 沪深300
        "IC": "000905.SH",  # 中证500
        "IM": "000852.SH",  # 中证1000
    }

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.index_map = cfg.get("index_map", self.INDEX_MAP)
        self.window = cfg.get("window", 252)
        self.min_periods = cfg.get("min_periods", 60)
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table
        self.logger = get_logger("processors.futures_basis")

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")
        fut_prefix = tuple(self.index_map.keys())
        fut_like = " OR ".join([f"ts_code LIKE '{p}%'" for p in fut_prefix])
        idx_codes = ",".join(f"'{c}'" for c in set(self.index_map.values()))

        fut_query = f"""
        SELECT trade_date, ts_code, close, oi
        FROM tushare.future_daily
        WHERE ({fut_like})
          AND trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
        """
        idx_query = f"""
        SELECT trade_date, ts_code, close
        FROM tushare.index_factor_pro
        WHERE ts_code IN ({idx_codes})
          AND trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
        """
        fut_rows = await self.db.fetch(fut_query)
        idx_rows = await self.db.fetch(idx_query)
        if not fut_rows or not idx_rows:
            self.logger.warning("期货或现货数据为空")
            return pd.DataFrame()

        fut_df = pd.DataFrame([dict(r) for r in fut_rows])
        idx_df = pd.DataFrame([dict(r) for r in idx_rows])
        fut_df["trade_date"] = pd.to_datetime(fut_df["trade_date"])
        idx_df["trade_date"] = pd.to_datetime(idx_df["trade_date"])
        fut_df["close"] = pd.to_numeric(fut_df["close"], errors="coerce")
        fut_df["oi"] = pd.to_numeric(fut_df["oi"], errors="coerce")
        idx_df["close"] = pd.to_numeric(idx_df["close"], errors="coerce")
        return pd.DataFrame({"_fut": [fut_df], "_idx": [idx_df]})

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()
        fut_df = data["_fut"].iloc[0]
        idx_df = data["_idx"].iloc[0]
        if fut_df.empty or idx_df.empty:
            return pd.DataFrame()

        results = pd.DataFrame()
        for fut_type, idx_code in self.index_map.items():
            f_data = fut_df[fut_df["ts_code"].str.startswith(fut_type)].copy()
            i_data = idx_df[idx_df["ts_code"] == idx_code].copy()
            if f_data.empty or i_data.empty:
                continue
            merged = pd.merge(f_data, i_data, on="trade_date", suffixes=("_fut", "_idx"))
            # 过滤无效价格/持仓
            merged = merged[(merged["close_fut"] > 0) & (merged["close_idx"] > 0) & (merged["oi"] > 0)]
            merged["basis"] = merged["close_idx"] - merged["close_fut"]

            def weighted_avg(x):
                if x["oi"].sum() == 0:
                    return np.nan
                return (x["basis"] * x["oi"]).sum() / x["oi"].sum()

            daily_basis = merged.groupby("trade_date").apply(weighted_avg)
            daily_basis.index = pd.to_datetime(daily_basis.index)
            idx_close = i_data.set_index("trade_date")["close"]
            ratio = daily_basis / idx_close.reindex(daily_basis.index)

            results[f"{fut_type}_Basis"] = daily_basis
            results[f"{fut_type}_Basis_Ratio"] = ratio

        if results.empty:
            return results

        # 滚动标准化与分位（对每个序列独立计算）
        for col in list(results.columns):
            results[f"{col}_ZScore"] = rolling_zscore(results[col], window=self.window, min_periods=self.min_periods)
            results[f"{col}_Pctl"] = rolling_percentile(results[col], window=self.window, min_periods=self.min_periods)

        results = results.replace([np.inf, -np.inf], np.nan)
        return results.sort_index()

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
class MemberPositionTask(ProcessorTaskBase):
    """期货会员持仓聚合"""

    name = "member_position"
    table_name = "processor_member_position"
    description = "期货会员净多、净变化、多空比"
    source_tables = ["tushare.future_holding"]
    primary_keys = ["trade_date"]

    FUTURE_TYPES = ["IF", "IC", "IH", "IM"]

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.future_types = cfg.get("future_types", self.FUTURE_TYPES)
        self.window = cfg.get("window", 120)
        self.min_periods = cfg.get("min_periods", 30)
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table
        self.logger = get_logger("processors.member_position")

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")
        result_frames = []
        for fut_type in self.future_types:
            query = f"""
            SELECT
                trade_date,
                SUM(COALESCE(long_hld, 0)) AS total_long_hld,
                SUM(COALESCE(short_hld, 0)) AS total_short_hld,
                SUM(COALESCE(long_chg, 0)) AS total_long_chg,
                SUM(COALESCE(short_chg, 0)) AS total_short_chg
            FROM tushare.future_holding
            WHERE symbol LIKE '{fut_type}%'
              AND trade_date >= '{start_date}'
              AND trade_date <= '{end_date}'
            GROUP BY trade_date
            ORDER BY trade_date
            """
            rows = await self.db.fetch(query)
            if not rows:
                continue
            df = pd.DataFrame([dict(r) for r in rows])
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.set_index("trade_date").astype(float)
            df[f"{fut_type}_NET_LONG"] = df["total_long_hld"] - df["total_short_hld"]
            df[f"{fut_type}_NET_CHG"] = df["total_long_chg"] - df["total_short_chg"]
            df[f"{fut_type}_RATIO"] = df.apply(
                lambda row: (row["total_long_hld"] / row["total_short_hld"]) if row["total_short_hld"] > 0 else np.nan,
                axis=1,
            )
            result_frames.append(df[[f"{fut_type}_NET_LONG", f"{fut_type}_NET_CHG", f"{fut_type}_RATIO"]])

        if not result_frames:
            return pd.DataFrame()
        result = result_frames[0]
        for df in result_frames[1:]:
            result = result.join(df, how="outer")
        if result.empty:
            return result

        # 对净多/净变/多空比做滚动标准化/分位
        for col in result.columns:
            result[f"{col}_ZScore"] = rolling_zscore(result[col], window=self.window, min_periods=self.min_periods)
            result[f"{col}_Pctl"] = rolling_percentile(result[col], window=self.window, min_periods=self.min_periods)

        result = result.replace([np.inf, -np.inf], np.nan)
        return result.sort_index()

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        """聚合后的数据直接返回"""
        if data is None or data.empty:
            return pd.DataFrame()
        return data

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

