#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期权隐含波动率任务（简化版）

基于 tushare.option_daily/option_basic 与对应 ETF 价格，计算聚合标的的近月/次近月 ATM IV
以及简化 VIX 类指标。若 scipy 不可用或 BS 反推失败，则回退为价格/行权价比代理。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import math
import pandas as pd
import numpy as np

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ....common.logging_utils import get_logger

# 可选依赖：scipy 用于精确 BS 反推
try:
    from scipy.stats import norm
    from scipy.optimize import brentq

    SCIPY_AVAILABLE = True
except Exception:
    SCIPY_AVAILABLE = False


@task_register()
class OptionIVTask(ProcessorTaskBase):
    """期权 IV 任务"""

    name = "option_iv"
    table_name = "processor_option_iv"
    description = "近月/次近月 ATM IV 及 VIX 类指标（简化）"
    source_tables = ["tushare.option_daily", "tushare.option_basic", "tushare.fund_daily"]
    primary_keys = ["trade_date"]

    # 标的映射：opt_code -> (聚合标的, etf_code)
    OPTION_UNDERLYING_MAP = {
        "OP510050.SH": ("ETF50", "510050.SH"),
        "OP510300.SH": ("HS300", "510300.SH"),
        "OP159919.SZ": ("HS300", "159919.SZ"),
        "OP510500.SH": ("ZZ500", "510500.SH"),
        "OP159922.SZ": ("ZZ500", "159922.SZ"),
        "OP588000.SH": ("KC50", "588000.SH"),
        "OP588080.SH": ("KC50", "588080.SH"),
        "OP159901.SZ": ("SZ100", "159901.SZ"),
        "OP159915.SZ": ("CYB", "159915.SZ"),
    }

    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_connection=db_connection)
        cfg = config or {}
        self.option_map = cfg.get("option_underlying_map", self.OPTION_UNDERLYING_MAP)
        self.result_table = cfg.get("result_table", self.table_name)
        self.table_name = self.result_table
        self.logger = get_logger("processors.option_iv")

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        start_date = kwargs.get("start_date", "20190101")
        end_date = kwargs.get("end_date", "20991231")
        opt_codes = list(self.option_map.keys())
        if not opt_codes:
            self.logger.warning("未配置期权代码映射")
            return pd.DataFrame()

        opt_codes_str = ",".join(f"'{c}'" for c in opt_codes)
        query_options = f"""
        WITH option_prices AS (
            SELECT 
                od.trade_date,
                od.ts_code,
                od.close as opt_price,
                od.oi,
                ob.call_put,
                ob.exercise_price,
                ob.maturity_date,
                (ob.maturity_date - od.trade_date) as days_to_expiry
            FROM tushare.option_daily od
            JOIN tushare.option_basic ob ON od.ts_code = ob.ts_code
            WHERE od.trade_date >= '{start_date}'
              AND od.trade_date <= '{end_date}'
              AND od.close > 0 AND od.oi > 0
              AND ob.opt_code IN ({opt_codes_str})
              AND (ob.maturity_date - od.trade_date) BETWEEN 7 AND 90
        )
        SELECT * FROM option_prices
        ORDER BY trade_date, ts_code
        """

        # ETF 价格
        etf_codes = list({v[1] for v in self.option_map.values()})
        etf_codes_str = ",".join(f"'{c}'" for c in etf_codes)
        query_etf = f"""
        SELECT trade_date, ts_code, close
        FROM tushare.fund_daily
        WHERE ts_code IN ({etf_codes_str})
          AND trade_date >= '{start_date}'
          AND trade_date <= '{end_date}'
        ORDER BY trade_date, ts_code
        """

        opt_rows = await self.db.fetch(query_options)
        etf_rows = await self.db.fetch(query_etf)

        if not opt_rows or not etf_rows:
            self.logger.warning("期权或ETF数据为空")
            return pd.DataFrame()

        opt_df = pd.DataFrame([dict(r) for r in opt_rows])
        etf_df = pd.DataFrame([dict(r) for r in etf_rows])

        opt_df["trade_date"] = pd.to_datetime(opt_df["trade_date"])
        opt_df["maturity_date"] = pd.to_datetime(opt_df["maturity_date"])
        opt_df["exercise_price"] = pd.to_numeric(opt_df["exercise_price"], errors="coerce")
        opt_df["opt_price"] = pd.to_numeric(opt_df["opt_price"], errors="coerce")
        opt_df["days_to_expiry"] = pd.to_numeric(opt_df["days_to_expiry"], errors="coerce")

        etf_df["trade_date"] = pd.to_datetime(etf_df["trade_date"])
        etf_df["close"] = pd.to_numeric(etf_df["close"], errors="coerce")

        return pd.DataFrame({
            "_opt_df": [opt_df],
            "_etf_df": [etf_df],
        })

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        if data is None or data.empty:
            return pd.DataFrame()

        opt_df = data["_opt_df"].iloc[0]
        etf_df = data["_etf_df"].iloc[0]
        if opt_df.empty or etf_df.empty:
            return pd.DataFrame()

        # ETF 价格 pivot
        etf_pivot = etf_df.pivot(index="trade_date", columns="ts_code", values="close")

        # 聚合结果
        result = pd.DataFrame(index=etf_pivot.index)

        for opt_code, (underlying, etf_code) in self.option_map.items():
            # 近月/次近月，按到期天数排序
            df_opt = opt_df[opt_df["ts_code"].str.startswith(opt_code[:6])].copy()
            if df_opt.empty or etf_code not in etf_pivot.columns:
                continue

            df_opt["T_years"] = df_opt["days_to_expiry"] / 365.0
            # ATM 近似：行权价最接近标的现价
            for trade_date, group in df_opt.groupby("trade_date"):
                S = etf_pivot.loc[trade_date, etf_code] if trade_date in etf_pivot.index else np.nan
                if pd.isna(S):
                    continue
                group = group.assign(k_diff=(group["exercise_price"] - S).abs())
                group = group.sort_values(["k_diff", "days_to_expiry"])
                top = group.head(2)
                iv_list: List[Tuple[float, float]] = []
                for i, (_, row) in enumerate(top.iterrows()):
                    iv = self._implied_vol(row["call_put"], S, row["exercise_price"], row["T_years"], 0.02, row["opt_price"])
                    if pd.isna(iv):
                        iv = self._proxy_iv(row["opt_price"], S, row["exercise_price"], row["T_years"])
                    col = f"{underlying}_IV_{'Near' if i == 0 else 'Next'}"
                    result.loc[trade_date, col] = iv
                    iv_list.append((iv, row["T_years"]))

                # 30 天等效 IV（方差线性插值）
                target_T = 30 / 365.0
                near_val = result.get(f"{underlying}_IV_Near")
                next_val = result.get(f"{underlying}_IV_Next")
                if near_val is not None and next_val is not None:
                    t1 = iv_list[0][1] if len(iv_list) > 0 else np.nan
                    t2 = iv_list[1][1] if len(iv_list) > 1 else np.nan
                    iv1 = iv_list[0][0] if len(iv_list) > 0 else np.nan
                    iv2 = iv_list[1][0] if len(iv_list) > 1 else np.nan
                    if not any(pd.isna(x) for x in [t1, t2, iv1, iv2]) and t1 != t2:
                        var1 = iv1 * iv1 * t1
                        var2 = iv2 * iv2 * t2
                        w = (target_T - t1) / (t2 - t1)
                        target_var = var1 + (var2 - var1) * w
                        result.loc[trade_date, f"{underlying}_IV_30D"] = math.sqrt(max(target_var, 0) / target_T)

            # 简化 VIX：近月/次近月平均
            near_col = f"{underlying}_IV_Near"
            next_col = f"{underlying}_IV_Next"
            if near_col in result.columns or next_col in result.columns:
                result[f"{underlying}_IV_ShortTerm"] = result[[c for c in [near_col, next_col] if c in result.columns]].mean(axis=1)

        return result.sort_index()

    def _bs_price(self, option_type: str, S: float, K: float, T: float, r: float, sigma: float) -> float:
        if sigma <= 0 or T <= 0:
            intrinsic = max(S - K, 0) if option_type == "C" else max(K - S, 0)
            return intrinsic
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        if option_type == "C":
            return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

    def _implied_vol(self, option_type: str, S: float, K: float, T: float, r: float, price: float) -> float:
        # 约束条件
        if not SCIPY_AVAILABLE or any(x is None or x <= 0 for x in [S, K, T, price]):
            return np.nan
        intrinsic = max(S - K, 0) if option_type == "C" else max(K - S, 0)
        if price <= intrinsic * 0.99:
            return np.nan

        def objective(sig: float) -> float:
            return self._bs_price(option_type, S, K, T, r, sig) - price

        try:
            return float(brentq(objective, 1e-4, 5.0, maxiter=100))
        except Exception:
            return np.nan

    def _proxy_iv(self, price: float, S: float, K: float, T: float) -> float:
        """简化 IV 反推：scipy 不可用或求解失败时的兜底"""
        if any(x is None or x <= 0 for x in [price, S, K, T]):
            return np.nan
        ratio = price / K
        return float(np.clip(ratio, 0, 5))

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

