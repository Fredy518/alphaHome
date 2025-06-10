#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票技术面因子 (stk_factor_pro) 更新任务
获取股票的技术面因子数据。
继承自 TushareTask，按 trade_date 增量更新。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np  # 引入 numpy 用于处理可能的 inf/-inf
import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register

# 导入批处理工具
from ...tools.batch_utils import generate_single_date_batches


@task_register()
class TushareStockFactorProTask(TushareTask):
    """获取股票技术面因子数据 (专业版)"""

    # 1. 核心属性
    name = "tushare_stock_factor_pro"
    description = "获取股票技术面因子数据 (专业版)"
    table_name = "tushare_stock_factor_pro"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19901219"  # A股最早交易日附近

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 8
    default_page_size = 10000

    # 2. TushareTask 特有属性
    api_name = "stk_factor_pro"  # Tushare API 名称
    # 包含所有 stk_factor_pro 接口返回的字段
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "open_hfq",
        "open_qfq",
        "high",
        "high_hfq",
        "high_qfq",
        "low",
        "low_hfq",
        "low_qfq",
        "close",
        "close_hfq",
        "close_qfq",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
        "adj_factor",
        "asi_bfq",
        "asi_hfq",
        "asi_qfq",
        "asit_bfq",
        "asit_hfq",
        "asit_qfq",
        "atr_bfq",
        "atr_hfq",
        "atr_qfq",
        "bbi_bfq",
        "bbi_hfq",
        "bbi_qfq",
        "bias1_bfq",
        "bias1_hfq",
        "bias1_qfq",
        "bias2_bfq",
        "bias2_hfq",
        "bias2_qfq",
        "bias3_bfq",
        "bias3_hfq",
        "bias3_qfq",
        "boll_lower_bfq",
        "boll_lower_hfq",
        "boll_lower_qfq",
        "boll_mid_bfq",
        "boll_mid_hfq",
        "boll_mid_qfq",
        "boll_upper_bfq",
        "boll_upper_hfq",
        "boll_upper_qfq",
        "brar_ar_bfq",
        "brar_ar_hfq",
        "brar_ar_qfq",
        "brar_br_bfq",
        "brar_br_hfq",
        "brar_br_qfq",
        "cci_bfq",
        "cci_hfq",
        "cci_qfq",
        "cr_bfq",
        "cr_hfq",
        "cr_qfq",
        "dfma_dif_bfq",
        "dfma_dif_hfq",
        "dfma_dif_qfq",
        "dfma_difma_bfq",
        "dfma_difma_hfq",
        "dfma_difma_qfq",
        "dmi_adx_bfq",
        "dmi_adx_hfq",
        "dmi_adx_qfq",
        "dmi_adxr_bfq",
        "dmi_adxr_hfq",
        "dmi_adxr_qfq",
        "dmi_mdi_bfq",
        "dmi_mdi_hfq",
        "dmi_mdi_qfq",
        "dmi_pdi_bfq",
        "dmi_pdi_hfq",
        "dmi_pdi_qfq",
        "downdays",
        "updays",
        "dpo_bfq",
        "dpo_hfq",
        "dpo_qfq",
        "madpo_bfq",
        "madpo_hfq",
        "madpo_qfq",
        "ema_bfq_10",
        "ema_bfq_20",
        "ema_bfq_250",
        "ema_bfq_30",
        "ema_bfq_5",
        "ema_bfq_60",
        "ema_bfq_90",
        "ema_hfq_10",
        "ema_hfq_20",
        "ema_hfq_250",
        "ema_hfq_30",
        "ema_hfq_5",
        "ema_hfq_60",
        "ema_hfq_90",
        "ema_qfq_10",
        "ema_qfq_20",
        "ema_qfq_250",
        "ema_qfq_30",
        "ema_qfq_5",
        "ema_qfq_60",
        "ema_qfq_90",
        "emv_bfq",
        "emv_hfq",
        "emv_qfq",
        "maemv_bfq",
        "maemv_hfq",
        "maemv_qfq",
        "expma_12_bfq",
        "expma_12_hfq",
        "expma_12_qfq",
        "expma_50_bfq",
        "expma_50_hfq",
        "expma_50_qfq",
        "kdj_bfq",
        "kdj_hfq",
        "kdj_qfq",
        "kdj_d_bfq",
        "kdj_d_hfq",
        "kdj_d_qfq",
        "kdj_k_bfq",
        "kdj_k_hfq",
        "kdj_k_qfq",
        "ktn_down_bfq",
        "ktn_down_hfq",
        "ktn_down_qfq",
        "ktn_mid_bfq",
        "ktn_mid_hfq",
        "ktn_mid_qfq",
        "ktn_upper_bfq",
        "ktn_upper_hfq",
        "ktn_upper_qfq",
        "lowdays",
        "topdays",
        "ma_bfq_10",
        "ma_bfq_20",
        "ma_bfq_250",
        "ma_bfq_30",
        "ma_bfq_5",
        "ma_bfq_60",
        "ma_bfq_90",
        "ma_hfq_10",
        "ma_hfq_20",
        "ma_hfq_250",
        "ma_hfq_30",
        "ma_hfq_5",
        "ma_hfq_60",
        "ma_hfq_90",
        "ma_qfq_10",
        "ma_qfq_20",
        "ma_qfq_250",
        "ma_qfq_30",
        "ma_qfq_5",
        "ma_qfq_60",
        "ma_qfq_90",
        "macd_bfq",
        "macd_hfq",
        "macd_qfq",
        "macd_dea_bfq",
        "macd_dea_hfq",
        "macd_dea_qfq",
        "macd_dif_bfq",
        "macd_dif_hfq",
        "macd_dif_qfq",
        "mass_bfq",
        "mass_hfq",
        "mass_qfq",
        "ma_mass_bfq",
        "ma_mass_hfq",
        "ma_mass_qfq",
        "mfi_bfq",
        "mfi_hfq",
        "mfi_qfq",
        "mtm_bfq",
        "mtm_hfq",
        "mtm_qfq",
        "mtmma_bfq",
        "mtmma_hfq",
        "mtmma_qfq",
        "obv_bfq",
        "obv_hfq",
        "obv_qfq",
        "psy_bfq",
        "psy_hfq",
        "psy_qfq",
        "psyma_bfq",
        "psyma_hfq",
        "psyma_qfq",
        "roc_bfq",
        "roc_hfq",
        "roc_qfq",
        "maroc_bfq",
        "maroc_hfq",
        "maroc_qfq",
        "rsi_bfq_12",
        "rsi_bfq_24",
        "rsi_bfq_6",
        "rsi_hfq_12",
        "rsi_hfq_24",
        "rsi_hfq_6",
        "rsi_qfq_12",
        "rsi_qfq_24",
        "rsi_qfq_6",
        "taq_down_bfq",
        "taq_down_hfq",
        "taq_down_qfq",
        "taq_mid_bfq",
        "taq_mid_hfq",
        "taq_mid_qfq",
        "taq_up_bfq",
        "taq_up_hfq",
        "taq_up_qfq",
        "trix_bfq",
        "trix_hfq",
        "trix_qfq",
        "trma_bfq",
        "trma_hfq",
        "trma_qfq",
        "vr_bfq",
        "vr_hfq",
        "vr_qfq",
        "wr_bfq",
        "wr_hfq",
        "wr_qfq",
        "wr1_bfq",
        "wr1_hfq",
        "wr1_qfq",
        "xsii_td1_bfq",
        "xsii_td1_hfq",
        "xsii_td1_qfq",
        "xsii_td2_bfq",
        "xsii_td2_hfq",
        "xsii_td2_qfq",
        "xsii_td3_bfq",
        "xsii_td3_hfq",
        "xsii_td3_qfq",
        "xsii_td4_bfq",
        "xsii_td4_hfq",
        "xsii_td4_qfq",
    ]

    # 3. 列名映射
    column_mapping = {"vol": "volume"}

    # 4. 自定义索引
    indexes = [
        {
            "name": "idx_stkfactor_code_date",
            "columns": ["ts_code", "trade_date"],
            "unique": True,
        },
        {"name": "idx_stkfactor_date", "columns": "trade_date"},
        {"name": "idx_stkfactor_code", "columns": "ts_code"},
        {"name": "idx_stkfactor_update_time", "columns": "update_time"},
    ]

    # 5. 表结构定义 (所有数值型字段使用 NUMERIC)
    # 准备需要排除的非数值字段或已特殊处理的字段
    schema = {
        "ts_code": {
            "type": "VARCHAR(10)",
            "constraints": "NOT NULL",
        },  # 股票代码通常较短
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(18, 6)"},
        "high": {"type": "NUMERIC(18, 6)"},
        "low": {"type": "NUMERIC(18, 6)"},
        "close": {"type": "NUMERIC(18, 6)"},
        "volume": {
            "type": "NUMERIC(20, 2)"
        },  # 显式定义映射后的 volume，使用较大范围和2位小数
        "amount": {
            "type": "NUMERIC(20, 2)"
        },  # 显式定义映射后的 amount，使用较大范围和2位小数
        # 动态生成其他所有数值字段的 schema
        **{
            col: {"type": "NUMERIC(18, 6)"}
            for col in fields
            if col
            not in [
                "ts_code",
                "trade_date",
                "vol",
                "amount",
                "open",
                "high",
                "low",
                "close",
            ]
        },
    }

    # 6. 数据类型转换 (所有数值型字段转换为 float)
    transformations = {
        col: float for col in fields if col not in ["ts_code", "trade_date"]
    }

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用单日期批次工具)。
        为每个交易日生成单独的批次，使用 trade_date 参数。
        """
        start_date_overall = kwargs.get("start_date")
        end_date_overall = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的股票代码

        # 确定总体起止日期
        if not start_date_overall:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date_overall = (
                    pd.to_datetime(latest_db_date) + pd.Timedelta(days=1)
                ).strftime("%Y%m%d")
            else:
                start_date_overall = self.default_start_date
            self.logger.info(
                f"未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date_overall}"
            )

        if not end_date_overall:
            end_date_overall = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"未提供 end_date，使用当前日期: {end_date_overall}")

        if pd.to_datetime(start_date_overall) > pd.to_datetime(end_date_overall):
            self.logger.info(
                f"起始日期 ({start_date_overall}) 晚于结束日期 ({end_date_overall})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 使用单日期批次工具生成批处理列表，范围: {start_date_overall} 到 {end_date_overall}, 股票代码: {ts_code if ts_code else '所有'}"
        )

        try:
            batch_list = await generate_single_date_batches(
                start_date=start_date_overall,
                end_date=end_date_overall,
                date_field="trade_date",  # API 使用 trade_date 参数
                ts_code=ts_code,  # 可选的股票代码
                exchange="SSE",  # 明确指定使用上交所日历作为A股代表
                logger=self.logger,
            )
            self.logger.info(f"成功生成 {len(batch_list)} 个单日期批次。")
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成单日期批次时出错: {e}", exc_info=True
            )
            return []

    # 重写 specific_transform 以处理可能的无穷大值
    async def specific_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理 DataFrame 中的无穷大值"""
        # 选择所有数值类型的列进行处理
        numeric_cols = df.select_dtypes(include=np.number).columns
        # 将无穷大值替换为 NaN
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
        self.logger.debug(
            f"执行特定转换: 将 {len(numeric_cols)} 个数值列中的 inf/-inf 替换为 NaN"
        )
        return df
