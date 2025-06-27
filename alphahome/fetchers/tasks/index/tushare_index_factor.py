#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
指数技术面因子 (idx_factor_pro) 更新任务
获取指数的技术面因子数据。
继承自 TushareTask，按 trade_date 增量更新。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_single_date_batches


@task_register()
class TushareIndexFactorProTask(TushareTask):
    """获取指数技术面因子数据 (专业版)"""

    # 1. 核心属性
    name = "tushare_index_factor_pro"
    description = "获取指数技术面因子数据 (专业版)"
    table_name = "index_factor_pro"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20050101"  # 指数因子需要较长历史数据
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 15  # Tushare Pro 积分限制优先，此为参考并发数
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "idx_factor_pro"  # Tushare API 名称
    # 包含所有 idx_factor_pro 接口返回的字段
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_change",
        "vol",
        "amount",
        "asi_bfq",
        "asit_bfq",
        "atr_bfq",
        "bbi_bfq",
        "bias1_bfq",
        "bias2_bfq",
        "bias3_bfq",
        "boll_lower_bfq",
        "boll_mid_bfq",
        "boll_upper_bfq",
        "brar_ar_bfq",
        "brar_br_bfq",
        "cci_bfq",
        "cr_bfq",
        "dfma_dif_bfq",
        "dfma_difma_bfq",
        "dmi_adx_bfq",
        "dmi_adxr_bfq",
        "dmi_mdi_bfq",
        "dmi_pdi_bfq",
        "downdays",
        "updays",
        "dpo_bfq",
        "madpo_bfq",
        "ema_bfq_10",
        "ema_bfq_20",
        "ema_bfq_250",
        "ema_bfq_30",
        "ema_bfq_5",
        "ema_bfq_60",
        "ema_bfq_90",
        "emv_bfq",
        "maemv_bfq",
        "expma_12_bfq",
        "expma_50_bfq",
        "kdj_bfq",
        "kdj_d_bfq",
        "kdj_j_bfq",
        "kdj_k_bfq",
        "ktn_down_bfq",
        "ktn_mid_bfq",
        "ktn_upper_bfq",
        "lowdays",
        "topdays",
        "ma_bfq_10",
        "ma_bfq_20",
        "ma_bfq_250",
        "ma_bfq_30",
        "ma_bfq_5",
        "ma_bfq_60",
        "ma_bfq_90",
        "macd_bfq",
        "macd_dea_bfq",
        "macd_dif_bfq",
        "mass_bfq",
        "ma_mass_bfq",
        "mfi_bfq",
        "mtm_bfq",
        "mtmma_bfq",
        "obv_bfq",
        "psy_bfq",
        "psyma_bfq",
        "roc_bfq",
        "maroc_bfq",
        "rsi_bfq_12",
        "rsi_bfq_24",
        "rsi_bfq_6",
        "taq_down_bfq",
        "taq_mid_bfq",
        "taq_up_bfq",
        "trix_bfq",
        "trma_bfq",
        "vr_bfq",
        "wr_bfq",
        "wr1_bfq",
        "xsii_td1_bfq",
        "xsii_td2_bfq",
        "xsii_td3_bfq",
        "xsii_td4_bfq",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，无需映射)
    column_mapping = {"vol": "volume"}

    # 4. 自定义索引
    indexes = [
        {
            "name": "idx_factorpro_code_date",
            "columns": ["ts_code", "trade_date"],
            "unique": True,
        },
        {"name": "idx_factorpro_date", "columns": "trade_date"},
        {"name": "idx_factorpro_update_time", "columns": "update_time"},
    ]

    # 4.1 数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['trade_date'].notna(),
        lambda df: df['close'] > 0,
        lambda df: df['high'] >= df['low'],
        lambda df: df['volume'] >= 0,
        lambda df: df['amount'] >= 0,
        lambda df: df['rsi_bfq_12'].between(0, 100), # RSI指标应在0-100之间
        lambda df: df['kdj_k_bfq'].between(0, 100), # KDJ.K指标应在0-100之间
    ]

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "est_peg": {"type": "FLOAT"},
        # 先定义基础行情字段
        "open": {"type": "NUMERIC(18,6)"},
        "high": {"type": "NUMERIC(18,6)"},
        "low": {"type": "NUMERIC(18,6)"},
        "close": {"type": "NUMERIC(18,6)"},
        "volume": {"type": "NUMERIC(18,6)"},  # volume放在行情字段组
        "amount": {"type": "NUMERIC(18,6)"},
        # 然后动态生成其他因子字段
        **{
            col: {"type": "NUMERIC(18,6)"}
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
        ts_code = kwargs.get("ts_code")  # 可选的指数代码

        # 确定总体起止日期
        if not start_date_overall:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date_overall = (
                    latest_db_date + pd.Timedelta(days=1)
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
            f"任务 {self.name}: 使用单日期批次工具生成批处理列表，范围: {start_date_overall} 到 {end_date_overall}, 指数代码: {ts_code if ts_code else '所有'}"
        )

        try:
            batch_list = await generate_single_date_batches(
                start_date=start_date_overall,
                end_date=end_date_overall,
                ts_code=ts_code,  # 可选的指数代码
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
