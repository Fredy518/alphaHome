#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票技术面因子 (stk_factor_pro) 更新任务
获取股票的技术面因子数据。
继承自 TushareTask，按 trade_date 增量更新。
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np  # 引入 numpy 用于处理可能的 inf/-inf
import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register





@task_register()
class TushareStockFactorProTask(TushareTask):
    """获取股票技术面因子数据 (专业版)"""

    # 1. 核心属性
    name = "tushare_stock_factor_pro"
    description = "获取股票技术面因子数据 (专业版)"
    table_name = "stock_factor_pro"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19901219"  # A股最早交易日附近
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 10000

    # 2. TushareTask 特有属性
    api_name = "stk_factor_pro"
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "open_hfq",
        "high",
        "high_hfq",
        "low",
        "low_hfq",
        "close",
        "close_hfq",
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
        "asit_bfq",
        "asit_hfq",
        "atr_bfq",
        "atr_hfq",
        "bbi_bfq",
        "bbi_hfq",
        "bias1_bfq",
        "bias1_hfq",
        "bias2_bfq",
        "bias2_hfq",
        "bias3_bfq",
        "bias3_hfq",
        "boll_lower_bfq",
        "boll_lower_hfq",
        "boll_mid_bfq",
        "boll_mid_hfq",
        "boll_upper_bfq",
        "boll_upper_hfq",
        "brar_ar_bfq",
        "brar_ar_hfq",
        "brar_br_bfq",
        "brar_br_hfq",
        "cci_bfq",
        "cci_hfq",
        "cr_bfq",
        "cr_hfq",
        "dfma_dif_bfq",
        "dfma_dif_hfq",
        "dfma_difma_bfq",
        "dfma_difma_hfq",
        "dmi_adx_bfq",
        "dmi_adx_hfq",
        "dmi_adxr_bfq",
        "dmi_adxr_hfq",
        "dmi_mdi_bfq",
        "dmi_mdi_hfq",
        "dmi_pdi_bfq",
        "dmi_pdi_hfq",
        "downdays",
        "updays",
        "dpo_bfq",
        "dpo_hfq",
        "madpo_bfq",
        "madpo_hfq",
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
        "emv_bfq",
        "emv_hfq",
        "maemv_bfq",
        "maemv_hfq",
        "expma_12_bfq",
        "expma_12_hfq",
        "expma_50_bfq",
        "expma_50_hfq",
        "kdj_bfq",
        "kdj_hfq",
        "kdj_d_bfq",
        "kdj_d_hfq",
        "kdj_k_bfq",
        "kdj_k_hfq",
        "ktn_down_bfq",
        "ktn_down_hfq",
        "ktn_mid_bfq",
        "ktn_mid_hfq",
        "ktn_upper_bfq",
        "ktn_upper_hfq",
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
        "macd_bfq",
        "macd_hfq",
        "macd_dea_bfq",
        "macd_dea_hfq",
        "macd_dif_bfq",
        "macd_dif_hfq",
        "mass_bfq",
        "mass_hfq",
        "ma_mass_bfq",
        "ma_mass_hfq",
        "mfi_bfq",
        "mfi_hfq",
        "mtm_bfq",
        "mtm_hfq",
        "mtmma_bfq",
        "mtmma_hfq",
        "obv_bfq",
        "obv_hfq",
        "psy_bfq",
        "psy_hfq",
        "psyma_bfq",
        "psyma_hfq",
        "roc_bfq",
        "roc_hfq",
        "maroc_bfq",
        "maroc_hfq",
        "rsi_bfq_12",
        "rsi_bfq_24",
        "rsi_bfq_6",
        "rsi_hfq_12",
        "rsi_hfq_24",
        "rsi_hfq_6",
        "taq_down_bfq",
        "taq_down_hfq",
        "taq_mid_bfq",
        "taq_mid_hfq",
        "taq_up_bfq",
        "taq_up_hfq",
        "trix_bfq",
        "trix_hfq",
        "trma_bfq",
        "trma_hfq",
        "vr_bfq",
        "vr_hfq",
        "wr_bfq",
        "wr_hfq",
        "wr1_bfq",
        "wr1_hfq",
        "xsii_td1_bfq",
        "xsii_td1_hfq",
        "xsii_td2_bfq",
        "xsii_td2_hfq",
        "xsii_td3_bfq",
        "xsii_td3_hfq",
        "xsii_td4_bfq",
        "xsii_td4_hfq",
    ]

    # 3. 列名映射
    column_mapping = {"vol": "volume"}

    # 4. 数据类型转换 (所有数值型字段转换为 float)
    transformations = {
        col: float for col in fields if col not in ["ts_code", "trade_date"]
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        # 动态生成其他所有数值字段的 schema
        **{
            col: {"type": "NUMERIC(18, 6)"}
            for col in fields
            if col not in ["ts_code", "trade_date", "downdays", "updays", "lowdays", "topdays"]
        },
        "downdays": {"type": "INTEGER"},
        "updays": {"type": "INTEGER"},
        "lowdays": {"type": "INTEGER"},
        "topdays": {"type": "INTEGER"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stkfactor_code_date", "columns": ["ts_code", "trade_date"], "unique": True},
        {"name": "idx_stkfactor_date", "columns": "trade_date"},
        {"name": "idx_stkfactor_code", "columns": "ts_code"},
        {"name": "idx_stkfactor_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证函数 (用于调用时机验证)
    def _process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """处理从API获取的数据"""
        if data.empty:
            return data

        # 调用父类的通用处理逻辑
        data = super()._process_data(data, **kwargs)

        # 替换 inf/-inf 为 NaN，在后续处理中会被转换为 NULL
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            data.loc[:, col] = data[col].replace([np.inf, -np.inf], np.nan)

        return data

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        为每个交易日生成单独的批次，使用 trade_date 参数。

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date_overall = kwargs.get("start_date")
        end_date_overall = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的股票代码

        # 确定总体起止日期
        if not start_date_overall:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                next_day_obj = latest_db_date + timedelta(days=1)
                start_date_overall = next_day_obj.strftime("%Y%m%d") # type: ignore
            else:
                start_date_overall = self.default_start_date
            self.logger.info(
                f"未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date_overall}"
            )

        if not end_date_overall:
            end_date_overall = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"未提供 end_date，使用当前日期: {end_date_overall}")

        if datetime.strptime(str(start_date_overall), "%Y%m%d") > datetime.strptime(str(end_date_overall), "%Y%m%d"): # type: ignore
            self.logger.info(
                f"起始日期 ({start_date_overall}) 晚于结束日期 ({end_date_overall})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 使用 BatchPlanner 生成批处理列表，范围: {start_date_overall} 到 {end_date_overall}, 股票代码: {ts_code if ts_code else '所有'}"
        )

        try:
            # 使用标准的单日期批次生成工具
            from ...sources.tushare.batch_utils import generate_single_date_batches

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_single_date_batches(
                start_date=start_date_overall,
                end_date=end_date_overall,
                date_field="trade_date",  # 指定日期字段名
                ts_code=ts_code,
                exchange="SSE",
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            return []


# 导出任务类
__all__ = ["TushareStockFactorProTask"]
