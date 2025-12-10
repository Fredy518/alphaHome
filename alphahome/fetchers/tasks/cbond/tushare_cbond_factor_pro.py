#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
可转债技术因子(专业版) (cb_factor_pro) 数据任务

接口文档: https://tushare.pro/document/2?doc_id=223
数据说明:
- 获取可转债每日技术面因子数据，用于跟踪可转债当前走势情况
- 数据由Tushare社区自产，覆盖全历史
- 单次最大10000条

权限要求: 5000积分每分钟30次，8000积分以上每分钟500次
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_trade_day_batches
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes


@task_register()
class TushareCbondFactorProTask(TushareTask):
    """获取可转债技术因子数据 (cb_factor_pro)"""

    # 1. 核心属性
    name = "tushare_cbond_factor_pro"
    description = "获取可转债每日技术面因子数据"
    table_name = "cbond_factor_pro"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20170101"
    data_source = "tushare"
    domain = "cbond"

    # --- 默认配置 ---
    default_concurrent_limit = 3
    default_page_size = 10000

    # 2. TushareTask 特有属性
    api_name = "cb_factor_pro"
    fields = [
        "ts_code", "trade_date", "open", "high", "low", "close",
        "pre_close", "change", "pct_change", "vol", "amount",
        # 技术因子
        "asi_bfq", "asit_bfq", "atr_bfq", "bbi_bfq",
        "bias1_bfq", "bias2_bfq", "bias3_bfq",
        "boll_lower_bfq", "boll_mid_bfq", "boll_upper_bfq",
        "brar_ar_bfq", "brar_br_bfq", "cci_bfq", "cr_bfq",
        "dfma_dif_bfq", "dfma_difma_bfq",
        "dmi_adx_bfq", "dmi_adxr_bfq", "dmi_mdi_bfq", "dmi_pdi_bfq",
        "downdays", "updays",
        "dpo_bfq", "madpo_bfq",
        "ema_bfq_5", "ema_bfq_10", "ema_bfq_20", "ema_bfq_30",
        "ema_bfq_60", "ema_bfq_90", "ema_bfq_250",
        "emv_bfq", "maemv_bfq",
        "expma_12_bfq", "expma_50_bfq",
        "kdj_bfq", "kdj_d_bfq", "kdj_k_bfq",
        "ktn_down_bfq", "ktn_mid_bfq", "ktn_upper_bfq",
        "lowdays", "topdays",
        "ma_bfq_5", "ma_bfq_10", "ma_bfq_20", "ma_bfq_30",
        "ma_bfq_60", "ma_bfq_90", "ma_bfq_250",
        "macd_bfq", "macd_dea_bfq", "macd_dif_bfq",
        "mass_bfq", "ma_mass_bfq", "mfi_bfq",
        "mtm_bfq", "mtmma_bfq", "obv_bfq",
        "psy_bfq", "psyma_bfq",
        "roc_bfq", "maroc_bfq",
        "rsi_bfq_6", "rsi_bfq_12", "rsi_bfq_24",
        "taq_down_bfq", "taq_mid_bfq", "taq_up_bfq",
        "trix_bfq", "trma_bfq", "vr_bfq",
        "wr_bfq", "wr1_bfq",
        "xsii_td1_bfq", "xsii_td2_bfq", "xsii_td3_bfq", "xsii_td4_bfq",
    ]

    # 3. 列名映射 (vol -> volume)
    column_mapping: Dict[str, str] = {
        "vol": "volume",
    }

    # 4. 数据类型转换
    transformations = {
        "open": lambda x: pd.to_numeric(x, errors="coerce"),
        "high": lambda x: pd.to_numeric(x, errors="coerce"),
        "low": lambda x: pd.to_numeric(x, errors="coerce"),
        "close": lambda x: pd.to_numeric(x, errors="coerce"),
        "pre_close": lambda x: pd.to_numeric(x, errors="coerce"),
        "change": lambda x: pd.to_numeric(x, errors="coerce"),
        "pct_change": lambda x: pd.to_numeric(x, errors="coerce"),
        "volume": lambda x: pd.to_numeric(x, errors="coerce"),
        "amount": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "pre_close": {"type": "NUMERIC(15,4)"},
        "change": {"type": "NUMERIC(15,4)"},
        "pct_change": {"type": "NUMERIC(10,4)"},
        "volume": {"type": "NUMERIC(20,4)"},
        "amount": {"type": "NUMERIC(20,4)"},
        # 技术因子
        "asi_bfq": {"type": "NUMERIC(20,4)"},
        "asit_bfq": {"type": "NUMERIC(20,4)"},
        "atr_bfq": {"type": "NUMERIC(15,4)"},
        "bbi_bfq": {"type": "NUMERIC(15,4)"},
        "bias1_bfq": {"type": "NUMERIC(15,4)"},
        "bias2_bfq": {"type": "NUMERIC(15,4)"},
        "bias3_bfq": {"type": "NUMERIC(15,4)"},
        "boll_lower_bfq": {"type": "NUMERIC(15,4)"},
        "boll_mid_bfq": {"type": "NUMERIC(15,4)"},
        "boll_upper_bfq": {"type": "NUMERIC(15,4)"},
        "brar_ar_bfq": {"type": "NUMERIC(15,4)"},
        "brar_br_bfq": {"type": "NUMERIC(15,4)"},
        "cci_bfq": {"type": "NUMERIC(15,4)"},
        "cr_bfq": {"type": "NUMERIC(15,4)"},
        "dfma_dif_bfq": {"type": "NUMERIC(15,4)"},
        "dfma_difma_bfq": {"type": "NUMERIC(15,4)"},
        "dmi_adx_bfq": {"type": "NUMERIC(15,4)"},
        "dmi_adxr_bfq": {"type": "NUMERIC(15,4)"},
        "dmi_mdi_bfq": {"type": "NUMERIC(15,4)"},
        "dmi_pdi_bfq": {"type": "NUMERIC(15,4)"},
        "downdays": {"type": "NUMERIC(10,0)"},
        "updays": {"type": "NUMERIC(10,0)"},
        "dpo_bfq": {"type": "NUMERIC(15,4)"},
        "madpo_bfq": {"type": "NUMERIC(15,4)"},
        "ema_bfq_5": {"type": "NUMERIC(15,4)"},
        "ema_bfq_10": {"type": "NUMERIC(15,4)"},
        "ema_bfq_20": {"type": "NUMERIC(15,4)"},
        "ema_bfq_30": {"type": "NUMERIC(15,4)"},
        "ema_bfq_60": {"type": "NUMERIC(15,4)"},
        "ema_bfq_90": {"type": "NUMERIC(15,4)"},
        "ema_bfq_250": {"type": "NUMERIC(15,4)"},
        "emv_bfq": {"type": "NUMERIC(15,4)"},
        "maemv_bfq": {"type": "NUMERIC(15,4)"},
        "expma_12_bfq": {"type": "NUMERIC(15,4)"},
        "expma_50_bfq": {"type": "NUMERIC(15,4)"},
        "kdj_bfq": {"type": "NUMERIC(15,4)"},
        "kdj_d_bfq": {"type": "NUMERIC(15,4)"},
        "kdj_k_bfq": {"type": "NUMERIC(15,4)"},
        "ktn_down_bfq": {"type": "NUMERIC(15,4)"},
        "ktn_mid_bfq": {"type": "NUMERIC(15,4)"},
        "ktn_upper_bfq": {"type": "NUMERIC(15,4)"},
        "lowdays": {"type": "NUMERIC(10,0)"},
        "topdays": {"type": "NUMERIC(10,0)"},
        "ma_bfq_5": {"type": "NUMERIC(15,4)"},
        "ma_bfq_10": {"type": "NUMERIC(15,4)"},
        "ma_bfq_20": {"type": "NUMERIC(15,4)"},
        "ma_bfq_30": {"type": "NUMERIC(15,4)"},
        "ma_bfq_60": {"type": "NUMERIC(15,4)"},
        "ma_bfq_90": {"type": "NUMERIC(15,4)"},
        "ma_bfq_250": {"type": "NUMERIC(15,4)"},
        "macd_bfq": {"type": "NUMERIC(15,4)"},
        "macd_dea_bfq": {"type": "NUMERIC(15,4)"},
        "macd_dif_bfq": {"type": "NUMERIC(15,4)"},
        "mass_bfq": {"type": "NUMERIC(15,4)"},
        "ma_mass_bfq": {"type": "NUMERIC(15,4)"},
        "mfi_bfq": {"type": "NUMERIC(15,4)"},
        "mtm_bfq": {"type": "NUMERIC(15,4)"},
        "mtmma_bfq": {"type": "NUMERIC(15,4)"},
        "obv_bfq": {"type": "NUMERIC(20,4)"},
        "psy_bfq": {"type": "NUMERIC(15,4)"},
        "psyma_bfq": {"type": "NUMERIC(15,4)"},
        "roc_bfq": {"type": "NUMERIC(15,4)"},
        "maroc_bfq": {"type": "NUMERIC(15,4)"},
        "rsi_bfq_6": {"type": "NUMERIC(15,4)"},
        "rsi_bfq_12": {"type": "NUMERIC(15,4)"},
        "rsi_bfq_24": {"type": "NUMERIC(15,4)"},
        "taq_down_bfq": {"type": "NUMERIC(15,4)"},
        "taq_mid_bfq": {"type": "NUMERIC(15,4)"},
        "taq_up_bfq": {"type": "NUMERIC(15,4)"},
        "trix_bfq": {"type": "NUMERIC(15,4)"},
        "trma_bfq": {"type": "NUMERIC(15,4)"},
        "vr_bfq": {"type": "NUMERIC(15,4)"},
        "wr_bfq": {"type": "NUMERIC(15,4)"},
        "wr1_bfq": {"type": "NUMERIC(15,4)"},
        "xsii_td1_bfq": {"type": "NUMERIC(15,4)"},
        "xsii_td2_bfq": {"type": "NUMERIC(15,4)"},
        "xsii_td3_bfq": {"type": "NUMERIC(15,4)"},
        "xsii_td4_bfq": {"type": "NUMERIC(15,4)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_cbond_factor_pro_ts_code", "columns": "ts_code"},
        {"name": "idx_cbond_factor_pro_trade_date", "columns": "trade_date"},
        {"name": "idx_cbond_factor_pro_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "转债代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
    ]

    # 8. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        - 全量模式：按 ts_code 分批，从 cbond_basic 表获取可转债代码
        - 增量模式：按 trade_date 分批
        """
        update_type = kwargs.get("update_type", "incremental")

        if update_type == UpdateTypes.FULL:
            return await self._get_full_batch_list(**kwargs)
        else:
            return await self._get_incremental_batch_list(**kwargs)

    async def _get_full_batch_list(self, **kwargs: Any) -> List[Dict]:
        """全量模式：从 cbond_basic 表获取可转债代码，按 ts_code 分批"""
        self.logger.info(f"任务 {self.name}: 全量模式，按可转债代码分批")

        # 从数据库获取可转债代码
        query = """
            SELECT ts_code FROM tushare.cbond_basic 
            ORDER BY ts_code
        """
        try:
            result = await self.db.fetch(query)
            ts_codes = [row["ts_code"] for row in result]
            self.logger.info(f"从 cbond_basic 获取到 {len(ts_codes)} 个可转债代码")
        except Exception as e:
            self.logger.error(f"获取可转债代码失败: {e}")
            return []

        if not ts_codes:
            self.logger.warning("未找到可转债代码，请先执行 tushare_cbond_basic 任务")
            return []

        # 每个可转债代码一个批次
        batch_list = [{"ts_code": ts_code} for ts_code in ts_codes]
        self.logger.info(f"生成了 {len(batch_list)} 个批次")
        return batch_list

    async def _get_incremental_batch_list(self, **kwargs: Any) -> List[Dict]:
        """增量模式：按 trade_date 分批"""
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        if not start_date:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date = (latest_db_date + timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = self.default_start_date

        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")

        if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(
            str(end_date), "%Y%m%d"
        ):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 增量模式，按交易日分批 {start_date} ~ {end_date}"
        )

        # 生成交易日批次
        trade_day_batches = await generate_trade_day_batches(
            start_date=start_date,
            end_date=end_date,
            batch_size=1,
            logger=self.logger,
        )

        # 转换为 trade_date 参数格式
        batch_list = []
        for batch in trade_day_batches:
            trade_date = batch.get("start_date") or batch.get("trade_date")
            if trade_date:
                batch_list.append({"trade_date": trade_date})

        return batch_list

    async def pre_execute(self):
        """预执行处理"""
        update_type = getattr(self, "update_type", "incremental")
        if update_type == UpdateTypes.FULL:
            self.logger.info(f"任务 {self.name}: 全量更新模式，清空表数据")
            await self.clear_table()


__all__ = ["TushareCbondFactorProTask"]
