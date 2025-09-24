#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 个股资金流向数据任务

接口文档: https://tushare.pro/document/2?doc_id=170
数据说明:
- 获取个股资金流向数据，包括主力资金、超大单、大单、中单、小单等流向信息
- 数据从2010年开始提供
- 按交易日分批获取数据

权限要求: 需要至少120积分，单次最大6000条
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import (
    generate_single_date_batches
)
from ....common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class TushareStockMoneyFlowTask(TushareTask):
    """获取个股资金流向数据 (moneyflow)

    实现要求:
    - 按交易日分批获取个股资金流向数据
    - 支持全量和增量更新模式
    """

    # 1. 核心属性
    name = "tushare_stock_moneyflow"
    description = "获取个股资金流向数据"
    table_name = "stock_moneyflow"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 交易日期
    default_start_date = "20100101"  # 默认开始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 适中的并发限制
    default_page_size = 6000  # API限制单次最大6000条

    # 2. TushareTask 特有属性
    api_name = "moneyflow"
    # Tushare moneyflow 接口返回的字段
    fields = [
        "ts_code",         # TS股票代码
        "trade_date",      # 交易日期
        "buy_sm_vol",      # 小单买入量(手)
        "buy_sm_amount",   # 小单买入金额(万元)
        "sell_sm_vol",     # 小单卖出量(手)
        "sell_sm_amount",  # 小单卖出金额(万元)
        "buy_md_vol",      # 中单买入量(手)
        "buy_md_amount",   # 中单买入金额(万元)
        "sell_md_vol",     # 中单卖出量(手)
        "sell_md_amount",  # 中单卖出金额(万元)
        "buy_lg_vol",      # 大单买入量(手)
        "buy_lg_amount",   # 大单买入金额(万元)
        "sell_lg_vol",     # 大单卖出量(手)
        "sell_lg_amount",  # 大单卖出金额(万元)
        "buy_elg_vol",     # 特大单买入量(手)
        "buy_elg_amount",  # 特大单买入金额(万元)
        "sell_elg_vol",    # 特大单卖出量(手)
        "sell_elg_amount", # 特大单卖出金额(万元)
        "net_mf_vol",      # 净流入量(手)
        "net_mf_amount",   # 净流入额(万元)
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "buy_sm_vol": float,
        "buy_sm_amount": float,
        "sell_sm_vol": float,
        "sell_sm_amount": float,
        "buy_md_vol": float,
        "buy_md_amount": float,
        "sell_md_vol": float,
        "sell_md_amount": float,
        "buy_lg_vol": float,
        "buy_lg_amount": float,
        "sell_lg_vol": float,
        "sell_lg_amount": float,
        "buy_elg_vol": float,
        "buy_elg_amount": float,
        "sell_elg_vol": float,
        "sell_elg_amount": float,
        "net_mf_vol": float,
        "net_mf_amount": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "buy_sm_vol": {"type": "NUMERIC(15,2)"},
        "buy_sm_amount": {"type": "NUMERIC(15,2)"},
        "sell_sm_vol": {"type": "NUMERIC(15,2)"},
        "sell_sm_amount": {"type": "NUMERIC(15,2)"},
        "buy_md_vol": {"type": "NUMERIC(15,2)"},
        "buy_md_amount": {"type": "NUMERIC(15,2)"},
        "sell_md_vol": {"type": "NUMERIC(15,2)"},
        "sell_md_amount": {"type": "NUMERIC(15,2)"},
        "buy_lg_vol": {"type": "NUMERIC(15,2)"},
        "buy_lg_amount": {"type": "NUMERIC(15,2)"},
        "sell_lg_vol": {"type": "NUMERIC(15,2)"},
        "sell_lg_amount": {"type": "NUMERIC(15,2)"},
        "buy_elg_vol": {"type": "NUMERIC(15,2)"},
        "buy_elg_amount": {"type": "NUMERIC(15,2)"},
        "sell_elg_vol": {"type": "NUMERIC(15,2)"},
        "sell_elg_amount": {"type": "NUMERIC(15,2)"},
        "net_mf_vol": {"type": "NUMERIC(15,2)"},
        "net_mf_amount": {"type": "NUMERIC(15,2)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_moneyflow_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_moneyflow_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_moneyflow_net_mf_amount", "columns": "net_mf_amount"},  # 净流入额索引，便于查询
        {"name": "idx_stock_moneyflow_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['buy_sm_vol'] >= 0, "小单买入量必须非负"),
        (lambda df: df['sell_sm_vol'] >= 0, "小单卖出量必须非负"),
        (lambda df: df['buy_sm_amount'] >= 0, "小单买入金额必须非负"),
        (lambda df: df['sell_sm_amount'] >= 0, "小单卖出金额必须非负"),
        # 可以添加更多验证规则，如大单、中单、特大单的验证
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        - 按交易日分批获取个股资金流向数据
        - 每个交易日生成一个批次

        Args:
            **kwargs: 包含start_date, end_date等参数

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 确定总体起止日期
        if not start_date:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                next_day_obj = latest_db_date + timedelta(days=1)
                start_date = next_day_obj.strftime("%Y%m%d")
            else:
                start_date = self.default_start_date
            self.logger.info(
                f"任务 {self.name}: 未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date}"
            )

        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 按交易日分批获取资金流向数据，范围: {start_date} 到 {end_date}"
        )

        try:
            return await generate_single_date_batches(
                start_date=start_date,
                end_date=end_date,
                date_field="trade_date",
                logger=self.logger,
                exchange=kwargs.get("exchange", "SSE"),
                additional_params={"fields": ",".join(self.fields or [])}
            )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: BatchPlanner 生成批次时出错: {e}", exc_info=True)
            return []

    async def pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行前的准备工作"""
        await super().pre_execute(stop_event=stop_event, **kwargs)
        # 可以在这里添加特定于此任务的预处理逻辑

    async def post_execute(
        self,
        result: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None,
        **kwargs,
    ):
        """任务执行后的清理工作"""
        await super().post_execute(result, stop_event=stop_event, **kwargs)
        # 可以在这里添加特定于此任务的后处理逻辑


# 导出任务类
__all__ = ["TushareStockMoneyFlowTask"]
