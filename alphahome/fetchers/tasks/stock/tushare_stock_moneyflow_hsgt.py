#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 沪深港通资金流向数据任务

接口文档: https://tushare.pro/document/2?doc_id=47
数据说明:
- 获取沪股通、深股通、港股通每日资金流向数据
- 用于构建北向/南向资金流、跨市场风险偏好等因子
"""

from datetime import datetime
from typing import Dict, List

from ....common.task_system.task_decorator import task_register
from ...sources.tushare.batch_utils import generate_single_date_batches
from ...sources.tushare.tushare_task import TushareTask


@task_register()
class TushareStockMoneyflowHsgtTask(TushareTask):
    """获取沪深港通每日资金流向数据 (moneyflow_hsgt)。"""

    name = "tushare_stock_moneyflow_hsgt"
    description = "获取沪深港通每日资金流向数据"
    table_name = "stock_moneyflow_hsgt"
    primary_keys = ["trade_date"]
    date_column = "trade_date"
    default_start_date = "20141117"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 3

    default_concurrent_limit = 1
    default_page_size = 300

    api_name = "moneyflow_hsgt"
    fields = [
        "trade_date",
        "ggt_ss",
        "ggt_sz",
        "hgt",
        "sgt",
        "north_money",
        "south_money",
    ]

    column_mapping: Dict[str, str] = {}

    transformations = {
        "ggt_ss": float,
        "ggt_sz": float,
        "hgt": float,
        "sgt": float,
        "north_money": float,
        "south_money": float,
    }

    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ggt_ss": {"type": "NUMERIC(20,4)"},
        "ggt_sz": {"type": "NUMERIC(20,4)"},
        "hgt": {"type": "NUMERIC(20,4)"},
        "sgt": {"type": "NUMERIC(20,4)"},
        "north_money": {"type": "NUMERIC(20,4)"},
        "south_money": {"type": "NUMERIC(20,4)"},
    }

    indexes = [
        {"name": "idx_stock_moneyflow_hsgt_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_moneyflow_hsgt_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
    ]

    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """按交易日生成批次，覆盖 GUI 全量、手动增量、智能增量三种模式。"""
        start_date = kwargs.get("start_date") or self.default_start_date
        end_date = kwargs.get("end_date") or datetime.now().strftime("%Y%m%d")

        if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
            self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行")
            return []

        self.logger.info(f"按交易日分批获取沪深港通资金流: {start_date} ~ {end_date}")
        return await generate_single_date_batches(
            start_date=start_date,
            end_date=end_date,
            date_field="trade_date",
            logger=self.logger,
            exchange=kwargs.get("exchange", "SSE"),
        )


__all__ = ["TushareStockMoneyflowHsgtTask"]
