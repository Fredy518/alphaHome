#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 沪深港股通持股明细数据任务

接口文档: https://tushare.pro/document/2?doc_id=188
数据说明:
- 获取沪深港股通持股明细，数据来源港交所
- 可用于北向持股比例、持股数量变化和外资拥挤度研究
"""

from datetime import datetime
from typing import Dict, List

from ....common.task_system.task_decorator import task_register
from ...sources.tushare.batch_utils import generate_single_date_batches
from ...sources.tushare.tushare_task import TushareTask


@task_register()
class TushareStockHkHoldTask(TushareTask):
    """获取沪深港股通持股明细数据 (hk_hold)。"""

    name = "tushare_stock_hk_hold"
    description = "获取沪深港股通持股明细数据"
    table_name = "stock_hk_hold"
    primary_keys = ["trade_date", "exchange", "ts_code"]
    date_column = "trade_date"
    default_start_date = "20141117"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 3

    default_concurrent_limit = 1
    default_page_size = 5000

    api_name = "hk_hold"
    fields = [
        "code",
        "trade_date",
        "ts_code",
        "name",
        "vol",
        "ratio",
        "exchange",
    ]

    column_mapping: Dict[str, str] = {}

    transformations = {
        "vol": float,
        "ratio": float,
    }

    schema_def = {
        "code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "vol": {"type": "NUMERIC(20,0)"},
        "ratio": {"type": "NUMERIC(10,4)"},
        "exchange": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
    }

    indexes = [
        {"name": "idx_stock_hk_hold_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_hk_hold_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_hk_hold_code", "columns": "code"},
        {"name": "idx_stock_hk_hold_exchange", "columns": "exchange"},
        {"name": "idx_stock_hk_hold_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["exchange"].notna(), "交易所不能为空"),
        (lambda df: (df["vol"] >= 0) | df["vol"].isna(), "持股数量必须非负"),
        (lambda df: (df["ratio"] >= 0) | df["ratio"].isna(), "持股比例必须非负"),
    ]

    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """按交易日生成批次，覆盖 GUI 全量、手动增量、智能增量三种模式。"""
        start_date = kwargs.get("start_date") or self.default_start_date
        end_date = kwargs.get("end_date") or datetime.now().strftime("%Y%m%d")

        if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
            self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行")
            return []

        additional_params = {}
        if kwargs.get("ts_code"):
            additional_params["ts_code"] = kwargs["ts_code"]
        if kwargs.get("hold_exchange"):
            additional_params["exchange"] = kwargs["hold_exchange"]

        self.logger.info(f"按交易日分批获取沪深港股通持股明细: {start_date} ~ {end_date}")
        return await generate_single_date_batches(
            start_date=start_date,
            end_date=end_date,
            date_field="trade_date",
            logger=self.logger,
            exchange=kwargs.get("exchange", "SSE"),
            additional_params=additional_params,
        )


__all__ = ["TushareStockHkHoldTask"]
