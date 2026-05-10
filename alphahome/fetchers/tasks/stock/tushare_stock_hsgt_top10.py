#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 沪深股通十大成交股数据任务

接口文档: https://tushare.pro/document/2?doc_id=48
数据说明:
- 获取沪股通、深股通每日前十大成交详细数据
- 每天 18~20 点之间完成当日更新
"""

from datetime import datetime
from typing import Dict, List

from ....common.task_system.task_decorator import task_register
from ...sources.tushare.batch_utils import generate_trade_day_batches
from ...sources.tushare.tushare_task import TushareTask


@task_register()
class TushareStockHsgtTop10Task(TushareTask):
    """获取沪深股通十大成交股数据 (hsgt_top10)。"""

    name = "tushare_stock_hsgt_top10"
    description = "获取沪深股通十大成交股数据"
    table_name = "stock_hsgt_top10"
    primary_keys = ["trade_date", "market_type", "rank"]
    date_column = "trade_date"
    default_start_date = "20141117"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 3

    default_concurrent_limit = 1
    default_page_size = 5000
    batch_trade_days = 200

    api_name = "hsgt_top10"
    fields = [
        "trade_date",
        "ts_code",
        "name",
        "close",
        "change",
        "rank",
        "market_type",
        "amount",
        "net_amount",
        "buy",
        "sell",
    ]

    column_mapping: Dict[str, str] = {}

    transformations = {
        "close": float,
        "change": float,
        "rank": int,
        "market_type": int,
        "amount": float,
        "net_amount": float,
        "buy": float,
        "sell": float,
    }

    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(50)"},
        "close": {"type": "NUMERIC(15,4)"},
        "change": {"type": "NUMERIC(10,4)"},
        "rank": {"type": "SMALLINT", "constraints": "NOT NULL"},
        "market_type": {"type": "SMALLINT", "constraints": "NOT NULL"},
        "amount": {"type": "NUMERIC(20,2)"},
        "net_amount": {"type": "NUMERIC(20,2)"},
        "buy": {"type": "NUMERIC(20,2)"},
        "sell": {"type": "NUMERIC(20,2)"},
    }

    indexes = [
        {"name": "idx_stock_hsgt_top10_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_hsgt_top10_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_hsgt_top10_market_type", "columns": "market_type"},
        {"name": "idx_stock_hsgt_top10_net_amount", "columns": "net_amount"},
        {"name": "idx_stock_hsgt_top10_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["market_type"].notna(), "市场类型不能为空"),
        (lambda df: df["rank"].notna(), "排名不能为空"),
        (lambda df: (df["amount"] >= 0) | df["amount"].isna(), "成交额必须非负"),
        (lambda df: (df["buy"] >= 0) | df["buy"].isna(), "买入额必须非负"),
        (lambda df: (df["sell"] >= 0) | df["sell"].isna(), "卖出额必须非负"),
    ]

    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """按交易日范围生成批次，覆盖 GUI 全量、手动增量、智能增量三种模式。"""
        start_date = kwargs.get("start_date") or self.default_start_date
        end_date = kwargs.get("end_date") or datetime.now().strftime("%Y%m%d")

        if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
            self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行")
            return []

        additional_params = {}
        if kwargs.get("market_type"):
            additional_params["market_type"] = kwargs["market_type"]
        if kwargs.get("ts_code"):
            additional_params["ts_code"] = kwargs["ts_code"]

        self.logger.info(
            f"按交易日范围分批获取沪深股通十大成交股: {start_date} ~ {end_date}, "
            f"每批 {self.batch_trade_days} 个交易日"
        )
        return await generate_trade_day_batches(
            start_date=start_date,
            end_date=end_date,
            batch_size=self.batch_trade_days,
            logger=self.logger,
            exchange=kwargs.get("exchange", "SSE"),
            additional_params=additional_params,
        )


__all__ = ["TushareStockHsgtTop10Task"]
