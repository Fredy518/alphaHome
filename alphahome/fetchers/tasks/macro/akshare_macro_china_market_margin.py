#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""融资融券余额（沪/深）

AkShare:
- macro_china_market_margin_sh
- macro_china_market_margin_sz

数据为按日期一行的宽表。
"""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


class _BaseMarginTask(AkShareNoDateSingleBatchTask):
    smart_lookback_days = 60
    domain = "macro"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20100331"

    api_params: Dict[str, Any] = {}

    column_mapping = {
        "日期": "date",
        "融资买入额": "financing_buy",
        "融资余额": "financing_balance",
        "融券卖出量": "securities_sell_volume",
        "融券余量": "securities_balance_volume",
        "融券余额": "securities_balance",
        "融资融券余额": "margin_balance",
    }

    transformations = {
        "financing_buy": float,
        "financing_balance": float,
        "securities_sell_volume": float,
        "securities_balance_volume": float,
        "securities_balance": float,
        "margin_balance": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL"},
        "financing_buy": {"type": "NUMERIC(20,2)"},
        "financing_balance": {"type": "NUMERIC(20,2)"},
        "securities_sell_volume": {"type": "NUMERIC(20,0)"},
        "securities_balance_volume": {"type": "NUMERIC(20,0)"},
        "securities_balance": {"type": "NUMERIC(20,2)"},
        "margin_balance": {"type": "NUMERIC(20,2)"},
    }

    indexes = [
        {"name": "idx_margin_date", "columns": "date"},
        {"name": "idx_margin_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
    ]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if data is None or data.empty:
            return data
        data = super().process_data(data, **kwargs)
        data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
        data = data.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
        return data


@task_register()
class AkShareMacroChinaMarketMarginSZTask(_BaseMarginTask):
    name = "akshare_macro_china_market_margin_sz"
    description = "深圳融资融券余额（AkShare/Jin10）"
    table_name = "macro_china_market_margin_sz"
    api_name = "macro_china_market_margin_sz"


@task_register()
class AkShareMacroChinaMarketMarginSHTask(_BaseMarginTask):
    name = "akshare_macro_china_market_margin_sh"
    description = "上海融资融券余额（AkShare/Jin10）"
    table_name = "macro_china_market_margin_sh"
    api_name = "macro_china_market_margin_sh"
