#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft margin financing and securities lending backup tasks."""

from __future__ import annotations

from typing import Any, List

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import get_row_value, ts_code_to_tinysoft_symbol_any
from .tinysoft_stock_p0_base import (
    TinySoftMarketCodeInfoArrayTask,
    TinySoftStockSymbolInfoArrayTask,
)


MARGIN_NUMERIC_FIELDS = (
    "rzmre",
    "rzche",
    "rzye",
    "rqmcl",
    "rqchl",
    "rqyl",
    "rqye",
    "rzrqye",
)

MARGIN_SOURCE_FIELD_MAPPING = {
    "截止日": "trade_date",
    "融资买入额": "rzmre",
    "融资偿还额": "rzche",
    "融资余额": "rzye",
    "融券卖出量": "rqmcl",
    "融券偿还量": "rqchl",
    "融券余量": "rqyl",
    "融券余额": "rqye",
    "融资融券余额": "rzrqye",
}

MARGIN_VALUE_SCHEMA = {
    "rzye": {"type": "NUMERIC(24,4)"},
    "rzmre": {"type": "NUMERIC(24,4)"},
    "rzche": {"type": "NUMERIC(24,4)"},
    "rqye": {"type": "NUMERIC(24,4)"},
    "rqmcl": {"type": "NUMERIC(24,4)"},
    "rqchl": {"type": "NUMERIC(24,4)"},
    "rzrqye": {"type": "NUMERIC(24,4)"},
    "rqyl": {"type": "NUMERIC(24,4)"},
}


@task_register()
class TinySoftStockMarginTask(TinySoftMarketCodeInfoArrayTask):
    """Collect table 165 as a source-level backup for Tushare ``margin``."""

    name = "tinysoft_stock_margin"
    description = "获取融资融券每日交易汇总（Tinysoft 表165，Tushare备用）"
    table_name = "stock_margin"
    primary_keys = ["trade_date", "exchange_id"]
    date_column = "trade_date"
    default_start_date = "20100331"
    smart_lookback_days = 7
    infoarray_table_id = 165
    source_table_name = "股票.融资融券汇总"
    default_codes = ["RZRQ000001", "RZRQ000002", "RZRQ000003"]
    default_code_batch_size = 3
    default_smart_code_batch_size = 3
    default_include_raw_json = False
    exchange_id_by_market_code = {
        "RZRQ000001": "SSE",
        "RZRQ000002": "SZSE",
        "RZRQ000003": "BSE",
    }
    field_mapping = {
        "StockID": "market_code",
        "stockid": "market_code",
        **MARGIN_SOURCE_FIELD_MAPPING,
    }
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "exchange_id": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        **MARGIN_VALUE_SCHEMA,
        "market_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_margin_trade_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_margin_exchange_id", "columns": "exchange_id"},
    ]
    validations = [
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["exchange_id"].isin(["SSE", "SZSE", "BSE"]), "exchange_id 必须为 SSE/SZSE/BSE"),
        (lambda df: df["rzye"].isna() | (df["rzye"] >= 0), "融资余额必须非负或为空"),
        (lambda df: df["rqye"].isna() | (df["rqye"] >= 0), "融券余额必须非负或为空"),
        (lambda df: df["rzrqye"].isna() | (df["rzrqye"] >= 0), "融资融券余额必须非负或为空"),
    ]
    date_fields = ("trade_date",)
    numeric_fields = MARGIN_NUMERIC_FIELDS
    text_fields = ("exchange_id", "market_code")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        df["exchange_id"] = df["market_code"].map(
            lambda value: self.exchange_id_by_market_code.get(str(value or "").upper())
        )
        return df


@task_register()
class TinySoftStockMarginDetailTask(TinySoftStockSymbolInfoArrayTask):
    """Collect table 126 as a source-level backup for Tushare ``margin_detail``."""

    name = "tinysoft_stock_margindetail"
    description = "获取融资融券每日交易明细（Tinysoft 表126，Tushare备用）"
    table_name = "stock_margindetail"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20100331"
    smart_lookback_days = 7
    infoarray_table_id = 126
    source_table_name = "股票.融资融券明细"
    include_inactive_symbols = True
    symbol_source_tables = (
        "tushare.stock_basic",
        "tushare.fund_basic",
        "tushare.fund_etf_basic",
        "tushare.stock_margindetail",
        "tinysoft.stock_margindetail",
    )
    default_code_batch_size = 50
    default_smart_code_batch_size = 200
    default_include_raw_json = False
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        **MARGIN_SOURCE_FIELD_MAPPING,
    }
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        **MARGIN_VALUE_SCHEMA,
        "tsl_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_margindetail_trade_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_margindetail_ts_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["rzye"].isna() | (df["rzye"] >= 0), "融资余额必须非负或为空"),
        (lambda df: df["rqye"].isna() | (df["rqye"] >= 0), "融券余额必须非负或为空"),
        (lambda df: df["rzrqye"].isna() | (df["rzrqye"] >= 0), "融资融券余额必须非负或为空"),
        (lambda df: df["rqyl"].isna() | (df["rqyl"] >= 0), "融券余量必须非负或为空"),
    ]
    date_fields = ("trade_date",)
    numeric_fields = MARGIN_NUMERIC_FIELDS
    text_fields = ("ts_code", "tsl_code")

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        """Load the full exchange-security universe, including funds and delisted codes."""
        if not self.db:
            return []

        codes: List[str] = []
        for table in self.symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue
                rows = await self.db.fetch(
                    f"""
                    SELECT DISTINCT ts_code
                    FROM "{schema}"."{table_name}"
                    WHERE ts_code ~ '^[0-9]{{6}}\\.(SH|SZ|BJ)$'
                    ORDER BY ts_code
                    """
                )
                for row in rows or []:
                    symbol = ts_code_to_tinysoft_symbol_any(get_row_value(row, "ts_code"))
                    if symbol:
                        codes.append(symbol)
            except Exception as exc:
                if not silent:
                    self.logger.warning("从 %s 加载融资融券证券代码失败: %s", table, exc)
        return list(dict.fromkeys(codes))


__all__ = [
    "TinySoftStockMarginTask",
    "TinySoftStockMarginDetailTask",
]
