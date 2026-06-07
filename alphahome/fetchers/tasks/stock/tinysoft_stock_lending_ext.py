#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft stock lending P0 tasks."""

from __future__ import annotations

from ....common.task_system.task_decorator import task_register
from .tinysoft_stock_p0_base import TinySoftStockSymbolInfoArrayTask

@task_register()
class TinySoftStockLendingSummaryTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_lending_summary"
    description = "获取转融通证券出借交易（Tinysoft）"
    table_name = "stock_lending_summary"
    primary_keys = ["ts_code", "trade_date", "tenor_days", "declare_type", "data_type"]
    date_column = "trade_date"
    default_start_date = "20130228"
    infoarray_table_id = 151
    source_table_name = "股票.转融通证券出借交易"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "证券代码": "tsl_code",
        "截止日": "trade_date",
        "期限": "tenor_days",
        "费率(%)": "rate_pct",
        "费率": "rate_pct",
        "申报类型": "declare_type",
        "成交量": "deal_volume",
        "数据类型": "data_type",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "tenor_days": {"type": "INTEGER", "constraints": "NOT NULL"},
        "rate_pct": {"type": "NUMERIC(18,8)"},
        "declare_type": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "deal_volume": {"type": "NUMERIC(24,4)"},
        "data_type": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_lending_summary_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_lending_summary_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
    ]
    date_fields = ("trade_date",)
    numeric_fields = ("tenor_days", "rate_pct", "deal_volume")
    integer_fields = ("tenor_days",)
    text_fields = ("declare_type", "data_type")


@task_register()
class TinySoftStockLendingTradeTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_lending_trade"
    description = "获取转融券交易明细（Tinysoft）"
    table_name = "stock_lending_trade"
    primary_keys = ["ts_code", "trade_date", "tenor_days"]
    date_column = "trade_date"
    default_start_date = "20130228"
    infoarray_table_id = 152
    source_table_name = "股票.转融券交易明细"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "证券代码": "tsl_code",
        "截止日": "trade_date",
        "期限": "tenor_days",
        "费率(%)": "rate_pct",
        "费率": "rate_pct",
        "融出数量": "lend_volume",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "tenor_days": {"type": "INTEGER", "constraints": "NOT NULL"},
        "rate_pct": {"type": "NUMERIC(18,8)"},
        "lend_volume": {"type": "NUMERIC(24,4)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_lending_trade_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_lending_trade_code", "columns": "ts_code"},
    ]
    validations = TinySoftStockLendingSummaryTask.validations
    date_fields = ("trade_date",)
    numeric_fields = ("tenor_days", "rate_pct", "lend_volume")
    integer_fields = ("tenor_days",)


@task_register()
class TinySoftStockLendingBalanceTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_lending_balance"
    description = "获取转融券余量（Tinysoft）"
    table_name = "stock_lending_balance"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20130228"
    infoarray_table_id = 153
    source_table_name = "股票.转融券余量"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "证券代码": "tsl_code",
        "截止日": "trade_date",
        "余量": "balance_volume",
        "余额": "balance_amount",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "balance_volume": {"type": "NUMERIC(24,4)"},
        "balance_amount": {"type": "NUMERIC(24,4)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_lending_balance_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_lending_balance_code", "columns": "ts_code"},
    ]
    validations = TinySoftStockLendingSummaryTask.validations
    date_fields = ("trade_date",)
    numeric_fields = ("balance_volume", "balance_amount")


__all__ = [
    "TinySoftStockLendingSummaryTask",
    "TinySoftStockLendingTradeTask",
    "TinySoftStockLendingBalanceTask",
]
