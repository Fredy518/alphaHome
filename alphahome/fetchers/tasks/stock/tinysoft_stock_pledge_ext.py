#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft stock pledge P0 tasks."""

from __future__ import annotations

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import tinysoft_symbol_to_ts_code_any
from .tinysoft_stock_p0_base import TinySoftMarketCodeInfoArrayTask, TinySoftStockSymbolInfoArrayTask

@task_register()
class TinySoftStockPledgeSummaryTask(TinySoftMarketCodeInfoArrayTask):
    name = "tinysoft_stock_pledge_summary"
    description = "获取股票质押回购交易汇总（Tinysoft）"
    table_name = "stock_pledge_summary"
    primary_keys = ["market_code", "trade_date"]
    date_column = "trade_date"
    infoarray_table_id = 144
    source_table_name = "股票.股票质押回购交易汇总"
    field_mapping = {
        "代码": "market_code",
        "截止日": "trade_date",
        "初始交易金额": "initial_trade_amount",
        "购回交易金额": "repurchase_trade_amount",
    }
    schema_def = {
        "market_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "initial_trade_amount": {"type": "NUMERIC(24,4)"},
        "repurchase_trade_amount": {"type": "NUMERIC(24,4)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [{"name": "idx_tinysoft_stock_pledge_summary_date", "columns": "trade_date"}]
    validations = [
        (lambda df: df["market_code"].notna(), "market_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
    ]
    date_fields = ("trade_date",)
    numeric_fields = ("initial_trade_amount", "repurchase_trade_amount")
    text_fields = ("market_code",)


@task_register()
class TinySoftStockPledgeDetailTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_pledge_detail"
    description = "获取股票质押回购交易明细（Tinysoft）"
    table_name = "stock_pledge_detail"
    primary_keys = ["security_code_raw", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20130624"
    infoarray_table_id = 145
    source_table_name = "股票.股票质押回购交易明细"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "代码": "security_code_raw",
        "截止日": "trade_date",
        "初始交易数量": "initial_trade_volume",
        "购回交易数量": "repurchase_trade_volume",
    }
    schema_def = {
        "security_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "initial_trade_volume": {"type": "NUMERIC(24,4)"},
        "repurchase_trade_volume": {"type": "NUMERIC(24,4)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_pledge_detail_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_pledge_detail_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["security_code_raw"].notna(), "security_code_raw 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
    ]
    date_fields = ("trade_date",)
    numeric_fields = ("initial_trade_volume", "repurchase_trade_volume")
    text_fields = ("security_code_raw", "tsl_code")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        if "security_code_raw" not in df.columns and "tsl_code" in df.columns:
            df["security_code_raw"] = df["tsl_code"]
        if "ts_code" not in df.columns and "security_code_raw" in df.columns:
            df["ts_code"] = df["security_code_raw"].map(tinysoft_symbol_to_ts_code_any)
        return df


@task_register()
class TinySoftStockPledgeBalanceTask(TinySoftStockPledgeDetailTask):
    name = "tinysoft_stock_pledge_balance"
    description = "获取股票质押回购余量（Tinysoft）"
    table_name = "stock_pledge_balance"
    primary_keys = ["security_code_raw", "trade_date", "data_source"]
    infoarray_table_id = 146
    source_table_name = "股票.股票质押回购余量"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "代码": "security_code_raw",
        "截止日": "trade_date",
        "余量": "balance_volume",
        "无限售股份余量": "unrestricted_balance_volume",
        "有限售股份余量": "restricted_balance_volume",
        "数据来源": "data_source",
    }
    schema_def = {
        "security_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "balance_volume": {"type": "NUMERIC(24,4)"},
        "unrestricted_balance_volume": {"type": "NUMERIC(24,4)"},
        "restricted_balance_volume": {"type": "NUMERIC(24,4)"},
        "data_source": {"type": "VARCHAR(120)", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_pledge_balance_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_pledge_balance_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["security_code_raw"].notna(), "security_code_raw 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["data_source"].notna(), "data_source 不能为空"),
    ]
    numeric_fields = ("balance_volume", "unrestricted_balance_volume", "restricted_balance_volume")
    text_fields = ("security_code_raw", "tsl_code", "data_source")


@task_register()
class TinySoftStockPledgeRateTask(TinySoftMarketCodeInfoArrayTask):
    name = "tinysoft_stock_pledge_rate"
    description = "获取股票质押回购平均质押率（Tinysoft）"
    table_name = "stock_pledge_rate"
    primary_keys = ["market_code", "trade_date"]
    date_column = "trade_date"
    infoarray_table_id = 147
    source_table_name = "股票.股票质押回购平均质押率"
    field_mapping = {
        "代码": "market_code",
        "截止日": "trade_date",
        "无限售条件股份质押率(%)": "unrestricted_pledge_rate_pct",
        "有限售条件股份质押率(%)": "restricted_pledge_rate_pct",
    }
    schema_def = {
        "market_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "unrestricted_pledge_rate_pct": {"type": "NUMERIC(20,8)"},
        "restricted_pledge_rate_pct": {"type": "NUMERIC(20,8)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [{"name": "idx_tinysoft_stock_pledge_rate_date", "columns": "trade_date"}]
    validations = TinySoftStockPledgeSummaryTask.validations
    date_fields = ("trade_date",)
    numeric_fields = ("unrestricted_pledge_rate_pct", "restricted_pledge_rate_pct")
    text_fields = ("market_code",)


__all__ = [
    "TinySoftStockPledgeSummaryTask",
    "TinySoftStockPledgeDetailTask",
    "TinySoftStockPledgeBalanceTask",
    "TinySoftStockPledgeRateTask",
]
