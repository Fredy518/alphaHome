#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft stock event/disclosure P0 tasks."""

from __future__ import annotations

from ....common.task_system.task_decorator import task_register
from .tinysoft_stock_p0_base import TinySoftStockSymbolInfoArrayTask

@task_register()
class TinySoftStockPublicTradeInfoTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_public_trade_info"
    description = "获取股票交易公开信息（Tinysoft）"
    table_name = "stock_public_trade_info"
    primary_keys = ["ts_code", "trade_date", "abnormal_type", "broker_short_name", "trade_action"]
    date_column = "trade_date"
    default_start_date = "20050101"
    infoarray_table_id = 129
    source_table_name = "股票.交易公开信息"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "截止日": "trade_date",
        "交易动作": "trade_action",
        "营业部全称": "broker_full_name",
        "买入金额": "buy_amount",
        "卖出金额": "sell_amount",
        "异动类型": "abnormal_type",
        "异动详情": "abnormal_detail",
        "机构简称": "institution_short_name",
        "营业部简称": "broker_short_name",
        "异动开始日": "abnormal_start_date",
        "异动截止日": "abnormal_end_date",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "trade_action": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "broker_full_name": {"type": "VARCHAR(200)"},
        "broker_short_name": {"type": "VARCHAR(120)", "constraints": "NOT NULL"},
        "institution_short_name": {"type": "VARCHAR(120)"},
        "buy_amount": {"type": "NUMERIC(24,4)"},
        "sell_amount": {"type": "NUMERIC(24,4)"},
        "abnormal_type": {"type": "INTEGER", "constraints": "NOT NULL"},
        "abnormal_detail": {"type": "TEXT"},
        "abnormal_start_date": {"type": "DATE"},
        "abnormal_end_date": {"type": "DATE"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_public_trade_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_public_trade_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["trade_action"].notna(), "trade_action 不能为空"),
        (lambda df: df["broker_short_name"].notna(), "broker_short_name 不能为空"),
        (lambda df: df["abnormal_type"].notna(), "abnormal_type 不能为空"),
    ]
    date_fields = ("trade_date", "abnormal_start_date", "abnormal_end_date")
    numeric_fields = ("buy_amount", "sell_amount", "abnormal_type")
    integer_fields = ("abnormal_type",)
    text_fields = ("tsl_code", "trade_action", "broker_full_name", "broker_short_name", "institution_short_name", "abnormal_detail")


@task_register()
class TinySoftStockUnlockScheduleTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_unlock_schedule"
    description = "获取限售解禁（Tinysoft）"
    table_name = "stock_unlock_schedule"
    primary_keys = ["ts_code", "unlock_date", "lock_type"]
    date_column = "unlock_date"
    where_date_field = "解禁日"
    default_start_date = "20050101"
    smart_lookback_days = 730
    infoarray_table_id = 154
    source_table_name = "股票.限售解禁"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "解禁日": "unlock_date",
        "解禁数量": "unlock_volume",
        "实际可流通数量": "actual_float_volume",
        "限售类型": "lock_type",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "unlock_date": {"type": "DATE", "constraints": "NOT NULL"},
        "unlock_volume": {"type": "NUMERIC(24,4)"},
        "actual_float_volume": {"type": "NUMERIC(24,4)"},
        "lock_type": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_unlock_date", "columns": "unlock_date"},
        {"name": "idx_tinysoft_stock_unlock_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["unlock_date"].notna(), "unlock_date 不能为空"),
        (lambda df: df["lock_type"].notna(), "lock_type 不能为空"),
    ]
    date_fields = ("unlock_date",)
    numeric_fields = ("unlock_volume", "actual_float_volume")
    text_fields = ("tsl_code", "lock_type")


@task_register()
class TinySoftStockHolderChangeExtTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_holder_change_ext"
    description = "获取股东增减持（Tinysoft）"
    table_name = "stock_holder_change_ext"
    primary_keys = ["ts_code", "ann_date", "holder_name", "change_direction", "change_reason"]
    date_column = "ann_date"
    where_date_field = "公布日"
    default_start_date = "20050101"
    infoarray_table_id = 157
    source_table_name = "股票.股东增减持"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "变动开始日": "change_start_date",
        "变动截止日": "change_end_date",
        "公布日": "ann_date",
        "股东名称": "holder_name",
        "变动原因": "change_reason",
        "变动方向": "change_direction",
        "变动数量": "change_volume",
        "变动后持股数": "holding_after",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "change_start_date": {"type": "DATE"},
        "change_end_date": {"type": "DATE"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "holder_name": {"type": "VARCHAR(512)", "constraints": "NOT NULL"},
        "change_reason": {"type": "VARCHAR(120)", "constraints": "NOT NULL"},
        "change_direction": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "change_volume": {"type": "NUMERIC(24,4)"},
        "holding_after": {"type": "NUMERIC(24,4)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_holder_change_ann", "columns": "ann_date"},
        {"name": "idx_tinysoft_stock_holder_change_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["ann_date"].notna(), "ann_date 不能为空"),
        (lambda df: df["holder_name"].notna(), "holder_name 不能为空"),
        (lambda df: df["change_direction"].notna(), "change_direction 不能为空"),
        (lambda df: df["change_reason"].notna(), "change_reason 不能为空"),
    ]
    date_fields = ("change_start_date", "change_end_date", "ann_date")
    numeric_fields = ("change_volume", "holding_after")
    text_fields = ("tsl_code", "holder_name", "change_reason", "change_direction")


@task_register()
class TinySoftStockRepurchaseExtTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_repurchase_ext"
    description = "获取股份回购（Tinysoft）"
    table_name = "stock_repurchase_ext"
    primary_keys = ["ts_code", "ann_date", "report_date", "repurchase_type"]
    date_column = "ann_date"
    where_date_field = "公布日"
    default_start_date = "20050101"
    infoarray_table_id = 160
    source_table_name = "股票.股份回购"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "代码": "tsl_code",
        "首次信息发布日": "first_info_date",
        "截止日": "report_date",
        "公布日": "ann_date",
        "回购类型": "repurchase_type",
        "股票种类": "stock_type",
        "累计回购数量": "cum_repurchase_volume",
        "累计回购金额": "cum_repurchase_amount",
        "回购均价": "avg_price",
        "回购最高价": "high_price",
        "回购最低价": "low_price",
        "回购方案是否结束": "is_finished",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "first_info_date": {"type": "DATE"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "repurchase_type": {"type": "VARCHAR(80)", "constraints": "NOT NULL"},
        "stock_type": {"type": "VARCHAR(50)"},
        "cum_repurchase_volume": {"type": "NUMERIC(24,4)"},
        "cum_repurchase_amount": {"type": "NUMERIC(24,4)"},
        "avg_price": {"type": "NUMERIC(20,8)"},
        "high_price": {"type": "NUMERIC(20,8)"},
        "low_price": {"type": "NUMERIC(20,8)"},
        "is_finished": {"type": "VARCHAR(20)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_repurchase_ann", "columns": "ann_date"},
        {"name": "idx_tinysoft_stock_repurchase_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["ann_date"].notna(), "ann_date 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["repurchase_type"].notna(), "repurchase_type 不能为空"),
    ]
    date_fields = ("first_info_date", "report_date", "ann_date")
    numeric_fields = ("cum_repurchase_volume", "cum_repurchase_amount", "avg_price", "high_price", "low_price")
    text_fields = ("tsl_code", "repurchase_type", "stock_type", "is_finished", "remark")


__all__ = [
    "TinySoftStockPublicTradeInfoTask",
    "TinySoftStockUnlockScheduleTask",
    "TinySoftStockHolderChangeExtTask",
    "TinySoftStockRepurchaseExtTask",
]
