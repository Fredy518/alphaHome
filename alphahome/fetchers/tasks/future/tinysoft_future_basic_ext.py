#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft futures basic extension tables."""

from __future__ import annotations

import re
from typing import Any, List

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import (
    TinySoftP0InfoArrayTask,
    clean_text,
    get_row_value,
)


FUTURE_EXCHANGE_SUFFIXES = {
    "中国金融期货交易所": "CFX",
    "上海期货交易所": "SHF",
    "上海国际能源交易中心": "INE",
    "大连商品交易所": "DCE",
    "郑州商品交易所": "ZCE",
    "广州期货交易所": "GFE",
}


def future_ts_code_to_tinysoft_symbol(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    raw = text.upper()
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw or None


def future_contract_code_to_ts_code(code: Any, exchange_name: Any = None) -> str | None:
    raw = future_ts_code_to_tinysoft_symbol(code)
    if not raw:
        return None
    suffix = FUTURE_EXCHANGE_SUFFIXES.get(str(exchange_name or "").strip())
    if not suffix:
        return None
    return f"{raw}.{suffix}"


class TinySoftFutureCodeInfoArrayTask(TinySoftP0InfoArrayTask):
    domain = "future"
    default_start_date = "19950417"
    default_code_batch_size = 200
    default_smart_code_batch_size = 1000
    code_config_keys = ("future_codes", "ts_codes", "ts_code", "codes")
    default_symbol_source_tables = ["tushare.future_basic", "rawdata.future_basic"]

    def _get_codes_from_mapping(self, params: dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        return list(dict.fromkeys(code for code in (future_ts_code_to_tinysoft_symbol(x) for x in raw_codes) if code))

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []
        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue
                rows = await self.db.fetch(f'SELECT ts_code FROM "{schema}"."{table_name}" ORDER BY ts_code')
                codes = [future_ts_code_to_tinysoft_symbol(get_row_value(row, "ts_code")) for row in rows or []]
                codes = [code for code in codes if code]
                if codes:
                    return list(dict.fromkeys(codes))
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载期货合约代码失败: %s", table, e)
        return []


@task_register()
class TinySoftFutureBasicExtTask(TinySoftFutureCodeInfoArrayTask):
    name = "tinysoft_future_basic_ext"
    description = "获取期货合约基本信息扩展字段（Tinysoft）"
    table_name = "future_basic_ext"
    primary_keys = ["contract_code_raw", "change_date"]
    date_column = "change_date"
    infoarray_table_id = 703
    source_table_name = "期货.期货基本信息"
    where_date_field = "变动日"
    field_mapping = {
        "StockID": "source_code",
        "stockid": "source_code",
        "合约代码": "contract_code_raw",
        "变动日": "change_date",
        "交易代码": "product_code",
        "交割年份": "delivery_year",
        "交割月份": "delivery_month",
        "交易品种": "product_name",
        "合约乘数": "contract_multiplier",
        "合约乘数单位": "contract_multiplier_unit",
        "报价单位": "quote_unit",
        "最小变动价位": "min_price_change",
        "每日价格最大波动下限(%)": "daily_price_limit_down_pct",
        "每日价格最大波动上限(%)": "daily_price_limit_up_pct",
        "最后交易日参照标准": "last_trade_ref_standard",
        "最后交易日相对参照标准偏移月份": "last_trade_ref_offset_months",
        "最后交易日类别": "last_trade_day_type",
        "最后交易日相对最后交易日所在月份偏移天数": "last_trade_offset_days",
        "最后交易日是否假日顺延": "last_trade_holiday_adjust",
        "最后交易日": "last_trade_date",
        "最后交割日参照标准": "last_delivery_ref_standard",
        "最后交割日相对参照标准偏移月份": "last_delivery_ref_offset_months",
        "最后交割日类别": "last_delivery_day_type",
        "最后交割日相对最后交割日所在月份偏移天数": "last_delivery_offset_days",
        "最后交割日是否假日顺延": "last_delivery_holiday_adjust",
        "最后交割日": "last_delivery_date",
        "最低交易保证金(%)": "min_trade_margin_pct",
        "交割方式": "delivery_method",
        "上市地": "exchange_name",
        "期货类别": "future_category",
        "商品期货类别": "commodity_category",
        "基准代码": "benchmark_code",
    }
    schema_def = {
        "contract_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(30)"},
        "source_code": {"type": "VARCHAR(30)"},
        "change_date": {"type": "DATE", "constraints": "NOT NULL"},
        "product_code": {"type": "VARCHAR(30)"},
        "delivery_year": {"type": "INTEGER"},
        "delivery_month": {"type": "INTEGER"},
        "product_name": {"type": "VARCHAR(120)"},
        "contract_multiplier": {"type": "NUMERIC(20,8)"},
        "contract_multiplier_unit": {"type": "VARCHAR(50)"},
        "quote_unit": {"type": "VARCHAR(80)"},
        "min_price_change": {"type": "NUMERIC(20,8)"},
        "daily_price_limit_down_pct": {"type": "NUMERIC(20,8)"},
        "daily_price_limit_up_pct": {"type": "NUMERIC(20,8)"},
        "last_trade_ref_standard": {"type": "VARCHAR(120)"},
        "last_trade_ref_offset_months": {"type": "INTEGER"},
        "last_trade_day_type": {"type": "VARCHAR(80)"},
        "last_trade_offset_days": {"type": "INTEGER"},
        "last_trade_holiday_adjust": {"type": "VARCHAR(30)"},
        "last_trade_date": {"type": "DATE"},
        "last_delivery_ref_standard": {"type": "VARCHAR(120)"},
        "last_delivery_ref_offset_months": {"type": "INTEGER"},
        "last_delivery_day_type": {"type": "VARCHAR(80)"},
        "last_delivery_offset_days": {"type": "INTEGER"},
        "last_delivery_holiday_adjust": {"type": "VARCHAR(30)"},
        "last_delivery_date": {"type": "DATE"},
        "min_trade_margin_pct": {"type": "NUMERIC(20,8)"},
        "delivery_method": {"type": "VARCHAR(80)"},
        "exchange_name": {"type": "VARCHAR(120)"},
        "future_category": {"type": "VARCHAR(80)"},
        "commodity_category": {"type": "VARCHAR(80)"},
        "benchmark_code": {"type": "VARCHAR(30)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_future_basic_ext_ts", "columns": "ts_code"},
        {"name": "idx_tinysoft_future_basic_ext_product", "columns": "product_code"},
        {"name": "idx_tinysoft_future_basic_ext_change", "columns": "change_date"},
        {"name": "idx_tinysoft_future_basic_ext_last_trade", "columns": "last_trade_date"},
    ]
    validations = [
        (lambda df: df["contract_code_raw"].notna(), "contract_code_raw 不能为空"),
        (lambda df: df["change_date"].notna(), "change_date 不能为空"),
    ]
    date_fields = ("change_date", "last_trade_date", "last_delivery_date")
    numeric_fields = (
        "delivery_year",
        "delivery_month",
        "contract_multiplier",
        "min_price_change",
        "daily_price_limit_down_pct",
        "daily_price_limit_up_pct",
        "last_trade_ref_offset_months",
        "last_trade_offset_days",
        "last_delivery_ref_offset_months",
        "last_delivery_offset_days",
        "min_trade_margin_pct",
    )
    integer_fields = (
        "delivery_year",
        "delivery_month",
        "last_trade_ref_offset_months",
        "last_trade_offset_days",
        "last_delivery_ref_offset_months",
        "last_delivery_offset_days",
    )
    text_fields = (
        "contract_code_raw",
        "ts_code",
        "source_code",
        "product_code",
        "product_name",
        "contract_multiplier_unit",
        "quote_unit",
        "last_trade_ref_standard",
        "last_trade_day_type",
        "last_trade_holiday_adjust",
        "last_delivery_ref_standard",
        "last_delivery_day_type",
        "last_delivery_holiday_adjust",
        "delivery_method",
        "exchange_name",
        "future_category",
        "commodity_category",
        "benchmark_code",
    )

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "source_code" not in df.columns and "request_code" in df.columns:
            df["source_code"] = df["request_code"]
        if "contract_code_raw" not in df.columns and "source_code" in df.columns:
            df["contract_code_raw"] = df["source_code"]
        df["contract_code_raw"] = df["contract_code_raw"].map(future_ts_code_to_tinysoft_symbol)
        df["product_code"] = df["product_code"].map(lambda x: str(x).strip().upper() if clean_text(x) else None)
        df["ts_code"] = df.apply(
            lambda row: future_contract_code_to_ts_code(row.get("contract_code_raw"), row.get("exchange_name")),
            axis=1,
        )
        return df


@task_register()
class TinySoftFutureProductMappingExtTask(TinySoftP0InfoArrayTask):
    name = "tinysoft_future_product_mapping_ext"
    description = "获取期货品种代码对照及主力映射（Tinysoft）"
    table_name = "future_product_mapping_ext"
    domain = "future"
    primary_keys = ["product_code", "change_date"]
    date_column = "change_date"
    default_start_date = "19950417"
    smart_lookback_days = 30
    default_code_batch_size = 200
    default_smart_code_batch_size = 500
    code_config_keys = ("product_codes", "future_codes", "codes")
    code_column = "product_code"
    infoarray_table_id = 708
    source_table_name = "期货.期货品种代码对照表"
    where_date_field = "变动日"
    default_symbol_source_tables = ["tushare.future_basic", "rawdata.future_basic"]
    field_mapping = {
        "StockID": "source_code",
        "stockid": "source_code",
        "品种代码": "product_code",
        "变动日": "change_date",
        "品种名称": "product_name",
        "主力代码": "main_contract_code",
        "主力代码2": "main_contract_code_2",
        "次主力代码": "secondary_main_contract_code",
        "指数线代码": "index_contract_code",
        "连续代码": "continuous_contract_code",
        "连一代码": "continuous_contract_code_1",
        "连二代码": "continuous_contract_code_2",
        "连三代码": "continuous_contract_code_3",
        "连四代码": "continuous_contract_code_4",
    }
    schema_def = {
        "product_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "source_code": {"type": "VARCHAR(30)"},
        "change_date": {"type": "DATE", "constraints": "NOT NULL"},
        "product_name": {"type": "VARCHAR(120)"},
        "main_contract_code": {"type": "VARCHAR(30)"},
        "main_contract_code_2": {"type": "VARCHAR(30)"},
        "secondary_main_contract_code": {"type": "VARCHAR(30)"},
        "index_contract_code": {"type": "VARCHAR(30)"},
        "continuous_contract_code": {"type": "VARCHAR(30)"},
        "continuous_contract_code_1": {"type": "VARCHAR(30)"},
        "continuous_contract_code_2": {"type": "VARCHAR(30)"},
        "continuous_contract_code_3": {"type": "VARCHAR(30)"},
        "continuous_contract_code_4": {"type": "VARCHAR(30)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_future_product_mapping_change", "columns": "change_date"},
        {"name": "idx_tinysoft_future_product_mapping_main", "columns": "main_contract_code"},
    ]
    validations = [
        (lambda df: df["product_code"].notna(), "product_code 不能为空"),
        (lambda df: df["change_date"].notna(), "change_date 不能为空"),
    ]
    date_fields = ("change_date",)
    text_fields = tuple(col for col in schema_def if col not in {"change_date", "source_table_id", "raw_json"})

    def _get_codes_from_mapping(self, params: dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        codes = []
        for raw_code in raw_codes:
            text = future_ts_code_to_tinysoft_symbol(raw_code)
            if not text:
                continue
            match = re.match(r"^[A-Z]+", text)
            codes.append(match.group(0) if match else text)
        return list(dict.fromkeys(codes))

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []
        codes: List[str] = []
        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns:
                    continue
                if "fut_code" in columns:
                    rows = await self.db.fetch(f'SELECT DISTINCT fut_code FROM "{schema}"."{table_name}" ORDER BY fut_code')
                    codes.extend(str(get_row_value(row, "fut_code") or "").strip().upper() for row in rows or [])
                elif "ts_code" in columns:
                    rows = await self.db.fetch(f'SELECT DISTINCT ts_code FROM "{schema}"."{table_name}" ORDER BY ts_code')
                    for row in rows or []:
                        symbol = future_ts_code_to_tinysoft_symbol(get_row_value(row, "ts_code"))
                        match = re.match(r"^[A-Z]+", symbol or "")
                        if match:
                            codes.append(match.group(0))
                if codes:
                    break
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载期货品种代码失败: %s", table, e)
        return list(dict.fromkeys(code for code in codes if code))

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "source_code" not in df.columns and "request_code" in df.columns:
            df["source_code"] = df["request_code"]
        if "product_code" not in df.columns and "source_code" in df.columns:
            df["product_code"] = df["source_code"]
        for col in (
            "product_code",
            "source_code",
            "main_contract_code",
            "main_contract_code_2",
            "secondary_main_contract_code",
            "index_contract_code",
            "continuous_contract_code",
            "continuous_contract_code_1",
            "continuous_contract_code_2",
            "continuous_contract_code_3",
            "continuous_contract_code_4",
        ):
            if col in df.columns:
                df[col] = df[col].map(lambda x: str(x).strip().upper() if clean_text(x) else None)
        return df


__all__ = ["TinySoftFutureBasicExtTask", "TinySoftFutureProductMappingExtTask"]
