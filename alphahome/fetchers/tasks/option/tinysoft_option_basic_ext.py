#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft option contract daily basic/status extension tables."""

from __future__ import annotations

import re
from typing import Any, List

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import (
    TinySoftP0InfoArrayTask,
    clean_text,
    get_row_value,
    tinysoft_symbol_to_ts_code_any,
)


OPTION_EXCHANGE_SUFFIXES = {
    "上海证券交易所": "SH",
    "深圳证券交易所": "SZ",
    "中国金融期货交易所": "CFX",
    "上海期货交易所": "SHF",
    "上海国际能源交易中心": "INE",
    "大连商品交易所": "DCE",
    "郑州商品交易所": "ZCE",
    "广州期货交易所": "GFE",
}

FINANCIAL_OPTION_EXCHANGE_NAMES = {
    "上海证券交易所",
    "深圳证券交易所",
    "中国金融期货交易所",
}
FINANCIAL_OPTION_EXCHANGE_CODES = {"SSE", "SZSE", "CFFEX", "SH", "SZ", "CFX"}
FINANCIAL_OPTION_TS_SUFFIXES = {"SH", "SZ", "CFX"}
FINANCIAL_INDEX_OPTION_PREFIXES = ("IO", "MO", "HO")


def option_ts_code_to_tinysoft_symbol(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    raw = text.upper()
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw or None


def option_contract_code_to_ts_code(code: Any, exchange_name: Any = None) -> str | None:
    raw = option_ts_code_to_tinysoft_symbol(code)
    if not raw:
        return None
    suffix = OPTION_EXCHANGE_SUFFIXES.get(str(exchange_name or "").strip())
    if not suffix:
        return None
    return f"{raw}.{suffix}"


def is_financial_option_exchange(value: Any) -> bool:
    text = clean_text(value)
    if not text:
        return False
    raw = text.upper()
    if raw in FINANCIAL_OPTION_EXCHANGE_CODES:
        return True
    return any(name in text for name in FINANCIAL_OPTION_EXCHANGE_NAMES)


def is_financial_option_ts_code(value: Any) -> bool:
    raw = clean_text(value)
    if not raw:
        return False
    text = raw.upper()
    if "." in text:
        suffix = text.rsplit(".", 1)[1]
        return suffix in FINANCIAL_OPTION_TS_SUFFIXES

    symbol = option_ts_code_to_tinysoft_symbol(text)
    if not symbol:
        return False
    if re.fullmatch(r"\d{6,}", symbol):
        return True
    if re.fullmatch(r"\d{6}[CP].*", symbol):
        return True
    return symbol.startswith(FINANCIAL_INDEX_OPTION_PREFIXES)


@task_register()
class TinySoftOptionBasicDailyExtTask(TinySoftP0InfoArrayTask):
    name = "tinysoft_option_basic_daily_ext"
    description = "获取期权合约每日基本信息及状态（Tinysoft）"
    table_name = "option_basic_daily_ext"
    domain = "option"
    primary_keys = ["contract_code_raw", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20150209"
    smart_lookback_days = 30
    default_code_batch_size = 300
    default_smart_code_batch_size = 1000
    default_include_raw_json = False
    code_config_keys = ("option_codes", "ts_codes", "ts_code", "codes")
    code_column = "contract_code_raw"
    infoarray_table_id = 720
    source_table_name = "期权.期权基本信息"
    where_date_field = "截止日"
    default_symbol_source_tables = ["tushare.option_basic", "rawdata.option_basic"]
    field_mapping = {
        "StockID": "source_code",
        "stockid": "source_code",
        "截止日": "trade_date",
        "合约交易代码": "contract_trade_code",
        "合约简称": "contract_short_name",
        "标的证券代码": "underlying_code_raw",
        "标的证券名称": "underlying_name",
        "标的证券类型": "underlying_type",
        "行权方式": "exercise_style",
        "期权类型": "option_type",
        "合约单位": "contract_unit",
        "行权价": "exercise_price",
        "首个交易日": "first_trade_date",
        "最后交易日": "last_trade_date",
        "行权日": "exercise_date",
        "行权交割日": "exercise_delivery_date",
        "到期日": "maturity_date",
        "合约未平仓数": "open_interest",
        "合约前收盘价": "pre_close",
        "合约前结算价": "pre_settle",
        "标的证券前收盘": "underlying_pre_close",
        "是否有涨跌幅限制": "has_price_limit",
        "涨幅上限价格": "upper_limit_price",
        "跌幅下限价格": "lower_limit_price",
        "单位保证金": "unit_margin",
        "保证金计算比例参数一": "margin_param_1_pct",
        "保证金计算比例参数二": "margin_param_2_pct",
        "整手数": "round_lot",
        "单笔限价申报下限": "limit_order_min_qty",
        "单笔限价申报上限": "limit_order_max_qty",
        "单笔市价申报下限": "market_order_min_qty",
        "单笔市价申报上限": "market_order_max_qty",
        "单笔报价申报下限": "quote_order_min_qty",
        "单笔报价申报上限": "quote_order_max_qty",
        "最小报价单位": "min_quote_unit",
        "期权合约状态信息": "contract_status",
        "开仓状态": "open_position_status",
        "是否连续停牌": "is_continuous_suspend",
        "是否距离到期日不足5个交易日": "is_less_than_5_trade_days_to_maturity",
        "是否距离到期日不足 5 个交易日": "is_less_than_5_trade_days_to_maturity",
        "最近5个交易日内合约是否发生过调整": "has_recent_5d_adjustment",
        "最近 5 个交易日内合约是否发生过调整": "has_recent_5d_adjustment",
        "是否新挂牌合约": "is_new_contract",
        "上市地": "exchange_name",
    }
    schema_def = {
        "contract_code_raw": {"type": "VARCHAR(40)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(40)"},
        "source_code": {"type": "VARCHAR(40)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "contract_trade_code": {"type": "VARCHAR(40)"},
        "contract_short_name": {"type": "VARCHAR(120)"},
        "underlying_code_raw": {"type": "VARCHAR(40)"},
        "underlying_ts_code": {"type": "VARCHAR(30)"},
        "underlying_name": {"type": "VARCHAR(120)"},
        "underlying_type": {"type": "VARCHAR(80)"},
        "exercise_style": {"type": "VARCHAR(50)"},
        "option_type": {"type": "VARCHAR(50)"},
        "contract_unit": {"type": "NUMERIC(20,4)"},
        "exercise_price": {"type": "NUMERIC(20,8)"},
        "first_trade_date": {"type": "DATE"},
        "last_trade_date": {"type": "DATE"},
        "exercise_date": {"type": "DATE"},
        "exercise_delivery_date": {"type": "DATE"},
        "maturity_date": {"type": "DATE"},
        "open_interest": {"type": "NUMERIC(24,4)"},
        "pre_close": {"type": "NUMERIC(20,8)"},
        "pre_settle": {"type": "NUMERIC(20,8)"},
        "underlying_pre_close": {"type": "NUMERIC(20,8)"},
        "has_price_limit": {"type": "VARCHAR(30)"},
        "upper_limit_price": {"type": "NUMERIC(20,8)"},
        "lower_limit_price": {"type": "NUMERIC(20,8)"},
        "unit_margin": {"type": "NUMERIC(24,8)"},
        "margin_param_1_pct": {"type": "NUMERIC(20,8)"},
        "margin_param_2_pct": {"type": "NUMERIC(20,8)"},
        "round_lot": {"type": "NUMERIC(20,4)"},
        "limit_order_min_qty": {"type": "NUMERIC(20,4)"},
        "limit_order_max_qty": {"type": "NUMERIC(20,4)"},
        "market_order_min_qty": {"type": "NUMERIC(20,4)"},
        "market_order_max_qty": {"type": "NUMERIC(20,4)"},
        "quote_order_min_qty": {"type": "NUMERIC(20,4)"},
        "quote_order_max_qty": {"type": "NUMERIC(20,4)"},
        "min_quote_unit": {"type": "NUMERIC(20,8)"},
        "contract_status": {"type": "VARCHAR(120)"},
        "open_position_status": {"type": "VARCHAR(120)"},
        "is_continuous_suspend": {"type": "VARCHAR(30)"},
        "is_less_than_5_trade_days_to_maturity": {"type": "VARCHAR(30)"},
        "has_recent_5d_adjustment": {"type": "VARCHAR(30)"},
        "is_new_contract": {"type": "VARCHAR(30)"},
        "exchange_name": {"type": "VARCHAR(120)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_option_basic_daily_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_option_basic_daily_ts", "columns": "ts_code"},
        {"name": "idx_tinysoft_option_basic_daily_underlying", "columns": "underlying_ts_code"},
        {"name": "idx_tinysoft_option_basic_daily_maturity", "columns": "maturity_date"},
    ]
    validations = [
        (lambda df: df["contract_code_raw"].notna(), "contract_code_raw 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
    ]
    date_fields = (
        "trade_date",
        "first_trade_date",
        "last_trade_date",
        "exercise_date",
        "exercise_delivery_date",
        "maturity_date",
    )
    numeric_fields = (
        "contract_unit",
        "exercise_price",
        "open_interest",
        "pre_close",
        "pre_settle",
        "underlying_pre_close",
        "upper_limit_price",
        "lower_limit_price",
        "unit_margin",
        "margin_param_1_pct",
        "margin_param_2_pct",
        "round_lot",
        "limit_order_min_qty",
        "limit_order_max_qty",
        "market_order_min_qty",
        "market_order_max_qty",
        "quote_order_min_qty",
        "quote_order_max_qty",
        "min_quote_unit",
    )
    text_fields = (
        "contract_code_raw",
        "ts_code",
        "source_code",
        "contract_trade_code",
        "contract_short_name",
        "underlying_code_raw",
        "underlying_ts_code",
        "underlying_name",
        "underlying_type",
        "exercise_style",
        "option_type",
        "has_price_limit",
        "contract_status",
        "open_position_status",
        "is_continuous_suspend",
        "is_less_than_5_trade_days_to_maturity",
        "has_recent_5d_adjustment",
        "is_new_contract",
        "exchange_name",
        "source_table_name",
    )

    def _get_codes_from_mapping(self, params: dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        return list(
            dict.fromkeys(
                code
                for code in (option_ts_code_to_tinysoft_symbol(x) for x in raw_codes)
                if code and is_financial_option_ts_code(code)
            )
        )

    async def _resolve_codes(self, **kwargs: Any) -> List[str]:
        self._code_pool_start_date = kwargs.get("start_date")
        self._code_pool_end_date = kwargs.get("end_date")
        return await super()._resolve_codes(**kwargs)

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []

        start_date = getattr(self, "_effective_start_date", None) or getattr(self, "_code_pool_start_date", None)
        end_date = getattr(self, "_effective_end_date", None) or getattr(self, "_code_pool_end_date", None)
        codes: List[str] = []
        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue
                query = f'SELECT ts_code FROM "{schema}"."{table_name}" WHERE ts_code IS NOT NULL'
                if "exchange" in columns:
                    query += " AND UPPER(exchange) IN ('CFFEX', 'SSE', 'SZSE')"
                if start_date and "delist_date" in columns:
                    query += f" AND (delist_date IS NULL OR delist_date >= DATE '{pd.to_datetime(start_date).date()}')"
                if end_date and "list_date" in columns:
                    query += f" AND (list_date IS NULL OR list_date <= DATE '{pd.to_datetime(end_date).date()}')"
                query += " ORDER BY ts_code"
                rows = await self.db.fetch(query)
                for row in rows or []:
                    ts_code = get_row_value(row, "ts_code")
                    if not is_financial_option_ts_code(ts_code):
                        continue
                    code = option_ts_code_to_tinysoft_symbol(ts_code)
                    if code:
                        codes.append(code)
                if codes:
                    return list(dict.fromkeys(codes))
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载期权合约代码失败: %s", table, e)
        return []

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "source_code" not in df.columns and "request_code" in df.columns:
            df["source_code"] = df["request_code"]
        if "contract_code_raw" not in df.columns:
            if "contract_trade_code" in df.columns:
                trade_code = df["contract_trade_code"].map(clean_text)
                df["contract_code_raw"] = trade_code.where(trade_code.notna(), df.get("source_code"))
            elif "source_code" in df.columns:
                df["contract_code_raw"] = df["source_code"]
        df["contract_code_raw"] = df["contract_code_raw"].map(option_ts_code_to_tinysoft_symbol)
        df["ts_code"] = df.apply(
            lambda row: option_contract_code_to_ts_code(row.get("contract_code_raw"), row.get("exchange_name")),
            axis=1,
        )
        if "underlying_code_raw" in df.columns:
            df["underlying_ts_code"] = df["underlying_code_raw"].map(tinysoft_symbol_to_ts_code_any)
        df = self._filter_financial_options(df)
        return df

    def _filter_financial_options(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        if "exchange_name" in df.columns:
            exchange_allowed = df["exchange_name"].map(is_financial_option_exchange)
            if "ts_code" in df.columns:
                code_allowed = df["ts_code"].map(is_financial_option_ts_code)
                exchange_missing = df["exchange_name"].map(clean_text).isna()
                mask = exchange_allowed | (exchange_missing & code_allowed)
            else:
                mask = exchange_allowed
        elif "ts_code" in df.columns:
            mask = df["ts_code"].map(is_financial_option_ts_code)
        else:
            mask = pd.Series(False, index=df.index)

        filtered = df.loc[mask].copy()
        dropped = len(df) - len(filtered)
        if dropped:
            self.logger.info("任务 %s: 已过滤 %s 条商品期权 basic 记录。", self.name, dropped)
        return filtered


__all__ = ["TinySoftOptionBasicDailyExtTask"]
