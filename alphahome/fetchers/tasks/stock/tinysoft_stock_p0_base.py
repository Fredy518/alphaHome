#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Shared bases for Tinysoft stock-side P0 InfoArray tasks."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ..tinysoft_p0_base import (
    CHANNEL_NAMES,
    TinySoftP0InfoArrayTask,
    clean_text,
    get_row_value,
    tinysoft_symbol_to_ts_code_any,
    ts_code_to_tinysoft_symbol_any,
)


class TinySoftStockSymbolInfoArrayTask(TinySoftP0InfoArrayTask):
    """Base class for stock-code keyed P0 InfoArray tables."""

    domain = "stock"
    default_code_batch_size = 50
    default_smart_code_batch_size = 200
    default_start_date = "20180101"
    include_inactive_symbols = False
    code_config_keys = ("ts_codes", "ts_code", "codes")
    default_symbol_source_tables = ["tushare.stock_basic", "rawdata.stock_basic"]

    def _get_codes_from_mapping(self, params: Dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        codes: List[str] = []
        for raw_code in raw_codes:
            codes.append(ts_code_to_tinysoft_symbol_any(raw_code) or str(raw_code).strip().upper())
        return list(dict.fromkeys(code for code in codes if code))

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []
        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue
                query = f"""
                SELECT ts_code
                FROM "{schema}"."{table_name}"
                WHERE ts_code ~ '^[0-9]{{6}}\\.(SH|SZ|BJ)$'
                """
                if "list_status" in columns and not self.include_inactive_symbols:
                    query += " AND list_status = 'L'"
                query += " ORDER BY ts_code"
                rows = await self.db.fetch(query)
                codes: List[str] = []
                for row in rows or []:
                    value = get_row_value(row, "ts_code")
                    symbol = ts_code_to_tinysoft_symbol_any(value)
                    if symbol:
                        codes.append(symbol)
                if codes:
                    return list(dict.fromkeys(codes))
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载股票代码失败: %s", table, e)
        return []

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "tsl_code" not in df.columns and "StockID" in df.columns:
            df["tsl_code"] = df["StockID"]
        if "ts_code" not in df.columns and "tsl_code" in df.columns:
            df["ts_code"] = df["tsl_code"].map(tinysoft_symbol_to_ts_code_any)
        return df


class TinySoftHsgtChannelTask(TinySoftP0InfoArrayTask):
    """Base class for channel-code keyed HSGT tables."""

    domain = "stock"
    default_start_date = "20141117"
    default_code_batch_size = 1
    default_smart_code_batch_size = 4
    default_codes = ["HG000001", "HG000002", "HG000003", "HG000004"]
    code_config_keys = ("channel_codes", "codes")
    code_column = "channel_code"

    def _set_channel_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        if "channel_code" not in df.columns and "request_code" in df.columns:
            df["channel_code"] = df["request_code"]
        if "channel_code" not in df.columns and "StockID" in df.columns:
            df["channel_code"] = df["StockID"]
        if "channel_code" not in df.columns and "stockid" in df.columns:
            df["channel_code"] = df["stockid"]
        if "channel_code" not in df.columns:
            df["channel_code"] = None
        df["channel_name"] = df["channel_code"].map(lambda x: CHANNEL_NAMES.get(str(x).upper()))
        df["currency_hint"] = df["channel_code"].map(
            lambda x: "HKD" if str(x).upper() in {"HG000001", "HG000003"} else "CNY"
        )
        df["disclosure_cycle"] = "daily"
        return df


class TinySoftMarketCodeInfoArrayTask(TinySoftP0InfoArrayTask):
    """Base class for exchange/market-code keyed stock P1 tables."""

    domain = "stock"
    default_start_date = "20130624"
    smart_lookback_days = 370
    default_code_batch_size = 1
    default_smart_code_batch_size = 2
    default_codes = ["SH000001", "SZ399106"]
    code_config_keys = ("market_codes", "codes")
    code_column = "market_code"

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "market_code" not in df.columns and "request_code" in df.columns:
            df["market_code"] = df["request_code"]
        if "market_code" not in df.columns and "StockID" in df.columns:
            df["market_code"] = df["StockID"]
        if "market_code" not in df.columns and "stockid" in df.columns:
            df["market_code"] = df["stockid"]
        if "market_code" not in df.columns:
            df["market_code"] = None
        df["market_code"] = df["market_code"].map(clean_text)
        return df


__all__ = [
    "TinySoftStockSymbolInfoArrayTask",
    "TinySoftHsgtChannelTask",
    "TinySoftMarketCodeInfoArrayTask",
]
