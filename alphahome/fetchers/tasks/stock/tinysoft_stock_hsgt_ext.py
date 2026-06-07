#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft stock HSGT P0 tasks."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import CHANNEL_NAMES, map_hsgt_security_to_ts_code
from .tinysoft_stock_p0_base import TinySoftHsgtChannelTask, TinySoftStockSymbolInfoArrayTask


def _northbound_channel_from_stock_symbol(value: Any) -> Optional[str]:
    raw = str(value or "").strip().upper()
    if not raw:
        return None
    if raw.startswith("SH") or raw.endswith(".SH"):
        return "HG000002"
    if raw.startswith("SZ") or raw.endswith(".SZ"):
        return "HG000004"
    if raw.isdigit() and len(raw) == 6:
        if raw.startswith(("5", "6", "9")):
            return "HG000002"
        if raw.startswith(("0", "1", "2", "3")):
            return "HG000004"
    return None


def _first_present(row: pd.Series, names: tuple[str, ...]) -> Any:
    for name in names:
        if name in row.index:
            value = row.get(name)
            if pd.notna(value) and str(value).strip():
                return value
    return None


class _TinySoftNorthboundStockTask(TinySoftStockSymbolInfoArrayTask):
    """Base class for HSGT stock-level InfoArray tables keyed by A-share code."""

    default_start_date = "20141117"

    def _filter_northbound_codes(self, codes: list[str]) -> list[str]:
        return [code for code in codes if _northbound_channel_from_stock_symbol(code)]

    def _get_codes_from_mapping(self, params: dict[str, Any]) -> list[str]:
        return self._filter_northbound_codes(super()._get_codes_from_mapping(params))

    async def _load_codes_from_db(self, *, silent: bool = False) -> list[str]:
        return self._filter_northbound_codes(await super()._load_codes_from_db(silent=silent))

    def _set_stock_channel_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super()._postprocess_frame(df)
        if "security_code_raw" not in df.columns:
            if "tsl_code" in df.columns:
                df["security_code_raw"] = df["tsl_code"]
            elif "StockID" in df.columns:
                df["security_code_raw"] = df["StockID"]
            elif self.request_code_column in df.columns:
                df["security_code_raw"] = df[self.request_code_column]

        if "channel_code" not in df.columns:
            df["channel_code"] = df.apply(
                lambda row: _northbound_channel_from_stock_symbol(
                    _first_present(
                        row,
                        (
                            "security_code_raw",
                            "tsl_code",
                            "StockID",
                            "stockid",
                            "ts_code",
                            self.request_code_column,
                            self.code_column,
                        ),
                    )
                ),
                axis=1,
            )
        else:
            df["channel_code"] = df["channel_code"].where(
                df["channel_code"].notna(),
                df.apply(
                    lambda row: _northbound_channel_from_stock_symbol(
                        _first_present(
                            row,
                            (
                                "security_code_raw",
                                "tsl_code",
                                "StockID",
                                "stockid",
                                "ts_code",
                                self.request_code_column,
                                self.code_column,
                            ),
                        )
                    ),
                    axis=1,
                ),
            )

        df["channel_name"] = df["channel_code"].map(lambda x: CHANNEL_NAMES.get(str(x).upper()))
        if "ts_code" not in df.columns:
            df["ts_code"] = df.apply(
                lambda row: map_hsgt_security_to_ts_code(row.get("security_code_raw"), row.get("channel_code")),
                axis=1,
            )
        return df

@task_register()
class TinySoftStockHsgtDailyTask(TinySoftHsgtChannelTask):
    name = "tinysoft_stock_hsgt_daily"
    description = "获取沪深港通每日成交汇总（Tinysoft）"
    table_name = "stock_hsgt_daily"
    primary_keys = ["channel_code", "trade_date"]
    date_column = "trade_date"
    infoarray_table_id = 130
    source_table_name = "股票.沪深港通每日成交汇总"

    schema_def = {
        "channel_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "channel_name": {"type": "VARCHAR(50)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "amount_total_rmb": {"type": "NUMERIC(24,4)"},
        "buy_amount_rmb": {"type": "NUMERIC(24,4)"},
        "sell_amount_rmb": {"type": "NUMERIC(24,4)"},
        "amount_total_hkd": {"type": "NUMERIC(24,4)"},
        "buy_amount_hkd": {"type": "NUMERIC(24,4)"},
        "sell_amount_hkd": {"type": "NUMERIC(24,4)"},
        "trade_count_total": {"type": "NUMERIC(24,4)"},
        "buy_count": {"type": "NUMERIC(24,4)"},
        "sell_count": {"type": "NUMERIC(24,4)"},
        "quota_balance": {"type": "NUMERIC(24,4)"},
        "stock_etf_amount": {"type": "NUMERIC(24,4)"},
        "etf_amount": {"type": "NUMERIC(24,4)"},
        "currency_hint": {"type": "VARCHAR(10)"},
        "disclosure_cycle": {"type": "VARCHAR(20)"},
        "is_disclosure_missing": {"type": "BOOLEAN"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_hsgt_daily_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_hsgt_daily_channel", "columns": "channel_code"},
    ]
    validations = [
        (lambda df: df["channel_code"].notna(), "channel_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
    ]
    field_mapping = {
        "截止日": "trade_date",
        "买入及卖出成交额(元)": "amount_total_rmb",
        "买入成交额(元)": "buy_amount_rmb",
        "卖出成交额(元)": "sell_amount_rmb",
        "买入及卖出成交额(港币)": "amount_total_hkd",
        "买入成交额(港币)": "buy_amount_hkd",
        "卖出成交额(港币)": "sell_amount_hkd",
        "买入及卖出成交数目": "trade_count_total",
        "买入成交数目": "buy_count",
        "卖出成交数目": "sell_count",
        "每日额度余额": "quota_balance",
        "股票买入及卖出成交额": "stock_etf_amount",
        "ETF 买入及卖出成交额": "etf_amount",
        "ETF买入及卖出成交额": "etf_amount",
    }
    date_fields = ("trade_date",)
    numeric_fields = tuple(k for k in schema_def if k.endswith(("rmb", "hkd", "count", "balance", "amount")))

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = self._set_channel_defaults(df)
        post_change = pd.to_datetime("20240819").date()
        df["is_disclosure_missing"] = (
            df["trade_date"].notna()
            & (df["trade_date"] >= post_change)
            & df["channel_code"].isin(["HG000002", "HG000004"])
            & df[["buy_amount_rmb", "sell_amount_rmb", "buy_count", "sell_count"]].isna().all(axis=1)
        )
        return df


@task_register()
class TinySoftStockHsgtTop10Task(TinySoftHsgtChannelTask):
    name = "tinysoft_stock_hsgt_top10"
    description = "获取沪深港通每日十大成交活跃股（Tinysoft）"
    table_name = "stock_hsgt_top10"
    primary_keys = ["channel_code", "trade_date", "rank_no", "security_code_raw"]
    date_column = "trade_date"
    infoarray_table_id = 131
    source_table_name = "股票.沪深港通每日十大成交活跃股"

    schema_def = {
        "channel_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "channel_name": {"type": "VARCHAR(50)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "rank_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "security_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)"},
        "security_name": {"type": "VARCHAR(100)"},
        "buy_amount": {"type": "NUMERIC(24,4)"},
        "sell_amount": {"type": "NUMERIC(24,4)"},
        "amount_total": {"type": "NUMERIC(24,4)"},
        "currency_hint": {"type": "VARCHAR(10)"},
        "disclosure_cycle": {"type": "VARCHAR(20)"},
        "is_disclosure_missing": {"type": "BOOLEAN"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_hsgt_top10_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_hsgt_top10_security", "columns": "security_code_raw"},
        {"name": "idx_tinysoft_stock_hsgt_top10_ts_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["channel_code"].notna(), "channel_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["rank_no"].notna(), "rank_no 不能为空"),
        (lambda df: df["security_code_raw"].notna(), "security_code_raw 不能为空"),
    ]
    field_mapping = {
        "截止日": "trade_date",
        "股票代码": "security_code_raw",
        "股票名称": "security_name",
        "买入金额": "buy_amount",
        "卖出金额": "sell_amount",
        "买入及卖出金额": "amount_total",
        "排名": "rank_no",
    }
    date_fields = ("trade_date",)
    numeric_fields = ("rank_no", "buy_amount", "sell_amount", "amount_total")
    integer_fields = ("rank_no",)
    text_fields = ("security_code_raw", "security_name")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = self._set_channel_defaults(df)
        if "ts_code" not in df.columns:
            df["ts_code"] = df.apply(
                lambda row: map_hsgt_security_to_ts_code(row.get("security_code_raw"), row.get("channel_code")),
                axis=1,
            )
        post_change = pd.to_datetime("20240819").date()
        df["is_disclosure_missing"] = (
            df["trade_date"].notna()
            & (df["trade_date"] >= post_change)
            & df["channel_code"].isin(["HG000002", "HG000004"])
            & df[["buy_amount", "sell_amount"]].isna().all(axis=1)
        )
        return df


@task_register()
class TinySoftStockHsgtHoldTask(_TinySoftNorthboundStockTask):
    name = "tinysoft_stock_hsgt_hold"
    description = "获取沪深港通持股明细（Tinysoft）"
    table_name = "stock_hsgt_hold"
    primary_keys = ["channel_code", "trade_date", "security_code_raw"]
    date_column = "trade_date"
    infoarray_table_id = 132
    source_table_name = "股票.沪深港通持股明细"
    # 2024-08-19 后沪/深股通改为季度披露，SMART 需要覆盖迟到披露窗口。
    smart_lookback_days = 370
    field_mapping = {
        "截止日": "trade_date",
        "股票代码": "security_code_raw",
        "证券代码": "security_code_raw",
        "代码": "security_code_raw",
        "StockName": "security_name",
        "股票名称": "security_name",
        "证券名称": "security_name",
        "名称": "security_name",
        "股数": "holding_volume",
        "持股数量": "holding_volume",
        "占总股本比例(%)": "total_share_ratio_pct",
    }
    schema_def = {
        "channel_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "channel_name": {"type": "VARCHAR(50)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "security_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)"},
        "security_name": {"type": "VARCHAR(100)"},
        "holding_volume": {"type": "NUMERIC(24,4)"},
        "total_share_ratio_pct": {"type": "NUMERIC(20,8)"},
        "disclosure_cycle": {"type": "VARCHAR(20)"},
        "is_disclosure_missing": {"type": "BOOLEAN"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_hsgt_hold_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_hsgt_hold_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_stock_hsgt_hold_channel", "columns": "channel_code"},
    ]
    validations = [
        (lambda df: df["channel_code"].notna(), "channel_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["security_code_raw"].notna(), "security_code_raw 不能为空"),
    ]
    date_fields = ("trade_date",)
    numeric_fields = ("holding_volume", "total_share_ratio_pct")
    text_fields = ("security_code_raw", "security_name", "disclosure_cycle")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = self._set_stock_channel_defaults(df)
        if "ts_code" not in df.columns:
            df["ts_code"] = df.apply(
                lambda row: map_hsgt_security_to_ts_code(row.get("security_code_raw"), row.get("channel_code")),
                axis=1,
            )
        for col in ("holding_volume", "total_share_ratio_pct"):
            if col not in df.columns:
                df[col] = None
        post_change = pd.to_datetime("20240819").date()
        df["disclosure_cycle"] = df["channel_code"].map(
            lambda x: "quarterly" if str(x).upper() in {"HG000002", "HG000004"} else "daily"
        )
        df["is_disclosure_missing"] = (
            df["trade_date"].notna()
            & (df["trade_date"] >= post_change)
            & df["channel_code"].isin(["HG000002", "HG000004"])
            & df[["holding_volume", "total_share_ratio_pct"]].isna().all(axis=1)
        )
        return df


@task_register()
class TinySoftStockHsgtShortBalanceTask(_TinySoftNorthboundStockTask):
    name = "tinysoft_stock_hsgt_short_balance"
    description = "获取沪深股通股票卖空数据（Tinysoft）"
    table_name = "stock_hsgt_short_balance"
    primary_keys = ["channel_code", "trade_date", "security_code_raw"]
    date_column = "trade_date"
    infoarray_table_id = 161
    source_table_name = "股票.沪深股通股票卖空数据"
    field_mapping = {
        "截止日": "trade_date",
        "股票代码": "security_code_raw",
        "证券代码": "security_code_raw",
        "代码": "security_code_raw",
        "StockName": "security_name",
        "股票名称": "security_name",
        "证券名称": "security_name",
        "名称": "security_name",
        "可供卖空股数余额": "short_balance_volume",
        "卖空股数余额": "short_balance_volume",
    }
    schema_def = {
        "channel_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "channel_name": {"type": "VARCHAR(50)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "security_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)"},
        "security_name": {"type": "VARCHAR(100)"},
        "short_balance_volume": {"type": "NUMERIC(24,4)"},
        "disclosure_cycle": {"type": "VARCHAR(20)"},
        "is_available_status": {"type": "BOOLEAN"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_hsgt_short_balance_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_hsgt_short_balance_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["channel_code"].notna(), "channel_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["security_code_raw"].notna(), "security_code_raw 不能为空"),
    ]
    date_fields = ("trade_date",)
    numeric_fields = ("short_balance_volume",)
    text_fields = ("security_code_raw", "security_name", "disclosure_cycle")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = self._set_stock_channel_defaults(df)
        if "ts_code" not in df.columns:
            df["ts_code"] = df.apply(
                lambda row: map_hsgt_security_to_ts_code(row.get("security_code_raw"), row.get("channel_code")),
                axis=1,
            )
        if "short_balance_volume" not in df.columns:
            df["short_balance_volume"] = None
        df["disclosure_cycle"] = "daily"
        if "is_available_status" not in df.columns:
            df["is_available_status"] = df["short_balance_volume"].isna()
        return df


__all__ = [
    "TinySoftStockHsgtDailyTask",
    "TinySoftStockHsgtTop10Task",
    "TinySoftStockHsgtHoldTask",
    "TinySoftStockHsgtShortBalanceTask",
]
