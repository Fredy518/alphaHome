#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft stock-side P0 cross-domain tables."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import (
    CHANNEL_NAMES,
    TinySoftP0InfoArrayTask,
    clean_text,
    get_row_value,
    map_hsgt_security_to_ts_code,
    tinysoft_symbol_to_ts_code_any,
    ts_code_to_tinysoft_symbol_any,
)


class TinySoftStockSymbolInfoArrayTask(TinySoftP0InfoArrayTask):
    """Base class for stock-code keyed P0 InfoArray tables."""

    domain = "stock"
    default_code_batch_size = 50
    default_smart_code_batch_size = 200
    default_start_date = "20180101"
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
                if "list_status" in columns:
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


@task_register()
class TinySoftStockBasicExtTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_basic_ext"
    description = "获取股票基本信息扩展字段（Tinysoft）"
    table_name = "stock_basic_ext"
    primary_keys = ["ts_code"]
    date_column = None
    default_start_date = "19901219"
    default_code_batch_size = 500
    default_smart_code_batch_size = 2000
    infoarray_table_id = 10
    source_table_name = "股票.基本信息"
    where_date_field = None
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "A股代码": "a_share_code",
        "公司中文全称": "company_full_name",
        "公司中文简称": "company_short_name",
        "B股代码": "b_share_code",
        "B股简称": "b_share_short_name",
        "公司英文全称": "company_full_name_en",
        "公司英文简称": "company_short_name_en",
        "注册资本": "registered_capital",
        "法定代表人": "legal_representative",
        "成立日期": "establish_date",
        "公司注册地址": "registered_address",
        "邮政编码": "postcode",
        "经营范围": "business_scope",
        "主营业务": "main_business",
        "电话号码": "phone",
        "传真号码": "fax",
        "互联网网址": "website",
        "电子信箱": "email",
        "联系人": "contact_person",
        "公司简介": "company_profile",
        "地域": "area",
        "股票种类": "stock_type",
        "当前状态": "current_status",
        "上市地": "list_location",
        "所属市场": "market",
        "申万一级行业": "sw_industry_l1",
        "申万二级行业": "sw_industry_l2",
        "申万三级行业": "sw_industry_l3",
        "申万一级行业代码": "sw_industry_l1_code",
        "申万二级行业代码": "sw_industry_l2_code",
        "申万三级行业代码": "sw_industry_l3_code",
        "中证一级行业": "csi_industry_l1",
        "中证二级行业": "csi_industry_l2",
        "中证三级行业": "csi_industry_l3",
        "中证四级行业": "csi_industry_l4",
        "中证一级行业代码": "csi_industry_l1_code",
        "中证二级行业代码": "csi_industry_l2_code",
        "中证三级行业代码": "csi_industry_l3_code",
        "中证四级行业代码": "csi_industry_l4_code",
        "证监会一级行业代码": "csrc_industry_l1_code",
        "证监会二级行业代码": "csrc_industry_l2_code",
        "证监会一级行业名称": "csrc_industry_l1",
        "证监会二级行业名称": "csrc_industry_l2",
        "H股代码": "h_share_code",
        "H股简称": "h_share_short_name",
        "股本单位": "capital_unit",
        "转换比例": "capital_conversion_ratio",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "a_share_code": {"type": "VARCHAR(20)"},
        "company_full_name": {"type": "VARCHAR(255)"},
        "company_short_name": {"type": "VARCHAR(120)"},
        "b_share_code": {"type": "VARCHAR(20)"},
        "b_share_short_name": {"type": "VARCHAR(120)"},
        "company_full_name_en": {"type": "VARCHAR(255)"},
        "company_short_name_en": {"type": "VARCHAR(120)"},
        "registered_capital": {"type": "NUMERIC(24,4)"},
        "legal_representative": {"type": "VARCHAR(120)"},
        "establish_date": {"type": "DATE"},
        "registered_address": {"type": "TEXT"},
        "postcode": {"type": "VARCHAR(20)"},
        "business_scope": {"type": "TEXT"},
        "main_business": {"type": "TEXT"},
        "phone": {"type": "VARCHAR(120)"},
        "fax": {"type": "VARCHAR(120)"},
        "website": {"type": "VARCHAR(255)"},
        "email": {"type": "VARCHAR(255)"},
        "contact_person": {"type": "VARCHAR(120)"},
        "company_profile": {"type": "TEXT"},
        "area": {"type": "VARCHAR(80)"},
        "stock_type": {"type": "VARCHAR(50)"},
        "current_status": {"type": "VARCHAR(50)"},
        "list_location": {"type": "VARCHAR(80)"},
        "market": {"type": "VARCHAR(80)"},
        "sw_industry_l1": {"type": "VARCHAR(120)"},
        "sw_industry_l2": {"type": "VARCHAR(120)"},
        "sw_industry_l3": {"type": "VARCHAR(120)"},
        "sw_industry_l1_code": {"type": "VARCHAR(30)"},
        "sw_industry_l2_code": {"type": "VARCHAR(30)"},
        "sw_industry_l3_code": {"type": "VARCHAR(30)"},
        "csi_industry_l1": {"type": "VARCHAR(120)"},
        "csi_industry_l2": {"type": "VARCHAR(120)"},
        "csi_industry_l3": {"type": "VARCHAR(120)"},
        "csi_industry_l4": {"type": "VARCHAR(120)"},
        "csi_industry_l1_code": {"type": "VARCHAR(30)"},
        "csi_industry_l2_code": {"type": "VARCHAR(30)"},
        "csi_industry_l3_code": {"type": "VARCHAR(30)"},
        "csi_industry_l4_code": {"type": "VARCHAR(30)"},
        "csrc_industry_l1_code": {"type": "VARCHAR(30)"},
        "csrc_industry_l2_code": {"type": "VARCHAR(30)"},
        "csrc_industry_l1": {"type": "VARCHAR(120)"},
        "csrc_industry_l2": {"type": "VARCHAR(120)"},
        "h_share_code": {"type": "VARCHAR(20)"},
        "h_share_short_name": {"type": "VARCHAR(120)"},
        "capital_unit": {"type": "VARCHAR(30)"},
        "capital_conversion_ratio": {"type": "NUMERIC(20,8)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_stock_basic_ext_tsl", "columns": "tsl_code"},
        {"name": "idx_tinysoft_stock_basic_ext_name", "columns": "company_short_name"},
        {"name": "idx_tinysoft_stock_basic_ext_status", "columns": "current_status"},
        {"name": "idx_tinysoft_stock_basic_ext_sw", "columns": "sw_industry_l1_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
    ]
    date_fields = ("establish_date",)
    numeric_fields = ("registered_capital", "capital_conversion_ratio")
    text_fields = tuple(
        col
        for col in schema_def
        if col
        not in {
            "registered_capital",
            "establish_date",
            "capital_conversion_ratio",
            "source_table_id",
            "raw_json",
        }
    )

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        """Stock basic should cover listed and delisted names, so do not filter list_status."""
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
                ORDER BY ts_code
                """
                rows = await self.db.fetch(query)
                codes = []
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
class TinySoftStockHsgtHoldTask(TinySoftHsgtChannelTask):
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
        df = self._set_channel_defaults(df)
        if "security_code_raw" not in df.columns and "StockID" in df.columns:
            df["security_code_raw"] = df["StockID"].where(~df["StockID"].isin(CHANNEL_NAMES.keys()))
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
class TinySoftStockHsgtShortBalanceTask(TinySoftHsgtChannelTask):
    name = "tinysoft_stock_hsgt_short_balance"
    description = "获取沪深股通股票卖空数据（Tinysoft）"
    table_name = "stock_hsgt_short_balance"
    primary_keys = ["channel_code", "trade_date", "security_code_raw"]
    date_column = "trade_date"
    infoarray_table_id = 161
    source_table_name = "股票.沪深股通股票卖空数据"
    default_codes = ["HG000002", "HG000004"]
    field_mapping = {
        "截止日": "trade_date",
        "股票代码": "security_code_raw",
        "证券代码": "security_code_raw",
        "代码": "security_code_raw",
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
        df = self._set_channel_defaults(df)
        if "security_code_raw" not in df.columns and "StockID" in df.columns:
            df["security_code_raw"] = df["StockID"].where(~df["StockID"].isin(CHANNEL_NAMES.keys()))
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
        "holder_name": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
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
    "TinySoftStockBasicExtTask",
    "TinySoftStockHsgtDailyTask",
    "TinySoftStockHsgtTop10Task",
    "TinySoftStockHsgtHoldTask",
    "TinySoftStockHsgtShortBalanceTask",
    "TinySoftStockLendingSummaryTask",
    "TinySoftStockLendingTradeTask",
    "TinySoftStockLendingBalanceTask",
    "TinySoftStockPublicTradeInfoTask",
    "TinySoftStockUnlockScheduleTask",
    "TinySoftStockHolderChangeExtTask",
    "TinySoftStockRepurchaseExtTask",
    "TinySoftStockPledgeSummaryTask",
    "TinySoftStockPledgeDetailTask",
    "TinySoftStockPledgeBalanceTask",
    "TinySoftStockPledgeRateTask",
]
