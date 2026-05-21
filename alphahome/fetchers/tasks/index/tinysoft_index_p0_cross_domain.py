#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft index-side P0 cross-domain tables."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import (
    TinySoftP0InfoArrayTask,
    clean_text,
    tinysoft_symbol_to_ts_code_any,
)


MARKET_CALENDAR_NAMES = {
    "SH000001": "A股市场",
    "HKHSI001": "港股市场",
    "HSG000001": "南向交易日历",
    "HSG000002": "北向交易日历",
    "CBICBA00301": "银行间债券市场",
}

DEFAULT_INDEX_CODES = ["SH000001", "SZ399006", "CSI000300", "CSI000500", "CSI000905"]

def _normalize_index_code(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    raw = text.upper()
    mapped = tinysoft_symbol_to_ts_code_any(raw)
    if mapped:
        return raw
    if "." in raw:
        code, suffix = raw.rsplit(".", 1)
        if suffix in {"SH", "SZ"}:
            return f"{suffix}{code}"
        if suffix in {"CSI", "CNI"}:
            return f"{suffix}{code}"
    return raw


def _index_code_to_ts_code(value: Any) -> str | None:
    raw = clean_text(value)
    if not raw:
        return None
    mapped = tinysoft_symbol_to_ts_code_any(raw)
    if mapped:
        return mapped
    text = raw.upper()
    if text.startswith("CSI") and len(text) == 9:
        return f"{text[-6:]}.CSI"
    if text.startswith("CNI") and len(text) == 9:
        return f"{text[-6:]}.CNI"
    return None


def _latest_date_from_columns(df: pd.DataFrame, columns: tuple[str, ...]):
    available = [col for col in columns if col in df.columns]
    if not available:
        return None
    dates = pd.DataFrame({col: pd.to_datetime(df[col], errors="coerce") for col in available})
    return dates.max(axis=1).dt.date


class TinySoftIndexCodeInfoArrayTask(TinySoftP0InfoArrayTask):
    domain = "index"
    default_start_date = "20000101"
    smart_lookback_days = 370
    default_code_batch_size = 1
    default_smart_code_batch_size = 5
    default_codes = DEFAULT_INDEX_CODES
    code_config_keys = ("index_codes", "ts_codes", "codes")
    code_column = "index_code_raw"
    default_symbol_source_tables = ["tushare.index_basic", "rawdata.index_basic"]

    def _get_codes_from_mapping(self, params: Dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        codes = [_normalize_index_code(code) for code in raw_codes]
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
                rows = await self.db.fetch(f'SELECT ts_code FROM "{schema}"."{table_name}" ORDER BY ts_code')
                codes = [_normalize_index_code(row["ts_code"]) for row in rows or []]
                codes = [code for code in codes if code]
                if codes:
                    return list(dict.fromkeys(codes))
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载指数代码失败: %s", table, e)
        return list(self.default_codes)



def _to_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().lower()
    if text in {"", "none", "nan", "null"}:
        return None
    if text in {"1", "true", "yes", "y", "是", "交易日"}:
        return True
    if text in {"0", "false", "no", "n", "否", "非交易日", "休市"}:
        return False
    try:
        return bool(int(float(text)))
    except Exception:
        return None


@task_register()
class TinySoftMarketCalendarMultiTask(TinySoftP0InfoArrayTask):
    name = "tinysoft_market_calendar_multi"
    description = "获取多市场交易日历（Tinysoft）"
    table_name = "market_calendar_multi"
    domain = "index"
    primary_keys = ["market_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19901219"
    smart_lookback_days = 370
    default_code_batch_size = 1
    default_smart_code_batch_size = 5
    default_codes = ["SH000001", "HKHSI001", "HSG000001", "HSG000002", "CBICBA00301"]
    code_config_keys = ("market_codes", "codes")
    code_column = "market_code"
    infoarray_table_id = 753
    source_table_name = "指数.市场交易日历"
    where_date_field = "截止日"
    field_mapping = {
        "截止日": "trade_date",
        "是否交易日": "is_trade_day",
        "交易日类别": "trade_day_type",
        "备注": "remark",
    }
    schema_def = {
        "market_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "market_name": {"type": "VARCHAR(100)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "is_trade_day": {"type": "BOOLEAN"},
        "trade_day_type": {"type": "VARCHAR(50)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_market_calendar_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_market_calendar_code", "columns": "market_code"},
    ]
    validations = [
        (lambda df: df["market_code"].notna(), "market_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
    ]
    date_fields = ("trade_date",)
    text_fields = ("trade_day_type", "remark")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "market_code" not in df.columns and "request_code" in df.columns:
            df["market_code"] = df["request_code"]
        if "market_code" not in df.columns and "StockID" in df.columns:
            df["market_code"] = df["StockID"]
        if "market_code" not in df.columns and "stockid" in df.columns:
            df["market_code"] = df["stockid"]
        if "market_code" not in df.columns and "证券代码" in df.columns:
            df["market_code"] = df["证券代码"]
        if "market_code" not in df.columns:
            df["market_code"] = None
        df["market_code"] = df["market_code"].map(clean_text)
        df["market_name"] = df["market_code"].map(lambda x: MARKET_CALENDAR_NAMES.get(str(x).upper()))
        if "is_trade_day" in df.columns:
            df["is_trade_day"] = df["is_trade_day"].map(_to_optional_bool)
        return df


@task_register()
class TinySoftIndexMemberVersionedTask(TinySoftIndexCodeInfoArrayTask):
    name = "tinysoft_index_member_versioned"
    description = "获取指数成份历史变更（Tinysoft）"
    table_name = "index_member_versioned"
    primary_keys = ["index_code_raw", "con_code_raw", "in_date"]
    date_column = "latest_change_date"
    where_date_field = None
    infoarray_table_id = 752
    source_table_name = "指数.指数成份"
    field_mapping = {
        "StockID": "index_code_raw",
        "stockid": "index_code_raw",
        "证券代码": "con_code_raw",
        "代码": "con_code_raw",
        "入选日期": "in_date",
        "剔除日期": "out_date",
        "成份标志": "member_flag",
        "入选公布日": "in_ann_date",
        "剔除公布日": "out_ann_date",
        "入选调整类型": "in_adjust_type",
        "剔除调整类型": "out_adjust_type",
    }
    schema_def = {
        "index_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "index_ts_code": {"type": "VARCHAR(15)"},
        "con_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "con_ts_code": {"type": "VARCHAR(15)"},
        "in_date": {"type": "DATE", "constraints": "NOT NULL"},
        "out_date": {"type": "DATE"},
        "in_ann_date": {"type": "DATE"},
        "out_ann_date": {"type": "DATE"},
        "latest_change_date": {"type": "DATE"},
        "member_flag": {"type": "INTEGER"},
        "in_adjust_type": {"type": "VARCHAR(80)"},
        "out_adjust_type": {"type": "VARCHAR(80)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_index_member_index", "columns": "index_code_raw"},
        {"name": "idx_tinysoft_index_member_con", "columns": "con_ts_code"},
        {"name": "idx_tinysoft_index_member_out", "columns": "out_date"},
        {"name": "idx_tinysoft_index_member_change", "columns": "latest_change_date"},
    ]
    validations = [
        (lambda df: df["index_code_raw"].notna(), "index_code_raw 不能为空"),
        (lambda df: df["con_code_raw"].notna(), "con_code_raw 不能为空"),
        (lambda df: df["in_date"].notna(), "in_date 不能为空"),
    ]
    date_fields = ("in_date", "out_date", "in_ann_date", "out_ann_date")
    numeric_fields = ("member_flag",)
    integer_fields = ("member_flag",)
    text_fields = ("index_code_raw", "con_code_raw", "in_adjust_type", "out_adjust_type")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "index_code_raw" not in df.columns and "request_code" in df.columns:
            df["index_code_raw"] = df["request_code"]
        df["index_code_raw"] = df["index_code_raw"].map(_normalize_index_code)
        if "index_ts_code" not in df.columns:
            df["index_ts_code"] = df["index_code_raw"].map(_index_code_to_ts_code)
        if "con_ts_code" not in df.columns and "con_code_raw" in df.columns:
            df["con_ts_code"] = df["con_code_raw"].map(tinysoft_symbol_to_ts_code_any)
        if "in_date" in df.columns:
            df["in_date"] = pd.to_datetime(df["in_date"], errors="coerce").dt.date
            df["in_date"] = df["in_date"].fillna(pd.Timestamp("1900-01-01").date())
        date_cols = [col for col in ("in_date", "out_date", "in_ann_date", "out_ann_date") if col in df.columns]
        if date_cols:
            as_ts = pd.DataFrame({col: pd.to_datetime(df[col], errors="coerce") for col in date_cols})
            df["latest_change_date"] = as_ts.max(axis=1).dt.date
            df["latest_change_date"] = df["latest_change_date"].fillna(df["in_date"])
        return df


@task_register()
class TinySoftIndexBasicExtTask(TinySoftIndexCodeInfoArrayTask):
    name = "tinysoft_index_basic_ext"
    description = "获取指数基本信息扩展字段（Tinysoft）"
    table_name = "index_basic_ext"
    primary_keys = ["index_code_raw"]
    date_column = None
    default_start_date = "19900101"
    default_code_batch_size = 100
    default_smart_code_batch_size = 500
    infoarray_table_id = 750
    source_table_name = "指数.指数基本信息"
    where_date_field = None
    field_mapping = {
        "StockID": "index_code_raw",
        "stockid": "index_code_raw",
        "证券代码": "index_code_raw",
        "指数代码": "index_code_raw",
        "指数简称": "short_name",
        "指数全称": "full_name",
        "指数类型": "index_type",
        "指数标的": "index_target",
        "指数所属公司": "publisher",
        "开始日期": "start_date",
        "成立日期": "found_date",
        "指数起始点数": "base_point",
        "加权方式": "weighting_method",
        "样本个数": "sample_count",
        "样本调整周期": "sample_adjust_frequency",
        "备注": "remark",
        "停用日期": "stop_date",
        "指数一级分类": "category_l1",
        "指数二级分类": "category_l2",
        "指数三级分类": "category_l3",
        "指数四级分类": "category_l4",
        "指数主代码": "main_index_code_raw",
    }
    schema_def = {
        "index_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "index_ts_code": {"type": "VARCHAR(15)"},
        "short_name": {"type": "VARCHAR(100)"},
        "full_name": {"type": "VARCHAR(200)"},
        "index_type": {"type": "VARCHAR(100)"},
        "index_target": {"type": "VARCHAR(100)"},
        "publisher": {"type": "VARCHAR(100)"},
        "start_date": {"type": "DATE"},
        "found_date": {"type": "DATE"},
        "stop_date": {"type": "DATE"},
        "latest_change_date": {"type": "DATE"},
        "base_point": {"type": "NUMERIC(20,8)"},
        "weighting_method": {"type": "VARCHAR(100)"},
        "sample_count": {"type": "INTEGER"},
        "sample_adjust_frequency": {"type": "VARCHAR(100)"},
        "category_l1": {"type": "VARCHAR(100)"},
        "category_l2": {"type": "VARCHAR(100)"},
        "category_l3": {"type": "VARCHAR(100)"},
        "category_l4": {"type": "VARCHAR(100)"},
        "main_index_code_raw": {"type": "VARCHAR(30)"},
        "main_index_ts_code": {"type": "VARCHAR(15)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_index_basic_ext_ts", "columns": "index_ts_code"},
        {"name": "idx_tinysoft_index_basic_ext_pub", "columns": "publisher"},
        {"name": "idx_tinysoft_index_basic_ext_type", "columns": "index_type"},
        {"name": "idx_tinysoft_index_basic_ext_found", "columns": "found_date"},
    ]
    validations = [
        (lambda df: df["index_code_raw"].notna(), "index_code_raw 不能为空"),
    ]
    date_fields = ("start_date", "found_date", "stop_date")
    numeric_fields = ("base_point", "sample_count")
    integer_fields = ("sample_count",)
    text_fields = (
        "index_code_raw",
        "short_name",
        "full_name",
        "index_type",
        "index_target",
        "publisher",
        "weighting_method",
        "sample_adjust_frequency",
        "category_l1",
        "category_l2",
        "category_l3",
        "category_l4",
        "main_index_code_raw",
        "remark",
    )

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "index_code_raw" not in df.columns and "request_code" in df.columns:
            df["index_code_raw"] = df["request_code"]
        if "index_code_raw" not in df.columns and "main_index_code_raw" in df.columns:
            df["index_code_raw"] = df["main_index_code_raw"]
        df["index_code_raw"] = df["index_code_raw"].map(_normalize_index_code)
        if "index_ts_code" not in df.columns:
            df["index_ts_code"] = df["index_code_raw"].map(_index_code_to_ts_code)
        if "main_index_code_raw" in df.columns:
            df["main_index_code_raw"] = df["main_index_code_raw"].map(_normalize_index_code)
            df["main_index_ts_code"] = df["main_index_code_raw"].map(_index_code_to_ts_code)
        latest = _latest_date_from_columns(df, ("start_date", "found_date", "stop_date"))
        if latest is not None:
            df["latest_change_date"] = latest
        return df


__all__ = [
    "TinySoftIndexBasicExtTask",
    "TinySoftIndexMemberVersionedTask",
    "TinySoftMarketCalendarMultiTask",
]
