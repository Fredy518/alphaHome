#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft bond-side basic extension tables."""

from __future__ import annotations

from typing import Any, List

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import (
    TinySoftP0InfoArrayTask,
    clean_text,
    get_row_value,
    tinysoft_symbol_to_ts_code_any,
    ts_code_to_tinysoft_symbol_any,
)


def _bond_code_to_tinysoft_symbol(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    raw = text.upper()
    if raw.startswith(("SH", "SZ")):
        return raw
    mapped = ts_code_to_tinysoft_symbol_any(raw)
    if mapped:
        return mapped
    return raw


@task_register()
class TinySoftBondBasicExtTask(TinySoftP0InfoArrayTask):
    name = "tinysoft_bond_basic_ext"
    description = "获取债券基本信息扩展字段（Tinysoft）"
    table_name = "bond_basic_ext"
    domain = "cbond"
    primary_keys = ["bond_code_raw"]
    date_column = None
    default_start_date = "19900101"
    default_code_batch_size = 500
    default_smart_code_batch_size = 2000
    code_config_keys = ("bond_codes", "ts_codes", "ts_code", "codes")
    code_column = "bond_code_raw"
    infoarray_table_id = 502
    source_table_name = "债券.基本信息"
    where_date_field = None
    default_symbol_source_tables = [
        "tinysoft.fund_bond_holding_detail",
        "tinysoft.fund_cbond_holding_detail",
        "rawdata.fund_bond_holding_detail",
        "rawdata.fund_cbond_holding_detail",
        "tushare.cbond_basic",
        "rawdata.cbond_basic",
    ]
    field_mapping = {
        "StockID": "source_code",
        "stockid": "source_code",
        "债券代码": "bond_code_raw",
        "债券全称": "bond_full_name",
        "债券简称": "bond_short_name",
        "英文全称": "bond_full_name_en",
        "发行年度": "issue_year",
        "发行起始日": "issue_start_date",
        "发行截止日": "issue_end_date",
        "发行额": "issue_amount",
        "发行数量": "issue_volume",
        "发行价格": "issue_price",
        "面额": "par_value",
        "凭证类别": "certificate_type",
        "利率品种": "rate_type",
        "债券种类": "bond_type",
        "计息方式": "interest_calc_method",
        "偿还年限": "maturity_years",
        "票面利率(%)": "coupon_rate_pct",
        "基准利率代码": "base_rate_code",
        "基本利差(%)": "base_spread_pct",
        "递进利率(%)": "step_rate_pct",
        "付息方式": "interest_payment_method",
        "付息说明": "interest_payment_desc",
        "付息频率": "interest_payment_frequency",
        "计息日": "interest_start_date",
        "上市日": "list_date",
        "到期日": "maturity_date",
        "兑付起始日": "redeem_start_date",
        "兑付截止日": "redeem_end_date",
        "上市地点": "list_location",
        "发行对象": "issue_target",
        "备注": "remark",
        "正股代码": "underlying_code_raw",
        "上交所债券代码": "sh_bond_code",
        "深交所债券代码": "sz_bond_code",
        "银行间债券代码": "interbank_bond_code",
        "信用等级": "credit_rating",
        "外部信用担保方式": "external_guarantee_method",
        "债券类别(评价用)": "eval_bond_category",
        "债券评级(评价用)": "eval_bond_rating",
        "正股所属申万一级行业": "underlying_sw_industry_l1",
        "正股所属证监会一级行业": "underlying_csrc_industry_l1",
        "债券主体代码": "issuer_code",
        "债券主体名称": "issuer_name",
        "债券所属行业": "bond_industry",
        "是否含权债": "is_option_embedded",
        "是否可提前兑付": "is_early_redeemable",
        "债券主体性质": "issuer_nature",
        "实际到期日": "actual_maturity_date",
        "摘牌日": "delist_date",
        "停止交易日": "stop_trade_date",
        "转股开始日": "convert_start_date",
        "停止转股日": "convert_end_date",
    }
    schema_def = {
        "bond_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "bond_ts_code": {"type": "VARCHAR(20)"},
        "source_code": {"type": "VARCHAR(30)"},
        "bond_full_name": {"type": "VARCHAR(255)"},
        "bond_short_name": {"type": "VARCHAR(120)"},
        "bond_full_name_en": {"type": "VARCHAR(255)"},
        "issue_year": {"type": "INTEGER"},
        "issue_start_date": {"type": "DATE"},
        "issue_end_date": {"type": "DATE"},
        "issue_amount": {"type": "NUMERIC(24,4)"},
        "issue_volume": {"type": "NUMERIC(24,4)"},
        "issue_price": {"type": "NUMERIC(20,8)"},
        "par_value": {"type": "NUMERIC(20,8)"},
        "certificate_type": {"type": "VARCHAR(80)"},
        "rate_type": {"type": "VARCHAR(80)"},
        "bond_type": {"type": "VARCHAR(120)"},
        "interest_calc_method": {"type": "VARCHAR(120)"},
        "maturity_years": {"type": "NUMERIC(16,6)"},
        "coupon_rate_pct": {"type": "NUMERIC(20,8)"},
        "base_rate_code": {"type": "VARCHAR(50)"},
        "base_spread_pct": {"type": "NUMERIC(20,8)"},
        "step_rate_pct": {"type": "NUMERIC(20,8)"},
        "interest_payment_method": {"type": "VARCHAR(120)"},
        "interest_payment_desc": {"type": "TEXT"},
        "interest_payment_frequency": {"type": "NUMERIC(16,6)"},
        "interest_start_date": {"type": "DATE"},
        "list_date": {"type": "DATE"},
        "maturity_date": {"type": "DATE"},
        "actual_maturity_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "stop_trade_date": {"type": "DATE"},
        "redeem_start_date": {"type": "DATE"},
        "redeem_end_date": {"type": "DATE"},
        "list_location": {"type": "VARCHAR(120)"},
        "issue_target": {"type": "VARCHAR(255)"},
        "remark": {"type": "TEXT"},
        "underlying_code_raw": {"type": "VARCHAR(30)"},
        "underlying_ts_code": {"type": "VARCHAR(15)"},
        "sh_bond_code": {"type": "VARCHAR(30)"},
        "sz_bond_code": {"type": "VARCHAR(30)"},
        "interbank_bond_code": {"type": "VARCHAR(50)"},
        "credit_rating": {"type": "VARCHAR(50)"},
        "external_guarantee_method": {"type": "VARCHAR(120)"},
        "eval_bond_category": {"type": "VARCHAR(120)"},
        "eval_bond_rating": {"type": "VARCHAR(120)"},
        "underlying_sw_industry_l1": {"type": "VARCHAR(120)"},
        "underlying_csrc_industry_l1": {"type": "VARCHAR(120)"},
        "issuer_code": {"type": "VARCHAR(50)"},
        "issuer_name": {"type": "VARCHAR(255)"},
        "bond_industry": {"type": "VARCHAR(120)"},
        "is_option_embedded": {"type": "VARCHAR(30)"},
        "is_early_redeemable": {"type": "VARCHAR(30)"},
        "issuer_nature": {"type": "VARCHAR(120)"},
        "convert_start_date": {"type": "DATE"},
        "convert_end_date": {"type": "DATE"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_bond_basic_ext_ts", "columns": "bond_ts_code"},
        {"name": "idx_tinysoft_bond_basic_ext_name", "columns": "bond_short_name"},
        {"name": "idx_tinysoft_bond_basic_ext_maturity", "columns": "maturity_date"},
        {"name": "idx_tinysoft_bond_basic_ext_rating", "columns": "credit_rating"},
    ]
    validations = [
        (lambda df: df["bond_code_raw"].notna(), "bond_code_raw 不能为空"),
    ]
    date_fields = (
        "issue_start_date",
        "issue_end_date",
        "interest_start_date",
        "list_date",
        "maturity_date",
        "actual_maturity_date",
        "delist_date",
        "stop_trade_date",
        "redeem_start_date",
        "redeem_end_date",
        "convert_start_date",
        "convert_end_date",
    )
    numeric_fields = (
        "issue_year",
        "issue_amount",
        "issue_volume",
        "issue_price",
        "par_value",
        "maturity_years",
        "coupon_rate_pct",
        "base_spread_pct",
        "step_rate_pct",
        "interest_payment_frequency",
    )
    integer_fields = ("issue_year",)
    text_fields = (
        "bond_code_raw",
        "bond_ts_code",
        "source_code",
        "bond_full_name",
        "bond_short_name",
        "bond_full_name_en",
        "certificate_type",
        "rate_type",
        "bond_type",
        "interest_calc_method",
        "base_rate_code",
        "interest_payment_method",
        "interest_payment_desc",
        "list_location",
        "issue_target",
        "remark",
        "underlying_code_raw",
        "underlying_ts_code",
        "sh_bond_code",
        "sz_bond_code",
        "interbank_bond_code",
        "credit_rating",
        "external_guarantee_method",
        "eval_bond_category",
        "eval_bond_rating",
        "underlying_sw_industry_l1",
        "underlying_csrc_industry_l1",
        "issuer_code",
        "issuer_name",
        "bond_industry",
        "is_option_embedded",
        "is_early_redeemable",
        "issuer_nature",
        "source_table_name",
    )

    def _get_codes_from_mapping(self, params: dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        return list(dict.fromkeys(code for code in (_bond_code_to_tinysoft_symbol(x) for x in raw_codes) if code))

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
                candidates = [
                    col
                    for col in ("bond_code_raw", "bond_ts_code", "cbond_code_raw", "cbond_ts_code", "ts_code")
                    if col in columns
                ]
                if not candidates:
                    continue
                selects = ", ".join(f'"{col}"' for col in candidates)
                rows = await self.db.fetch(f'SELECT DISTINCT {selects} FROM "{schema}"."{table_name}"')
                for row in rows or []:
                    for col in candidates:
                        code = _bond_code_to_tinysoft_symbol(get_row_value(row, col))
                        if code:
                            codes.append(code)
                            break
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载债券代码失败: %s", table, e)
        return list(dict.fromkeys(codes))

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "source_code" not in df.columns and "request_code" in df.columns:
            df["source_code"] = df["request_code"]
        if "bond_code_raw" not in df.columns and "source_code" in df.columns:
            df["bond_code_raw"] = df["source_code"]
        df["bond_code_raw"] = df["bond_code_raw"].map(clean_text)
        if "bond_ts_code" not in df.columns:
            df["bond_ts_code"] = df["bond_code_raw"].map(tinysoft_symbol_to_ts_code_any)
        if "underlying_code_raw" in df.columns:
            df["underlying_ts_code"] = df["underlying_code_raw"].map(tinysoft_symbol_to_ts_code_any)
        return df


__all__ = ["TinySoftBondBasicExtTask"]
