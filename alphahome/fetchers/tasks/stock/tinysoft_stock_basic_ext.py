#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft stock basic P0 task."""

from __future__ import annotations

from typing import List

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import get_row_value, ts_code_to_tinysoft_symbol_any
from .tinysoft_stock_p0_base import TinySoftStockSymbolInfoArrayTask

@task_register()
class TinySoftStockBasicExtTask(TinySoftStockSymbolInfoArrayTask):
    name = "tinysoft_stock_basic_ext"
    description = "获取股票基本信息扩展字段（Tinysoft）"
    table_name = "stock_basic_ext"
    primary_keys = ["ts_code"]
    date_column = None
    default_start_date = "19901219"
    smart_refresh_interval_days = 30
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


__all__ = ["TinySoftStockBasicExtTask"]
