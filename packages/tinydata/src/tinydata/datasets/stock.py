"""Stock dataset APIs."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd

from .specs import DatasetSpec, fetch_dataset


STOCK_BASIC_EXT = DatasetSpec(
    name="stock_basic_ext",
    table_id=10,
    source_table_name="股票.基本信息",
    code_pool="stock",
    code_batch_size=2000,
    field_mapping={
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "A股代码": "a_share_code",
        "公司中文全称": "company_full_name",
        "公司中文简称": "company_short_name",
        "注册资本": "registered_capital",
        "法定代表人": "legal_representative",
        "成立日期": "establish_date",
        "主营业务": "main_business",
        "地域": "area",
        "股票种类": "stock_type",
        "当前状态": "current_status",
        "上市地": "list_location",
        "所属市场": "market",
        "申万一级行业": "sw_industry_l1",
        "申万二级行业": "sw_industry_l2",
        "申万三级行业": "sw_industry_l3",
    },
    date_columns=("establish_date",),
    numeric_columns=("registered_capital",),
)


def stock_basic_ext(codes: Optional[Iterable[Any]] = None, refresh: bool = False) -> pd.DataFrame:
    return fetch_dataset(STOCK_BASIC_EXT, codes=codes, refresh=refresh)
