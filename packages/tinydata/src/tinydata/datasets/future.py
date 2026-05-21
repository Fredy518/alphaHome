"""Futures dataset APIs."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd

from .specs import DatasetSpec, fetch_dataset


FUTURE_BASIC_EXT = DatasetSpec(
    name="future_basic_ext",
    table_id=703,
    source_table_name="期货.期货基本信息",
    date_field="变动日",
    code_pool="future",
    code_batch_size=1000,
    field_mapping={
        "StockID": "source_code",
        "stockid": "source_code",
        "合约代码": "contract_code_raw",
        "变动日": "change_date",
        "交易代码": "product_code",
        "交割年份": "delivery_year",
        "交割月份": "delivery_month",
        "交易品种": "product_name",
        "合约乘数": "contract_multiplier",
        "报价单位": "quote_unit",
        "最小变动价位": "min_price_change",
        "最后交易日": "last_trade_date",
        "最后交割日": "last_delivery_date",
        "交割方式": "delivery_method",
        "上市地": "exchange_name",
    },
    date_columns=("change_date", "last_trade_date", "last_delivery_date"),
    numeric_columns=("contract_multiplier", "min_price_change"),
    integer_columns=("delivery_year", "delivery_month"),
)


def future_basic_ext(codes: Optional[Iterable[Any]] = None, refresh: bool = False) -> pd.DataFrame:
    return fetch_dataset(FUTURE_BASIC_EXT, codes=codes, refresh=refresh)
