"""Option dataset APIs."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd

from .specs import DatasetSpec, fetch_dataset


OPTION_BASIC_DAILY_EXT = DatasetSpec(
    name="option_basic_daily_ext",
    table_id=720,
    source_table_name="期权.期权基本信息",
    date_field="截止日",
    code_pool="option",
    code_batch_size=1000,
    field_mapping={
        "StockID": "source_code",
        "stockid": "source_code",
        "截止日": "trade_date",
        "合约交易代码": "contract_trade_code",
        "合约简称": "contract_short_name",
        "标的证券代码": "underlying_code_raw",
        "标的证券名称": "underlying_name",
        "行权方式": "exercise_style",
        "期权类型": "option_type",
        "合约单位": "contract_unit",
        "行权价": "exercise_price",
        "首个交易日": "first_trade_date",
        "最后交易日": "last_trade_date",
        "行权日": "exercise_date",
        "到期日": "maturity_date",
        "合约未平仓数": "open_interest",
        "合约前收盘价": "pre_close",
        "期权合约状态信息": "contract_status",
        "开仓状态": "open_position_status",
        "上市地": "exchange_name",
    },
    date_columns=("trade_date", "first_trade_date", "last_trade_date", "exercise_date", "maturity_date"),
    numeric_columns=("contract_unit", "exercise_price", "open_interest", "pre_close"),
)


def option_basic_daily_ext(
    codes: Optional[Iterable[Any]] = None,
    trade_date: Any = None,
    start_date: Any = None,
    end_date: Any = None,
    refresh: bool = False,
) -> pd.DataFrame:
    return fetch_dataset(
        OPTION_BASIC_DAILY_EXT,
        codes=codes,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        refresh=refresh,
    )
