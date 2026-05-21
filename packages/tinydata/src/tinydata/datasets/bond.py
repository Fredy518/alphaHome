"""Bond dataset APIs."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd

from .specs import DatasetSpec, fetch_dataset


BOND_BASIC_EXT = DatasetSpec(
    name="bond_basic_ext",
    table_id=502,
    source_table_name="债券.基本信息",
    code_pool="bond",
    code_batch_size=2000,
    field_mapping={
        "StockID": "source_code",
        "stockid": "source_code",
        "债券代码": "bond_code_raw",
        "债券全称": "bond_full_name",
        "债券简称": "bond_short_name",
        "发行年度": "issue_year",
        "发行起始日": "issue_start_date",
        "发行截止日": "issue_end_date",
        "发行额": "issue_amount",
        "发行价格": "issue_price",
        "票面利率(%)": "coupon_rate_pct",
        "计息日": "interest_start_date",
        "上市日": "list_date",
        "到期日": "maturity_date",
        "上市地点": "list_location",
        "正股代码": "underlying_code_raw",
        "信用等级": "credit_rating",
        "债券主体名称": "issuer_name",
    },
    date_columns=(
        "issue_start_date",
        "issue_end_date",
        "interest_start_date",
        "list_date",
        "maturity_date",
    ),
    numeric_columns=("issue_amount", "issue_price", "coupon_rate_pct"),
    integer_columns=("issue_year",),
)


def bond_basic_ext(codes: Optional[Iterable[Any]] = None, refresh: bool = False) -> pd.DataFrame:
    return fetch_dataset(BOND_BASIC_EXT, codes=codes, refresh=refresh)
