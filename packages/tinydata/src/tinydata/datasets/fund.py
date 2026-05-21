"""Fund dataset APIs."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd

from .specs import DatasetSpec, fetch_dataset


FUND_BASIC = DatasetSpec(
    name="fund_basic",
    table_id=302,
    source_table_name="基金.基金基本信息",
    allow_full_table=False,
    code_pool="fund",
    code_batch_size=1000,
    field_mapping={
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "证券代码": "tsl_code",
        "基金名称": "fund_name",
        "基金简称": "fund_short_name",
        "基金类型": "fund_type",
        "交易方式": "trade_mode",
        "投资类型": "invest_type",
        "设立日": "found_date",
        "上市日": "list_date",
        "清算日": "liquidation_date",
        "基金管理人": "management",
        "基金托管人": "custodian",
        "业绩比较基准": "benchmark",
        "母基金代码": "parent_fund_code_raw",
    },
    date_columns=("found_date", "list_date", "liquidation_date"),
)

FUND_MANAGER = DatasetSpec(
    name="fund_manager",
    table_id=308,
    source_table_name="基金.基金经理",
    date_field="公布日",
    code_pool="fund",
    code_batch_size=1000,
    field_mapping={
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "公布日": "ann_date",
        "姓名": "manager_name",
        "任职日": "begin_date",
        "离职日": "end_date",
        "基金经理代码": "manager_code",
        "简历": "resume",
    },
    date_columns=("ann_date", "begin_date", "end_date"),
)

FUND_FOF_HOLDING_DETAIL = DatasetSpec(
    name="fund_fof_holding_detail",
    table_id=349,
    source_table_name="基金.基金明细",
    date_field="截止日",
    code_pool="fof_fund",
    code_batch_size=500,
    field_mapping={
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "名称": "holding_name",
        "代码": "holding_code_raw",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
        "是否属于关联基金": "is_related_fund",
    },
    date_columns=("report_date",),
    numeric_columns=("quantity", "market_value", "nav_ratio_pct", "rank_no"),
    integer_columns=("rank_no",),
)

FUND_STOCK_HOLDING = DatasetSpec(
    name="fund_stock_holding",
    table_id=318,
    source_table_name="基金.持股明细",
    date_field="截止日",
    code_pool="fund",
    code_batch_size=500,
    field_mapping={
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "代码": "security_code_raw",
        "名称": "security_name",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
    },
    date_columns=("report_date", "ann_date"),
    numeric_columns=("quantity", "market_value", "nav_ratio_pct", "rank_no"),
    integer_columns=("rank_no",),
)

FUND_BOND_HOLDING = DatasetSpec(
    name="fund_bond_holding",
    table_id=342,
    source_table_name="基金.持债明细",
    date_field="截止日",
    code_pool="fund",
    code_batch_size=500,
    field_mapping={
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "名称": "bond_name",
        "代码": "bond_code_raw",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
        "债券类型": "bond_type",
    },
    date_columns=("report_date", "ann_date"),
    numeric_columns=("quantity", "market_value", "nav_ratio_pct", "rank_no"),
    integer_columns=("rank_no",),
)


def fund_basic(codes: Optional[Iterable[Any]] = None, refresh: bool = False) -> pd.DataFrame:
    return fetch_dataset(FUND_BASIC, codes=codes, refresh=refresh)


def fund_manager(
    codes: Optional[Iterable[Any]] = None,
    start_date: Any = None,
    end_date: Any = None,
    refresh: bool = False,
) -> pd.DataFrame:
    return fetch_dataset(FUND_MANAGER, codes=codes, start_date=start_date, end_date=end_date, refresh=refresh)


def fund_fof_holding_detail(
    codes: Optional[Iterable[Any]] = None,
    report_period: Any = None,
    start_date: Any = None,
    end_date: Any = None,
    refresh: bool = False,
) -> pd.DataFrame:
    return fetch_dataset(
        FUND_FOF_HOLDING_DETAIL,
        codes=codes,
        report_period=report_period,
        start_date=start_date,
        end_date=end_date,
        refresh=refresh,
    )


def fund_stock_holding(
    codes: Optional[Iterable[Any]] = None,
    report_period: Any = None,
    start_date: Any = None,
    end_date: Any = None,
    refresh: bool = False,
) -> pd.DataFrame:
    return fetch_dataset(
        FUND_STOCK_HOLDING,
        codes=codes,
        report_period=report_period,
        start_date=start_date,
        end_date=end_date,
        refresh=refresh,
    )


def fund_bond_holding(
    codes: Optional[Iterable[Any]] = None,
    report_period: Any = None,
    start_date: Any = None,
    end_date: Any = None,
    refresh: bool = False,
) -> pd.DataFrame:
    return fetch_dataset(
        FUND_BOND_HOLDING,
        codes=codes,
        report_period=report_period,
        start_date=start_date,
        end_date=end_date,
        refresh=refresh,
    )
