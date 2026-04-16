#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date

import pandas as pd
import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.tasks.macro.akshare_macro_china_market_margin import (
    AkShareMacroChinaMarketMarginSHTask,
    AkShareMacroChinaMarketMarginSZTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_china_nbs_nation import (
    AkShareMacroChinaNBSNationTask,
    _parse_period_end,
)
from alphahome.fetchers.tasks.macro.akshare_macro_china_rmb_fixing import (
    AkShareMacroChinaRmbFixingTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_ths_rmb_deposit import (
    AkShareMacroThsRmbDepositTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_ths_rmb_loan import (
    AkShareMacroThsRmbLoanTask,
)


class _MockDB:
    async def get_column_names(self, target):
        return []

    async def fetch(self, query, *args, **kwargs):
        return []

    async def table_exists(self, target):
        return False


def _run_akshare_pipeline(task, raw_df: pd.DataFrame) -> pd.DataFrame:
    transformed = task.data_transformer.process_data(raw_df.copy())
    return task.process_data(transformed)


def test_rmb_loan_process_data_converts_percent_and_deduplicates():
    task = AkShareMacroThsRmbLoanTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {
                "月份": "2024-01",
                "新增人民币贷款-总额": 1000.0,
                "新增人民币贷款-同比": "10.50%",
                "新增人民币贷款-环比": "-2.00%",
                "累计人民币贷款-总额": 3000.0,
                "累计人民币贷款-同比": "11.00%",
            },
            {
                "月份": "2024-01",
                "新增人民币贷款-总额": 1200.0,
                "新增人民币贷款-同比": "12.50%",
                "新增人民币贷款-环比": "1.00%",
                "累计人民币贷款-总额": 3200.0,
                "累计人民币贷款-同比": "12.00%",
            },
            {
                "月份": "2024-02",
                "新增人民币贷款-总额": 1500.0,
                "新增人民币贷款-同比": "--",
                "新增人民币贷款-环比": "3.25%",
                "累计人民币贷款-总额": 4700.0,
                "累计人民币贷款-同比": "12.20%",
            },
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 2
    january_row = processed.loc[processed["month_end_date"] == date(2024, 1, 31)].iloc[0]
    february_row = processed.loc[processed["month_end_date"] == date(2024, 2, 29)].iloc[0]

    assert january_row["new_loan_total"] == 1200.0
    assert january_row["new_loan_yoy"] == 12.5
    assert january_row["new_loan_mom"] == 1.0
    assert pd.isna(february_row["new_loan_yoy"])
    assert february_row["loan_yoy"] == 12.2


def test_rmb_deposit_process_data_applies_manual_window():
    task = AkShareMacroThsRmbDepositTask(
        db_connection=_MockDB(),
        update_type=UpdateTypes.MANUAL,
        start_date="2024-02-01",
        end_date="2024-02-29",
    )
    raw_df = pd.DataFrame(
        [
            {
                "月份": "2024-01",
                "新增存款-数量": 100.0,
                "新增存款-同比": "1.50%",
                "新增存款-环比": "2.00%",
                "新增企业存款-数量": 10.0,
                "新增企业存款-同比": "3.00%",
                "新增企业存款-环比": "4.00%",
                "新增储蓄存款-数量": 20.0,
                "新增储蓄存款-同比": "5.00%",
                "新增储蓄存款-环比": "6.00%",
                "新增其他存款-数量": 30.0,
                "新增其他存款-同比": "7.00%",
                "新增其他存款-环比": "8.00%",
            },
            {
                "月份": "2024-02",
                "新增存款-数量": 200.0,
                "新增存款-同比": "2.50%",
                "新增存款-环比": "3.00%",
                "新增企业存款-数量": 11.0,
                "新增企业存款-同比": "3.50%",
                "新增企业存款-环比": "4.50%",
                "新增储蓄存款-数量": 21.0,
                "新增储蓄存款-同比": "5.50%",
                "新增储蓄存款-环比": "6.50%",
                "新增其他存款-数量": 31.0,
                "新增其他存款-同比": "7.50%",
                "新增其他存款-环比": "8.50%",
            },
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["month_end_date"] == date(2024, 2, 29)
    assert row["new_deposit_total"] == 200.0
    assert row["new_deposit_other_mom"] == 8.5


def test_market_margin_indexes_are_unique_across_tasks():
    index_names = [
        item["name"]
        for item in (
            AkShareMacroChinaMarketMarginSHTask.indexes
            + AkShareMacroChinaMarketMarginSZTask.indexes
        )
    ]

    assert len(index_names) == len(set(index_names))


def test_market_margin_process_data_normalizes_dates_and_deduplicates():
    task = AkShareMacroChinaMarketMarginSHTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {
                "日期": "2024-04-01",
                "融资买入额": 10,
                "融资余额": 20,
                "融券卖出量": 30,
                "融券余量": 40,
                "融券余额": 50,
                "融资融券余额": 60,
            },
            {
                "日期": "2024-04-01",
                "融资买入额": 11,
                "融资余额": 21,
                "融券卖出量": 31,
                "融券余量": 41,
                "融券余额": 51,
                "融资融券余额": 61,
            },
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["date"] == date(2024, 4, 1)
    assert row["financing_buy"] == 11.0
    assert row["margin_balance"] == 61.0


def test_rmb_fixing_process_data_melts_and_filters_unknown_metrics():
    task = AkShareMacroChinaRmbFixingTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {
                "日期": "2024-04-01",
                "美元/人民币_中间价": 7.1,
                "人民币/泰铢_定价": 5.0,
                "备注": "ignore",
            },
            {
                "日期": "2024-04-02",
                "美元/人民币_中间价": 7.2,
                "人民币/泰铢_定价": 5.1,
                "备注": "ignore",
            },
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 4
    assert set(processed["metric"]) == {"fix"}
    assert set(processed["pair"]) == {"美元/人民币", "人民币/泰铢"}
    assert processed["date"].tolist() == [
        date(2024, 4, 1),
        date(2024, 4, 2),
        date(2024, 4, 1),
        date(2024, 4, 2),
    ]


def test_nbs_parse_period_end_handles_cumulative_month_ranges():
    assert _parse_period_end("2024年1-2月") == date(2024, 2, 29)
    assert _parse_period_end("2024年1-2月份") == date(2024, 2, 29)
    assert _parse_period_end("2024年03月") == date(2024, 3, 31)


class _StubNBSAPI:
    async def call(self, func_name, kind, path, period, stop_event=None):
        assert func_name == "macro_china_nbs_nation"
        assert kind == "月度数据"
        assert path == "工业>增加值"
        assert period == "LAST10"
        return pd.DataFrame(
            {
                "2024年1-2月份": [5.2],
                "2024年03月": [5.8],
            },
            index=pd.Index(["工业增加值同比"], name=None),
        )


@pytest.mark.asyncio
async def test_nbs_fetch_batch_melts_data_and_filters_manual_window():
    task = AkShareMacroChinaNBSNationTask(
        db_connection=_MockDB(),
        api=_StubNBSAPI(),
        update_type=UpdateTypes.MANUAL,
        start_date="2024-02-01",
        end_date="2024-02-29",
        task_config={
            "series": [
                {
                    "id": "industry_growth",
                    "kind": "月度数据",
                    "path": "工业>增加值",
                    "period": "LAST10",
                }
            ]
        },
    )

    processed = await task.fetch_batch(
        {
            "series_id": "industry_growth",
            "kind": "月度数据",
            "path": "工业>增加值",
            "period": "LAST10",
        }
    )

    assert len(processed) == 1
    row = processed.iloc[0]
    assert row["series_id"] == "industry_growth"
    assert row["indicator"] == "工业增加值同比"
    assert row["period_label"] == "2024年1-2月份"
    assert row["period_end_date"] == date(2024, 2, 29)
    assert row["value"] == 5.2
