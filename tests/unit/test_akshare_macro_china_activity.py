from __future__ import annotations

from datetime import date

import pandas as pd

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.tasks.macro.akshare_macro_china_activity import (
    AkShareMacroFixedAssetInvestmentTask,
    AkShareMacroIndustrialValueAddedTask,
    AkShareMacroRetailSalesTask,
)


class _MockDB:
    async def get_column_names(self, target):
        return []

    async def fetch(self, query, *args, **kwargs):
        return []

    async def table_exists(self, target):
        return False


def _pipeline(task, raw):
    transformed = task.data_transformer.process_data(raw.copy())
    return task.process_data(transformed)


def test_fixed_asset_investment_matches_legacy_monthly_yoy_contract():
    task = AkShareMacroFixedAssetInvestmentTask(
        db_connection=_MockDB(), update_type=UpdateTypes.FULL
    )
    raw = pd.DataFrame(
        [
            {
                "月份": "2024年07月份",
                "当月": 42220,
                "同比增长": -1.32,
                "环比增长": -26.43,
                "自年初累计": 287611,
            },
            {
                "月份": "2024年08月份",
                "当月": 41774,
                "同比增长": 1.53,
                "环比增长": -1.06,
                "自年初累计": 329385,
            },
        ]
    )
    result = _pipeline(task, raw)
    assert result["period_end_date"].tolist() == [date(2024, 7, 31), date(2024, 8, 31)]
    assert result["monthly_yoy"].tolist() == [-1.32, 1.53]


def test_industrial_source_date_is_not_treated_as_release_date():
    task = AkShareMacroIndustrialValueAddedTask(
        db_connection=_MockDB(), update_type=UpdateTypes.FULL
    )
    raw = pd.DataFrame(
        [
            {
                "月份": "2025年05月份",
                "同比增长": 5.8,
                "累计增长": 6.3,
                "发布时间": "2025-05-01",
            }
        ]
    )
    result = _pipeline(task, raw)
    assert result.iloc[0]["period_end_date"] == date(2025, 5, 31)
    assert result.iloc[0]["period_source_date"] == date(2025, 5, 1)
    assert "release_date" not in result.columns


def test_retail_sales_manual_window_and_consumer_vote_field():
    task = AkShareMacroRetailSalesTask(
        db_connection=_MockDB(),
        update_type=UpdateTypes.MANUAL,
        start_date="2025-05-01",
        end_date="2025-05-31",
    )
    task._effective_start_date = "20250501"
    task._effective_end_date = "20250531"
    raw = pd.DataFrame(
        [
            {
                "月份": "2025年04月份",
                "当月": 37174,
                "同比增长": 5.1,
                "环比增长": 1.0,
                "累计": 161000,
                "累计-同比增长": 4.7,
            },
            {
                "月份": "2025年05月份",
                "当月": 41326,
                "同比增长": 6.4,
                "环比增长": 11.2,
                "累计": 202326,
                "累计-同比增长": 5.0,
            },
        ]
    )
    result = _pipeline(task, raw)
    assert len(result) == 1
    assert result.iloc[0]["period_end_date"] == date(2025, 5, 31)
    assert result.iloc[0]["monthly_yoy"] == 6.4
    assert "tot_retail_sales_yoy" in task.schema_def["monthly_yoy"]["comment"]
