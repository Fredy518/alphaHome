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
from alphahome.fetchers.tasks.macro.akshare_macro_china_rmb_fixing import (
    AkShareMacroChinaRmbFixingTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_lpr import AkShareMacroLprTask
from alphahome.fetchers.tasks.macro.akshare_macro_repo_rate import (
    AkShareMacroRepoRateTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_ths_rmb_deposit import (
    AkShareMacroThsRmbDepositTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_ths_rmb_loan import (
    AkShareMacroThsRmbLoanTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_usa_cpi import (
    AkShareMacroUsaCpiTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_usa_fed_decision import (
    AkShareMacroUsaFedDecisionTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_core_pce import (
    AkShareMacroCorePceTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_usa_nonfarm import (
    AkShareMacroUsaNonfarmTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_usa_unemployment import (
    AkShareMacroUsaUnemploymentTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_ecb_rate import (
    AkShareMacroEcbRateTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_boj_rate import (
    AkShareMacroBojRateTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_money_supply import (
    AkShareMacroMoneySupplyTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_cn_cb_balance import (
    AkShareMacroCnCbBalanceTask,
)
from alphahome.fetchers.tasks.macro.akshare_macro_cci import AkShareMacroCciTask


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


def test_usa_cpi_attributes():
    assert AkShareMacroUsaCpiTask.name == "akshare_macro_usa_cpi"
    assert AkShareMacroUsaCpiTask.table_name == "macro_usa_cpi"
    assert AkShareMacroUsaCpiTask.primary_keys == ["date"]
    assert AkShareMacroUsaCpiTask.api_name == "macro_usa_cpi_yoy"
    assert AkShareMacroUsaCpiTask.data_source == "akshare"


def test_usa_cpi_process_data_drops_unreleased_and_deduplicates():
    task = AkShareMacroUsaCpiTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {"时间": "2024-04-01", "发布日期": "2024-05-15", "现值": 3.4, "前值": 3.5},
            {"时间": "2024-05-01", "发布日期": "2024-06-12", "现值": 3.3, "前值": 3.4},
            # 未发布月份：现值为 NaN，应被丢弃
            {"时间": "2024-06-01", "发布日期": "2024-07-11", "现值": None, "前值": 3.3},
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 2
    assert processed["date"].tolist() == [date(2024, 4, 1), date(2024, 5, 1)]
    assert processed["release_date"].tolist() == [date(2024, 5, 15), date(2024, 6, 12)]
    assert processed["cpi_yoy"].tolist() == [3.4, 3.3]
    assert processed["cpi_prev_yoy"].tolist() == [3.5, 3.4]


def test_lpr_attributes():
    assert AkShareMacroLprTask.name == "akshare_macro_lpr"
    assert AkShareMacroLprTask.table_name == "macro_policy_rate"
    assert AkShareMacroLprTask.primary_keys == ["date"]
    assert "mlf_1y" in AkShareMacroLprTask.schema_def  # 预留 MLF 列


def test_lpr_process_data_adds_mlf_placeholder_and_deduplicates():
    task = AkShareMacroLprTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            # 2019 年后：LPR 有值，基准利率列也有值
            {"TRADE_DATE": "2024-05-20", "LPR1Y": 3.1, "LPR5Y": 3.6, "RATE_1": 4.35, "RATE_2": 4.9},
            # 重复行（去重保留最后一条）
            {"TRADE_DATE": "2024-05-20", "LPR1Y": 3.1, "LPR5Y": 3.6, "RATE_1": 4.35, "RATE_2": 4.9},
            # 2019 年前：LPR 为 NaN，仅有基准贷款利率
            {"TRADE_DATE": "2015-03-01", "LPR1Y": None, "LPR5Y": None, "RATE_1": 5.35, "RATE_2": 5.9},
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 2  # 去重后两行
    may_row = processed.loc[processed["date"] == date(2024, 5, 20)].iloc[0]
    old_row = processed.loc[processed["date"] == date(2015, 3, 1)].iloc[0]
    assert may_row["lpr_1y"] == 3.1
    assert may_row["lpr_5y"] == 3.6
    assert pd.isna(old_row["lpr_1y"])
    assert old_row["benchmark_loan_1y"] == 5.35
    # MLF 预留列恒为 NULL
    assert pd.isna(may_row["mlf_1y"])
    assert pd.isna(old_row["mlf_1y"])


def test_repo_rate_attributes():
    assert AkShareMacroRepoRateTask.name == "akshare_macro_repo_rate"
    assert AkShareMacroRepoRateTask.table_name == "macro_repo_rate"
    assert AkShareMacroRepoRateTask.primary_keys == ["date"]
    assert AkShareMacroRepoRateTask.api_name == "repo_rate_query"
    assert AkShareMacroRepoRateTask.api_params == {"symbol": "回购定盘利率"}


def test_repo_rate_process_data_converts_types_and_deduplicates():
    task = AkShareMacroRepoRateTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {"date": "2024-04-01", "FR001": 1.85, "FR007": 2.05, "FR014": 2.15},
            {"date": "2024-04-01", "FR001": 1.86, "FR007": 2.06, "FR014": 2.16},
            {"date": "2024-04-02", "FR001": 1.90, "FR007": 2.10, "FR014": 2.20},
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 2  # 4-01 去重保留最后
    assert processed["date"].tolist() == [date(2024, 4, 1), date(2024, 4, 2)]
    assert processed["fr007"].tolist() == [2.06, 2.10]
    assert processed["fr001"].tolist() == [1.86, 1.90]


def test_usa_fed_decision_attributes():
    assert AkShareMacroUsaFedDecisionTask.name == "akshare_macro_usa_fed_decision"
    assert AkShareMacroUsaFedDecisionTask.table_name == "macro_fed_decision"
    assert AkShareMacroUsaFedDecisionTask.primary_keys == ["date"]
    assert AkShareMacroUsaFedDecisionTask.api_name == "macro_bank_usa_interest_rate"


def test_usa_fed_decision_process_data_drops_unreleased_and_deduplicates():
    task = AkShareMacroUsaFedDecisionTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {"商品": "美联储利率决议报告", "日期": "2024-06-12", "今值": 5.25, "预测值": 5.25, "前值": 5.25},
            {"商品": "美联储利率决议报告", "日期": "2024-07-31", "今值": 5.50, "预测值": 5.50, "前值": 5.25},
            # 重复行（去重保留最后一条）
            {"商品": "美联储利率决议报告", "日期": "2024-07-31", "今值": 5.50, "预测值": 5.50, "前值": 5.25},
            # 未发布决议：今值为 NaN，应被丢弃
            {"商品": "美联储利率决议报告", "日期": "2024-09-18", "今值": None, "预测值": None, "前值": 5.50},
        ]
    )

    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 2  # 丢弃未发布 + 去重
    assert processed["date"].tolist() == [date(2024, 6, 12), date(2024, 7, 31)]
    assert processed["rate"].tolist() == [5.25, 5.50]
    assert processed["rate_prev"].tolist() == [5.25, 5.25]
    # 「商品」列未在 schema_def 中，应被事件类基类丢弃
    assert "商品" not in processed.columns
    assert "rate_forecast" in processed.columns


# --------------------------------------------------------------------------
# 第二批：事件类任务（共享 AkShareMacroEventTask 基类）
# --------------------------------------------------------------------------

def test_core_pce_attributes():
    assert AkShareMacroCorePceTask.name == "akshare_macro_core_pce"
    assert AkShareMacroCorePceTask.table_name == "macro_core_pce"
    assert AkShareMacroCorePceTask.api_name == "macro_usa_core_pce_price"
    assert AkShareMacroCorePceTask.primary_keys == ["date"]


def test_core_pce_event_task_drops_unreleased_and_keeps_schema_cols():
    """事件类基类通用行为：丢弃未发布（rate NaN）行、仅保留 schema_def 列、去重。"""
    task = AkShareMacroCorePceTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {"商品": "美国核心PCE物价指数年率", "日期": "2024-05-31", "今值": 2.6, "预测值": 2.6, "前值": 2.7},
            {"商品": "美国核心PCE物价指数年率", "日期": "2024-06-28", "今值": 2.6, "预测值": 2.6, "前值": 2.6},
            # 未发布：今值 NaN，应丢弃
            {"商品": "美国核心PCE物价指数年率", "日期": "2024-07-31", "今值": None, "预测值": None, "前值": 2.6},
        ]
    )
    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 2
    assert processed["date"].tolist() == [date(2024, 5, 31), date(2024, 6, 28)]
    assert processed["rate"].tolist() == [2.6, 2.6]
    assert "商品" not in processed.columns
    assert set(processed.columns) == {"date", "rate", "rate_forecast", "rate_prev"}


def test_event_task_attributes_batch():
    """其余 4 个事件类任务的属性核验。"""
    cases = [
        (AkShareMacroUsaNonfarmTask, "akshare_macro_usa_nonfarm", "macro_usa_nonfarm", "macro_usa_non_farm"),
        (AkShareMacroUsaUnemploymentTask, "akshare_macro_usa_unemployment", "macro_usa_unemployment", "macro_usa_unemployment_rate"),
        (AkShareMacroEcbRateTask, "akshare_macro_ecb_rate", "macro_ecb_rate", "macro_bank_euro_interest_rate"),
        (AkShareMacroBojRateTask, "akshare_macro_boj_rate", "macro_boj_rate", "macro_bank_japan_interest_rate"),
    ]
    for cls, name, table, api in cases:
        assert cls.name == name, f"{name} name mismatch"
        assert cls.table_name == table, f"{name} table mismatch"
        assert cls.api_name == api, f"{name} api mismatch"
        assert cls.primary_keys == ["date"], f"{name} pk mismatch"


# --------------------------------------------------------------------------
# 第二批：宽表 melt 任务
# --------------------------------------------------------------------------

def test_money_supply_attributes():
    assert AkShareMacroMoneySupplyTask.name == "akshare_macro_money_supply"
    assert AkShareMacroMoneySupplyTask.table_name == "macro_money_supply"
    assert AkShareMacroMoneySupplyTask.primary_keys == ["month", "aggregate", "measure"]
    assert AkShareMacroMoneySupplyTask.melt_config is not None


def test_money_supply_melts_wide_to_long_and_parses_month():
    task = AkShareMacroMoneySupplyTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {
                "月份": "2024年05月份",
                "货币和准货币(M2)-数量(亿元)": 3000000.0,
                "货币和准货币(M2)-同比增长": 7.0,
                "货币(M1)-数量(亿元)": 600000.0,
                "货币(M1)-同比增长": -4.0,
            },
            {
                "月份": "2024年04月份",
                "货币和准货币(M2)-数量(亿元)": 2950000.0,
                "货币和准货币(M2)-同比增长": 7.2,
                "货币(M1)-数量(亿元)": 610000.0,
                "货币(M1)-同比增长": -3.0,
            },
        ]
    )
    processed = _run_akshare_pipeline(task, raw_df)

    # 2 月 × 4 值列 = 8 行长表
    assert len(processed) == 8
    assert set(processed.columns) == {"month", "aggregate", "measure", "value"}
    # month 解析为 YYYYMM
    assert set(processed["month"]) == {"202405", "202404"}
    # aggregate 与 measure 解析正确
    assert set(processed["aggregate"]) == {"M2", "M1"}
    assert set(processed["measure"]) == {"amount", "yoy"}
    # 抽查：202405 M2 同比 = 7.0
    may_m2_yoy = processed[
        (processed["month"] == "202405") & (processed["aggregate"] == "M2") & (processed["measure"] == "yoy")
    ]
    assert may_m2_yoy["value"].iloc[0] == 7.0


def test_cn_cb_balance_attributes():
    assert AkShareMacroCnCbBalanceTask.name == "akshare_macro_cn_cb_balance"
    assert AkShareMacroCnCbBalanceTask.table_name == "macro_cn_cb_balance"
    assert AkShareMacroCnCbBalanceTask.primary_keys == ["date", "item"]


def test_cn_cb_balance_melts_items_to_long():
    task = AkShareMacroCnCbBalanceTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {"统计时间": "2024.5", "外汇": 220000.0, "储备货币": 330000.0, "总资产": 450000.0},
            {"统计时间": "2024.4", "外汇": 221000.0, "储备货币": 329000.0, "总资产": 448000.0},
        ]
    )
    processed = _run_akshare_pipeline(task, raw_df)

    # 2 月 × 3 项目 = 6 行
    assert len(processed) == 6
    assert set(processed.columns) == {"date", "item", "value"}
    assert set(processed["item"]) == {"外汇", "储备货币", "总资产"}
    # date 解析为月初日（transformer 将 2024.5 转 datetime，基类再转 date）
    assert date(2024, 5, 1) in processed["date"].tolist()


# --------------------------------------------------------------------------
# 第二批：CCI 指数任务
# --------------------------------------------------------------------------

def test_cci_attributes():
    assert AkShareMacroCciTask.name == "akshare_macro_cci"
    assert AkShareMacroCciTask.table_name == "macro_cci"
    assert AkShareMacroCciTask.api_name == "index_cci_cx"
    assert AkShareMacroCciTask.primary_keys == ["date"]


def test_cci_process_data_converts_and_deduplicates():
    task = AkShareMacroCciTask(db_connection=_MockDB(), update_type=UpdateTypes.FULL)
    raw_df = pd.DataFrame(
        [
            {"日期": "2024-04-01", "大宗商品指数": "200.5", "变化值": "-1.2"},
            {"日期": "2024-04-01", "大宗商品指数": "200.8", "变化值": "-0.9"},  # 去重保留最后
            {"日期": "2024-04-02", "大宗商品指数": "201.0", "变化值": "0.2"},
        ]
    )
    processed = _run_akshare_pipeline(task, raw_df)

    assert len(processed) == 2
    assert processed["date"].tolist() == [date(2024, 4, 1), date(2024, 4, 2)]
    assert processed["cci"].tolist() == [200.8, 201.0]
    assert processed["change"].tolist() == [-0.9, 0.2]
