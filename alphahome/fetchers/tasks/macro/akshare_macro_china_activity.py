#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""中国月度活动数据：固定资产投资、工业增加值、社会消费品零售。

这些接口由 AkShare 封装东方财富公开宏观数据页，替代旧策略中的 Wind 数值输入。
源数据的月份字段是统计期，不是发布日期；本任务不会把它伪装成 availability_date。
需要 PIT 时应继续使用保守固定滞后，或另行与可审计的发布日历连接。
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register


def _month_end(value: Any):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    match = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", text)
    if match:
        return (
            pd.Timestamp(int(match.group(1)), int(match.group(2)), 1)
            + pd.offsets.MonthEnd(0)
        ).date()
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return (pd.Timestamp(parsed).replace(day=1) + pd.offsets.MonthEnd(0)).date()


class _AkShareMonthlyActivityTask(AkShareNoDateSingleBatchTask):
    """三个同构月度活动任务的共用清洗逻辑。"""

    date_column = "period_end_date"
    smart_lookback_days = 120
    source_url: str

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        frame = data.copy()
        frame["period_end_date"] = frame["period_label"].apply(_month_end)
        frame = frame.dropna(subset=["period_end_date"])
        frame["source_url"] = self.source_url

        if "period_source_date" in frame.columns:
            # AkShare 字段原名虽为“发布时间”，实值只是当月 1 日，不能作为 PIT 发布日。
            frame["period_source_date"] = pd.to_datetime(
                frame["period_source_date"], errors="coerce"
            ).dt.date

        if self.update_type in (UpdateTypes.SMART, UpdateTypes.MANUAL):
            start = getattr(self, "_effective_start_date", None) or getattr(
                self, "start_date", None
            )
            end = getattr(self, "_effective_end_date", None) or getattr(
                self, "end_date", None
            )
            if start:
                start_date = pd.Timestamp(start).date()
                frame = frame[frame["period_end_date"] >= start_date]
            if end:
                end_date = pd.Timestamp(end).date()
                frame = frame[frame["period_end_date"] <= end_date]

        frame = frame.drop_duplicates(subset=["period_end_date"], keep="last")
        keep = [column for column in self.schema_def if column in frame.columns]
        return frame[keep].sort_values("period_end_date").reset_index(drop=True)


@task_register()
class AkShareMacroFixedAssetInvestmentTask(_AkShareMonthlyActivityTask):
    domain = "macro"
    name = "akshare_macro_fixed_asset_investment"
    description = "中国固定资产投资月度值、同比与累计值（东方财富，经 AkShare）"
    table_name = "macro_fixed_asset_investment"
    api_name = "macro_china_gdzctz"
    default_start_date = "20120201"
    source_url = "https://data.eastmoney.com/cjsj/gdzctz.html"

    primary_keys = ["period_end_date"]
    column_mapping = {
        "月份": "period_label",
        "当月": "monthly_value",
        "同比增长": "monthly_yoy",
        "环比增长": "monthly_mom",
        "自年初累计": "cumulative_value",
    }
    transformations = {
        "monthly_value": float,
        "monthly_yoy": float,
        "monthly_mom": float,
        "cumulative_value": float,
    }
    schema_def = {
        "period_end_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "统计期月末，非发布日期",
        },
        "period_label": {"type": "VARCHAR(32)"},
        "monthly_value": {"type": "NUMERIC(20,4)", "comment": "当月固定资产投资，亿元"},
        "monthly_yoy": {
            "type": "NUMERIC(20,6)",
            "comment": "当月同比，旧策略 fixedasset_investment_yoy 对应口径",
        },
        "monthly_mom": {"type": "NUMERIC(20,6)"},
        "cumulative_value": {"type": "NUMERIC(20,4)", "comment": "自年初累计，亿元"},
        "source_url": {"type": "TEXT", "constraints": "NOT NULL"},
    }
    indexes = [
        {"name": "idx_macro_fai_period", "columns": "period_end_date", "unique": True},
        {"name": "idx_macro_fai_update_time", "columns": "update_time"},
    ]
    validations = [
        (lambda df: df["period_end_date"].notna(), "统计期不能为空"),
        (
            lambda df: df["monthly_yoy"].dropna().between(-100, 200).all(),
            "固定资产投资当月同比超出合理范围",
        ),
    ]


@task_register()
class AkShareMacroIndustrialValueAddedTask(_AkShareMonthlyActivityTask):
    domain = "macro"
    name = "akshare_macro_industrial_value_added"
    description = "中国规模以上工业增加值同比（东方财富，经 AkShare）"
    table_name = "macro_industrial_value_added"
    api_name = "macro_china_gyzjz"
    default_start_date = "20080201"
    source_url = "https://data.eastmoney.com/cjsj/gyzjz.html"

    primary_keys = ["period_end_date"]
    column_mapping = {
        "月份": "period_label",
        "同比增长": "monthly_yoy",
        "累计增长": "cumulative_yoy",
        "发布时间": "period_source_date",
    }
    transformations = {"monthly_yoy": float, "cumulative_yoy": float}
    schema_def = {
        "period_end_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "统计期月末，非发布日期",
        },
        "period_label": {"type": "VARCHAR(32)"},
        "monthly_yoy": {
            "type": "NUMERIC(20,6)",
            "comment": "当月同比，旧策略 industrial_value_added_yoy 对应口径",
        },
        "cumulative_yoy": {"type": "NUMERIC(20,6)"},
        "period_source_date": {
            "type": "DATE",
            "comment": "上游重复的统计期月初；明确不是发布日期",
        },
        "source_url": {"type": "TEXT", "constraints": "NOT NULL"},
    }
    indexes = [
        {"name": "idx_macro_iva_period", "columns": "period_end_date", "unique": True},
        {"name": "idx_macro_iva_update_time", "columns": "update_time"},
    ]
    validations = [
        (lambda df: df["period_end_date"].notna(), "统计期不能为空"),
        (
            lambda df: df["monthly_yoy"].dropna().between(-50, 100).all(),
            "工业增加值同比超出合理范围",
        ),
    ]


@task_register()
class AkShareMacroRetailSalesTask(_AkShareMonthlyActivityTask):
    domain = "macro"
    name = "akshare_macro_retail_sales"
    description = "中国社会消费品零售总额月度同比（东方财富，经 AkShare）"
    table_name = "macro_retail_sales"
    api_name = "macro_china_consumer_goods_retail"
    default_start_date = "20080101"
    source_url = "https://data.eastmoney.com/cjsj/xfp.html"

    primary_keys = ["period_end_date"]
    column_mapping = {
        "月份": "period_label",
        "当月": "monthly_value",
        "同比增长": "monthly_yoy",
        "环比增长": "monthly_mom",
        "累计": "cumulative_value",
        "累计-同比增长": "cumulative_yoy",
    }
    transformations = {
        "monthly_value": float,
        "monthly_yoy": float,
        "monthly_mom": float,
        "cumulative_value": float,
        "cumulative_yoy": float,
    }
    schema_def = {
        "period_end_date": {
            "type": "DATE",
            "constraints": "NOT NULL",
            "comment": "统计期月末，非发布日期",
        },
        "period_label": {"type": "VARCHAR(32)"},
        "monthly_value": {
            "type": "NUMERIC(20,4)",
            "comment": "社会消费品零售总额当月值，亿元",
        },
        "monthly_yoy": {
            "type": "NUMERIC(20,6)",
            "comment": "当月同比，修正后第 13 票 tot_retail_sales_yoy 对应口径",
        },
        "monthly_mom": {"type": "NUMERIC(20,6)"},
        "cumulative_value": {"type": "NUMERIC(20,4)"},
        "cumulative_yoy": {"type": "NUMERIC(20,6)"},
        "source_url": {"type": "TEXT", "constraints": "NOT NULL"},
    }
    indexes = [
        {
            "name": "idx_macro_retail_period",
            "columns": "period_end_date",
            "unique": True,
        },
        {"name": "idx_macro_retail_update_time", "columns": "update_time"},
    ]
    validations = [
        (lambda df: df["period_end_date"].notna(), "统计期不能为空"),
        (
            lambda df: df["monthly_yoy"].dropna().between(-100, 200).all(),
            "社零当月同比超出合理范围",
        ),
    ]


__all__ = [
    "AkShareMacroFixedAssetInvestmentTask",
    "AkShareMacroIndustrialValueAddedTask",
    "AkShareMacroRetailSalesTask",
]
