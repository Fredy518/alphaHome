#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""中国央行资产负债表采集任务（akshare macro_china_central_bank_balance）。

SPEC-015 用途：银行体系流动性的根源。外汇占款（基础货币投放主渠道）+ 储备货币 +
政府存款等，反映央行基础货币创造与对冲操作。长表存储：date/item/value。

数据源：akshare macro_china_central_bank_balance（央行月度资产负债表，免费，已实测可用，
353 行回溯 1993）。原始为宽表：统计时间 + 26 个项目列（外汇/储备货币/政府存款/总资产等）。
通过 melt_config 转长表，列名即项目名。

注：统计时间格式为 `2026.5`（YYYY.M），解析为 YYYYMM。
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroCnCbBalanceTask(AkShareNoDateSingleBatchTask):
    """获取中国央行资产负债表（macro_china_central_bank_balance，melt 长表）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_cn_cb_balance"
    table_name = "macro_cn_cb_balance"
    description = "获取中国央行资产负债表（外汇占款/储备货币/政府存款等），流动性根源"

    api_name = "macro_china_central_bank_balance"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date", "item"]
    date_column = "date"
    default_start_date = "19930101"

    column_mapping = {
        "统计时间": "date",
    }

    melt_config = {
        "id_vars": ["date"],
        "value_vars": None,  # 自动推断（除 date 外全部项目列），兼容列子集
        "var_name": "item",
        "value_name": "value",
    }

    transformations = {
        "value": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "月份（月初日）"},
        "item": {"type": "VARCHAR(64)", "constraints": "NOT NULL", "comment": "资产负债项目（中文）"},
        "value": {"type": "NUMERIC(20,4)", "comment": "余额（亿元）"},
    }

    indexes = [
        {"name": "idx_macro_cn_cb_balance_date", "columns": "date"},
        {"name": "idx_macro_cn_cb_balance_item", "columns": "item"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["item"].notna(), "item 不能为空"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return [{}]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """规整 date 为 date 类型并去重（transformer 已将 `2026.5` 解析为 datetime）。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        # transformer 已通过 schema_def DATE 将 date 转为 datetime；此处统一为 date
        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
            data = data.dropna(subset=["date"])

        # 去重（按主键保留最后一条）
        data = data.drop_duplicates(subset=["date", "item"], keep="last").reset_index(drop=True)
        return data
