#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美国核心 PCE 采集任务（akshare macro_usa_core_pce_price）。

SPEC-015 用途：美联储通胀目标锚定核心 PCE 2%（非 CPI）。FOMC 决策真正依据核心 PCE，
比美国 CPI 更关键。核心 PCE 剔除食品和能源，更能反映潜在通胀趋势。

数据源：akshare macro_usa_core_pce_price（美国核心 PCE 物价指数年率，670 行，月频事件，
免费，已实测可用）。清洗逻辑由 AkShareMacroEventTask 基类统一处理。
"""

from __future__ import annotations

from typing import Any, Dict, List

from ...sources.akshare.akshare_macro_event_task import AkShareMacroEventTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroCorePceTask(AkShareMacroEventTask):
    """获取美国核心 PCE 物价指数年率（macro_usa_core_pce_price，事件类）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_core_pce"
    table_name = "macro_core_pce"
    description = "获取美国核心 PCE 物价指数年率，美联储通胀锚（比 CPI 更关键）"

    api_name = "macro_usa_core_pce_price"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20000101"

    column_mapping = {
        "日期": "date",
        "今值": "rate",
        "预测值": "rate_forecast",
        "前值": "rate_prev",
    }

    transformations = {
        "rate": float,
        "rate_forecast": float,
        "rate_prev": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "PCE 发布日"},
        "rate": {"type": "NUMERIC(10,4)", "comment": "核心 PCE 年率今值（%，美联储通胀锚）"},
        "rate_forecast": {"type": "NUMERIC(10,4)", "comment": "市场预测值（%）"},
        "rate_prev": {"type": "NUMERIC(10,4)", "comment": "前值（%）"},
    }

    indexes = [
        {"name": "idx_macro_core_pce_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["rate"].dropna().between(-5, 20).all(), "rate 应在合理区间 [-5, 20]"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return [{}]
