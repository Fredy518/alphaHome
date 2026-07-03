#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美国非农就业采集任务（akshare macro_usa_non_farm）。

SPEC-015 用途：美联储双目标的就业侧，FOMC 决策核心。非农就业反映劳动力市场强弱，
是利率预期的重要驱动。

数据源：akshare macro_usa_non_farm（美国非农就业人数，669 行，月频事件，免费，已实测可用）。
清洗逻辑由 AkShareMacroEventTask 基类统一处理。
"""

from __future__ import annotations

from typing import Any, Dict, List

from ...sources.akshare.akshare_macro_event_task import AkShareMacroEventTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroUsaNonfarmTask(AkShareMacroEventTask):
    """获取美国非农就业人数（macro_usa_non_farm，事件类）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_usa_nonfarm"
    table_name = "macro_usa_nonfarm"
    description = "获取美国非农就业人数，美联储双目标就业侧（FOMC 决策核心）"

    api_name = "macro_usa_non_farm"
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
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "非农发布日"},
        "rate": {"type": "NUMERIC(10,4)", "comment": "非农就业新增今值（万人）"},
        "rate_forecast": {"type": "NUMERIC(10,4)", "comment": "市场预测值（万人）"},
        "rate_prev": {"type": "NUMERIC(10,4)", "comment": "前值（万人）"},
    }

    indexes = [
        {"name": "idx_macro_usa_nonfarm_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["rate"].dropna().between(-500, 2000).all(), "rate 应在合理区间 [-500, 2000]"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return [{}]
