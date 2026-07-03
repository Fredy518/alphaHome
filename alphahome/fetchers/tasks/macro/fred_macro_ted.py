#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""TED 利差采集任务（FRED TEDRATE）。

SPEC-015 用途：可选 global_score 输入（MVP 非阻塞）。

数据源：FRED TEDRATE（TED 利差 = 3 个月 LIBOR - 3 个月国债，日频，keyless，
经 fredgraph.csv 端点实测可用，无需 API key）。

重要：FRED 已于 2022-01-21 停用 TEDRATE（LIBOR 退出导致），该序列自此后不再更新。
本任务仍建表以保留历史数据，但下游需知晓数据冻结于 2022-01-21，不作实时信号使用。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroTedTask(FredTask):
    """获取 TED 利差（FRED TEDRATE，已停用，仅历史数据）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_ted"
    table_name = "macro_ted"
    description = "获取 TED 利差（FRED TEDRATE，序列已停用，仅保留历史数据）"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20000101"

    series_ids = ["TEDRATE"]
    column_mapping = {"TEDRATE": "ted_spread"}

    transformations = {"ted_spread": float}

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "ted_spread": {"type": "NUMERIC(10,4)", "comment": "TED 利差（pp，序列已于 2022-01-21 停用）"},
    }

    indexes = [
        {"name": "idx_macro_ted_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["ted_spread"].dropna().between(-5, 15).all(), "ted_spread 应在合理区间 [-5, 15]"),
    ]
