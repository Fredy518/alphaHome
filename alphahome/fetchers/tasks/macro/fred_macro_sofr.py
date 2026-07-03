#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""SOFR（担保隔夜融资利率）采集任务（FRED SOFR）。

SPEC-015 用途：可选全球短期利率参考（MVP 非阻塞，但补齐提升 global_score 覆盖）。

数据源：FRED SOFR（担保隔夜融资利率，日频，2018 至今，keyless，经 fredgraph.csv
端点实测可用，无需 API key）。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroSofrTask(FredTask):
    """获取 SOFR 担保隔夜融资利率（FRED SOFR，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_sofr"
    table_name = "macro_sofr"
    description = "获取 SOFR 担保隔夜融资利率（FRED SOFR，全球短期利率参考）"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20180101"

    series_ids = ["SOFR"]
    column_mapping = {"SOFR": "sofr"}

    transformations = {"sofr": float}

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "sofr": {"type": "NUMERIC(10,4)", "comment": "担保隔夜融资利率（%）"},
    }

    indexes = [
        {"name": "idx_macro_sofr_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["sofr"].dropna().between(0, 25).all(), "sofr 应在合理区间 [0, 25]"),
    ]
