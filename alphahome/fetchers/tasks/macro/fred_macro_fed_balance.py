#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美联储资产负债表采集任务（FRED WALCL）。

SPEC-015 用途：全球流动性总闸门。美联储缩表（QT）/扩表（QE）直接决定全球美元
流动性，比利率更前沿地反映流动性收缩/扩张。WALCL 为美联储总资产规模（周频，
百万美元），是观察 QT 进程的核心指标。

数据源：FRED WALCL（keyless，经 fredgraph.csv 端点实测可用，无需 API key），
周频（每周三发布），2002 至今。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroFedBalanceTask(FredTask):
    """获取美联储资产负债表总资产规模（FRED WALCL，周频）。"""

    smart_lookback_days = 30
    domain = "macro"
    name = "fred_macro_fed_balance"
    table_name = "macro_fed_balance"
    description = "获取美联储总资产规模 WALCL，全球流动性总闸门（QT/QE 观测）"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20030101"

    series_ids = ["WALCL"]
    column_mapping = {"WALCL": "total_assets"}

    transformations = {"total_assets": float}

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "发布日（周三）"},
        "total_assets": {"type": "NUMERIC(18,2)", "comment": "美联储总资产（百万美元，QT/QE 进程指标）"},
    }

    indexes = [
        {"name": "idx_macro_fed_balance_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["total_assets"].dropna().between(0, 20000000).all(), "total_assets 应在合理区间"),
    ]
