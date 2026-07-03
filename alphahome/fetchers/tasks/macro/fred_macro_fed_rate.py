#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美联储联邦基金利率采集任务（FRED DFEDTARU/DFEDTARL/DFF）。

SPEC-015 用途：global_liquidity_metrics.fed_target_rate；美联储政策姿态主指标。

数据源：FRED 三序列（keyless，经 fredgraph.csv 端点实测可用，无需 API key）：
- DFEDTARU：目标区间上限（%），日频
- DFEDTARL：目标区间下限（%），日频
- DFF：有效联邦基金利率（%），日频（市场实际成交加权利率）

合并策略：三序列按 observation_date 外连接，缺失日补 NULL。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroFedRateTask(FredTask):
    """获取美联储联邦基金目标利率区间及有效利率（FRED，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_fed_rate"
    table_name = "macro_fed_rate"
    description = "获取美联储联邦基金目标利率上下限及有效利率（SPEC-015 fed_target_rate）"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20081216"

    series_ids = ["DFEDTARU", "DFEDTARL", "DFF"]
    column_mapping = {
        "DFEDTARU": "target_upper",
        "DFEDTARL": "target_lower",
        "DFF": "effective_rate",
    }

    transformations = {
        "target_upper": float,
        "target_lower": float,
        "effective_rate": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "target_upper": {"type": "NUMERIC(10,4)", "comment": "联邦基金目标利率上限（%）"},
        "target_lower": {"type": "NUMERIC(10,4)", "comment": "联邦基金目标利率下限（%）"},
        "effective_rate": {"type": "NUMERIC(10,4)", "comment": "有效联邦基金利率（%）"},
    }

    indexes = [
        {"name": "idx_macro_fed_rate_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["target_upper"].dropna().between(0, 25).all(), "target_upper 应在合理区间 [0, 25]"),
    ]
