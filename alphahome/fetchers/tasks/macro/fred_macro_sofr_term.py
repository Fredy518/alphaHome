#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""SOFR 期限结构采集任务（FRED SOFR 30/90/180 天复合平均）。

SPEC-015 用途：补充 SOFR 体系期限结构。SOFR 复合平均反映不同期限的融资成本，
与隔夜 SOFR（macro_sofr）共同构成 SOFR 利率曲线，供 liquidity/global_score 使用。

数据源：FRED 三序列（keyless，经 fredgraph.csv 端点实测可用，无需 API key）：
- SOFR30DAYAVG：30 天 SOFR 复合平均（2018-05 起）
- SOFR90DAYAVG：90 天 SOFR 复合平均（2018-07 起）
- SOFR180DAYAVG：180 天 SOFR 复合平均（2018-10 起）

合并策略：三序列按 observation_date 外连接，缺失日补 NULL。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroSofrTermTask(FredTask):
    """获取 SOFR 30/90/180 天复合平均期限结构（FRED，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_sofr_term"
    table_name = "macro_sofr_term"
    description = "获取 SOFR 30/90/180 天复合平均，补充 SOFR 期限结构"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20180502"

    series_ids = ["SOFR30DAYAVG", "SOFR90DAYAVG", "SOFR180DAYAVG"]
    column_mapping = {
        "SOFR30DAYAVG": "sofr_30d",
        "SOFR90DAYAVG": "sofr_90d",
        "SOFR180DAYAVG": "sofr_180d",
    }

    transformations = {
        "sofr_30d": float,
        "sofr_90d": float,
        "sofr_180d": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "sofr_30d": {"type": "NUMERIC(10,4)", "comment": "SOFR 30 天复合平均（%）"},
        "sofr_90d": {"type": "NUMERIC(10,4)", "comment": "SOFR 90 天复合平均（%）"},
        "sofr_180d": {"type": "NUMERIC(10,4)", "comment": "SOFR 180 天复合平均（%）"},
    }

    indexes = [
        {"name": "idx_macro_sofr_term_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["sofr_90d"].dropna().between(-2, 25).all(), "sofr_90d 应在合理区间 [-2, 25]"),
    ]
