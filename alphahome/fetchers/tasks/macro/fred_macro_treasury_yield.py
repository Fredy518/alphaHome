#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美国国债关键期限收益率采集任务（FRED DGS1MO/3MO/5/10）。

SPEC-015 用途：1M/3M 国债收益率用于算 SOFR-国债利差（经典 TED 利差的现代替代：
原 TED = 3M LIBOR - 3M 国债，LIBOR 停用后可用 SOFR-3M国债 或 SOFR-IORB 反映融资压力）。
配合 macro_sofr / macro_us_short_rate 使用。

数据源：FRED 四序列（keyless，经 fredgraph.csv 端点实测可用，无需 API key）：
- DGS1MO：1 个月美国国债收益率（2001-07 起）
- DGS3MO：3 个月美国国债收益率（1981-09 起，回溯最长）
- DGS5：5 年美国国债收益率（1962-01 起）
- DGS10：10 年美国国债收益率（1962-01 起）

合并策略：四序列按 observation_date 外连接，起点不一，缺失日补 NULL。

5Y/10Y 与 macro_bond_rate 中的第三方镜像并存，供需要官方美国财政部/FRED
口径的研究使用；1M/3M 继续用于短端融资压力指标。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroTreasuryYieldTask(FredTask):
    """获取美国 1M/3M/5Y/10Y 国债收益率（FRED，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_treasury_yield"
    table_name = "macro_treasury_yield"
    description = "获取美国 1M/3M/5Y/10Y 国债收益率；含风格轮动所需官方 5Y/10Y"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "19620102"

    series_ids = ["DGS1MO", "DGS3MO", "DGS5", "DGS10"]
    column_mapping = {
        "DGS1MO": "yield_1m",
        "DGS3MO": "yield_3m",
        "DGS5": "yield_5y",
        "DGS10": "yield_10y",
    }

    transformations = {
        "yield_1m": float,
        "yield_3m": float,
        "yield_5y": float,
        "yield_10y": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "yield_1m": {"type": "NUMERIC(10,4)", "comment": "1 个月美国国债收益率（%）"},
        "yield_3m": {
            "type": "NUMERIC(10,4)",
            "comment": "3 个月美国国债收益率（%，算 SOFR-国债利差替代 TED）",
        },
        "yield_5y": {
            "type": "NUMERIC(10,4)",
            "comment": "5 年美国国债收益率（%，FRED DGS5）",
        },
        "yield_10y": {
            "type": "NUMERIC(10,4)",
            "comment": "10 年美国国债收益率（%，FRED DGS10）",
        },
    }

    indexes = [
        {"name": "idx_macro_treasury_yield_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (
            lambda df: df["yield_3m"].dropna().between(-1, 25).all(),
            "yield_3m 应在合理区间 [-1, 25]",
        ),
        (
            lambda df: df["yield_5y"].dropna().between(-1, 25).all(),
            "yield_5y 应在合理区间 [-1, 25]",
        ),
        (
            lambda df: df["yield_10y"].dropna().between(-1, 25).all(),
            "yield_10y 应在合理区间 [-1, 25]",
        ),
    ]
