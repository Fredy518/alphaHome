#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美国短端国债收益率采集任务（FRED DGS1MO + DGS3MO）。

SPEC-015 用途：1M/3M 国债收益率用于算 SOFR-国债利差（经典 TED 利差的现代替代：
原 TED = 3M LIBOR - 3M 国债，LIBOR 停用后可用 SOFR-3M国债 或 SOFR-IORB 反映融资压力）。
配合 macro_sofr / macro_us_short_rate 使用。

数据源：FRED 两序列（keyless，经 fredgraph.csv 端点实测可用，无需 API key）：
- DGS1MO：1 个月美国国债收益率（2001-07 起）
- DGS3MO：3 个月美国国债收益率（1981-09 起，回溯最长）

合并策略：两序列按 observation_date 外连接，起点不一，缺失日补 NULL。

注：AlphaDB 现有 macro_bond_rate 含中美国债 2y/5y/10y/30y，但无 1M/3M 短端，本表补短端。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroTreasuryYieldTask(FredTask):
    """获取美国 1M/3M 短端国债收益率（FRED，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_treasury_yield"
    table_name = "macro_treasury_yield"
    description = "获取美国 1M/3M 短端国债收益率，算 SOFR-国债利差（TED 现代替代）"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "19810901"

    series_ids = ["DGS1MO", "DGS3MO"]
    column_mapping = {
        "DGS1MO": "yield_1m",
        "DGS3MO": "yield_3m",
    }

    transformations = {
        "yield_1m": float,
        "yield_3m": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "yield_1m": {"type": "NUMERIC(10,4)", "comment": "1 个月美国国债收益率（%）"},
        "yield_3m": {"type": "NUMERIC(10,4)", "comment": "3 个月美国国债收益率（%，算 SOFR-国债利差替代 TED）"},
    }

    indexes = [
        {"name": "idx_macro_treasury_yield_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["yield_3m"].dropna().between(-1, 25).all(), "yield_3m 应在合理区间 [-1, 25]"),
    ]
