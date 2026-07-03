#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美元隔夜短期利率集合采集任务（FRED IORB + OBFR + ON RRP）。

SPEC-015 用途：补充美元隔夜短期利率体系，与 SOFR（担保）/ fed_rate（联邦基金）
共同构成美元短端利率全貌。其中 IORB 用于算 SOFR-IORB 利差（替代已停用的 TED 利差，
反映实时融资压力）。

数据源：FRED 三序列（keyless，经 fredgraph.csv 端点实测可用，无需 API key）：
- IORB：准备金利率（2021-07 起，政策利率，IOER 的后继）
- OBFR：隔夜银行融资利率（2016-03 起，无担保口径，与 SOFR 担保口径对比）
- RRPONTSYAWARD：隔夜逆回购 ON RRP（2013-09 起，联储政策利率下限）

合并策略：三序列按 observation_date 外连接，起点不一（2013/2016/2021），缺失日补 NULL。

注：IOER（IORB 前身，2008-2021，已停用）未纳入；EFFR 与已建 macro_fed_rate.effective_rate
(DFF) 重复，未纳入。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroUsShortRateTask(FredTask):
    """获取美元隔夜短期利率集合 IORB/OBFR/ON RRP（FRED，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_us_short_rate"
    table_name = "macro_us_short_rate"
    description = "获取美元隔夜短期利率 IORB/OBFR/ON RRP，IORB 算 SOFR-IORB 利差替代 TED"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20130923"

    series_ids = ["IORB", "OBFR", "RRPONTSYAWARD"]
    column_mapping = {
        "IORB": "iorb",
        "OBFR": "obfr",
        "RRPONTSYAWARD": "on_rrp",
    }

    transformations = {
        "iorb": float,
        "obfr": float,
        "on_rrp": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "iorb": {"type": "NUMERIC(10,4)", "comment": "准备金利率（%，算 SOFR-IORB 利差替代 TED）"},
        "obfr": {"type": "NUMERIC(10,4)", "comment": "隔夜银行融资利率（%，无担保口径）"},
        "on_rrp": {"type": "NUMERIC(10,4)", "comment": "隔夜逆回购利率（%，联储政策下限）"},
    }

    indexes = [
        {"name": "idx_macro_us_short_rate_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["iorb"].dropna().between(-1, 25).all(), "iorb 应在合理区间 [-1, 25]"),
    ]
