#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美国信用利差采集任务（FRED BAAFF）。

SPEC-015 用途：信用风险与风险偏好先行指标。流动性紧张最早在信用利差走阔体现
（Baa 级企业债与 Aaa 级的利差扩大 = 市场要求更多信用补偿 = 风险偏好下降）。
与 VIX（股市波动）、SOFR-IORB（融资压力）共同构成多维度风险监测。

数据源：FRED BAAFF（Baa-Aaa 级企业债收益率利差，keyless，经 fredgraph.csv 端点
实测可用，无需 API key），日频，1986 至今，单位 pp。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroCreditSpreadTask(FredTask):
    """获取美国 Baa-Aaa 企业债信用利差（FRED BAAFF，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_credit_spread"
    table_name = "macro_credit_spread"
    description = "获取美国 Baa-Aaa 企业债信用利差 BAAFF，信用风险先行指标"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "19860101"

    series_ids = ["BAAFF"]
    column_mapping = {"BAAFF": "credit_spread"}

    transformations = {"credit_spread": float}

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "credit_spread": {"type": "NUMERIC(10,4)", "comment": "Baa-Aaa 企业债收益率利差（pp，信用风险先行指标）"},
    }

    indexes = [
        {"name": "idx_macro_credit_spread_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["credit_spread"].dropna().between(-2, 15).all(), "credit_spread 应在合理区间 [-2, 15]"),
    ]
