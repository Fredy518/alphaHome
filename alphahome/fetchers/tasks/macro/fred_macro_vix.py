#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""VIX 波动率指数采集任务（FRED VIXCLS）。

SPEC-015 用途：market_regime_label 分类辅助（risk_on/risk_off 信号）。

数据源：FRED VIXCLS（CBOE VIX 收盘，日频，1990 至今，keyless，经 fredgraph.csv
端点实测可用，无需 API key）。

口径说明（重要）：SPEC-015 目标为 CBOE VIX（含 OHLC），但：
- CBOE 官方 VIX_History.csv 在当前环境实测返回 403 AccessDenied（封锁程序化访问）；
- yfinance ^VIX 实测被 429 限流。
主源用 FRED VIXCLS（仅收盘价）；当 FRED 不可达时自动 fallback 至 Yahoo v8 直连
^VIX（同为 CBOE VIX 收盘，口径一致，仅取收盘）。下游 market_regime 仅需收盘价作风险
偏好信号，口径可接受。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroVixTask(FredTask):
    """获取 VIX 波动率指数收盘（FRED VIXCLS，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_vix"
    table_name = "macro_vix"
    description = "获取 CBOE VIX 收盘价（FRED VIXCLS，SPEC-015 market_regime 辅助）"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "19900102"

    series_ids = ["VIXCLS"]
    column_mapping = {"VIXCLS": "vix_close"}
    # Yahoo fallback：FRED 不可达时用 Yahoo v8 直连 ^VIX（同为 CBOE VIX 收盘，口径一致）
    yahoo_fallback = {"VIXCLS": "^VIX"}

    transformations = {"vix_close": float}

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "美股交易日"},
        "vix_close": {"type": "NUMERIC(10,4)", "comment": "VIX 收盘（VIXCLS，仅收盘价）"},
    }

    indexes = [
        {"name": "idx_macro_vix_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["vix_close"].dropna().between(0, 150).all(), "vix_close 应在合理区间 [0, 150]"),
    ]
