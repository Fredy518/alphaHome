#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美元指数采集任务（FRED DTWEXBGS）。

SPEC-015 用途：global_liquidity_metrics.usd_index；global_score 外部因子。

数据源：FRED DTWEXBGS（贸易加权广义美元指数，日频，2006 至今，keyless，
经 fredgraph.csv 端点实测可用，无需 API key）。

口径说明（重要）：SPEC-015 目标为 ICE 美元指数（DXY），但 CBOE/yfinance 在当前环境
均不可得（CBOE CSV 403、yfinance 429 限流）。主源用 FRED DTWEXBGS（贸易加权广义
美元指数，含商品与服务，2006 基期=100）作代理；当 FRED 不可达时自动 fallback 至
Yahoo v8 直连 DX-Y.NYB（ICE DXY 本尊，6 发达货币篮子，基期不同）：
- DTWEXBGS 与 DX-Y.NYB 走势相关但口径、数值、基期均不同；
- fallback 触发时数据口径会从 DTWEXBGS 切换为 ICE DXY，下游需按 source 标注或降权。
"""

from __future__ import annotations

from ...sources.fred.fred_task import FredTask
from ....common.task_system.task_decorator import task_register


@task_register()
class FredMacroDxyTask(FredTask):
    """获取美元指数代理（FRED DTWEXBGS，日频）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "fred_macro_dxy"
    table_name = "macro_dxy"
    description = "获取 FRED 贸易加权广义美元指数 DTWEXBGS，作 DXY 代理（SPEC-015 usd_index）"

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20060101"

    series_ids = ["DTWEXBGS"]
    column_mapping = {"DTWEXBGS": "dxy_close"}
    # Yahoo fallback：FRED 不可达时用 Yahoo v8 直连取 DX-Y.NYB（ICE DXY，口径与 DTWEXBGS 不同，详见注释）
    yahoo_fallback = {"DTWEXBGS": "DX-Y.NYB"}

    transformations = {"dxy_close": float}

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "dxy_close": {"type": "NUMERIC(12,4)", "comment": "美元指数收盘（主源 DTWEXBGS 2006基期；FRED不可达时 fallback 至 ICE DXY DX-Y.NYB）"},
    }

    indexes = [
        {"name": "idx_macro_dxy_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["dxy_close"].dropna().between(50, 200).all(), "dxy_close 应在合理区间 [50, 200]"),
    ]
