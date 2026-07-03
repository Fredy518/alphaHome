#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美联储利率决议采集任务（akshare macro_bank_usa_interest_rate）。

SPEC-015 用途：与 fred_macro_fed_rate 互补。
- fred_macro_fed_rate（FRED DFEDTARU/L/DFF）：日频，目标区间上下限 + 有效利率（市场实际）。
- 本任务（akshare macro_bank_usa_interest_rate）：事件频（FOMC 决议日），今值/预测值/前值，
  反映政策动作点与市场预期差。
两者口径互补：决议=政策动作时点，有效利率=市场实际成交。下游按需选用。

数据源：akshare macro_bank_usa_interest_rate（美联储利率决议报告，294 行，回溯 1982，
免费，已实测可用）。原始列为中文：商品/日期/今值/预测值/前值。清洗逻辑由
AkShareMacroEventTask 基类统一处理。
"""

from __future__ import annotations

from typing import Any, Dict, List

from ...sources.akshare.akshare_macro_event_task import AkShareMacroEventTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroUsaFedDecisionTask(AkShareMacroEventTask):
    """获取美联储利率决议（macro_bank_usa_interest_rate，全历史单批）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_usa_fed_decision"
    table_name = "macro_fed_decision"
    description = "获取美联储利率决议今值/预测值/前值，与 FRED 有效利率互补（SPEC-015 fed_target_rate）"

    api_name = "macro_bank_usa_interest_rate"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "19820928"

    column_mapping = {
        "日期": "date",
        "今值": "rate",
        "预测值": "rate_forecast",
        "前值": "rate_prev",
    }

    transformations = {
        "rate": float,
        "rate_forecast": float,
        "rate_prev": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "FOMC 决议日"},
        "rate": {"type": "NUMERIC(10,4)", "comment": "美联储利率决议今值（%）"},
        "rate_forecast": {"type": "NUMERIC(10,4)", "comment": "市场预测值（%）"},
        "rate_prev": {"type": "NUMERIC(10,4)", "comment": "前值（%）"},
    }

    indexes = [
        {"name": "idx_macro_fed_decision_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["rate"].dropna().between(0, 30).all(), "rate 应在合理区间 [0, 30]"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        # macro_bank_usa_interest_rate 不接受日期参数，返回全历史；生效窗口过滤由基类处理
        return [{}]
