#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""日央行利率决议采集任务（akshare macro_bank_japan_interest_rate）。

SPEC-015 用途：全球流动性第三引擎。日央行政策姿态影响日元套息交易（carry trade）
与全球风险偏好，是亚太流动性核心。与欧央行、美联储决议共同构成全球主要央行政策图景。

数据源：akshare macro_bank_japan_interest_rate（日本央行决议报告，203 行，不定期事件，
免费，已实测可用）。清洗逻辑由 AkShareMacroEventTask 基类统一处理。
"""

from __future__ import annotations

from typing import Any, Dict, List

from ...sources.akshare.akshare_macro_event_task import AkShareMacroEventTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroBojRateTask(AkShareMacroEventTask):
    """获取日央行利率决议（macro_bank_japan_interest_rate，事件类）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_boj_rate"
    table_name = "macro_boj_rate"
    description = "获取日央行利率决议，全球流动性第三引擎（日元套息核心）"

    api_name = "macro_bank_japan_interest_rate"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20080101"

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
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "日央行决议日"},
        "rate": {"type": "NUMERIC(10,4)", "comment": "日央行政策利率今值（%）"},
        "rate_forecast": {"type": "NUMERIC(10,4)", "comment": "市场预测值（%）"},
        "rate_prev": {"type": "NUMERIC(10,4)", "comment": "前值（%）"},
    }

    indexes = [
        {"name": "idx_macro_boj_rate_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["rate"].dropna().between(-1, 25).all(), "rate 应在合理区间 [-1, 25]"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return [{}]
