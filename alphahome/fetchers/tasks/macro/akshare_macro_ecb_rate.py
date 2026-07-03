#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""欧央行利率决议采集任务（akshare macro_bank_euro_interest_rate）。

SPEC-015 用途：全球流动性第二引擎。欧央行政策姿态影响欧元区流动性及欧元汇率，
单看美联储不完整。与日央行、美联储决议共同构成全球主要央行政策图景。

数据源：akshare macro_bank_euro_interest_rate（欧洲央行决议报告，279 行，不定期事件，
免费，已实测可用）。清洗逻辑由 AkShareMacroEventTask 基类统一处理。
"""

from __future__ import annotations

from typing import Any, Dict, List

from ...sources.akshare.akshare_macro_event_task import AkShareMacroEventTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroEcbRateTask(AkShareMacroEventTask):
    """获取欧央行利率决议（macro_bank_euro_interest_rate，事件类）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_ecb_rate"
    table_name = "macro_ecb_rate"
    description = "获取欧央行利率决议，全球流动性第二引擎"

    api_name = "macro_bank_euro_interest_rate"
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
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "欧央行决议日"},
        "rate": {"type": "NUMERIC(10,4)", "comment": "欧央行主要再融资利率今值（%）"},
        "rate_forecast": {"type": "NUMERIC(10,4)", "comment": "市场预测值（%）"},
        "rate_prev": {"type": "NUMERIC(10,4)", "comment": "前值（%）"},
    }

    indexes = [
        {"name": "idx_macro_ecb_rate_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["rate"].dropna().between(-1, 25).all(), "rate 应在合理区间 [-1, 25]"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return [{}]
