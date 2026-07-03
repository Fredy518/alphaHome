#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""银行间回购定盘利率采集任务（akshare repo_rate_query）。

SPEC-015 用途：liquidity_metrics.dr007 —— 国内流动性核心指标。

数据源：akshare repo_rate_query(symbol="回购定盘利率")，返回 date/FR001/FR007/FR014
（回购定盘利率，日频，回溯至 2023-06，免费，已实测可用）。

口径说明（重要）：SPEC-015 目标为 DR007（存款类机构质押式回购 7 天加权利率），
但 akshare 1.18.64 不提供 DR007（已实测确认）。此处用 FR007（回购定盘利率）作代理：
- FR007 为定盘利率（报价撮合），DR007 为成交加权利率；
- 两者走势高度相关但口径不同，下游 evidence confidence 需降权处理。

注：repo_rate_query 不接受 start/end 日期参数，返回全历史；生效窗口过滤由基类处理。
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroRepoRateTask(AkShareNoDateSingleBatchTask):
    """获取银行间回购定盘利率（repo_rate_query，全历史单批）。

    FR007 用作 DR007 的代理（akshare 无 DR007 直连接口）。
    """

    smart_lookback_days = 15
    domain = "macro"
    name = "akshare_macro_repo_rate"
    table_name = "macro_repo_rate"
    description = "获取银行间回购定盘利率 FR001/FR007/FR014，FR007 作 DR007 代理"

    api_name = "repo_rate_query"
    api_params: Dict[str, Any] = {"symbol": "回购定盘利率"}

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20230619"

    column_mapping = {
        "date": "date",
        "FR001": "fr001",
        "FR007": "fr007",
        "FR014": "fr014",
    }

    transformations = {
        "fr001": float,
        "fr007": float,
        "fr014": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "fr001": {"type": "NUMERIC(10,4)", "comment": "回购定盘利率 1 天（%）"},
        "fr007": {"type": "NUMERIC(10,4)", "comment": "回购定盘利率 7 天（%），DR007 代理"},
        "fr014": {"type": "NUMERIC(10,4)", "comment": "回购定盘利率 14 天（%）"},
    }

    indexes = [
        {"name": "idx_macro_repo_rate_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["fr007"].dropna().between(0, 50).all(), "fr007 应在合理区间 [0, 50]"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        # repo_rate_query 仅接受 symbol 参数（已通过 api_params 固定），不透传日期
        return [{}]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用基础转换后，规整日期并去重。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        # 规整日期列为 date 类型（FULL 模式下基类不做此转换）
        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
            data = data.dropna(subset=["date"])
            data = data.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        return data
