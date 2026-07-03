#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""中国政策利率采集任务（akshare macro_china_lpr）。

SPEC-015 用途：macro_rate_metrics.policy_rate → P0 metric policy_rate_change_6m。

数据源：akshare macro_china_lpr（央行 LPR 公告，不定期/月频，免费，已实测可用）。
原始列：TRADE_DATE/LPR1Y/LPR5Y/RATE_1/RATE_2，其中 LPR1Y/LPR5Y 为贷款市场报价利率，
2019-08 前为 NaN（对应历史基准贷款利率 RATE_1/RATE_2）。

MLF（中期借贷便利）：akshare 1.18.64 无对应接口（已实测确认），目标表预留
mlf_1y 空列以便后续接入数据源时无需 schema 迁移；当前该列恒为 NULL。

注：akshare 该接口每行对应一次利率公告（非交易日序列），date_column 设为 date
以支持增量；smart_lookback_days 放宽以覆盖月度公告。
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroLprTask(AkShareNoDateSingleBatchTask):
    """获取中国 LPR 及历史基准贷款利率（macro_china_lpr，全历史单批）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_lpr"
    table_name = "macro_policy_rate"
    description = "获取中国 LPR（1Y/5Y）及历史基准贷款利率，支撑 SPEC-015 policy_rate"

    api_name = "macro_china_lpr"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "19910421"

    column_mapping = {
        "TRADE_DATE": "date",
        "LPR1Y": "lpr_1y",
        "LPR5Y": "lpr_5y",
        "RATE_1": "benchmark_loan_1y",
        "RATE_2": "benchmark_loan_5y",
    }

    transformations = {
        "lpr_1y": float,
        "lpr_5y": float,
        "benchmark_loan_1y": float,
        "benchmark_loan_5y": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "利率公告日"},
        "lpr_1y": {"type": "NUMERIC(10,4)", "comment": "1 年期 LPR（%）"},
        "lpr_5y": {"type": "NUMERIC(10,4)", "comment": "5 年期 LPR（%）"},
        "benchmark_loan_1y": {"type": "NUMERIC(10,4)", "comment": "历史 1 年期基准贷款利率（%）"},
        "benchmark_loan_5y": {"type": "NUMERIC(10,4)", "comment": "历史 5 年期以上基准贷款利率（%）"},
        "mlf_1y": {"type": "NUMERIC(10,4)", "comment": "1 年期 MLF 利率（预留列，当前数据源不可得）"},
    }

    indexes = [
        {"name": "idx_macro_policy_rate_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (
            lambda df: df[["lpr_1y", "lpr_5y", "benchmark_loan_1y", "benchmark_loan_5y"]]
            .dropna(how="all")
            .pipe(lambda s: not s.empty),
            "至少一个利率列需有值",
        ),
        (
            lambda df: df["lpr_1y"].dropna().between(0, 30).all(),
            "lpr_1y 应在合理区间 [0, 30]",
        ),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        # macro_china_lpr 不接受日期参数，返回全历史；生效窗口过滤由基类处理
        return [{}]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用基础转换后，规整日期、补齐预留的 mlf_1y 空列并去重。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        # 规整日期列为 date 类型（FULL 模式下基类不做此转换）
        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
            data = data.dropna(subset=["date"])

        # 预留 MLF 列（akshare 无该数据源），恒为 NULL
        data["mlf_1y"] = pd.NA

        if "date" in data.columns:
            data = data.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        return data
