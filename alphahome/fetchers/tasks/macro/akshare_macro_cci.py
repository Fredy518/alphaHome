#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""大宗商品指数采集任务（akshare index_cci_cx）。

SPEC-015 用途：输入型通胀综合指标。单看铜/油等单一商品不够，CCI 大宗商品指数综合
反映原材料价格趋势，是国内 PPI 输入型通胀的前瞻信号。与 future_daily（单品种）互补。

数据源：akshare index_cci_cx（大宗商品指数，4234 行，日频，免费，已实测可用）。
原始列：日期/大宗商品指数/变化值，直接映射。
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroCciTask(AkShareNoDateSingleBatchTask):
    """获取大宗商品指数（index_cci_cx，全历史单批）。"""

    smart_lookback_days = 15
    domain = "macro"
    name = "akshare_macro_cci"
    table_name = "macro_cci"
    description = "获取大宗商品指数 CCI，输入型通胀综合指标"

    api_name = "index_cci_cx"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20100101"

    column_mapping = {
        "日期": "date",
        "大宗商品指数": "cci",
        "变化值": "change",
    }

    transformations = {
        "cci": float,
        "change": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "交易日"},
        "cci": {"type": "NUMERIC(12,4)", "comment": "大宗商品指数（输入型通胀综合指标）"},
        "change": {"type": "NUMERIC(12,4)", "comment": "当日变化值"},
    }

    indexes = [
        {"name": "idx_macro_cci_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["cci"].dropna().between(0, 1000).all(), "cci 应在合理区间 [0, 1000]"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return [{}]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """规整日期并去重。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
            data = data.dropna(subset=["date"])
            data = data.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        return data
