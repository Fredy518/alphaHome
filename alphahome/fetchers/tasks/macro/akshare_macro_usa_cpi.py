#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""美国 CPI 同比数据采集任务（akshare macro_usa_cpi_yoy）。

SPEC-015 用途：计算美实际利率 us_real_yield_10y = 美债10Y名义 - 美国CPI同比，
支撑 P0 metric us_real_yield_10y_change_6m。

数据源：akshare macro_usa_cpi_yoy（美国 BLS，月频，免费，已实测可用）。
原始列为中文：时间/发布日期/现值/前值；数据为月频（时间列固定为月初日），
最新未发布月份的「现值」为 NaN，需在 process_data 中丢弃。

注：akshare 该接口仅提供同比（YoY），无环比（MoM），故目标表不含 cpi_mom。
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroUsaCpiTask(AkShareNoDateSingleBatchTask):
    """获取美国 CPI 同比数据（macro_usa_cpi_yoy，全历史单批）。"""

    smart_lookback_days = 40
    domain = "macro"
    name = "akshare_macro_usa_cpi"
    table_name = "macro_usa_cpi"
    description = "获取美国 CPI 同比数据，用于计算美实际利率（SPEC-015 us_real_yield_10y）"

    api_name = "macro_usa_cpi_yoy"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date"]
    date_column = "date"
    default_start_date = "20080101"

    column_mapping = {
        "时间": "date",
        "发布日期": "release_date",
        "现值": "cpi_yoy",
        "前值": "cpi_prev_yoy",
    }

    transformations = {
        "cpi_yoy": float,
        "cpi_prev_yoy": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL", "comment": "数据月份（月初日）"},
        "release_date": {"type": "DATE", "comment": "BLS 发布日期"},
        "cpi_yoy": {"type": "NUMERIC(10,4)", "comment": "美国 CPI 同比（%）"},
        "cpi_prev_yoy": {"type": "NUMERIC(10,4)", "comment": "前值同比（%）"},
    }

    indexes = [
        {"name": "idx_macro_usa_cpi_date", "columns": "date"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (
            lambda df: df["cpi_yoy"].dropna().between(-50, 100).all(),
            "cpi_yoy 应在合理区间 [-50, 100]",
        ),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        # macro_usa_cpi_yoy 不接受日期参数，返回全历史；生效窗口过滤由基类处理
        return [{}]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用基础转换后，规整日期、丢弃尚未发布（cpi_yoy 为 NaN）的行并去重。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        # 规整日期列为 date 类型（FULL 模式下基类不做此转换）
        if "date" in data.columns:
            data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
            data = data.dropna(subset=["date"])
        if "release_date" in data.columns:
            data["release_date"] = pd.to_datetime(data["release_date"], errors="coerce").dt.date

        # 丢弃尚未发布月份（现值为 NaN 的最新行）
        if "cpi_yoy" in data.columns:
            data = data.dropna(subset=["cpi_yoy"]).copy()

        # 去重（按 date 保留最后一条）
        if "date" in data.columns:
            data = data.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        return data
