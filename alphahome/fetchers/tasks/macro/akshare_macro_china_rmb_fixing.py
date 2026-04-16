#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""人民币汇率中间价（Jin10 数据中心）

AkShare 接口: macro_china_rmb
目标地址: https://datacenter.jin10.com/reportType/dc_rmb_data

原始数据为宽表（每个币对两列：中间价/涨跌幅），本任务存为长表：
- date: 日期
- pair: 币对名称（原始中文）
- metric: fix / chg
- metric_raw: 原始指标名（中间价/定价/涨跌幅）
- value: 数值
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroChinaRmbFixingTask(AkShareNoDateSingleBatchTask):
    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_china_rmb_fixing"
    description = "人民币汇率中间价（AkShare/Jin10）"
    table_name = "macro_china_rmb_fixing"

    api_name = "macro_china_rmb"
    api_params: Dict[str, Any] = {}

    primary_keys = ["date", "pair", "metric"]
    date_column = "date"
    default_start_date = "20170103"

    column_mapping = {
        "日期": "date",
    }

    melt_config = {
        "id_vars": ["date"],
        "value_vars": None,
        "var_name": "_original_col",
        "value_name": "value",
        "var_parser": lambda s: AkShareMacroChinaRmbFixingTask._parse_rmb_columns(s),
    }

    transformations = {
        "value": float,
    }

    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL"},
        "pair": {"type": "TEXT", "constraints": "NOT NULL"},
        "metric": {"type": "VARCHAR(8)", "constraints": "NOT NULL"},
        "metric_raw": {"type": "VARCHAR(16)", "constraints": "NOT NULL"},
        "value": {"type": "NUMERIC(18,6)"},
    }

    indexes = [
        {"name": "idx_rmb_fixing_date", "columns": "date"},
        {"name": "idx_rmb_fixing_pair", "columns": "pair"},
        {"name": "idx_rmb_fixing_metric", "columns": "metric"},
        {"name": "idx_rmb_fixing_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["date"].notna(), "date 不能为空"),
        (lambda df: df["pair"].notna(), "pair 不能为空"),
        (lambda df: df["metric"].isin(["fix", "chg"]) | df["metric"].isna(), "metric 应为 fix/chg"),
    ]

    @staticmethod
    def _parse_rmb_columns(column_series: pd.Series) -> pd.DataFrame:
        """解析币对列名：`美元/人民币_中间价` -> pair, metric_raw."""
        pair_list: List[str] = []
        metric_raw_list: List[str] = []
        for item in column_series.astype(str).tolist():
            text = item.strip()
            # 兜底：有些字段可能无下划线
            if "_" in text:
                pair, metric_raw = text.rsplit("_", 1)
            else:
                pair, metric_raw = text, ""
            pair_list.append(pair)
            metric_raw_list.append(metric_raw)

        return pd.DataFrame({"pair": pair_list, "metric_raw": metric_raw_list}, index=column_series.index)

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if data is None or data.empty:
            return data

        data = super().process_data(data, **kwargs)

        # metric 标准化
        raw = data.get("metric_raw")
        if raw is None:
            data["metric_raw"] = ""
            raw = data["metric_raw"]

        def norm_metric(x: Any) -> str:
            x = str(x).strip()
            if x in ("中间价", "定价"):
                return "fix"
            if "涨跌" in x:
                return "chg"
            return ""

        data["metric"] = raw.apply(norm_metric)

        # 过滤掉解析失败的列
        data = data[(data["pair"].notna()) & (data["pair"].astype(str).str.len() > 0)]
        data = data[data["metric"].isin(["fix", "chg"])].copy()

        # 日期转 date
        data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date
        data = data.dropna(subset=["date"])

        # 去重列名可能导致的重复行
        data = data.drop_duplicates(subset=["date", "pair", "metric"], keep="last")

        return data
