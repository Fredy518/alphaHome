#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""中国货币供应量采集任务（akshare macro_china_money_supply）。

SPEC-015 用途：M1-M2 剪刀差是流动性收紧最早信号（企业活期存款增速↓先于 M2）。
AlphaDB 已有 M2（macro_cn_m2），本表补齐 M0/M1/M2 全口径（数量+同比+环比），
使 M1-M2 剪刀差可算。长表存储：month/aggregate/measure/value。

数据源：akshare macro_china_money_supply（央行月度货币供应量，免费，已实测可用）。
原始为宽表：月份 + 9 列（M2/M1/M0 各 数量/同比/环比）。通过 melt_config + var_parser
解析列名 `货币和准货币(M2)-数量(亿元)` → aggregate=M2 + measure=amount，转为长表。

注：AlphaDB 现有 macro_cn_m2 仅含 M2 同比，本表为全口径增量，下游可统一改用本表。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareMacroMoneySupplyTask(AkShareNoDateSingleBatchTask):
    """获取中国 M0/M1/M2 货币供应量（macro_china_money_supply，全历史单批，melt 长表）。"""

    smart_lookback_days = 60
    domain = "macro"
    name = "akshare_macro_money_supply"
    table_name = "macro_money_supply"
    description = "获取中国 M0/M1/M2 货币供应量（数量+同比+环比），M1-M2 剪刀差可算"

    api_name = "macro_china_money_supply"
    api_params: Dict[str, Any] = {}

    primary_keys = ["month", "aggregate", "measure"]
    date_column = None  # 长表主键含 month，非单一日期增量
    default_start_date = "20080101"

    column_mapping = {
        "月份": "month",
    }

    # 宽表转长表：9 个值列 melt 成 (aggregate, measure, value)
    melt_config = {
        "id_vars": ["month"],
        "value_vars": None,  # 自动推断（除 month 外全部）
        "var_name": "_original_col",
        "value_name": "value",
        "var_parser": lambda s: AkShareMacroMoneySupplyTask._parse_columns(s),
    }

    transformations = {
        "value": float,
    }

    schema_def = {
        "month": {"type": "VARCHAR(10)", "constraints": "NOT NULL", "comment": "月份（YYYYMM）"},
        "aggregate": {"type": "VARCHAR(8)", "constraints": "NOT NULL", "comment": "货币层次：M0/M1/M2"},
        "measure": {"type": "VARCHAR(8)", "constraints": "NOT NULL", "comment": "度量：amount(亿元)/yoy(同比%)/mom(环比%)"},
        "value": {"type": "NUMERIC(20,4)", "comment": "数值（amount 单位亿元，yoy/mom 单位 %）"},
    }

    indexes = [
        {"name": "idx_macro_money_supply_month", "columns": "month"},
        {"name": "idx_macro_money_supply_agg", "columns": ["aggregate", "measure"]},
    ]

    validations = [
        (lambda df: df["month"].notna(), "month 不能为空"),
        (lambda df: df["aggregate"].isin(["M0", "M1", "M2"]).all(), "aggregate 必须为 M0/M1/M2"),
        (lambda df: df["measure"].isin(["amount", "yoy", "mom"]).all(), "measure 必须为 amount/yoy/mom"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return [{}]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """规整 month 为 YYYYMM 并按主键去重。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        # month: `2026年05月份` → `202605`
        if "month" in data.columns:
            data["month"] = (
                data["month"].astype(str).str.extract(r"(\d{4})年(\d{2})月份").apply(
                    lambda r: f"{r[0]}{r[1]}" if r[0] and r[1] else None, axis=1
                )
            )
            data = data.dropna(subset=["month"])

        # 去重（按主键保留最后一条）
        data = data.drop_duplicates(subset=["month", "aggregate", "measure"], keep="last").reset_index(drop=True)
        return data

    @staticmethod
    def _parse_columns(column_series: pd.Series) -> pd.DataFrame:
        """解析宽表列名 `货币和准货币(M2)-数量(亿元)` → aggregate + measure。

        返回两列 DataFrame（aggregate, measure），合并到 melted 结果并删除原 var 列。
        """
        agg_map = {"M2": "M2", "M1": "M1", "M0": "M0"}
        meas_map = {"数量": "amount", "同比增长": "yoy", "环比增长": "mom"}
        pattern = re.compile(r"[（(](M[012])[)）][-—](.*?)(?:[（(].*)?$")

        aggregates: List[str] = []
        measures: List[str] = []
        for col in column_series:
            m = pattern.search(str(col))
            if m:
                aggregates.append(agg_map.get(m.group(1), m.group(1)))
                measures.append(meas_map.get(m.group(2), m.group(2)))
            else:
                aggregates.append(None)
                measures.append(None)
        return pd.DataFrame({"aggregate": aggregates, "measure": measures})
