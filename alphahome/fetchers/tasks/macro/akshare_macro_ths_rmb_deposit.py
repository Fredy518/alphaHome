#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""同花顺-人民币存款余额

AkShare 接口: macro_rmb_deposit
目标地址: https://data.10jqka.com.cn/macro/rmb/

百分比字段统一转为 float（单位：%）。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from ...sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from ....common.task_system.task_decorator import task_register


def _to_float_or_nan(x: Any) -> Optional[float]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if s in ("", "--", "-", "—", "nan", "None"):
        return None
    s = s.replace(",", "")
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return None


def _month_end(value: Any) -> Optional[pd.Timestamp]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ts = pd.to_datetime(str(value).strip(), errors="coerce")
    if pd.isna(ts):
        return None
    return (pd.Timestamp(ts) + pd.offsets.MonthEnd(0)).normalize()


@task_register()
class AkShareMacroThsRmbDepositTask(AkShareNoDateSingleBatchTask):
    # 月度数据：智能增量默认回写近 15 个月
    smart_lookback_days = 450
    domain = "macro"
    name = "akshare_macro_ths_rmb_deposit"
    description = "人民币存款余额（AkShare/同花顺）"
    table_name = "macro_ths_rmb_deposit"

    api_name = "macro_rmb_deposit"
    api_params: Dict[str, Any] = {}

    primary_keys = ["month_end_date"]
    date_column = "month_end_date"
    default_start_date = "20000101"

    column_mapping = {
        "月份": "month",
        "新增存款-数量": "new_deposit_total",
        "新增存款-同比": "new_deposit_yoy",
        "新增存款-环比": "new_deposit_mom",
        "新增企业存款-数量": "new_deposit_corp",
        "新增企业存款-同比": "new_deposit_corp_yoy",
        "新增企业存款-环比": "new_deposit_corp_mom",
        "新增储蓄存款-数量": "new_deposit_saving",
        "新增储蓄存款-同比": "new_deposit_saving_yoy",
        "新增储蓄存款-环比": "new_deposit_saving_mom",
        "新增其他存款-数量": "new_deposit_other",
        "新增其他存款-同比": "new_deposit_other_yoy",
        "新增其他存款-环比": "new_deposit_other_mom",
    }

    schema_def = {
        "month": {"type": "VARCHAR(10)"},
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "new_deposit_total": {"type": "NUMERIC(20,2)"},
        "new_deposit_yoy": {"type": "NUMERIC(12,4)"},
        "new_deposit_mom": {"type": "NUMERIC(12,4)"},
        "new_deposit_corp": {"type": "NUMERIC(20,2)"},
        "new_deposit_corp_yoy": {"type": "NUMERIC(12,4)"},
        "new_deposit_corp_mom": {"type": "NUMERIC(12,4)"},
        "new_deposit_saving": {"type": "NUMERIC(20,2)"},
        "new_deposit_saving_yoy": {"type": "NUMERIC(12,4)"},
        "new_deposit_saving_mom": {"type": "NUMERIC(12,4)"},
        "new_deposit_other": {"type": "NUMERIC(20,2)"},
        "new_deposit_other_yoy": {"type": "NUMERIC(12,4)"},
        "new_deposit_other_mom": {"type": "NUMERIC(12,4)"},
    }

    indexes = [
        {"name": "idx_ths_rmb_deposit_month_end", "columns": "month_end_date"},
        {"name": "idx_ths_rmb_deposit_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["month_end_date"].notna(), "month_end_date 不能为空"),
    ]

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if data is None or data.empty:
            return data
        data = super().process_data(data, **kwargs)

        data["month_end_date"] = data["month"].apply(_month_end)
        data = data.dropna(subset=["month_end_date"]).copy()
        data["month_end_date"] = pd.to_datetime(data["month_end_date"]).dt.date

        # 智能/手动增量：按生效日期窗口回写（该列为派生列，需要在此处过滤）
        if self.update_type in ("smart", "manual"):
            start = getattr(self, "_effective_start_date", None) or self.start_date
            end = getattr(self, "_effective_end_date", None) or self.end_date
            s = pd.to_datetime(start, errors="coerce") if start else pd.NaT
            e = pd.to_datetime(end, errors="coerce") if end else pd.NaT
            if not pd.isna(s) and not pd.isna(e):
                sd = pd.Timestamp(s).date()
                ed = pd.Timestamp(e).date()
                data = data[(data["month_end_date"] >= sd) & (data["month_end_date"] <= ed)].copy()

        for col in [
            "new_deposit_total",
            "new_deposit_yoy",
            "new_deposit_mom",
            "new_deposit_corp",
            "new_deposit_corp_yoy",
            "new_deposit_corp_mom",
            "new_deposit_saving",
            "new_deposit_saving_yoy",
            "new_deposit_saving_mom",
            "new_deposit_other",
            "new_deposit_other_yoy",
            "new_deposit_other_mom",
        ]:
            if col in data.columns:
                data[col] = data[col].apply(_to_float_or_nan)

        data = data.drop_duplicates(subset=["month_end_date"], keep="last")
        return data
