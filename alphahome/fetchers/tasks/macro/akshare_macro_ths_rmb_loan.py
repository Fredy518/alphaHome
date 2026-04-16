#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""同花顺-新增人民币贷款

AkShare 接口: macro_rmb_loan
目标地址: https://data.10jqka.com.cn/macro/loan/

原始字段含百分比字符串（如 11.37%），本任务统一转为 float（单位：%）。
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
class AkShareMacroThsRmbLoanTask(AkShareNoDateSingleBatchTask):
    # 月度数据：智能增量默认回写近 15 个月，避免 10 天窗口导致“增量过滤为空”
    smart_lookback_days = 450
    domain = "macro"
    name = "akshare_macro_ths_rmb_loan"
    description = "新增人民币贷款（AkShare/同花顺）"
    table_name = "macro_ths_rmb_loan"

    api_name = "macro_rmb_loan"
    api_params: Dict[str, Any] = {}

    primary_keys = ["month_end_date"]
    date_column = "month_end_date"
    default_start_date = "20000101"

    column_mapping = {
        "月份": "month",
        "新增人民币贷款-总额": "new_loan_total",
        "新增人民币贷款-同比": "new_loan_yoy",
        "新增人民币贷款-环比": "new_loan_mom",
        "累计人民币贷款-总额": "loan_total",
        "累计人民币贷款-同比": "loan_yoy",
    }

    schema_def = {
        "month": {"type": "VARCHAR(10)"},
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "new_loan_total": {"type": "NUMERIC(20,2)"},
        "new_loan_yoy": {"type": "NUMERIC(12,4)"},
        "new_loan_mom": {"type": "NUMERIC(12,4)"},
        "loan_total": {"type": "NUMERIC(20,2)"},
        "loan_yoy": {"type": "NUMERIC(12,4)"},
    }

    indexes = [
        {"name": "idx_ths_rmb_loan_month_end", "columns": "month_end_date"},
        {"name": "idx_ths_rmb_loan_update_time", "columns": "update_time"},
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

        for col in ["new_loan_total", "new_loan_yoy", "new_loan_mom", "loan_total", "loan_yoy"]:
            if col in data.columns:
                data[col] = data[col].apply(_to_float_or_nan)

        data = data.drop_duplicates(subset=["month_end_date"], keep="last")
        return data
