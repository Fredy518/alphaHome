#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""国家统计局 NBS 全国数据（可配置指标集合）

AkShare 接口: macro_china_nbs_nation
https://data.stats.gov.cn/easyquery.htm

该接口是“通用”入口，覆盖大量宏观指标。由于参数化（kind/path/period），
本任务采用配置驱动：在 config.json 的 tasks 节点为该任务提供 series 列表。

输出为长表：
- series_id: 指标配置 ID（建议手工填，缺省会自动生成）
- kind/path: NBS 维度
- indicator: 指标名称（含单位）
- period_label: NBS 返回的时间列名（如 2023年/2023年02月/2023年第二季度）
- period_end_date: 解析出的期末日期（用于时序分析）
- value: 数值
"""

from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.akshare.akshare_task import AkShareTask
from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register


def _stable_series_id(kind: str, path: str) -> str:
    raw = f"{kind}||{path}".encode("utf-8")
    return hashlib.md5(raw).hexdigest()[:16]


def _parse_period_end(label: Any) -> Optional[date]:
    text = str(label).strip()

    # 年度：2023年
    m = re.match(r"^(\d{4})年$", text)
    if m:
        y = int(m.group(1))
        return date(y, 12, 31)

    # 月度：2023年02月
    m = re.match(r"^(\d{4})年(\d{1,2})月$", text)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        return (pd.Timestamp(year=y, month=mo, day=1) + pd.offsets.MonthEnd(0)).date()

    # 季度：2023年第2季度 / 2023年第二季度
    m = re.match(r"^(\d{4})年(?:第)?([0-4一二三四])季度$", text)
    if m:
        y = int(m.group(1))
        q = m.group(2)
        q_map = {"1": 1, "2": 2, "3": 3, "4": 4, "一": 1, "二": 2, "三": 3, "四": 4}
        if q not in q_map:
            return None
        quarter = q_map[q]
        month = quarter * 3
        return (pd.Timestamp(year=y, month=month, day=1) + pd.offsets.MonthEnd(0)).date()

    # 兜底：尽量用 pandas 解析
    normalized = (
        text.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
    )
    ts = pd.to_datetime(normalized, errors="coerce")
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).date()


@task_register()
class AkShareMacroChinaNBSNationTask(AkShareTask):
    domain = "macro"
    name = "akshare_macro_china_nbs_nation"
    description = "国家统计局 NBS 全国宏观数据（配置驱动，AkShare）"
    table_name = "macro_china_nbs_nation"

    api_name = "macro_china_nbs_nation"

    primary_keys = ["series_id", "indicator", "period_end_date"]
    date_column = "period_end_date"
    default_start_date = "19900101"

    default_concurrent_limit = 1

    schema_def = {
        "series_id": {"type": "VARCHAR(32)", "constraints": "NOT NULL"},
        "kind": {"type": "VARCHAR(16)", "constraints": "NOT NULL"},
        "path": {"type": "TEXT", "constraints": "NOT NULL"},
        "indicator": {"type": "TEXT", "constraints": "NOT NULL"},
        "period_label": {"type": "VARCHAR(32)", "constraints": "NOT NULL"},
        "period_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "value": {"type": "NUMERIC(20,6)"},
    }

    indexes = [
        {"name": "idx_nbs_nation_series_period", "columns": "series_id, period_end_date"},
        {"name": "idx_nbs_nation_period", "columns": "period_end_date"},
        {"name": "idx_nbs_nation_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["series_id"].notna(), "series_id 不能为空"),
        (lambda df: df["kind"].notna(), "kind 不能为空"),
        (lambda df: df["path"].notna(), "path 不能为空"),
        (lambda df: df["indicator"].notna(), "indicator 不能为空"),
        (lambda df: df["period_end_date"].notna(), "period_end_date 不能为空"),
    ]

    def _apply_config(self, task_config: Dict):
        super()._apply_config(task_config)
        self.series: List[Dict[str, str]] = list(task_config.get("series", []))

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        update_type = kwargs.get("update_type", UpdateTypes.SMART)

        if await self._should_skip_by_recent_update_time(update_type, max_age_days=7):
            return []

        if not getattr(self, "series", None):
            raise ValueError(
                "akshare_macro_china_nbs_nation 需要在 config.json 中配置 tasks.akshare_macro_china_nbs_nation.series"
            )

        batches: List[Dict[str, Any]] = []
        for item in self.series:
            kind = str(item.get("kind", "")).strip()
            path = str(item.get("path", "")).strip()
            period = str(item.get("period", "LAST10")).strip() or "LAST10"
            if not kind or not path:
                continue
            series_id = str(item.get("id") or "").strip() or _stable_series_id(kind, path)
            batches.append({"kind": kind, "path": path, "period": period, "series_id": series_id})

        if not batches:
            raise ValueError("NBS series 配置为空或无有效项")

        return batches

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        series_id = str(params.get("series_id"))
        kind = str(params.get("kind"))
        path = str(params.get("path"))
        period = str(params.get("period"))

        raw_df = await self.api.call(
            func_name=self.api_name,
            kind=kind,
            path=path,
            period=period,
            stop_event=stop_event,
        )

        if raw_df is None or raw_df.empty:
            return None

        df = raw_df.copy()
        df = df.reset_index().rename(columns={"index": "indicator"})

        melted = df.melt(
            id_vars=["indicator"],
            var_name="period_label",
            value_name="value",
        )

        melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
        melted["period_end_date"] = melted["period_label"].apply(_parse_period_end)

        melted["series_id"] = series_id
        melted["kind"] = kind
        melted["path"] = path

        melted = melted.dropna(subset=["period_end_date"]).copy()
        melted["period_end_date"] = pd.to_datetime(melted["period_end_date"]).dt.date

        melted = melted.drop_duplicates(subset=["series_id", "indicator", "period_end_date"], keep="last")
        return melted
