#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 中证指数列表

接口:
- ak.index_csindex_all()
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from ...sources.akshare.akshare_task import AkShareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareIndexCsindexAllTask(AkShareTask):
    domain = "index"
    name = "akshare_index_csindex_all"
    description = "中证指数-指数列表（AkShare index_csindex_all）"
    table_name = "index_csindex_all"
    data_source = "akshare"

    primary_keys = ["index_code"]
    date_column = "publish_date"
    default_start_date = "20050101"

    api_name = "index_csindex_all"
    api_params: Optional[Dict[str, Any]] = None

    column_mapping = {
        "指数代码": "index_code",
        "指数简称": "index_short_name",
        "指数全称": "index_full_name",
        "基日": "base_date",
        "基点": "base_point",
        "指数系列": "index_series",
        "样本数量": "sample_count",
        "最新收盘": "last_close",
        "近一个月收益率": "return_1m",
        "资产类别": "asset_class",
        "指数热点": "index_hotspot",
        "指数币种": "currency",
        "合作指数": "is_cooperate",
        "跟踪产品": "has_tracking_product",
        "指数合规": "compliance",
        "指数类别": "index_category",
        "发布时间": "publish_date",
    }

    schema_def = {
        "index_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(20)"},
        "index_short_name": {"type": "VARCHAR(50)"},
        "index_full_name": {"type": "VARCHAR(100)"},
        "base_date": {"type": "DATE"},
        "base_point": {"type": "NUMERIC(20,4)"},
        "index_series": {"type": "VARCHAR(50)"},
        "sample_count": {"type": "INTEGER"},
        "last_close": {"type": "NUMERIC(20,4)"},
        "return_1m": {"type": "NUMERIC(20,6)"},
        "asset_class": {"type": "VARCHAR(20)"},
        "index_hotspot": {"type": "VARCHAR(50)"},
        "currency": {"type": "VARCHAR(20)"},
        "is_cooperate": {"type": "VARCHAR(10)"},
        "has_tracking_product": {"type": "VARCHAR(10)"},
        "compliance": {"type": "VARCHAR(20)"},
        "index_category": {"type": "VARCHAR(20)"},
        "publish_date": {"type": "DATE"},
    }

    indexes = [
        {"name": "idx_index_csindex_all_category", "columns": "index_category"},
        {"name": "idx_index_csindex_all_publish_date", "columns": "publish_date"},
        {"name": "idx_index_csindex_all_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["index_code"].notna(), "指数代码不能为空"),
    ]
    validation_mode = "report"

    _index_code_to_ts_code_cache: Optional[Dict[str, str]] = None

    async def _pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        await super()._pre_execute(stop_event=stop_event, **kwargs)
        await self._load_index_ts_code_mapping()

    async def _load_index_ts_code_mapping(self) -> Dict[str, str]:
        if self._index_code_to_ts_code_cache is not None:
            return self._index_code_to_ts_code_cache

        try:
            rows = await self.db.fetch('SELECT ts_code FROM tushare.index_basic')
            mapping: Dict[str, str] = {}
            for row in rows:
                ts_code = row["ts_code"]
                if not ts_code:
                    continue
                idx = str(ts_code).split(".")[0].split("!")[0].strip()
                if idx.isdigit():
                    idx = idx.zfill(6)
                mapping[idx] = str(ts_code).strip()
            self._index_code_to_ts_code_cache = mapping
            self.logger.info(f"已加载 {len(mapping)} 条指数 ts_code 映射（tushare.index_basic）")
            return mapping
        except Exception as e:
            self.logger.warning(f"加载指数 ts_code 映射失败: {e}")
            self._index_code_to_ts_code_cache = {}
            return {}

    def process_data(self, data: Any, **kwargs):
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        if "index_code" in data.columns:
            data["index_code"] = data["index_code"].astype(str).str.strip().str.zfill(6)
            mapping = self._index_code_to_ts_code_cache or {}
            data["ts_code"] = data["index_code"].map(mapping)
        else:
            data["ts_code"] = None

        keep = [c for c in self.schema_def.keys() if c in data.columns]
        return data[keep]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        update_type = kwargs.get("update_type")
        if await self._should_skip_by_recent_update_time(update_type, max_age_days=30):
            return []
        return [{}]


__all__ = ["AkShareIndexCsindexAllTask"]
