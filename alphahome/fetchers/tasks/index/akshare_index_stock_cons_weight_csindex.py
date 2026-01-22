#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 中证指数成分权重（CSIndex）

接口:
- ak.index_stock_cons_weight_csindex(symbol="000300")
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from ...sources.akshare.akshare_task import AkShareTask
from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register


def _normalize_index_symbols(symbol: Optional[str], symbols: Optional[Iterable[str]]) -> List[str]:
    out: List[str] = []
    if symbols:
        out.extend([str(s).strip() for s in symbols if str(s).strip()])
    if symbol:
        out.append(str(symbol).strip())
    cleaned = []
    seen = set()
    for s in out:
        s = s.strip()
        if not s:
            continue
        s = s.zfill(6) if s.isdigit() else s
        if s not in seen:
            seen.add(s)
            cleaned.append(s)
    return cleaned


@task_register()
class AkShareIndexStockConsWeightCsindexTask(AkShareTask):
    domain = "index"
    name = "akshare_index_stock_cons_weight_csindex"
    description = "中证指数-样本权重（AkShare index_stock_cons_weight_csindex）"
    table_name = "index_stock_cons_weight_csindex"
    data_source = "akshare"

    primary_keys = ["index_code", "const_code", "as_of_date"]
    date_column = "as_of_date"
    default_start_date = "20050101"

    api_name = "index_stock_cons_weight_csindex"

    column_mapping = {
        "日期": "as_of_date",
        "指数代码": "index_code",
        "指数名称": "index_name",
        "指数英文名称": "index_name_en",
        "成分券代码": "const_code",
        "成分券名称": "const_name",
        "成分券英文名称": "const_name_en",
        "交易所": "exchange",
        "交易所英文名称": "exchange_en",
        "权重": "weight",
    }

    schema_def = {
        "as_of_date": {"type": "DATE", "constraints": "NOT NULL"},
        "index_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(20)"},
        "index_name": {"type": "VARCHAR(50)"},
        "index_name_en": {"type": "VARCHAR(100)"},
        "const_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "const_ts_code": {"type": "VARCHAR(15)"},
        "const_name": {"type": "VARCHAR(50)"},
        "const_name_en": {"type": "VARCHAR(100)"},
        "exchange": {"type": "VARCHAR(30)"},
        "exchange_en": {"type": "VARCHAR(50)"},
        "weight": {"type": "NUMERIC(20,6)"},
    }

    indexes = [
        {"name": "idx_index_cons_weight_csindex_idx", "columns": "index_code"},
        {"name": "idx_index_cons_weight_csindex_const", "columns": "const_code"},
        {"name": "idx_index_cons_weight_csindex_date", "columns": "as_of_date"},
        {"name": "idx_index_cons_weight_csindex_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["index_code"].notna(), "指数代码不能为空"),
        (lambda df: df["const_code"].notna(), "成分券代码不能为空"),
        (lambda df: df["as_of_date"].notna(), "成分日期不能为空"),
    ]
    validation_mode = "report"

    _code_suffix_cache: Optional[Dict[str, str]] = None
    _index_code_to_ts_code_cache: Optional[Dict[str, str]] = None

    async def _pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        await super()._pre_execute(stop_event=stop_event, **kwargs)
        await self._load_index_ts_code_mapping()
        await self._load_code_suffix_mapping()

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
            return mapping
        except Exception as e:
            self.logger.warning(f"加载指数 ts_code 映射失败: {e}")
            self._index_code_to_ts_code_cache = {}
            return {}

    async def _load_code_suffix_mapping(self) -> Dict[str, str]:
        if self._code_suffix_cache is not None:
            return self._code_suffix_cache
        try:
            rows = await self.db.fetch("SELECT ts_code FROM tushare.stock_basic")
            mapping: Dict[str, str] = {}
            for row in rows:
                ts_code = row["ts_code"]
                if ts_code and "." in ts_code:
                    symbol = ts_code.split(".")[0]
                    mapping[symbol] = ts_code
            self._code_suffix_cache = mapping
            return mapping
        except Exception as e:
            self.logger.error(f"加载股票代码映射失败: {e}")
            self._code_suffix_cache = {}
            return {}

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        if "index_code" in data.columns:
            data["index_code"] = data["index_code"].astype(str).str.strip().str.zfill(6)
            mapping = self._index_code_to_ts_code_cache or {}
            data["ts_code"] = data["index_code"].map(mapping)
        else:
            data["ts_code"] = None

        if "const_code" in data.columns:
            data["const_code"] = data["const_code"].astype(str).str.strip().str.zfill(6)

        mapping = self._code_suffix_cache or {}
        if mapping and "const_code" in data.columns:
            data["const_ts_code"] = data["const_code"].map(mapping)
        else:
            data["const_ts_code"] = None

        keep = [c for c in self.schema_def.keys() if c in data.columns]
        return data[keep]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        update_type = kwargs.get("update_type", UpdateTypes.SMART)
        if await self._should_skip_by_recent_update_time(update_type, max_age_days=30):
            return []
        symbols = _normalize_index_symbols(kwargs.get("symbol"), kwargs.get("symbols"))

        if not symbols:
            if update_type == UpdateTypes.MANUAL:
                self.logger.error(f"{self.name}: 手动模式需要提供 symbol/symbols 参数")
                return []
            symbols = ["000300", "000905", "000852"]

        return [{"symbol": s} for s in symbols]


__all__ = ["AkShareIndexStockConsWeightCsindexTask"]
