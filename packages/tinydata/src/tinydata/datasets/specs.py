"""Dataset metadata and processing helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional, Sequence

import pandas as pd

from ..cache import CacheManager, make_cache_key
from ..client import TinyClient
from ..codes import load_code_pool, tinysoft_symbol_to_ts_code
from ..errors import TinyDataCodePoolError
from ..infotable import InfoTableOptions, query_infotable


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    table_id: int
    source_table_name: str
    field_mapping: Dict[str, str]
    date_field: Optional[str] = None
    allow_full_table: bool = False
    code_pool: Optional[str] = None
    code_batch_size: int = 100
    field_version: str = "v1"
    date_columns: Sequence[str] = field(default_factory=tuple)
    numeric_columns: Sequence[str] = field(default_factory=tuple)
    integer_columns: Sequence[str] = field(default_factory=tuple)


def _normalize_dates(df: pd.DataFrame, columns: Sequence[str]) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date


def _normalize_numeric(df: pd.DataFrame, columns: Sequence[str]) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def _normalize_integer(df: pd.DataFrame, columns: Sequence[str]) -> None:
    for col in columns:
        if col in df.columns:
            values = pd.to_numeric(df[col], errors="coerce")
            df[col] = values.astype("Int64")


def process_dataset_frame(df: pd.DataFrame, spec: DatasetSpec) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    rename_map = {src: dst for src, dst in spec.field_mapping.items() if src in out.columns}
    if rename_map:
        out.rename(columns=rename_map, inplace=True)

    if "tsl_code" not in out.columns:
        if "StockID" in out.columns:
            out["tsl_code"] = out["StockID"]
        elif "stockid" in out.columns:
            out["tsl_code"] = out["stockid"]
        elif "request_code" in out.columns:
            out["tsl_code"] = out["request_code"]

    if "ts_code" not in out.columns and "tsl_code" in out.columns:
        out["ts_code"] = out["tsl_code"].map(tinysoft_symbol_to_ts_code)

    _normalize_dates(out, spec.date_columns)
    _normalize_numeric(out, spec.numeric_columns)
    _normalize_integer(out, spec.integer_columns)

    out["source_table_id"] = spec.table_id
    out["source_table_name"] = spec.source_table_name
    if "request_code" not in out.columns and "tsl_code" in out.columns:
        out["request_code"] = out["tsl_code"]
    if "request_code" not in out.columns and "source_code" in out.columns:
        out["request_code"] = out["source_code"]

    allowed_columns = set(spec.field_mapping.values()) | {
        "ts_code",
        "tsl_code",
        "request_code",
        "source_table_id",
        "source_table_name",
    }
    ordered_columns = [col for col in out.columns if col in allowed_columns]
    return out[ordered_columns]


def _dataset_query_fields(spec: DatasetSpec) -> Sequence[str]:
    fields = []
    seen = set()
    for field_name in spec.field_mapping:
        text = str(field_name or "").strip()
        key = text.lower()
        if not text or key in seen:
            continue
        fields.append(text)
        seen.add(key)
    return tuple(fields)


def fetch_dataset(
    spec: DatasetSpec,
    *,
    client: Optional[TinyClient] = None,
    codes: Optional[Iterable[Any]] = None,
    start_date: Any = None,
    end_date: Any = None,
    report_period: Any = None,
    trade_date: Any = None,
    refresh: bool = False,
    cache: bool = True,
) -> pd.DataFrame:
    if report_period is not None:
        start_date = report_period
        end_date = report_period
    if trade_date is not None:
        start_date = trade_date
        end_date = trade_date

    query_codes = list(codes) if codes is not None and not isinstance(codes, str) else codes
    if query_codes in (None, "") and not spec.allow_full_table:
        if not spec.code_pool:
            raise TinyDataCodePoolError(f"Dataset {spec.name} requires codes but has no code pool name.")
        pool_codes = load_code_pool(spec.code_pool)
        if not pool_codes:
            raise TinyDataCodePoolError(
                f"Dataset {spec.name} requires codes. Pass codes=... or create ~/.tinydata/codes/{spec.code_pool}.csv."
            )
        query_codes = pool_codes

    query_fields = _dataset_query_fields(spec)
    cache_params = {
        "codes": query_codes,
        "start_date": start_date,
        "end_date": end_date,
        "report_period": report_period,
        "trade_date": trade_date,
        "table_id": spec.table_id,
        "field_version": spec.field_version,
        "fields": query_fields,
    }
    manager = CacheManager()
    key = make_cache_key(spec.name, cache_params)
    if cache and not refresh:
        cached = manager.read(spec.name, key)
        if cached is not None:
            return cached

    raw = query_infotable(
        client or TinyClient(),
        spec.table_id,
        codes=query_codes,
        start_date=start_date,
        end_date=end_date,
        date_field=spec.date_field,
        fields=query_fields,
        allow_full_table=spec.allow_full_table,
        options=InfoTableOptions(code_batch_size=spec.code_batch_size),
    )
    processed = process_dataset_frame(raw, spec)
    if cache:
        manager.write(spec.name, key, processed)
    return processed
