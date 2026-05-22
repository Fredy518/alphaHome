"""InfoTable query helpers."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence

import pandas as pd

from .codes import normalize_codes
from .errors import TinyDataCodePoolError, TinyDataQueryError


def quote_tsl_string(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("Tinysoft string selector cannot be empty")
    return "'" + text.replace("'", "''") + "'"


def format_stock_selector(codes: Iterable[Any]) -> str:
    normalized = normalize_codes(codes)
    if not normalized:
        raise ValueError("Tinysoft stock selector cannot be empty")
    if len(normalized) == 1:
        return quote_tsl_string(normalized[0])
    return "array(" + ",".join(quote_tsl_string(code) for code in normalized) + ")"


def format_select_fields(fields: Optional[Sequence[str]]) -> str:
    if not fields:
        return "*"
    formatted: List[str] = []
    for field in fields:
        raw = str(field).strip()
        if not raw:
            continue
        if raw.startswith("[") or "(" in raw or "." in raw:
            formatted.append(raw)
        else:
            formatted.append(f'["{raw}"]')
    return ", ".join(formatted) if formatted else "*"


def build_where_clause(
    *,
    start_date: Any = None,
    end_date: Any = None,
    date_field: Optional[str] = None,
    extra_where: Optional[str] = None,
) -> Optional[str]:
    clauses: List[str] = []
    if date_field and start_date:
        start = pd.to_datetime(str(start_date), errors="raise").strftime("%Y%m%d")
        clauses.append(f'["{date_field}"]>={start}')
    if date_field and end_date:
        end = pd.to_datetime(str(end_date), errors="raise").strftime("%Y%m%d")
        clauses.append(f'["{date_field}"]<={end}')
    if extra_where:
        clauses.append(f"({extra_where})")
    return " and ".join(clauses) if clauses else None


def build_infotable_query(
    table_id: int,
    *,
    codes: Optional[Iterable[Any]] = None,
    fields: Optional[Sequence[str]] = None,
    where_clause: Optional[str] = None,
    allow_full_table: bool = False,
) -> str:
    select_part = format_select_fields(fields)
    normalized = normalize_codes(codes)
    if normalized:
        of_part = f" of {format_stock_selector(normalized)}"
    elif allow_full_table:
        of_part = ""
    else:
        raise TinyDataCodePoolError(
            "This InfoTable query requires codes. Pass codes=... or provide a local code pool."
        )
    where_part = f" where {where_clause}" if where_clause else ""
    return f"return select {select_part} from infotable {int(table_id)}{of_part}{where_part} end;"


def chunked(values: Sequence[str], size: int) -> List[List[str]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    return [list(values[i : i + size]) for i in range(0, len(values), size)]


def _has_symbol_identifier(df: pd.DataFrame) -> bool:
    columns = {str(col).lower() for col in df.columns}
    return bool({"stockid", "ts_code", "tsl_code"} & columns) or "证券代码" in set(df.columns)


@dataclass(frozen=True)
class InfoTableOptions:
    code_batch_size: int = 100
    retries: int = 3
    retry_delay: float = 1.0
    fallback_to_single: bool = True
    skip_failed_codes: bool = False
    timeout_ms: Optional[int] = None


def _exec_with_retries(client: Any, tsl_code: str, *, options: InfoTableOptions) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    for attempt in range(options.retries):
        try:
            return client.exec(tsl_code, as_dataframe=True, timeout_ms=options.timeout_ms)
        except Exception as exc:  # OPI error types are normalized by TinyClient.
            last_error = exc
            if attempt + 1 < options.retries:
                time.sleep(options.retry_delay * (attempt + 1))
    assert last_error is not None
    raise last_error


def query_infotable(
    client: Any,
    table_id: int,
    *,
    codes: Optional[Iterable[Any]] = None,
    start_date: Any = None,
    end_date: Any = None,
    date_field: Optional[str] = None,
    fields: Optional[Sequence[str]] = None,
    allow_full_table: bool = False,
    extra_where: Optional[str] = None,
    options: Optional[InfoTableOptions] = None,
) -> pd.DataFrame:
    opts = options or InfoTableOptions()
    where_clause = build_where_clause(
        start_date=start_date,
        end_date=end_date,
        date_field=date_field,
        extra_where=extra_where,
    )
    normalized_codes = normalize_codes(codes)

    if not normalized_codes:
        query = build_infotable_query(
            table_id,
            fields=fields,
            where_clause=where_clause,
            allow_full_table=allow_full_table,
        )
        try:
            return _exec_with_retries(client, query, options=opts)
        except Exception as exc:
            message = str(exc)
            if "select 查询的股票为空" in message:
                raise TinyDataQueryError(
                    "Tinysoft rejected a no-code InfoTable query. This table requires codes; "
                    "pass codes=... or configure a local code pool."
                ) from exc
            raise

    frames: List[pd.DataFrame] = []
    for batch in chunked(normalized_codes, opts.code_batch_size):
        try:
            query = build_infotable_query(
                table_id,
                codes=batch,
                fields=fields,
                where_clause=where_clause,
                allow_full_table=False,
            )
            df = _exec_with_retries(client, query, options=opts)
            if df is not None and not df.empty:
                if len(batch) == 1 and not _has_symbol_identifier(df):
                    df = df.copy()
                    df["StockID"] = batch[0]
                if len(batch) > 1 and not _has_symbol_identifier(df) and opts.fallback_to_single:
                    raise TinyDataQueryError("Batch result lacks a symbol identifier")
                frames.append(df)
            continue
        except Exception:
            if not opts.fallback_to_single or len(batch) == 1:
                if not opts.skip_failed_codes:
                    raise
                continue

        for code in batch:
            try:
                query = build_infotable_query(
                    table_id,
                    codes=[code],
                    fields=fields,
                    where_clause=where_clause,
                    allow_full_table=False,
                )
                one = _exec_with_retries(client, query, options=opts)
            except Exception:
                if not opts.skip_failed_codes:
                    raise
                continue
            if one is not None and not one.empty:
                one = one.copy()
                one["request_code"] = code
                if not _has_symbol_identifier(one):
                    one["StockID"] = code
                frames.append(one)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
