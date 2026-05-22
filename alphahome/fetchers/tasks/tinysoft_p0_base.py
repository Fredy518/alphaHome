#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Shared helpers for Tinysoft cross-domain P0 InfoArray tasks."""

from __future__ import annotations

import asyncio
import json
import math
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from tqdm.asyncio import tqdm

from ...common.constants import UpdateTypes
from ..sources.tinysoft import TinySoftTask
from ..sources.tushare.batch_utils import normalize_date_range


CHANNEL_NAMES = {
    "HG000001": "港股通(沪)",
    "HG000002": "沪股通",
    "HG000003": "港股通(深)",
    "HG000004": "深股通",
}


def parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    return default


def parse_positive_int(value: Any, default: int, *, min_value: int = 1) -> int:
    try:
        parsed = int(value)
        return max(min_value, parsed)
    except (TypeError, ValueError):
        return max(min_value, int(default))


def parse_list(raw_values: Any) -> List[str]:
    if raw_values is None:
        return []
    if isinstance(raw_values, str):
        items: Iterable[Any] = re.split(r"[,;\s]+", raw_values)
    elif isinstance(raw_values, (list, tuple, set)):
        items = raw_values
    else:
        items = [raw_values]

    values: List[str] = []
    for item in items:
        text = str(item or "").strip().upper()
        if text:
            values.append(text)
    return list(dict.fromkeys(values))


def to_date(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text in {"0", "00000000"}:
        return None
    if text.isdigit() and len(text) == 8:
        return pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(text, errors="coerce")


def to_numeric(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"none", "nan", "null", "可用"}:
        return None
    try:
        numeric = float(text)
        return numeric if math.isfinite(numeric) else None
    except Exception:
        return None


def to_int(value: Any) -> Optional[int]:
    numeric = to_numeric(value)
    if numeric is None:
        return None
    try:
        return int(numeric)
    except Exception:
        return None


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


def row_to_json(row: pd.Series) -> str:
    def _default(value: Any) -> str:
        if isinstance(value, (pd.Timestamp,)):
            return value.isoformat()
        return str(value)

    payload: Dict[str, Any] = {}
    for key, value in row.items():
        try:
            if pd.isna(value):
                payload[str(key)] = None
                continue
        except Exception:
            pass
        payload[str(key)] = value
    return json.dumps(payload, ensure_ascii=False, default=_default)


def get_row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return getattr(row, key, default)


def tinysoft_symbol_to_ts_code_any(symbol: Any) -> Optional[str]:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return None
    m = re.fullmatch(r"(SH|SZ|BJ)(\d{6})", raw)
    if m:
        return f"{m.group(2)}.{m.group(1)}"
    m = re.fullmatch(r"OF(\d{6})", raw)
    if m:
        return f"{m.group(1)}.OF"
    return None


def ts_code_to_tinysoft_symbol_any(ts_code: Any) -> Optional[str]:
    raw = str(ts_code or "").strip().upper()
    if not raw:
        return None
    if "." in raw:
        code, suffix = raw.rsplit(".", 1)
        if re.fullmatch(r"\d{6}", code) and suffix in {"SH", "SZ", "BJ", "OF"}:
            return f"{suffix}{code}"
    if re.fullmatch(r"(SH|SZ|BJ|OF)\d{6}", raw):
        return raw
    return None


def map_hsgt_security_to_ts_code(security_code: Any, channel_code: Any = None) -> Optional[str]:
    raw = str(security_code or "").strip().upper()
    if not raw:
        return None
    mapped = tinysoft_symbol_to_ts_code_any(raw)
    if mapped:
        return mapped
    if re.fullmatch(r"\d{6}", raw):
        channel = str(channel_code or "").strip().upper()
        if channel == "HG000002" or raw.startswith(("5", "6", "9")):
            return f"{raw}.SH"
        if channel == "HG000004" or raw.startswith(("0", "1", "2", "3")):
            return f"{raw}.SZ"
    return None


def parse_metric_components(field_name: str) -> Dict[str, str]:
    raw = str(field_name or "").strip()
    matches = list(re.finditer(r"\(([^()]*)\)", raw))
    if not matches:
        return {
            "metric_name": raw,
            "period_type": "report",
            "agg_method": "unknown",
            "sample_scope": "unknown",
        }

    last = matches[-1]
    tokens = [x.strip() for x in last.group(1).split(",") if x.strip()]
    metric_name = (raw[: last.start()] + raw[last.end() :]).strip() or raw
    period_type = "report"
    agg_method = "unknown"
    sample_scope = "unknown"

    if len(tokens) >= 3:
        period_type, agg_method, sample_scope = tokens[0], tokens[1], tokens[2]
    elif len(tokens) == 2:
        agg_method, sample_scope = tokens[0], tokens[1]
    elif len(tokens) == 1:
        agg_method = tokens[0]

    return {
        "metric_name": metric_name,
        "period_type": period_type,
        "agg_method": agg_method,
        "sample_scope": sample_scope,
    }


class TinySoftP0InfoArrayTask(TinySoftTask):
    """Declarative Tinysoft InfoArray task for P0 cross-domain tables."""

    default_concurrent_limit = 1
    default_query_timeout_ms = 60_000
    # Tinysoft InfoArray tables may receive supplier-side corrections after the
    # business/statistical date, so SMART needs a wider overlap than generic fetchers.
    smart_lookback_days = 30
    infoarray_table_id: int = 0
    source_table_name: str = ""
    default_code_batch_size = 50
    default_smart_code_batch_size: Optional[int] = None
    default_use_config_codes = False
    default_use_batch_infotable = True
    default_skip_failed_codes = True
    default_allow_full_table = False
    default_stream_batches = True
    default_continue_on_stream_batch_failure = False
    default_include_raw_json = True
    default_use_field_projection = True
    default_fallback_to_select_all_on_projection_error = True
    default_codes: List[str] = []
    code_config_keys: Sequence[str] = ("codes", "tinysoft_codes")
    code_column: str = "source_code"
    request_code_column: str = "request_code"
    where_date_field: Optional[str] = "截止日"
    field_mapping: Dict[str, str] = {}
    date_fields: Sequence[str] = ()
    numeric_fields: Sequence[str] = ()
    integer_fields: Sequence[str] = ()
    text_fields: Sequence[str] = ()
    validation_mode = "report"

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        return []

    def _get_codes_from_mapping(self, params: Dict[str, Any]) -> List[str]:
        for key in self.code_config_keys:
            values = parse_list(params.get(key))
            if values:
                return values
        return []

    def _get_config_codes(self, params: Optional[Dict[str, Any]] = None) -> List[str]:
        return self._get_codes_from_mapping(self.task_specific_config if params is None else params)

    async def _resolve_codes(self, **kwargs: Any) -> List[str]:
        codes = self._get_codes_from_mapping(kwargs)
        runtime_codes = bool(codes)

        use_config_codes = parse_bool(
            kwargs.get(
                "use_config_codes",
                self.task_specific_config.get("use_config_codes", self.default_use_config_codes),
            ),
            default=self.default_use_config_codes,
        )
        if not runtime_codes and use_config_codes:
            codes = self._get_config_codes()

        if not codes:
            codes = await self._load_codes_from_db()
        if not codes:
            codes = list(self.default_codes)

        max_codes = kwargs.get("max_codes")
        if max_codes is None and use_config_codes and not runtime_codes:
            max_codes = self.task_specific_config.get("max_codes")
        if max_codes is not None:
            try:
                codes = codes[: max(1, int(max_codes))]
            except Exception:
                self.logger.warning("max_codes 配置无效: %s", max_codes)

        return list(dict.fromkeys(str(code).strip().upper() for code in codes if str(code).strip()))

    def _build_where_clause(self, start_date: Any, end_date: Any = None) -> Optional[str]:
        if not self.where_date_field or not start_date:
            return None
        clauses: List[str] = []
        try:
            dt = pd.to_datetime(str(start_date), errors="raise")
            clauses.append(f'["{self.where_date_field}"]>={dt.strftime("%Y%m%d")}')
        except Exception:
            return None
        if end_date:
            try:
                end_dt = pd.to_datetime(str(end_date), errors="raise")
                clauses.append(f'["{self.where_date_field}"]<={end_dt.strftime("%Y%m%d")}')
            except Exception:
                pass
        return " and ".join(clauses) if clauses else None

    def _resolve_code_batch_size(self, kwargs: Dict[str, Any]) -> int:
        code_batch_size = parse_positive_int(
            kwargs.get(
                "code_batch_size",
                self.task_specific_config.get("code_batch_size", self.default_code_batch_size),
            ),
            self.default_code_batch_size,
        )

        update_type = str(kwargs.get("update_type") or "").strip().lower()
        if update_type != UpdateTypes.SMART:
            return code_batch_size

        smart_raw = kwargs.get(
            "smart_code_batch_size",
            self.task_specific_config.get("smart_code_batch_size", self.default_smart_code_batch_size),
        )
        if smart_raw is None:
            return code_batch_size
        return parse_positive_int(smart_raw, code_batch_size)

    def _include_raw_json(self, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        params = kwargs or {}
        return parse_bool(
            params.get(
                "include_raw_json",
                self.task_specific_config.get("include_raw_json", self.default_include_raw_json),
            ),
            default=self.default_include_raw_json,
        )

    def _use_field_projection(self, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        params = kwargs or {}
        return parse_bool(
            params.get(
                "use_field_projection",
                self.task_specific_config.get("use_field_projection", self.default_use_field_projection),
            ),
            default=self.default_use_field_projection,
        )

    def _fallback_to_select_all_on_projection_error(self, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        params = kwargs or {}
        return parse_bool(
            params.get(
                "fallback_to_select_all_on_projection_error",
                self.task_specific_config.get(
                    "fallback_to_select_all_on_projection_error",
                    self.default_fallback_to_select_all_on_projection_error,
                ),
            ),
            default=self.default_fallback_to_select_all_on_projection_error,
        )

    @staticmethod
    def _coerce_query_field_list(raw_values: Any) -> List[str]:
        if raw_values is None:
            return []
        if isinstance(raw_values, str):
            items: Iterable[Any] = re.split(r"[,;\n]+", raw_values)
        elif isinstance(raw_values, (list, tuple, set)):
            items = raw_values
        else:
            items = [raw_values]
        return [str(item).strip() for item in items if str(item or "").strip()]

    @staticmethod
    def _query_field_key(field: Any) -> str:
        return str(field or "").strip().lower()

    def _resolve_query_fields(self, kwargs: Optional[Dict[str, Any]] = None) -> Optional[List[str]]:
        if not self._use_field_projection(kwargs):
            return None

        params = kwargs or {}
        configured = params.get(
            "query_fields",
            self.task_specific_config.get(
                "query_fields",
                params.get("fields", self.task_specific_config.get("fields")),
            ),
        )
        source_fields = (
            self._coerce_query_field_list(configured)
            if configured is not None
            else list(self.field_mapping.keys())
        )

        fields: List[str] = []
        seen = set()

        def add_field(field: Any) -> None:
            text = str(field or "").strip()
            key = self._query_field_key(text)
            if not text or key in seen:
                return
            fields.append(text)
            seen.add(key)

        for field in source_fields:
            add_field(field)
        add_field("StockID")
        if self.where_date_field:
            add_field(self.where_date_field)

        return fields or None

    def _should_stream_batches(self, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        return super()._should_stream_batches(kwargs)

    def _continue_on_stream_batch_failure(self, kwargs: Optional[Dict[str, Any]] = None) -> bool:
        params = kwargs or {}
        return parse_bool(
            params.get(
                "continue_on_stream_batch_failure",
                self.task_specific_config.get(
                    "continue_on_stream_batch_failure",
                    self.default_continue_on_stream_batch_failure,
                ),
            ),
            default=self.default_continue_on_stream_batch_failure,
        )

    async def _get_effective_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        date_range = await self._determine_date_range()
        if not date_range:
            return []

        start_date = date_range["start_date"]
        end_date = date_range["end_date"]
        self._effective_start_date = start_date
        self._effective_end_date = end_date

        if self.start_date:
            kwargs["start_date"] = self.start_date
        if self.end_date:
            kwargs["end_date"] = self.end_date
        kwargs["update_type"] = self.update_type

        batch_gen_params = {**kwargs, "start_date": start_date, "end_date": end_date}
        from ..tools.calendar import reset_calendar_db_manager, set_calendar_db_manager

        calendar_token = set_calendar_db_manager(self.db)
        try:
            return await self.get_batch_list(**batch_gen_params)
        finally:
            reset_calendar_db_manager(calendar_token)

    async def _fetch_stream_batch_with_retry(
        self,
        batch: Dict[str, Any],
        *,
        stop_event: Optional[asyncio.Event],
    ) -> Dict[str, Any]:
        last_error = None
        for attempt in range(self.max_retries):
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError(f"{self.name} 流式批次拉取被取消")
            try:
                params = await self.prepare_params(batch)
                data = await self.fetch_batch(params, stop_event=stop_event)
                return {"success": True, "batch": batch, "params": params, "data": data}
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = e
                self.logger.warning(
                    "'%s' - Streaming batch %s failed on attempt %s/%s. Error: %s",
                    self.name,
                    batch,
                    attempt + 1,
                    self.max_retries,
                    e,
                )
                if attempt + 1 < self.max_retries:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))

        return {
            "success": False,
            "batch": batch,
            "error": str(last_error) if last_error else "unknown batch failure",
        }

    async def _process_stream_frame(
        self,
        raw_data: pd.DataFrame,
        *,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event],
        runtime_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        if raw_data is None or raw_data.empty:
            return {"data": None, "validation": True, "validation_details": None}

        process_kwargs = {**runtime_kwargs, **params}
        processed = self.process_data(raw_data, stop_event=stop_event, **process_kwargs)
        if asyncio.iscoroutine(processed):
            processed = await processed
        if processed is None or processed.empty:
            return {"data": None, "validation": True, "validation_details": None}

        validation_passed, validated_data, validation_details = self._validate_data(
            processed,
            stop_event=stop_event,
            validation_mode=getattr(self, "validation_mode", "report"),
        )
        if validated_data is None or validated_data.empty:
            return {
                "data": None,
                "validation": validation_passed,
                "validation_details": validation_details,
            }

        return {
            "data": validated_data,
            "validation": validation_passed,
            "validation_details": validation_details,
        }

    async def _save_stream_buffer(
        self,
        buffer: List[pd.DataFrame],
        *,
        stop_event: Optional[asyncio.Event],
        ensure_table: bool = True,
    ) -> Dict[str, Any]:
        if not buffer:
            return {"rows": 0, "table_checked": False}
        data = buffer[0] if len(buffer) == 1 else pd.concat(buffer, ignore_index=True)
        save_result = await self._save_data(
            data,
            stop_event=stop_event,
            ensure_table=ensure_table,
        )
        rows = save_result.get("rows", 0) if isinstance(save_result, dict) else 0
        return {"rows": rows, "table_checked": True}

    async def _process_and_save_stream_frame(
        self,
        raw_data: pd.DataFrame,
        *,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event],
        runtime_kwargs: Dict[str, Any],
        ensure_table: bool = True,
    ) -> Dict[str, Any]:
        process_result = await self._process_stream_frame(
            raw_data,
            params=params,
            stop_event=stop_event,
            runtime_kwargs=runtime_kwargs,
        )
        validated_data = process_result.get("data")
        if validated_data is None or validated_data.empty:
            return {
                "rows": 0,
                "validation": process_result.get("validation", True),
                "validation_details": process_result.get("validation_details"),
            }
        save_result = await self._save_stream_buffer(
            [validated_data],
            stop_event=stop_event,
            ensure_table=ensure_table,
        )
        return {
            "rows": save_result.get("rows", 0),
            "validation": process_result.get("validation", True),
            "validation_details": process_result.get("validation_details"),
            "table_checked": save_result.get("table_checked", False),
        }

    async def _execute_streaming(
        self,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        self.logger.info("'%s' - Streaming fetch/process/save enabled.", self.name)
        await self._pre_execute(stop_event=stop_event, **kwargs)

        batches = await self._get_effective_batch_list(**kwargs)
        if not batches:
            self.logger.info("'%s' - No batches to process. Task finished.", self.name)
            result = {"status": "no_data", "rows": 0}
            await self._post_execute(result, stop_event=stop_event)
            return result

        total_rows = 0
        processed_batches = 0
        empty_batches = 0
        saved_batches = 0
        failed_batches: List[Dict[str, Any]] = []
        validation_passed_all = True
        last_validation_details = None
        table_checked = False
        continue_on_failure = self._continue_on_stream_batch_failure(kwargs)
        concurrency = max(1, int(getattr(self, "concurrent_limit", 1)))
        stream_save_batch_size = self._resolve_stream_save_batch_size(kwargs)
        save_buffer: List[pd.DataFrame] = []
        save_buffer_rows = 0
        save_buffer_batch_count = 0
        progress_completed_batches = 0
        progress_log_interval = max(1, min(50, len(batches) // 20 or 1))

        progress_bar = tqdm(total=len(batches), desc=f"Executing {self.name}", unit="batch")
        iterator = iter(batches)
        pending: set[asyncio.Task] = set()

        def schedule_next() -> None:
            try:
                batch = next(iterator)
            except StopIteration:
                return
            pending.add(
                asyncio.create_task(
                    self._fetch_stream_batch_with_retry(batch, stop_event=stop_event)
                )
            )

        def update_stream_progress(batch_count: int = 1) -> None:
            nonlocal progress_completed_batches
            if batch_count <= 0:
                return

            progress_bar.update(batch_count)
            progress_completed_batches += batch_count
            progress_bar.set_postfix(
                rows=total_rows,
                buffered=save_buffer_rows,
                saves=saved_batches,
                empty=empty_batches,
                failed=len(failed_batches),
                refresh=False,
            )
            if (
                progress_completed_batches <= 3
                or progress_completed_batches >= len(batches)
                or progress_completed_batches % progress_log_interval == 0
            ):
                self.logger.info(
                    "'%s' - Streaming progress: %s/%s batches handled, %s rows saved, "
                    "%s rows buffered, %s empty, %s failed.",
                    self.name,
                    progress_completed_batches,
                    len(batches),
                    total_rows,
                    save_buffer_rows,
                    empty_batches,
                    len(failed_batches),
                )

        async def flush_save_buffer() -> None:
            nonlocal save_buffer, save_buffer_rows, save_buffer_batch_count
            nonlocal total_rows, table_checked, saved_batches
            if not save_buffer:
                return
            buffered_batch_count = save_buffer_batch_count
            save_result = await self._save_stream_buffer(
                save_buffer,
                stop_event=stop_event,
                ensure_table=not table_checked,
            )
            total_rows += int(save_result.get("rows", 0) or 0)
            if save_result.get("table_checked"):
                table_checked = True
                saved_batches += 1
            save_buffer = []
            save_buffer_rows = 0
            save_buffer_batch_count = 0
            update_stream_progress(buffered_batch_count)

        for _ in range(min(concurrency, len(batches))):
            schedule_next()

        try:
            while pending:
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError(f"{self.name} 流式执行被取消")

                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    batch_result = await task

                    if not batch_result.get("success"):
                        failed_batches.append(batch_result)
                        if not continue_on_failure:
                            update_stream_progress(1)
                            for item in pending:
                                item.cancel()
                            sample = batch_result.get("error", "unknown batch failure")
                            raise RuntimeError(f"'{self.name}' - Streaming batch failed: {sample}")
                        update_stream_progress(1)
                        schedule_next()
                        continue

                    raw_data = batch_result.get("data")
                    if raw_data is None or raw_data.empty:
                        empty_batches += 1
                        update_stream_progress(1)
                        schedule_next()
                        continue

                    processed_batches += 1
                    chunk_result = await self._process_stream_frame(
                        raw_data,
                        params=batch_result.get("params") or {},
                        stop_event=stop_event,
                        runtime_kwargs=kwargs,
                    )
                    if not chunk_result.get("validation", True):
                        validation_passed_all = False
                    if chunk_result.get("validation_details") is not None:
                        last_validation_details = chunk_result.get("validation_details")

                    validated_data = chunk_result.get("data")
                    if validated_data is not None and not validated_data.empty:
                        save_buffer.append(validated_data)
                        save_buffer_rows += len(validated_data)
                        save_buffer_batch_count += 1
                        if save_buffer_rows >= stream_save_batch_size:
                            await flush_save_buffer()
                    else:
                        update_stream_progress(1)

                    schedule_next()
            await flush_save_buffer()
        finally:
            progress_bar.close()

        status = "no_data" if total_rows == 0 and not failed_batches else "success"
        if failed_batches or not validation_passed_all:
            status = "partial_success" if total_rows > 0 else "error"

        final_result = {
            "status": status,
            "table": self.table_name,
            "rows": total_rows,
            "processed_batches": processed_batches,
            "empty_batches": empty_batches,
            "saved_batches": saved_batches,
            "failed_batches": len(failed_batches),
            "stream_batches": True,
            "validation": validation_passed_all,
            "validation_details": last_validation_details
            or {
                "status": "passed" if validation_passed_all else "failed",
                "validation_mode": getattr(self, "validation_mode", "report"),
            },
        }
        await self._post_execute(final_result, stop_event=stop_event)
        self.logger.info("任务执行完成: %s", final_result)
        return final_result

    async def execute(
        self,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs: Any,
    ):
        if not self._should_stream_batches(kwargs):
            return await super().execute(stop_event=stop_event, **kwargs)

        try:
            return await self._execute_streaming(stop_event=stop_event, **kwargs)
        except asyncio.CancelledError:
            self.logger.warning("任务 %s 被取消。", self.name)
            return self._handle_error(asyncio.CancelledError("任务被用户取消"))
        except Exception as e:
            self.logger.error(
                "任务执行失败: 类型=%s, 错误=%s",
                type(e).__name__,
                str(e),
                exc_info=True,
            )
            return self._handle_error(e)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        start_date, end_date = normalize_date_range(
            start_date=kwargs.get("start_date"),
            end_date=kwargs.get("end_date"),
            default_start_date=self.default_start_date,
            logger=self.logger,
            task_name=self.name,
        )

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info("起始日期 (%s) 晚于结束日期 (%s)，无需执行任务。", start_date, end_date)
            return []

        table_id = parse_positive_int(
            kwargs.get(
                "infoarray_table_id",
                self.task_specific_config.get("infoarray_table_id", self.infoarray_table_id),
            ),
            self.infoarray_table_id,
        )
        code_batch_size = self._resolve_code_batch_size(kwargs)
        codes = await self._resolve_codes(**kwargs)
        allow_full_table = parse_bool(
            kwargs.get(
                "allow_full_table",
                self.task_specific_config.get("allow_full_table", self.default_allow_full_table),
            ),
            default=self.default_allow_full_table,
        )

        if not codes and not allow_full_table:
            self.logger.warning("任务 %s 未获取到有效 Tinysoft 取数代码，跳过。", self.name)
            return []
        if not codes and allow_full_table:
            return [
                {
                    "codes": [],
                    "full_table": True,
                    "start_date": start_date,
                    "end_date": end_date,
                    "infoarray_table_id": table_id,
                    "service": self.service,
                    "timeout_ms": self.query_timeout_ms,
                }
            ]

        batches = []
        for i in range(0, len(codes), code_batch_size):
            batches.append(
                {
                    "codes": codes[i : i + code_batch_size],
                    "start_date": start_date,
                    "end_date": end_date,
                    "infoarray_table_id": table_id,
                    "service": self.service,
                    "timeout_ms": self.query_timeout_ms,
                }
            )

        self.logger.info(
            "任务 %s: 生成 %s 个批次（%s 个取数代码, 每批 %s 个）",
            self.name,
            len(batches),
            len(codes),
            code_batch_size,
        )
        return batches

    async def _fetch_single_code(
        self,
        *,
        code: str,
        table_id: int,
        where_clause: Optional[str],
        fields: Optional[Sequence[Any]],
        fallback_to_select_all_on_projection_error: bool,
        service: Optional[str],
        timeout_ms: Optional[int],
        stop_event: Optional[asyncio.Event],
    ) -> Optional[pd.DataFrame]:
        try:
            df = await self.api.call_dataframe(
                "infoarray",
                table_id,
                stock=code,
                where_clause=where_clause,
                fields=fields,
                service=service,
                timeout_ms=timeout_ms,
                stop_event=stop_event,
            )
        except Exception as exc:
            if not fields or not fallback_to_select_all_on_projection_error:
                raise
            self.logger.warning("%s 字段投影单代码查询失败，回退 select *: %s, 代码: %s", self.name, exc, code)
            df = await self.api.call_dataframe(
                "infoarray",
                table_id,
                stock=code,
                where_clause=where_clause,
                fields=None,
                service=service,
                timeout_ms=timeout_ms,
                stop_event=stop_event,
            )
        if df is None or df.empty:
            return None
        out = df.copy()
        out[self.request_code_column] = code
        if "StockID" not in out.columns and "stockid" not in {str(c).lower() for c in out.columns}:
            out["StockID"] = code
        return out

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        table_id = parse_positive_int(
            params.get("infoarray_table_id", self.infoarray_table_id),
            self.infoarray_table_id,
        )
        service = params.get("service", self.service)
        timeout_ms = parse_positive_int(params.get("timeout_ms", self.query_timeout_ms), self.query_timeout_ms)
        skip_failed_codes = parse_bool(
            params.get(
                "skip_failed_codes",
                self.task_specific_config.get("skip_failed_codes", self.default_skip_failed_codes),
            ),
            default=self.default_skip_failed_codes,
        )
        use_batch = parse_bool(
            params.get(
                "use_batch_infotable",
                self.task_specific_config.get("use_batch_infotable", self.default_use_batch_infotable),
            ),
            default=self.default_use_batch_infotable,
        )
        where_clause = self._build_where_clause(params.get("start_date"), params.get("end_date"))
        query_fields = self._resolve_query_fields(params)
        projection_fallback = self._fallback_to_select_all_on_projection_error(params)

        if params.get("full_table"):
            method = getattr(self.api, "call_dataframe_table", None)
            if not callable(method):
                raise RuntimeError("TinySoftAPI 不支持 call_dataframe_table")
            try:
                return await method(
                    "infoarray",
                    table_id,
                    where_clause=where_clause,
                    fields=query_fields,
                    service=service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
            except Exception as exc:
                if not query_fields or not projection_fallback:
                    raise
                self.logger.warning("%s 字段投影全表查询失败，回退 select *: %s", self.name, exc)
                return await method(
                    "infoarray",
                    table_id,
                    where_clause=where_clause,
                    fields=None,
                    service=service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )

        codes = [str(code).strip().upper() for code in params.get("codes", []) if str(code).strip()]
        if not codes:
            return None

        batch_method = getattr(self.api, "call_dataframe_for_stocks", None)
        if use_batch and len(codes) > 1 and callable(batch_method):
            try:
                batch_df = await batch_method(
                    "infoarray",
                    table_id,
                    stocks=codes,
                    where_clause=where_clause,
                    fields=query_fields,
                    service=service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
            except Exception as e:
                if query_fields and projection_fallback:
                    self.logger.warning("%s 字段投影批量拉取失败，回退 select *: %s", self.name, e)
                    try:
                        batch_df = await batch_method(
                            "infoarray",
                            table_id,
                            stocks=codes,
                            where_clause=where_clause,
                            fields=None,
                            service=service,
                            timeout_ms=timeout_ms,
                            stop_event=stop_event,
                        )
                    except Exception as fallback_error:
                        self.logger.warning("%s 批量拉取失败，将回退逐代码查询: %s", self.name, fallback_error)
                    else:
                        if batch_df is None or batch_df.empty:
                            return None
                        if self._has_symbol_identifier(batch_df):
                            return batch_df.copy()
                        self.logger.warning("%s 批量结果缺少标识列，回退逐代码查询。", self.name)
                else:
                    self.logger.warning("%s 批量拉取失败，将回退逐代码查询: %s", self.name, e)
            else:
                if batch_df is None or batch_df.empty:
                    return None
                if self._has_symbol_identifier(batch_df):
                    return batch_df.copy()
                self.logger.warning("%s 批量结果缺少标识列，回退逐代码查询。", self.name)

        frames: List[pd.DataFrame] = []
        for code in codes:
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError(f"{self.name} 批次拉取被取消")
            try:
                one = await self._fetch_single_code(
                    code=code,
                    table_id=table_id,
                    where_clause=where_clause,
                    fields=query_fields,
                    fallback_to_select_all_on_projection_error=projection_fallback,
                    service=service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
            except Exception as e:
                if not skip_failed_codes:
                    raise
                self._record_skipped_symbol(code, e)
                self.logger.warning("%s 拉取失败（跳过）: %s, 错误: %s", self.name, code, e)
                continue
            if one is not None and not one.empty:
                frames.append(one)

        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    def _apply_field_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        for src, dst in self.field_mapping.items():
            if src in df.columns and dst not in df.columns:
                df.rename(columns={src: dst}, inplace=True)
        return df

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        return df

    def _filter_effective_window(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if not self.date_column or self.date_column not in df.columns:
            return df
        start_date = getattr(self, "_effective_start_date", None) or kwargs.get("start_date")
        end_date = getattr(self, "_effective_end_date", None) or kwargs.get("end_date")
        out = df
        if start_date:
            start_dt = pd.to_datetime(str(start_date), errors="coerce")
            if not pd.isna(start_dt):
                out = out[out[self.date_column] >= start_dt.date()]
        if end_date:
            end_dt = pd.to_datetime(str(end_date), errors="coerce")
            if not pd.isna(end_dt):
                out = out[out[self.date_column] <= end_dt.date()]
        return out

    def _numeric_abs_limit(self, col: str) -> Optional[float]:
        col_def = self.schema_def.get(col)
        if not isinstance(col_def, dict):
            return None
        type_name = str(col_def.get("type", "")).upper()
        match = re.match(r"NUMERIC\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", type_name)
        if not match:
            return None
        precision = int(match.group(1))
        scale = int(match.group(2))
        integer_digits = precision - scale
        if integer_digits <= 0:
            return None
        return float(10 ** integer_digits)

    def _coerce_numeric_to_schema_bounds(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        for col in self.numeric_fields:
            if col not in df.columns:
                continue
            abs_limit = self._numeric_abs_limit(col)
            if abs_limit is None:
                continue
            values = pd.to_numeric(df[col], errors="coerce")
            overflow_mask = values.abs() >= abs_limit
            overflow_count = int(overflow_mask.sum())
            if overflow_count <= 0:
                continue
            df.loc[overflow_mask, col] = None
            self.logger.warning(
                "任务 %s: 数值字段 %s 有 %s 条记录超过数据库精度上限(abs<%s)，已置为NULL，原始值保留在 raw_json。",
                self.name,
                col,
                overflow_count,
                abs_limit,
            )
        return df

    def process_data(self, data, **kwargs):
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()
        if self._include_raw_json(kwargs):
            df["raw_json"] = data.apply(row_to_json, axis=1)

        df = self._apply_field_mapping(df)
        if self.request_code_column in df.columns and self.code_column not in df.columns:
            df[self.code_column] = df[self.request_code_column]

        for col in self.date_fields:
            if col in df.columns:
                df[col] = df[col].map(to_date)
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        for col in self.numeric_fields:
            if col in df.columns:
                df[col] = df[col].map(to_numeric)
        df = self._coerce_numeric_to_schema_bounds(df)
        for col in self.integer_fields:
            if col in df.columns:
                df[col] = df[col].map(to_int)
        for col in self.text_fields:
            if col in df.columns:
                df[col] = df[col].map(clean_text)

        df["source_table_id"] = parse_positive_int(
            kwargs.get(
                "infoarray_table_id",
                self.task_specific_config.get("infoarray_table_id", self.infoarray_table_id),
            ),
            self.infoarray_table_id,
        )
        df["source_table_name"] = self.source_table_name or self.description or self.name

        df = self._postprocess_frame(df, **kwargs)
        if df is None or df.empty:
            return pd.DataFrame()

        df = self._filter_effective_window(df, **kwargs)
        if df.empty:
            return pd.DataFrame()

        for col in self.schema_def:
            if col not in df.columns:
                df[col] = None

        df = super().process_data(df, **kwargs)
        target_columns = [c for c in self.schema_def.keys() if c in df.columns]
        df = df[target_columns]
        if self.primary_keys:
            df = df.dropna(subset=[pk for pk in self.primary_keys if pk in df.columns])
            if not df.empty:
                df = df.drop_duplicates(subset=self.primary_keys, keep="last")
        return df
