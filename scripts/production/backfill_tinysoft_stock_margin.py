#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Backfill Tinysoft tables 165 and 126 into source-level backup tables.

The regular collection tasks use the standard UPSERT path.  This one-time/full
backfill replaces one requested code slice per transaction and COPYs directly
into the new Tinysoft tables, which is substantially faster for millions of
historical rows while remaining restart-safe.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from typing import Any, AsyncIterator, Iterable, Sequence

import pandas as pd

from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks import discover_tasks
from alphahome.fetchers.tasks.tinysoft_p0_base import tinysoft_symbol_to_ts_code_any


LOGGER = logging.getLogger("backfill_tinysoft_stock_margin")


async def _dataframe_records(df: pd.DataFrame) -> AsyncIterator[tuple[Any, ...]]:
    for row in df.itertuples(index=False, name=None):
        yield tuple(None if pd.isna(value) else value for value in row)


async def _fetch_and_validate(task, batch: dict[str, Any]) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(1, task.max_retries + 1):
        try:
            params = await task.prepare_params(batch)
            raw = await task.fetch_batch(params)
            processed = task.process_data(raw, **params)
            if processed is None or processed.empty:
                return pd.DataFrame(columns=task.schema_def.keys())
            valid, data, details = task._validate_data(
                processed,
                validation_mode=task.validation_mode,
            )
            if not valid:
                raise RuntimeError(f"数据验证失败: {details}")
            return data
        except Exception as exc:
            last_error = exc
            if attempt >= task.max_retries:
                break
            delay = task.retry_delay * attempt
            LOGGER.warning(
                "%s 批次失败，%s 秒后重试（%s/%s）: %s",
                task.name,
                delay,
                attempt,
                task.max_retries,
                exc,
            )
            await asyncio.sleep(delay)
    raise RuntimeError(f"{task.name} 批次重试耗尽: {batch}") from last_error


def _detail_target_codes(request_codes: Iterable[str]) -> list[str]:
    return [
        ts_code
        for ts_code in (tinysoft_symbol_to_ts_code_any(code) for code in request_codes)
        if ts_code
    ]


def _summary_target_codes(task, request_codes: Iterable[str]) -> list[str]:
    return [
        exchange_id
        for exchange_id in (
            task.exchange_id_by_market_code.get(str(code).strip().upper())
            for code in request_codes
        )
        if exchange_id
    ]


async def _replace_code_slice(
    task,
    data: pd.DataFrame,
    *,
    key_column: str,
    target_codes: Sequence[str],
) -> int:
    columns = list(data.columns)
    async with task.db.pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f'DELETE FROM "tinysoft"."{task.table_name}" '
                f'WHERE "{key_column}" = ANY($1::text[])',
                list(target_codes),
            )
            if data.empty:
                return 0
            result = await conn.copy_records_to_table(
                task.table_name,
                schema_name="tinysoft",
                records=_dataframe_records(data),
                columns=columns,
                timeout=600,
            )
    if isinstance(result, str) and result.startswith("COPY "):
        return int(result.split()[1])
    return len(data)


async def _run_task_backfill(
    task_name: str,
    *,
    code_batch_size: int,
    query_timeout_ms: int,
    max_codes: int | None,
    missing_only: bool = False,
) -> dict[str, Any]:
    task = await UnifiedTaskFactory.create_task_instance(
        task_name,
        update_type=UpdateTypes.FULL,
        task_config={
            "code_batch_size": code_batch_size,
            "query_timeout_ms": query_timeout_ms,
            "include_raw_json": False,
        },
    )
    await task._ensure_table_exists()
    runtime_kwargs: dict[str, Any] = {"max_codes": max_codes}
    if missing_only and task_name == "tinysoft_stock_margindetail":
        all_codes = await task._resolve_codes()
        rows = await task.db.fetch(
            'SELECT DISTINCT ts_code FROM "tinysoft"."stock_margindetail"'
        )
        existing_codes = {row["ts_code"] for row in rows}
        runtime_kwargs["ts_codes"] = [
            code
            for code in all_codes
            if tinysoft_symbol_to_ts_code_any(code) not in existing_codes
        ]
        LOGGER.info(
            "%s 仅补目标表缺失代码：完整证券宇宙 %s 个，待查询 %s 个",
            task_name,
            len(all_codes),
            len(runtime_kwargs["ts_codes"]),
        )
    batches = await task._get_effective_batch_list(**runtime_kwargs)
    started = time.perf_counter()
    total_rows = 0

    try:
        for index, batch in enumerate(batches, start=1):
            batch_started = time.perf_counter()
            data = await _fetch_and_validate(task, batch)
            request_codes = batch.get("codes") or []
            if task_name == "tinysoft_stock_margindetail":
                key_column = "ts_code"
                target_codes = _detail_target_codes(request_codes)
            else:
                key_column = "exchange_id"
                target_codes = _summary_target_codes(task, request_codes)
            rows = await _replace_code_slice(
                task,
                data,
                key_column=key_column,
                target_codes=target_codes,
            )
            total_rows += rows
            LOGGER.info(
                "%s 进度 %s/%s：本批 %s 行，累计 %s 行，耗时 %.1f 秒",
                task_name,
                index,
                len(batches),
                rows,
                total_rows,
                time.perf_counter() - batch_started,
            )
    finally:
        close = getattr(task.api, "close", None)
        if callable(close):
            await close()

    await task.db.execute(f'ANALYZE "tinysoft"."{task.table_name}"')
    return {
        "task": task_name,
        "batches": len(batches),
        "rows": total_rows,
        "elapsed_seconds": round(time.perf_counter() - started, 2),
    }


async def _main(args: argparse.Namespace) -> int:
    discover_tasks(force_reload=True)
    await UnifiedTaskFactory.initialize()
    results = []
    try:
        if not args.detail_only:
            results.append(
                await _run_task_backfill(
                    "tinysoft_stock_margin",
                    code_batch_size=3,
                    query_timeout_ms=args.query_timeout_ms,
                    max_codes=None,
                )
            )
        if not args.summary_only:
            results.append(
                await _run_task_backfill(
                    "tinysoft_stock_margindetail",
                    code_batch_size=args.detail_code_batch_size,
                    query_timeout_ms=args.query_timeout_ms,
                    max_codes=args.max_codes,
                    missing_only=args.detail_missing_only,
                )
            )
    finally:
        await UnifiedTaskFactory.shutdown()

    for result in results:
        LOGGER.info("完成: %s", result)
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--detail-code-batch-size", type=int, default=100)
    parser.add_argument("--query-timeout-ms", type=int, default=180_000)
    parser.add_argument("--max-codes", type=int)
    parser.add_argument("--detail-missing-only", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--summary-only", action="store_true")
    mode.add_argument("--detail-only", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(asyncio.run(_main(_parse_args())))
