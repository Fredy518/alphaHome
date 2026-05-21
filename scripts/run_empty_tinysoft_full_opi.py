"""Run empty Tinysoft fetch tasks in FULL mode via the configured OPI backend.

The runner discovers registered Tinysoft fetch tasks, skips minute-line tasks,
checks which target tables are missing or empty, and executes those tasks
sequentially. It is intended as an operational helper for one-off backfills.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from typing import Any

from alphahome.common.constants import UpdateTypes
from alphahome.common.logging_utils import setup_logging
from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks import discover_tasks


SECRET_PATTERNS = [
    (re.compile(r"postgres(?:ql)?://[^@\s]+@", re.IGNORECASE), "postgresql://***@"),
    (re.compile(r"(password['\"\s:=]+)[^,}\]\s]+", re.IGNORECASE), r"\1***"),
    (re.compile(r"(token['\"\s:=]+)[^,}\]\s]+", re.IGNORECASE), r"\1***"),
    (
        re.compile(r"(Authorization['\"\s:=]+Basic\s+)[A-Za-z0-9+/=]+", re.IGNORECASE),
        r"\1***",
    ),
]


def redact(value: Any) -> str:
    text = str(value)
    for pattern, replacement in SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


class RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = ()
        return True


def configure_logging() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    setup_logging(log_level="INFO", log_to_file=False, reset=True)
    root = logging.getLogger()
    for handler in root.handlers:
        handler.addFilter(RedactFilter())

    # These loggers include low-level connection details at INFO level.
    logging.getLogger("unified_task_factory").setLevel(logging.WARNING)
    logging.getLogger("db_manager_async").setLevel(logging.WARNING)


def parse_env_list(name: str) -> list[str]:
    raw_value = os.environ.get(name, "")
    return [item.strip() for item in re.split(r"[,;\s]+", raw_value) if item.strip()]


def parse_env_int(name: str, default: int | None = None) -> int | None:
    raw_value = os.environ.get(name)
    if raw_value is None or str(raw_value).strip() == "":
        return default
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return default


def qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def normalize_index_columns(raw_columns: Any) -> list[str]:
    if raw_columns is None:
        return []
    if isinstance(raw_columns, str):
        return [
            item.strip().strip('"')
            for item in raw_columns.split(",")
            if item.strip().strip('"')
        ]
    if isinstance(raw_columns, (list, tuple)):
        return [str(item).strip().strip('"') for item in raw_columns if str(item).strip()]
    return [str(raw_columns).strip().strip('"')]


def schema_table(task_cls: Any) -> tuple[str, str]:
    table_name = getattr(task_cls, "table_name", None)
    if not table_name:
        return "", ""

    table_name = str(table_name)
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        return schema.strip('"'), table.strip('"')

    return str(getattr(task_cls, "data_source", None) or "public"), table_name.strip('"')


async def table_count(db: Any, schema: str, table: str) -> tuple[bool, int | None]:
    exists = await db.fetch_val(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema=$1 AND table_name=$2 AND table_type='BASE TABLE'
        )
        """,
        schema,
        table,
    )
    if not exists:
        return False, None

    count = await db.fetch_val(f"SELECT COUNT(*) FROM {qident(schema)}.{qident(table)}")
    return True, int(count or 0)


async def drop_task_indexes(db: Any, schema: str, table: str, log: logging.Logger) -> None:
    constraints = await db.fetch(
        """
        SELECT conname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname=$1 AND t.relname=$2 AND c.contype IN ('p', 'u')
        """,
        schema,
        table,
    )
    for row in constraints or []:
        conname = row["conname"]
        log.warning("[DROP_CONSTRAINT] %s.%s %s", schema, table, conname)
        await db.execute(
            f"ALTER TABLE {qident(schema)}.{qident(table)} "
            f"DROP CONSTRAINT IF EXISTS {qident(conname)}"
        )

    indexes = await db.fetch(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname=$1 AND tablename=$2
        """,
        schema,
        table,
    )
    for row in indexes or []:
        index_name = row["indexname"]
        log.warning("[DROP_INDEX] %s.%s", schema, index_name)
        await db.execute(f"DROP INDEX IF EXISTS {qident(schema)}.{qident(index_name)}")


async def rebuild_task_indexes(db: Any, task: Any, schema: str, table: str, log: logging.Logger) -> None:
    primary_keys = list(getattr(task, "primary_keys", None) or [])
    if primary_keys:
        existing_pk = await db.fetch_val(
            """
            SELECT conname
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname=$1 AND t.relname=$2 AND c.contype='p'
            LIMIT 1
            """,
            schema,
            table,
        )
        if not existing_pk:
            pk_name = f"{table}_pkey"
            pk_cols = ", ".join(qident(col) for col in primary_keys)
            log.warning("[REBUILD_PK] %s.%s %s", schema, table, pk_name)
            await db.execute(
                f"ALTER TABLE {qident(schema)}.{qident(table)} "
                f"ADD CONSTRAINT {qident(pk_name)} PRIMARY KEY ({pk_cols})"
            )

    date_column = getattr(task, "date_column", None)
    if date_column and date_column not in primary_keys:
        index_name = f"idx_{table}_{date_column}"
        log.warning("[REBUILD_INDEX] %s.%s", schema, index_name)
        await db.execute(
            f"CREATE INDEX IF NOT EXISTS {qident(index_name)} "
            f"ON {qident(schema)}.{qident(table)} ({qident(date_column)})"
        )

    for index_def in getattr(task, "indexes", None) or []:
        if isinstance(index_def, dict):
            columns = normalize_index_columns(index_def.get("columns"))
            if not columns:
                continue
            index_name = str(index_def.get("name") or f"idx_{table}_{'_'.join(columns)}")
            unique_sql = "UNIQUE " if index_def.get("unique") else ""
        else:
            columns = normalize_index_columns(index_def)
            if not columns:
                continue
            index_name = f"idx_{table}_{'_'.join(columns)}"
            unique_sql = ""
        col_sql = ", ".join(qident(col) for col in columns)
        log.warning("[REBUILD_INDEX] %s.%s", schema, index_name)
        await db.execute(
            f"CREATE {unique_sql}INDEX IF NOT EXISTS {qident(index_name)} "
            f"ON {qident(schema)}.{qident(table)} ({col_sql})"
        )


async def main() -> None:
    configure_logging()
    log = logging.getLogger("run_empty_tinysoft_full_opi")
    force_tasks = parse_env_list("TINYSOFT_FULL_FORCE_TASKS")
    override_concurrent_limit = parse_env_int("TINYSOFT_FULL_CONCURRENT_LIMIT")
    override_code_batch_size = parse_env_int("TINYSOFT_FULL_CODE_BATCH_SIZE")
    override_query_timeout_ms = parse_env_int("TINYSOFT_FULL_QUERY_TIMEOUT_MS")
    override_save_batch_size = parse_env_int("TINYSOFT_FULL_SAVE_BATCH_SIZE")
    override_stream_save_batch_size = parse_env_int("TINYSOFT_FULL_STREAM_SAVE_BATCH_SIZE")
    truncate_tasks = set(parse_env_list("TINYSOFT_FULL_TRUNCATE_TASKS"))
    drop_index_tasks = set(parse_env_list("TINYSOFT_FULL_DROP_INDEX_TASKS"))
    use_insert_mode = os.environ.get("TINYSOFT_FULL_USE_INSERT_MODE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }

    discover_tasks(force_reload=True)
    await UnifiedTaskFactory.initialize()
    results: list[dict[str, Any]] = []

    try:
        db = UnifiedTaskFactory.get_db_manager()
        tasks = UnifiedTaskFactory.get_tasks_by_type("fetch")

        candidates: list[dict[str, Any]] = []
        skipped_minutes: list[dict[str, Any]] = []

        for name, cls in sorted(tasks.items()):
            if getattr(cls, "data_source", None) != "tinysoft" and not name.startswith(
                "tinysoft_"
            ):
                continue

            schema, table = schema_table(cls)
            if not schema or not table:
                continue

            is_minute = "minute" in name.lower() or "minute" in table.lower()
            exists, count = await table_count(db, schema, table)
            if exists and (count or 0) > 0:
                continue

            item = {
                "name": name,
                "schema": schema,
                "table": table,
                "exists": exists,
                "count": count,
            }
            if is_minute:
                skipped_minutes.append(item)
            else:
                candidates.append(item)

        if force_tasks:
            forced_candidates = []
            skipped_forced_minutes = []
            seen = set()
            for name in force_tasks:
                cls = tasks.get(name)
                if cls is None:
                    log.warning("[SKIP_UNKNOWN] %s is not a registered fetch task", name)
                    continue
                schema, table = schema_table(cls)
                if not schema or not table:
                    log.warning("[SKIP_NO_TABLE] %s has no target table", name)
                    continue
                is_minute = "minute" in name.lower() or "minute" in table.lower()
                exists, count = await table_count(db, schema, table)
                item = {
                    "name": name,
                    "schema": schema,
                    "table": table,
                    "exists": exists,
                    "count": count,
                }
                if is_minute:
                    skipped_forced_minutes.append(item)
                    continue
                if name not in seen:
                    forced_candidates.append(item)
                    seen.add(name)

            candidates = forced_candidates
            skipped_minutes.extend(skipped_forced_minutes)
            log.info("Using forced task list from TINYSOFT_FULL_FORCE_TASKS.")

        log.info(
            "OPI FULL empty Tinysoft candidates=%s skipped_minute=%s",
            len(candidates),
            len(skipped_minutes),
        )
        for item in skipped_minutes:
            log.info("[SKIP_MINUTE] %s -> %s.%s", item["name"], item["schema"], item["table"])
        for item in candidates:
            log.info(
                "[CANDIDATE] %s -> %s.%s current=%s",
                item["name"],
                item["schema"],
                item["table"],
                item["count"],
            )

        for index, item in enumerate(candidates, start=1):
            name = item["name"]
            schema = item["schema"]
            table = item["table"]
            task = None
            try:
                task_config: dict[str, Any] = {}
                if override_concurrent_limit is not None:
                    task_config["concurrent_limit"] = override_concurrent_limit
                if override_code_batch_size is not None:
                    task_config["code_batch_size"] = override_code_batch_size
                if override_query_timeout_ms is not None:
                    task_config["query_timeout_ms"] = override_query_timeout_ms
                if override_save_batch_size is not None:
                    task_config["save_batch_size"] = override_save_batch_size
                if override_stream_save_batch_size is not None:
                    task_config["stream_save_batch_size"] = override_stream_save_batch_size

                task = await UnifiedTaskFactory.create_task_instance(
                    name,
                    update_type=UpdateTypes.FULL,
                    task_config=task_config,
                    use_insert_mode=use_insert_mode,
                )

                exists_before, count_before = await table_count(db, schema, table)
                if name in truncate_tasks and exists_before:
                    log.warning(
                        "[TRUNCATE] %s -> %s.%s rows_before=%s",
                        name,
                        schema,
                        table,
                        count_before,
                    )
                    await db.execute(f"TRUNCATE TABLE {qident(schema)}.{qident(table)}")
                    exists_before, count_before = await table_count(db, schema, table)

                if name in drop_index_tasks:
                    if not exists_before:
                        await db.create_table_from_schema(task)
                        exists_before, count_before = await table_count(db, schema, table)
                    await drop_task_indexes(db, schema, table, log)

                if not force_tasks and exists_before and (count_before or 0) > 0:
                    result = {
                        "task": name,
                        "status": "skipped_now_has_data",
                        "before_count": count_before,
                        "after_count": count_before,
                    }
                    results.append(result)
                    log.info("[SKIP_HAS_DATA] %s rows=%s", name, count_before)
                    continue

                log.info("[START] %s/%s %s -> %s.%s", index, len(candidates), name, schema, table)
                run_result = await task.execute()
                if name in drop_index_tasks:
                    await rebuild_task_indexes(db, task, schema, table, log)
                exists_after, count_after = await table_count(db, schema, table)
                result = {
                    "task": name,
                    "status": run_result.get("status")
                    if isinstance(run_result, dict)
                    else str(run_result),
                    "rows_reported": run_result.get("rows")
                    if isinstance(run_result, dict)
                    else None,
                    "before_exists": exists_before,
                    "before_count": count_before,
                    "after_exists": exists_after,
                    "after_count": count_after,
                    "result": run_result,
                }
                results.append(result)
                log.info(
                    "[DONE] %s status=%s rows_reported=%s after_count=%s",
                    name,
                    result["status"],
                    result["rows_reported"],
                    count_after,
                )
            except Exception as exc:
                exists_after, count_after = await table_count(db, schema, table)
                result = {
                    "task": name,
                    "status": "exception",
                    "error_type": type(exc).__name__,
                    "error": redact(exc),
                    "before_exists": exists_before,
                    "before_count": count_before,
                    "after_exists": exists_after,
                    "after_count": count_after,
                }
                results.append(result)
                log.exception("[FAILED_EXCEPTION] %s error=%s", name, redact(exc))
                if name in drop_index_tasks and task is not None:
                    try:
                        await rebuild_task_indexes(db, task, schema, table, log)
                    except Exception as rebuild_exc:
                        log.exception(
                            "[REBUILD_INDEX_FAILED] %s error=%s",
                            name,
                            redact(rebuild_exc),
                        )

        summary = {
            "candidate_count": len(candidates),
            "skipped_minute_count": len(skipped_minutes),
            "results": results,
        }
        print("RUN_SUMMARY_JSON=" + json.dumps(summary, ensure_ascii=False, default=str))
    finally:
        await UnifiedTaskFactory.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
