"""Persistent execution and audit metadata for the factor domain."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional
from uuid import UUID, uuid4

from psycopg2.extras import Json


DDL_PATH = Path(__file__).parent / "database" / "create_factor_governance_tables.sql"


def json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_ready(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def stable_config_hash(payload: Mapping[str, Any]) -> str:
    serialized = json.dumps(
        json_ready(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class FactorGovernanceStore:
    """Synchronous store used by the factor worker thread."""

    def __init__(self, db_manager: Any):
        if not hasattr(db_manager, "_get_sync_connection"):
            raise TypeError("FactorGovernanceStore需要同步DBManager")
        self.db_manager = db_manager

    def ensure_schema(self) -> None:
        sql = DDL_PATH.read_text(encoding="utf-8")
        connection = self.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def start_run(
        self,
        task_names: Iterable[str],
        mode: str,
        effective_cutoff_date: date,
        *,
        requested_start_date: Optional[date] = None,
        requested_end_date: Optional[date] = None,
        formula_versions: Optional[Mapping[str, str]] = None,
        config: Optional[Mapping[str, Any]] = None,
        source_watermarks: Optional[Mapping[str, Any]] = None,
        details: Optional[Mapping[str, Any]] = None,
    ) -> UUID:
        self.ensure_schema()
        run_id = uuid4()
        task_list = list(task_names)
        config_payload = dict(config or {})
        query = """
        INSERT INTO factors.factor_run (
            run_id, task_names, run_mode, status,
            requested_start_date, requested_end_date, effective_cutoff_date,
            formula_versions, config_hash, source_watermarks, details_json
        ) VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, %s, %s, %s)
        """
        connection = self.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        str(run_id),
                        task_list,
                        mode,
                        requested_start_date,
                        requested_end_date,
                        effective_cutoff_date,
                        Json(json_ready(formula_versions or {})),
                        stable_config_hash(config_payload),
                        Json(json_ready(source_watermarks or {})),
                        Json(json_ready(details or {})),
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return run_id

    def finish_run(
        self,
        run_id: UUID | str,
        status: str,
        *,
        details: Optional[Mapping[str, Any]] = None,
        source_watermarks: Optional[Mapping[str, Any]] = None,
    ) -> None:
        query = """
        UPDATE factors.factor_run
        SET status = %s,
            details_json = details_json || %s::jsonb,
            source_watermarks = CASE
                WHEN %s::jsonb = '{}'::jsonb THEN source_watermarks
                ELSE %s::jsonb
            END,
            finished_at = CURRENT_TIMESTAMP
        WHERE run_id = %s
        """
        details_json = json.dumps(json_ready(details or {}), ensure_ascii=False)
        watermarks_json = json.dumps(
            json_ready(source_watermarks or {}), ensure_ascii=False
        )
        connection = self.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        status,
                        details_json,
                        watermarks_json,
                        watermarks_json,
                        str(run_id),
                    ),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def record_date(
        self,
        run_id: UUID | str,
        task_name: str,
        calc_date: date,
        status: str,
        *,
        input_count: int = 0,
        output_count: int = 0,
        coverage_rate: Optional[float] = None,
        output_checksum: Optional[str] = None,
        duration_ms: Optional[int] = None,
        is_current: bool = False,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        connection = self.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                self.record_date_cursor(
                    cursor,
                    run_id,
                    task_name,
                    calc_date,
                    status,
                    input_count=input_count,
                    output_count=output_count,
                    coverage_rate=coverage_rate,
                    output_checksum=output_checksum,
                    duration_ms=duration_ms,
                    is_current=is_current,
                    details=details,
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    @staticmethod
    def record_date_cursor(
        cursor: Any,
        run_id: UUID | str,
        task_name: str,
        calc_date: date,
        status: str,
        *,
        input_count: int = 0,
        output_count: int = 0,
        coverage_rate: Optional[float] = None,
        output_checksum: Optional[str] = None,
        duration_ms: Optional[int] = None,
        is_current: bool = False,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if is_current:
            cursor.execute(
                """
                UPDATE factors.factor_run_date
                SET is_current = FALSE
                WHERE task_name = %s AND calc_date = %s AND is_current
                """,
                (task_name, calc_date),
            )
        cursor.execute(
            """
            INSERT INTO factors.factor_run_date (
                run_id, task_name, calc_date, status, input_count, output_count,
                coverage_rate, output_checksum, duration_ms, is_current, details_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, task_name, calc_date) DO UPDATE SET
                status = EXCLUDED.status,
                input_count = EXCLUDED.input_count,
                output_count = EXCLUDED.output_count,
                coverage_rate = EXCLUDED.coverage_rate,
                output_checksum = EXCLUDED.output_checksum,
                duration_ms = EXCLUDED.duration_ms,
                is_current = EXCLUDED.is_current,
                details_json = EXCLUDED.details_json,
                created_at = CURRENT_TIMESTAMP
            """,
            (
                str(run_id),
                task_name,
                calc_date,
                status,
                int(input_count),
                int(output_count),
                coverage_rate,
                output_checksum,
                duration_ms,
                bool(is_current),
                Json(json_ready(details or {})),
            ),
        )

    def latest_source_watermarks(self, task_name: str) -> Dict[str, Any]:
        row = self.db_manager.fetch_one_sync(
            """
            SELECT source_watermarks
            FROM factors.factor_run
            WHERE %s = ANY(task_names)
              AND status IN ('success', 'partial_success')
              AND source_watermarks <> '{}'::jsonb
            ORDER BY finished_at DESC NULLS LAST, started_at DESC
            LIMIT 1
            """,
            (task_name,),
        )
        if not row:
            return {}
        value = row.get("source_watermarks")
        return dict(value) if isinstance(value, Mapping) else {}

    def record_public_status(self, task_name: str, status: str, details: str) -> None:
        self.db_manager.execute_sync(
            """
            INSERT INTO public.task_status (task_name, status, details)
            VALUES (%s, %s, %s)
            """,
            (task_name, status, details),
        )


__all__ = [
    "DDL_PATH",
    "FactorGovernanceStore",
    "json_ready",
    "stable_config_hash",
]
