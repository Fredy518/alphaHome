"""Unified task wrapper for PIT managers."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import threading
from dataclasses import asdict, dataclass
from typing import Any, ClassVar, Dict, Iterable, Optional, Sequence, Type

from alphahome.common.constants import UpdateTypes
from alphahome.common.logging_utils import get_logger
from alphahome.common.schema_names import PIT_SCHEMA
from alphahome.common.task_system.base_task import BaseTask


PIT_MONTH_END_CUTOFF_CONFIG_KEY = "pit_month_end_cutoff"


def _class_path(value: Any) -> str:
    if isinstance(value, str):
        return value
    return f"{value.__module__}.{value.__name__}"


def _resolve_class(value: Type[Any] | str) -> Type[Any]:
    if not isinstance(value, str):
        return value
    module_name, class_name = value.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


@dataclass(frozen=True)
class PITTaskContract:
    """Serializable contract that describes one PIT output task."""

    task_name: str
    domain: str
    source_tables: Sequence[str]
    output_table: str
    pit_time_key: str
    primary_keys: Sequence[str]
    dependencies: Sequence[str]
    supported_modes: Sequence[str]
    manager_class: Type[Any] | str
    # Existing PIT tasks are stock-shaped and historically audited against the
    # current listed universe.  Keep those defaults so older registrations and
    # serialized payloads remain valid, while allowing aggregate outputs to
    # declare their real audit entity and PIT-time denominator.
    audit_entity_keys: Sequence[str] = ()
    audit_denominator: str = "current_listed_stocks"

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["source_tables"] = list(self.source_tables)
        payload["primary_keys"] = list(self.primary_keys)
        payload["dependencies"] = list(self.dependencies)
        payload["supported_modes"] = list(self.supported_modes)
        payload["audit_entity_keys"] = list(self.audit_entity_keys)
        payload["manager_class"] = _class_path(self.manager_class)
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "PITTaskContract":
        data = dict(payload)
        data["source_tables"] = tuple(data.get("source_tables") or ())
        data["primary_keys"] = tuple(data.get("primary_keys") or ())
        data["dependencies"] = tuple(data.get("dependencies") or ())
        data["supported_modes"] = tuple(data.get("supported_modes") or ())
        data["audit_entity_keys"] = tuple(data.get("audit_entity_keys") or ())
        data.setdefault("audit_denominator", "current_listed_stocks")
        return cls(**data)

    def resolve_manager_class(self) -> Type[Any]:
        return _resolve_class(self.manager_class)


class PITTask(BaseTask):
    """BaseTask adapter for synchronous PITTableManager implementations."""

    task_type = "pit"
    data_source = PIT_SCHEMA
    auto_add_update_time = False
    timestamp_column_name = None

    contract: ClassVar[PITTaskContract]
    description = ""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        contract = getattr(cls, "contract", None)
        if contract is None:
            return
        cls.name = getattr(cls, "name", None) or contract.task_name
        cls.table_name = getattr(cls, "table_name", None) or contract.output_table.split(".")[-1]
        cls.domain = getattr(cls, "domain", None) or contract.domain
        cls.primary_keys = list(contract.primary_keys)
        cls.source_tables = list(contract.source_tables)
        cls.dependencies = list(contract.dependencies)
        cls.date_column = contract.pit_time_key

    def __init__(self, db_connection, **kwargs):
        super().__init__(db_connection, **kwargs)
        self.logger = get_logger(f"pit.task.{self.name}")

    async def _fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        return {}

    def get_contract(self) -> PITTaskContract:
        return self.contract

    def supports_incremental_update(self) -> bool:
        return "incremental" in set(self.contract.supported_modes)

    async def execute(
        self,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        if stop_event and stop_event.is_set():
            return {"status": "cancelled", "task": self.name, "error": "任务在开始前被取消"}

        mode = self._resolve_pit_mode()
        if mode not in set(self.contract.supported_modes):
            return {
                "status": "error",
                "task": self.name,
                "error": f"PIT任务不支持执行模式: {mode}",
            }

        if mode == "audit_only":
            from alphahome.pit.audit_service import PITAuditService

            audit_service = PITAuditService(self.db)
            result = await audit_service.audit_task(self.name, persist=True)
            return {
                "status": "error" if result.get("status") == "error" else "success",
                "readiness_status": result.get("status"),
                "task": self.name,
                "table": self.table_name,
                "rows": int(result.get("row_count") or 0),
                "audit": result,
            }

        manager_class = self.contract.resolve_manager_class()
        call_name, call_kwargs = self._manager_call(mode)
        self.logger.info("开始执行PIT任务: %s, mode=%s, call=%s", self.name, mode, call_name)

        cancel_requested = threading.Event()
        worker = asyncio.create_task(asyncio.to_thread(
            self._run_manager, manager_class, call_name, call_kwargs, cancel_requested,
        ))
        try:
            result = await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancel_requested.set()
            # A synchronous manager may already be committing. Do not release the
            # caller's run lock or close its session while that worker is alive.
            while not worker.done():
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not worker.cancelled() and worker.exception() is None:
                self.last_execution_result = worker.result()
            raise
        except Exception as exc:
            self.logger.error("PIT任务执行失败: %s", exc, exc_info=True)
            return {"status": "error", "task": self.name, "table": self.table_name, "error": str(exc)}

        if stop_event and stop_event.is_set():
            result = {**result, "cancel_requested": True}
            if result.get("status") == "success":
                result["status"] = "cancelled"
        self.last_execution_result = result
        return result

    def _run_manager(self, manager_class, call_name, call_kwargs, cancel_requested):
        from ..planning_time import frozen_pit_time

        with frozen_pit_time(self.task_config.get("pit_business_date"), self.task_config.get("planned_date_range")):
            return self._run_bound_manager(manager_class, call_name, call_kwargs, cancel_requested)

    def _run_bound_manager(self, manager_class, call_name, call_kwargs, cancel_requested):
        """Construct, bind, execute, normalize and close on one worker thread."""
        if cancel_requested.is_set():
            return {"status": "cancelled", "task": self.name, "committed_rows": 0}
        manager = manager_class()
        if "planned_months" in self.task_config:
            from datetime import date
            manager._planned_months = [date.fromisoformat(value) for value in self.task_config["planned_months"]]
        connection_string = getattr(self.db, "connection_string", None)
        if isinstance(connection_string, str) and connection_string:
            manager.bind_database(database_url=connection_string)
        else:
            manager.bind_database(db_manager=self.db)
        with manager:
            if cancel_requested.is_set():
                return {"status": "cancelled", "task": self.name, "committed_rows": 0}
            result = self._call_manager_method(getattr(manager, call_name), call_kwargs)
            stats = getattr(manager, "stats", {})
            if isinstance(result, dict) and stats.get("error_records"):
                result = {**result, "error_records": max(int(result.get("error_records") or 0), stats["error_records"])}
            if isinstance(result, dict) and hasattr(manager, "_verified_committed_rows"):
                result = {**result, "committed_rows": manager._verified_committed_rows}
            self._sync_manager_stats_from_result(manager, result)
            normalized = self._normalize_result(result)
            normalized["plan_hash"] = self.task_config.get("plan_hash")
            if self.contract.pit_time_key == 'obs_date':
                completed = getattr(manager, '_verified_replacement_months', ())
                normalized['completed_months'] = [value.isoformat() for value in completed]
                requested = set(self.task_config.get('planned_months') or ())
                if requested - set(normalized['completed_months']):
                    normalized.update(status='error', error='pit_incomplete_planned_months')
            if cancel_requested.is_set():
                normalized["cancel_requested"] = True
            return normalized

    def _resolve_pit_mode(self) -> str:
        explicit = self.task_config.get("pit_mode") or self.task_config.get("mode")
        if explicit:
            return str(explicit)

        update_type = self.update_type
        if update_type in (UpdateTypes.SMART, UpdateTypes.SMART_DISPLAY, "incremental", "smart"):
            return "incremental"
        if update_type in (UpdateTypes.FULL, UpdateTypes.FULL_DISPLAY, "full", "full_backfill"):
            return "full_backfill"
        if update_type in (UpdateTypes.MANUAL, UpdateTypes.MANUAL_DISPLAY, "manual"):
            if self.task_config.get("ts_code"):
                return "single_backfill"
            return "manual_range"
        return "incremental"

    def _manager_call(self, mode: str) -> tuple[str, Dict[str, Any]]:
        if mode == "incremental":
            return "incremental_update", self._kwargs_for_incremental()
        if mode in ("full_backfill", "manual_range"):
            return "full_backfill", self._kwargs_for_backfill()
        if mode == "single_backfill":
            return "single_backfill", self._kwargs_for_single_backfill()
        raise ValueError(f"Unsupported PIT mode: {mode}")

    def _kwargs_for_incremental(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        for key in ("days", "months", "batch_size"):
            if self.task_config.get(key) is not None:
                kwargs[key] = self.task_config[key]
        cutoff_date = (
            self.task_config.get(PIT_MONTH_END_CUTOFF_CONFIG_KEY)
            if self.contract.pit_time_key == "obs_date"
            else None
        )
        if cutoff_date is not None:
            kwargs["cutoff_date"] = cutoff_date
        return kwargs

    def _kwargs_for_backfill(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        start_date = self.start_date or self.task_config.get("start_date")
        cutoff_date = (
            self.task_config.get(PIT_MONTH_END_CUTOFF_CONFIG_KEY)
            if self.contract.pit_time_key == "obs_date"
            else None
        )
        end_date = self.end_date or self.task_config.get("end_date") or cutoff_date
        if start_date:
            kwargs["start_date"] = start_date
        if end_date:
            kwargs["end_date"] = end_date
        if self.task_config.get("batch_size") is not None:
            kwargs["batch_size"] = self.task_config["batch_size"]
        return kwargs

    def _kwargs_for_single_backfill(self) -> Dict[str, Any]:
        kwargs = self._kwargs_for_backfill()
        ts_code = self.task_config.get("ts_code")
        if not ts_code:
            raise ValueError("single_backfill 模式必须提供 task_config.ts_code")
        kwargs["ts_code"] = ts_code
        if self.task_config.get("do_validate") is not None:
            kwargs["do_validate"] = bool(self.task_config["do_validate"])
        return kwargs

    @staticmethod
    def _call_manager_method(method, kwargs: Dict[str, Any]):
        signature = inspect.signature(method)
        accepts_var_kw = any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in signature.parameters.values()
        )
        if accepts_var_kw:
            return method(**kwargs)
        filtered = {k: v for k, v in kwargs.items() if k in signature.parameters}
        return method(**filtered)

    def _normalize_result(self, result: Any) -> Dict[str, Any]:
        if not isinstance(result, dict):
            return {"status": "success", "task": self.name, "table": self.table_name, "rows": 0, "result": result}

        rows = self._extract_row_count(result)
        committed_rows = self._extract_committed_rows(result)
        error_records = self._extract_error_count(result)
        status = "error" if result.get("error") else result.get("status", "success")
        if error_records and status != "cancelled":
            status = "partial_success" if committed_rows else "error"
            # A processed count is not evidence that any rows were committed.
            rows = committed_rows or 0
        normalized = {
            "status": status,
            "task": self.name,
            "table": self.table_name,
            "rows": rows,
            "committed_rows": committed_rows,
            "attempted_rows": (
                self._coerce_count(result["processed_records"])
                if result.get("processed_records") is not None
                else (committed_rows + error_records if committed_rows is not None else None)
            ),
            "error_records": error_records,
            "result": result,
        }
        if result.get("error") or error_records:
            normalized["error"] = result.get("error") or (
                f"PIT manager reported {error_records} failed records"
            )
        return normalized

    @classmethod
    def _extract_error_count(cls, result: Dict[str, Any]) -> int:
        counts = []
        for key in ("error_records", "errors", "error_count"):
            value = result.get(key)
            if isinstance(value, (list, tuple, dict, set)):
                counts.append(len(value))
            else:
                counts.append(cls._coerce_count(value))
        return max(counts)

    @classmethod
    def _sync_manager_stats_from_result(cls, manager: Any, result: Any) -> None:
        """Populate base-manager counters when a legacy manager did not do so itself."""
        stats = getattr(manager, "stats", None)
        if not isinstance(stats, dict) or not isinstance(result, dict):
            return

        counter_keys = (
            "processed_records",
            "success_records",
            "error_records",
            "skipped_records",
        )
        if any(cls._coerce_count(stats.get(key)) for key in counter_keys):
            return

        success_records = cls._extract_committed_rows(result) or 0
        error_records = cls._extract_error_count(result)
        if error_records == 0 and (result.get("error") or result.get("status") == "error"):
            error_records = 1

        processed_records = cls._coerce_count(result.get("processed_records"))
        if processed_records == 0:
            processed_records = success_records + error_records

        stats["processed_records"] = processed_records
        stats["success_records"] = success_records
        stats["error_records"] = error_records
        stats["skipped_records"] = cls._coerce_count(result.get("skipped_records"))

    @staticmethod
    def _coerce_count(value: Any) -> int:
        try:
            return max(int(value or 0), 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _extract_row_count(cls, result: Dict[str, Any]) -> int:
        committed = cls._extract_committed_rows(result)
        return committed if committed is not None else cls._coerce_count(result.get("processed_records"))

    @staticmethod
    def _extract_committed_rows(result: Dict[str, Any]) -> Optional[int]:
        candidates: Iterable[str] = (
            "committed_rows",
            "backfilled_records",
            "updated_records",
            "success_records",
            "inserted_records",
            "row_count",
            "rows",
        )
        for key in candidates:
            value = result.get(key)
            if value is None:
                continue
            try:
                return max(int(value), 0)
            except (TypeError, ValueError):
                continue
        return None
