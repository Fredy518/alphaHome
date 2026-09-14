"""Unified task wrapper for production P/G factor calculations."""

from __future__ import annotations

import asyncio
import importlib
from dataclasses import asdict, dataclass
from typing import Any, ClassVar, Dict, Optional, Sequence, Type

from alphahome.common.constants import UpdateTypes
from alphahome.common.db_manager import DBManager
from alphahome.common.logging_utils import get_logger
from alphahome.common.schema_names import FACTOR_SCHEMA
from alphahome.common.task_system.base_task import BaseTask


def _class_path(value: Any) -> str:
    if isinstance(value, str):
        return value
    return f"{value.__module__}.{value.__name__}"


def _resolve_class(value: Type[Any] | str) -> Type[Any]:
    if not isinstance(value, str):
        return value
    module_name, class_name = value.rsplit(".", 1)
    return getattr(importlib.import_module(module_name), class_name)


@dataclass(frozen=True)
class FactorTaskContract:
    """Serializable contract for one governed factor output."""

    task_name: str
    domain: str
    source_tables: Sequence[str]
    output_table: str
    calc_date_key: str
    primary_keys: Sequence[str]
    dependencies: Sequence[str]
    readiness_dependencies: Sequence[str]
    supported_modes: Sequence[str]
    calculator_class: Type[Any] | str
    formula_version: str
    cadence: str = "weekly_friday"
    date_strategy: str = "calendar_friday_last_complete"
    eligibility_policy: str = "task_defined"
    audit_denominator: str = "current_listed_stocks"
    history_lookback_days: int = 0

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        for key in (
            "source_tables",
            "primary_keys",
            "dependencies",
            "readiness_dependencies",
            "supported_modes",
        ):
            payload[key] = list(payload[key])
        payload["calculator_class"] = _class_path(self.calculator_class)
        return payload

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "FactorTaskContract":
        data = dict(payload)
        for key in (
            "source_tables",
            "primary_keys",
            "dependencies",
            "readiness_dependencies",
            "supported_modes",
        ):
            data[key] = tuple(data.get(key) or ())
        return cls(**data)

    def resolve_calculator_class(self) -> Type[Any]:
        return _resolve_class(self.calculator_class)


class FactorTask(BaseTask):
    """BaseTask adapter that runs the synchronous factor coordinator in a thread."""

    task_type = "factor"
    data_source = FACTOR_SCHEMA
    auto_add_update_time = False
    timestamp_column_name = None

    contract: ClassVar[FactorTaskContract]
    description = ""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        contract = getattr(cls, "contract", None)
        if contract is None:
            return
        cls.name = getattr(cls, "name", None) or contract.task_name
        cls.table_name = (
            getattr(cls, "table_name", None) or contract.output_table.split(".")[-1]
        )
        cls.domain = getattr(cls, "domain", None) or contract.domain
        cls.primary_keys = list(contract.primary_keys)
        cls.source_tables = list(contract.source_tables)
        cls.dependencies = list(contract.dependencies)
        cls.date_column = contract.calc_date_key

    def __init__(self, db_connection, **kwargs):
        super().__init__(db_connection, **kwargs)
        self.logger = get_logger(f"factor.task.{self.name}")

    async def _fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        return {}

    def get_contract(self) -> FactorTaskContract:
        return self.contract

    def supports_incremental_update(self) -> bool:
        return "smart" in set(self.contract.supported_modes)

    async def execute(
        self,
        stop_event: Optional[asyncio.Event] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        if stop_event and stop_event.is_set():
            return {
                "status": "cancelled",
                "task": self.name,
                "error": "任务在开始前被取消",
            }
        mode = self._resolve_factor_mode()
        if mode not in set(self.contract.supported_modes):
            return {
                "status": "error",
                "task": self.name,
                "error": f"因子任务不支持执行模式: {mode}",
            }
        if mode == "audit":
            from alphahome.factors.audit_service import FactorAuditService

            result = await FactorAuditService(self.db).audit_task(
                self.name, persist=True
            )
            return {
                "status": "error" if result.get("status") == "error" else "success",
                "readiness_status": result.get("status"),
                "task": self.name,
                "table": self.table_name,
                "rows": int(result.get("row_count") or 0),
                "audit": result,
            }

        db_url = getattr(self.db, "connection_string", None)
        if not db_url:
            return {
                "status": "error",
                "task": self.name,
                "error": "数据库连接未提供URL",
            }

        task_config = dict(self.task_config or {})
        expand_dependencies = bool(task_config.get("factor_expand_dependencies", True))
        max_dates = int(task_config.get("max_automatic_dates", 26))

        def _run(cancellation) -> Dict[str, Any]:
            from alphahome.factors.coordinator import FactorCoordinator

            sync_db = DBManager(db_url, mode="sync")
            try:
                coordinator = FactorCoordinator(sync_db, max_automatic_dates=max_dates)
                result = coordinator.run(
                    [self.name],
                    mode=mode,
                    start_date=self.start_date or task_config.get("start_date"),
                    end_date=self.end_date or task_config.get("end_date"),
                    expand_dependencies=expand_dependencies,
                    stop_requested=lambda: cancellation.is_set() or bool(stop_event and stop_event.is_set()),
                    submitted_plan=task_config.get("domain_plan"),
                    expected_plan_hash=task_config.get("expected_plan_hash"),
                )
                return result.to_dict()
            finally:
                sync_db.close_sync()

        try:
            from alphahome.common.async_worker import run_owned_worker
            result = await run_owned_worker(_run, on_cancelled_result=lambda value: setattr(self, "last_execution_result", value))
        except Exception as exc:
            self.logger.error("因子任务执行失败: %s", exc, exc_info=True)
            return {
                "status": "error",
                "task": self.name,
                "table": self.table_name,
                "error": str(exc),
            }
        return {
            "status": result.get("status", "success"),
            "task": self.name,
            "table": self.table_name,
            "rows": int(result.get("output_count") or 0),
            "result": result,
            "message": result.get("message"),
        }

    def _resolve_factor_mode(self) -> str:
        explicit = self.task_config.get("factor_mode") or self.task_config.get("mode")
        if explicit:
            aliases = {
                "incremental": "smart",
                "backfill": "manual",
                "audit_only": "audit",
            }
            return aliases.get(str(explicit), str(explicit))
        if self.update_type in (
            UpdateTypes.SMART,
            UpdateTypes.SMART_DISPLAY,
            "smart",
            "incremental",
        ):
            return "smart"
        if self.update_type in (UpdateTypes.FULL, UpdateTypes.FULL_DISPLAY, "full"):
            return "full"
        if self.update_type in (
            UpdateTypes.MANUAL,
            UpdateTypes.MANUAL_DISPLAY,
            "manual",
        ):
            return "manual"
        return "smart"


__all__ = ["FactorTask", "FactorTaskContract"]
