"""GUI-facing factor discovery, preflight, audit and diagnosis service."""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional

from ...common.db_manager import DBManager
from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory
from ...factors.audit_service import FactorAuditService
from ...factors.coordinator import FactorCoordinator


logger = get_logger(__name__)
_factor_task_cache: List[Dict[str, Any]] = []
_send_response_callback: Optional[Callable] = None


def initialize_factor_service(response_callback: Callable) -> None:
    global _send_response_callback
    _send_response_callback = response_callback


def get_cached_factor_tasks() -> List[Dict[str, Any]]:
    return _factor_task_cache


async def handle_get_factor_tasks() -> None:
    global _factor_task_cache
    success = False
    try:
        existing = {
            item["name"]: item.get("selected", False) for item in _factor_task_cache
        }
        service = FactorAuditService(UnifiedTaskFactory.get_db_manager())
        tasks = await service.list_factor_tasks()
        for task in tasks:
            task["selected"] = existing.get(task["name"], False)
        _factor_task_cache = tasks
        _send("FACTOR_TASK_LIST_UPDATE", tasks)
        success = True
    except Exception as exc:
        logger.error("获取因子任务失败: %s", exc, exc_info=True)
        _send("ERROR", f"获取因子任务失败: {exc}")
    finally:
        _send("FACTOR_REFRESH_COMPLETE", {"success": success})


async def handle_preflight(
    task_names: List[str], mode: str, start_date: Optional[str], end_date: Optional[str]
) -> None:
    try:
        async_db = UnifiedTaskFactory.get_db_manager()
        db_url = async_db.connection_string

        def _plan():
            sync_db = DBManager(db_url, mode="sync")
            try:
                return (
                    FactorCoordinator(sync_db)
                    .plan(
                        task_names,
                        mode=mode,
                        start_date=start_date,
                        end_date=end_date,
                        expand_dependencies=True,
                    )
                    .to_dict()
                )
            finally:
                sync_db.close_sync()

        plan = await asyncio.to_thread(_plan)
        _send("FACTOR_PREFLIGHT_COMPLETE", {"success": True, "plan": plan})
    except Exception as exc:
        logger.error("因子预检失败: %s", exc, exc_info=True)
        _send("FACTOR_PREFLIGHT_COMPLETE", {"success": False, "error": str(exc)})


async def handle_audit(task_names: Optional[List[str]] = None) -> None:
    try:
        service = FactorAuditService(UnifiedTaskFactory.get_db_manager())
        if task_names:
            results = [
                await service.audit_task(name, persist=True) for name in task_names
            ]
        else:
            results = await service.audit_all(persist=True)
        _send("FACTOR_AUDIT_COMPLETE", {"success": True, "results": results})
        await handle_get_factor_tasks()
    except Exception as exc:
        logger.error("因子审计失败: %s", exc, exc_info=True)
        _send("FACTOR_AUDIT_COMPLETE", {"success": False, "error": str(exc)})


async def handle_get_gaps(task_names: Optional[List[str]] = None) -> None:
    try:
        result = await FactorAuditService(
            UnifiedTaskFactory.get_db_manager()
        ).get_date_gaps(task_names)
        _send("FACTOR_GAPS_UPDATE", result)
    except Exception as exc:
        _send("ERROR", f"获取因子日期缺口失败: {exc}")


async def handle_diagnose_date(task_name: str, calc_date: str) -> None:
    try:
        result = await FactorAuditService(
            UnifiedTaskFactory.get_db_manager()
        ).diagnose_date(task_name, calc_date)
        _send("FACTOR_DATE_DIAGNOSIS_UPDATE", result)
    except Exception as exc:
        _send("ERROR", f"因子日期诊断失败: {exc}")


async def handle_diagnose_stock(ts_code: str) -> None:
    try:
        result = await FactorAuditService(
            UnifiedTaskFactory.get_db_manager()
        ).diagnose_stock(ts_code)
        _send("FACTOR_STOCK_DIAGNOSIS_UPDATE", result)
    except Exception as exc:
        _send("ERROR", f"因子单股诊断失败: {exc}")


def _send(command: str, payload: Any) -> None:
    if _send_response_callback:
        _send_response_callback(command, payload)


__all__ = [
    "get_cached_factor_tasks",
    "handle_audit",
    "handle_diagnose_date",
    "handle_diagnose_stock",
    "handle_get_factor_tasks",
    "handle_get_gaps",
    "handle_preflight",
    "initialize_factor_service",
]
