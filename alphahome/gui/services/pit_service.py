"""GUI service for PIT task discovery, audit, and diagnostics."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory
from ...pit.audit_service import PITAuditService

logger = get_logger(__name__)

_pit_task_cache: List[Dict[str, Any]] = []
_send_response_callback: Optional[Callable] = None


def initialize_pit_service(response_callback: Callable):
    global _send_response_callback
    _send_response_callback = response_callback
    logger.info("PIT服务已初始化。")


def get_cached_pit_tasks() -> List[Dict[str, Any]]:
    return _pit_task_cache


async def handle_get_pit_tasks():
    global _pit_task_cache
    success = False
    try:
        service = PITAuditService(UnifiedTaskFactory.get_db_manager())
        existing_selection = {item["name"]: item.get("selected", False) for item in _pit_task_cache}
        tasks = await service.list_pit_tasks()
        for task in tasks:
            task["selected"] = existing_selection.get(task["name"], False)
        _pit_task_cache = tasks
        if _send_response_callback:
            _send_response_callback("PIT_TASK_LIST_UPDATE", _pit_task_cache)
            _send_response_callback("STATUS", f"PIT任务列表已刷新 (共 {len(_pit_task_cache)} 个任务)")
        success = True
    except Exception as exc:
        logger.error("获取PIT任务列表失败: %s", exc, exc_info=True)
        if _send_response_callback:
            _send_response_callback("ERROR", f"获取PIT任务列表失败: {exc}")
    finally:
        if _send_response_callback:
            _send_response_callback("PIT_REFRESH_COMPLETE", {"success": success})


async def handle_audit_pit_tasks(task_names: Optional[List[str]] = None):
    try:
        service = PITAuditService(UnifiedTaskFactory.get_db_manager())
        if task_names:
            results = [await service.audit_task(name, persist=True) for name in task_names]
        else:
            results = await service.audit_all(persist=True)
        if _send_response_callback:
            _send_response_callback("PIT_AUDIT_COMPLETE", {"success": True, "results": results})
        await handle_get_pit_tasks()
    except Exception as exc:
        logger.error("PIT审计失败: %s", exc, exc_info=True)
        if _send_response_callback:
            _send_response_callback("PIT_AUDIT_COMPLETE", {"success": False, "error": str(exc)})


async def handle_get_coverage_matrix():
    try:
        service = PITAuditService(UnifiedTaskFactory.get_db_manager())
        matrix = await service.get_coverage_matrix()
        if _send_response_callback:
            _send_response_callback("PIT_COVERAGE_MATRIX_UPDATE", matrix)
    except Exception as exc:
        logger.error("获取PIT覆盖矩阵失败: %s", exc, exc_info=True)
        if _send_response_callback:
            _send_response_callback("ERROR", f"获取PIT覆盖矩阵失败: {exc}")


async def handle_diagnose_stock(ts_code: str):
    try:
        service = PITAuditService(UnifiedTaskFactory.get_db_manager())
        diagnosis = await service.diagnose_stock(ts_code)
        if _send_response_callback:
            _send_response_callback("PIT_STOCK_DIAGNOSIS_UPDATE", diagnosis)
    except Exception as exc:
        logger.error("PIT单股诊断失败: %s", exc, exc_info=True)
        if _send_response_callback:
            _send_response_callback("ERROR", f"PIT单股诊断失败: {exc}")


__all__ = [
    "initialize_pit_service",
    "get_cached_pit_tasks",
    "handle_get_pit_tasks",
    "handle_audit_pit_tasks",
    "handle_get_coverage_matrix",
    "handle_diagnose_stock",
]

