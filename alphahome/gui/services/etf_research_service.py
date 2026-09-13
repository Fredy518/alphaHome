"""GUI service for the ETF research-foundation maintenance workflow."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

from ...common.config_manager import get_database_url
from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory
from ...curation.etf_research_foundation import (
    get_etf_research_foundation_status,
    update_etf_research_foundation,
)

logger = get_logger(__name__)

_send_response_callback: Optional[Callable] = None


def initialize_etf_research_service(response_callback: Callable) -> None:
    global _send_response_callback
    _send_response_callback = response_callback
    logger.info("ETF研究底座服务已初始化。")


def _send(command: str, data: Any) -> None:
    if _send_response_callback:
        _send_response_callback(command, data)


async def handle_get_status() -> None:
    try:
        db_manager = UnifiedTaskFactory.get_db_manager()
        result = await get_etf_research_foundation_status(db_manager)
        _send("ETF_RESEARCH_STATUS_UPDATE", result)
    except Exception as exc:
        logger.error("获取ETF研究底座状态失败: %s", exc, exc_info=True)
        _send(
            "ETF_RESEARCH_STATUS_UPDATE",
            {"status": "error", "error": str(exc)},
        )


async def handle_update(candidate_snapshot: str) -> None:
    snapshot_path = Path(candidate_snapshot).expanduser()
    if not candidate_snapshot.strip() or snapshot_path.suffix.lower() != ".json":
        _send(
            "ETF_RESEARCH_UPDATE_COMPLETE",
            {"status": "error", "error": "请选择标准化候选母表 JSON 快照。"},
        )
        return
    if not snapshot_path.is_file():
        _send(
            "ETF_RESEARCH_UPDATE_COMPLETE",
            {"status": "error", "error": f"快照文件不存在: {snapshot_path}"},
        )
        return

    try:
        database_url = get_database_url()
        if not database_url:
            raise RuntimeError("AlphaHome database URL is not configured")
        db_manager = UnifiedTaskFactory.get_db_manager()

        def progress(data: dict[str, Any]) -> None:
            _send("ETF_RESEARCH_PROGRESS", data)

        result = await update_etf_research_foundation(
            db_manager,
            database_url,
            snapshot_path,
            verify_source_file=True,
            progress_callback=progress,
        )
        _send("ETF_RESEARCH_UPDATE_COMPLETE", result)
    except Exception as exc:
        logger.error("ETF研究底座维护失败: %s", exc, exc_info=True)
        _send(
            "ETF_RESEARCH_UPDATE_COMPLETE",
            {"status": "error", "error": str(exc)},
        )


__all__ = [
    "handle_get_status",
    "handle_update",
    "initialize_etf_research_service",
]
