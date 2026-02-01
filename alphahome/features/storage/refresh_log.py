"""
刷新日志写入工具

统一写入 features.mv_refresh_log 的逻辑，避免在多个模块中重复实现。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


async def log_mv_refresh(
    db_manager,
    *,
    view_name: str,
    schema_name: str,
    refresh_strategy: str,
    success: bool,
    duration_seconds: float,
    row_count: int = 0,
    error_message: Optional[str] = None,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
) -> None:
    """
    写入刷新日志到 features.mv_refresh_log。

    说明：
    - 如果提供 started_at/finished_at，则使用显式时间写入（适用于 REFRESH MATERIALIZED VIEW）。
    - 否则使用数据库 NOW() 结合 duration_seconds 推导 started_at（适用于增量/表刷新）。
    """
    if not db_manager:
        return

    if started_at is not None and finished_at is not None:
        sql = """
        INSERT INTO features.mv_refresh_log (
            view_name,
            schema_name,
            refresh_strategy,
            started_at,
            finished_at,
            duration_seconds,
            success,
            error_message,
            row_count
        ) VALUES (
            $1, $2, $3,
            $4 AT TIME ZONE 'Asia/Shanghai',
            $5 AT TIME ZONE 'Asia/Shanghai',
            $6, $7, $8, $9
        );
        """.strip()
        params = (
            view_name,
            schema_name,
            refresh_strategy,
            started_at,
            finished_at,
            duration_seconds,
            success,
            error_message,
            row_count,
        )
    else:
        # 与旧实现保持一致：用数据库 NOW() 推导时间，避免依赖本地时钟/时区
        sql = """
        INSERT INTO features.mv_refresh_log (
            view_name,
            schema_name,
            refresh_strategy,
            started_at,
            finished_at,
            duration_seconds,
            success,
            error_message,
            row_count
        ) VALUES (
            $1, $2, $3,
            (NOW() - INTERVAL '1 second' * $4) AT TIME ZONE 'Asia/Shanghai',
            NOW() AT TIME ZONE 'Asia/Shanghai',
            $4, $5, $6, $7
        );
        """.strip()
        params = (
            view_name,
            schema_name,
            refresh_strategy,
            duration_seconds,
            success,
            error_message,
            row_count,
        )

    try:
        await db_manager.execute(sql, *params)
    except Exception as e:
        # 日志写入失败不应阻断主流程
        logger.warning(f"Failed to log refresh to features.mv_refresh_log: {e}")

