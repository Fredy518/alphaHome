"""
任务执行处理器

负责处理所有与任务执行相关的逻辑，包括：
- 启动、停止任务
- 处理单个或批量任务的执行流程
- 更新任务状态
- 与控制器协调，传递执行结果
"""

import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ...common.db_manager import DBManager, create_async_manager
from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory, base_task
from ..utils.common import format_status_chinese

logger = get_logger(__name__)

# --- Module-level Callbacks ---
_send_response_callback = None

# 会话跟踪变量
_session_start_time = None
_show_history_mode = False  # False: 只显示当前会话, True: 显示所有历史

def set_response_callback(callback):
    """设置响应回调函数。"""
    global _send_response_callback
    _send_response_callback = callback

def initialize_session():
    """初始化会话，记录会话开始时间。"""
    global _session_start_time
    _session_start_time = datetime.now()
    logger.info(f"任务执行会话已初始化，开始时间: {_session_start_time}")

def toggle_history_mode():
    """切换历史显示模式。"""
    global _show_history_mode
    _show_history_mode = not _show_history_mode
    return _show_history_mode

def get_current_display_mode():
    """获取当前显示模式。"""
    return "历史任务" if _show_history_mode else "当前会话任务"

# --- Core Logic ---

async def get_all_task_status(db_manager: DBManager):
    """Fetches the latest status for all tasks from the database."""
    if not db_manager:
        logger.error("DB Manager not initialized in get_all_task_status.")
        return

    # 首先确保task_status表存在
    await _ensure_task_status_table_exists(db_manager)

    # 根据显示模式构建查询
    if _show_history_mode or _session_start_time is None:
        # 显示所有历史任务
        query = """
        SELECT DISTINCT ON (task_name)
            task_name, status, update_time, details
        FROM task_status
        ORDER BY task_name, update_time DESC;
        """
        query_params = []
        log_message = "正在从数据库刷新任务状态（历史模式）..."
    else:
        # 只显示当前会话的任务
        query = """
        SELECT DISTINCT ON (task_name)
            task_name, status, update_time, details
        FROM task_status
        WHERE update_time >= $1
        ORDER BY task_name, update_time DESC;
        """
        query_params = [_session_start_time]
        log_message = "正在从数据库刷新任务状态（当前会话）..."

    try:
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "info", "message": log_message})
        
        if query_params:
            records = await db_manager.fetch(query, *query_params)
        else:
            records = await db_manager.fetch(query)
            
        status_list = [dict(record) for record in records] if records else []
        for status in status_list:
            status["status_display"] = format_status_chinese(status.get("status"))
        
        if _send_response_callback:
            _send_response_callback("TASK_STATUS_UPDATE", status_list)
            mode_text = "历史" if _show_history_mode else "当前会话"
            _send_response_callback("LOG", {"level": "info", "message": f"成功加载 {len(status_list)} 个任务的状态（{mode_text}模式）。"})

    except Exception as e:
        logger.error(f"Failed to fetch task statuses: {e}", exc_info=True)
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "error", "message": f"获取任务状态失败: {e}"})


async def run_tasks(
    db_manager: DBManager,
    tasks_to_run: List[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    exec_mode: str,
):
    """Runs a list of selected tasks with the given parameters."""
    if not db_manager:
        logger.error("DB Manager not initialized in run_tasks.")
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "error", "message": "数据库未连接，无法执行任务。"})
        return

    # 首先确保task_status表存在
    await _ensure_task_status_table_exists(db_manager)

    total_tasks = len(tasks_to_run)
    for i, task_info in enumerate(tasks_to_run):
        task_name = task_info.get("task_name")
        if not task_name:
            continue

        log_msg = f"({i+1}/{total_tasks}) 正在准备任务: {task_name}"
        logger.info(log_msg)
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "info", "message": log_msg})

        # 记录任务开始状态
        await _record_task_status(db_manager, task_name, "running", f"开始执行 ({i+1}/{total_tasks})")
        # 立即刷新任务状态显示
        await get_all_task_status(db_manager)

        task_instance = await UnifiedTaskFactory.get_task(task_name)

        if not task_instance:
            log_msg = f"任务 {task_name} 创建失败，跳过。"
            logger.error(log_msg)
            if _send_response_callback:
                _send_response_callback("LOG", {"level": "error", "message": log_msg})
            # 记录任务失败状态
            await _record_task_status(db_manager, task_name, "error", "任务实例创建失败")
            # 立即刷新任务状态显示
            await get_all_task_status(db_manager)
            continue

        try:
            log_msg = f"开始执行任务: {task_name}"
            logger.info(log_msg)
            if _send_response_callback:
                _send_response_callback("LOG", {"level": "info", "message": log_msg})

            # 准备任务执行参数
            run_kwargs = {}
            if start_date:
                run_kwargs["start_date"] = start_date
            if end_date:
                run_kwargs["end_date"] = end_date
            
            # 根据执行模式设置参数
            if exec_mode == "智能增量":
                # 智能增量模式，让任务自己决定日期范围
                result = await task_instance.smart_incremental_update(**run_kwargs)
            else:
                # 其他模式，直接执行
                result = await task_instance.run(**run_kwargs)

            log_msg = f"任务 {task_name} 执行成功。"
            logger.info(log_msg)
            if _send_response_callback:
                _send_response_callback("LOG", {"level": "info", "message": log_msg})
            
            # 记录任务成功状态
            success_details = ""
            if isinstance(result, dict):
                rows = result.get("rows", 0)
                status = result.get("status", "success")
                success_details = f"处理了 {rows} 行数据, 状态: {status}"
            await _record_task_status(db_manager, task_name, "success", success_details)
            # 立即刷新任务状态显示
            await get_all_task_status(db_manager)

        except Exception as e:
            log_msg = f"任务 {task_name} 执行失败: {e}"
            logger.error(log_msg, exc_info=True)
            if _send_response_callback:
                _send_response_callback("LOG", {"level": "error", "message": log_msg})
            
            # 记录任务失败状态
            await _record_task_status(db_manager, task_name, "error", str(e))
            # 立即刷新任务状态显示
            await get_all_task_status(db_manager)

    final_message = "所有选定任务已执行完毕。"
    logger.info(final_message)
    if _send_response_callback:
        _send_response_callback("LOG", {"level": "info", "message": final_message})
    
    # Refresh task status after execution
    await get_all_task_status(db_manager)


async def _ensure_task_status_table_exists(db_manager: DBManager):
    """确保task_status表存在，如果不存在则创建。"""
    try:
        # 检查表是否存在
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'task_status'
        );
        """
        result = await db_manager.fetch_one(check_query)
        table_exists = result[0] if result else False
        
        if not table_exists:
            logger.info("task_status表不存在，正在创建...")
            create_table_query = """
            CREATE TABLE task_status (
                id SERIAL PRIMARY KEY,
                task_name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL,
                update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                details TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_task_status_name_time ON task_status (task_name, update_time DESC);
            """
            await db_manager.execute(create_table_query)
            logger.info("task_status表创建成功。")
            if _send_response_callback:
                _send_response_callback("LOG", {"level": "info", "message": "已创建task_status表。"})
        else:
            logger.debug("task_status表已存在。")
            
    except Exception as e:
        logger.warning(f"检查或创建task_status表时出错: {e}")
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "warning", "message": f"任务状态表检查失败，将跳过状态显示: {e}"})


async def _record_task_status(db_manager: DBManager, task_name: str, status: str, details: str):
    """记录任务状态到数据库。"""
    if not db_manager:
        logger.error("DB Manager not initialized in _record_task_status.")
        return

    try:
        query = """
        INSERT INTO task_status (task_name, status, details)
        VALUES ($1, $2, $3)
        RETURNING id;
        """
        result = await db_manager.fetch_one(query, task_name, status, details)
        task_id = result[0] if result else None
        
        if task_id:
            logger.debug(f"任务状态记录成功: {task_name} -> {status}")
        else:
            logger.warning(f"任务状态记录失败，任务: {task_name}")

    except Exception as e:
        logger.error(f"记录任务状态时出错: {e}", exc_info=True)
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "warning", "message": f"记录任务状态时出错: {e}"})