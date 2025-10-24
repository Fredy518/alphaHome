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
from ..utils.common import format_status_chinese, format_datetime_for_display
from ...common.constants import UpdateTypes

logger = get_logger(__name__)

# --- Module-level Callbacks ---
_send_response_callback = None

# 会话跟踪变量
_session_start_time = None
_show_history_mode = False  # False: 只显示当前会话, True: 显示所有历史

# 任务停止控制
_global_stop_event = None
_current_running_tasks = []  # 跟踪当前运行的任务

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
            status["status_display"] = format_status_chinese(status.get("status") or "")
            # 格式化更新时间
            update_time_obj = status.get("update_time")
            if isinstance(update_time_obj, datetime):
                status["update_time"] = format_datetime_for_display(update_time_obj)
            elif isinstance(update_time_obj, str):
                try:
                    # 如果是字符串，先解析再格式化
                    dt = datetime.fromisoformat(update_time_obj.replace('Z', '+00:00'))
                    status["update_time"] = format_datetime_for_display(dt)
                except Exception as e:
                    logger.warning(f"解析时间字符串失败: {e}, 值: {update_time_obj}")
                    # 解析失败则保持原样
        
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
    use_insert_mode: bool = False,
):
    """Runs a list of selected tasks with the given parameters."""
    global _global_stop_event, _current_running_tasks
    
    if not db_manager:
        logger.error("DB Manager not initialized in run_tasks.")
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "error", "message": "数据库未连接，无法执行任务。"})
        return

    # 创建新的停止事件
    _global_stop_event = asyncio.Event()
    _current_running_tasks = []
    
    # 首先确保task_status表存在
    await _ensure_task_status_table_exists(db_manager)

    total_tasks = len(tasks_to_run)
    
    try:
        for i, task_info in enumerate(tasks_to_run):
            task_name = task_info.get("task_name")
            if not task_name:
                continue

            # 检查是否收到停止信号
            if _global_stop_event and _global_stop_event.is_set():
                log_msg = f"收到停止信号，跳过剩余任务"
                logger.info(log_msg)
                if _send_response_callback:
                    _send_response_callback("LOG", {"level": "warning", "message": log_msg})
                break

            log_msg = f"({i+1}/{total_tasks}) 正在准备任务: {task_name}"
            logger.info(log_msg)
            if _send_response_callback:
                _send_response_callback("LOG", {"level": "info", "message": log_msg})

            # 记录任务开始状态
            await _record_task_status(db_manager, task_name, "running", f"开始执行 ({i+1}/{total_tasks})")
            # 立即刷新任务状态显示
            await get_all_task_status(db_manager)

            # --- 核心重构：使用新的 create_task_instance 工厂方法 ---
            task_init_params = {}
            if exec_mode == "智能增量":
                task_init_params['update_type'] = UpdateTypes.SMART
            elif exec_mode == "全量更新":
                task_init_params['update_type'] = UpdateTypes.FULL
            elif exec_mode == "手动增量":
                task_init_params['update_type'] = UpdateTypes.MANUAL
                task_init_params['start_date'] = start_date
                task_init_params['end_date'] = end_date
            else:
                logger.warning(f"未知的执行模式: {exec_mode}，将使用默认的 '{UpdateTypes.SMART}' 模式。")
                task_init_params['update_type'] = UpdateTypes.SMART

            # 添加数据保存策略参数
            task_init_params['use_insert_mode'] = use_insert_mode

            try:
                task_instance = await UnifiedTaskFactory.create_task_instance(
                    task_name, **task_init_params
                )
            except Exception as factory_e:
                log_msg = f"任务 {task_name} 实例创建失败: {factory_e}"
                logger.error(log_msg, exc_info=True)
                if _send_response_callback:
                    _send_response_callback("LOG", {"level": "error", "message": log_msg})
                await _record_task_status(db_manager, task_name, "error", "任务实例创建失败")
                await get_all_task_status(db_manager)
                continue
            # --- 重构结束 ---

            # 检查任务是否支持选择的执行模式，如果不支持智能增量则自动切换到全量更新
            actual_exec_mode = exec_mode
            if exec_mode == "智能增量" and not task_instance.supports_incremental_update():
                # 不支持智能增量的任务在智能增量模式下自动切换到全量更新
                actual_exec_mode = "全量更新"
                skip_reason = "该任务不支持智能增量更新，已自动切换到全量更新模式"
                if hasattr(task_instance, 'get_incremental_skip_reason'):
                    try:
                        custom_reason = task_instance.get_incremental_skip_reason()
                        if custom_reason:
                            skip_reason = f"{skip_reason}。{custom_reason}"
                    except Exception as e:
                        logger.error(f"获取任务 {task_name} 的跳过原因时出错: {e}")

                log_msg = f"任务 '{task_instance.get_display_name()}': {skip_reason}"
                logger.info(log_msg)
                if _send_response_callback:
                    _send_response_callback("LOG", {"level": "info", "message": log_msg})

                # 更新任务初始化参数为全量更新模式
                task_init_params['update_type'] = UpdateTypes.FULL

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

            # 将任务添加到运行列表
            _current_running_tasks.append(task_name)

            try:
                log_msg = f"开始执行任务: {task_name}"
                logger.info(log_msg)
                if _send_response_callback:
                    _send_response_callback("LOG", {"level": "info", "message": log_msg})

                # --- 核心重构：统一调用 execute ---
                # 所有参数已在初始化时注入，这里只传递 stop_event
                result = await task_instance.execute(stop_event=_global_stop_event)
                # --- 重构结束 ---

                # 检查任务结果是否为取消状态
                if isinstance(result, dict) and result.get("status") == "cancelled":
                    log_msg = f"任务 {task_name} 被用户取消。"
                    logger.info(log_msg)
                    if _send_response_callback:
                        _send_response_callback("LOG", {"level": "warning", "message": log_msg})
                    
                    # 记录任务取消状态
                    await _record_task_status(db_manager, task_name, "cancelled", "任务被用户取消")
                else:
                    log_msg = f"任务 {task_name} 执行成功。"
                    logger.info(log_msg)
                    if _send_response_callback:
                        _send_response_callback("LOG", {"level": "info", "message": log_msg})
                    
                    # 记录任务成功状态
                    success_details = ""
                    if isinstance(result, dict):
                        rows = result.get("rows", 0)
                        status = result.get("status", "success")
                        success_details = f"处理完成 (行数: {rows})"
                        if status == 'skipped':
                            success_details = result.get('message', '已跳过')

                    await _record_task_status(db_manager, task_name, "success", success_details)


            except Exception as e:
                log_msg = f"任务 {task_name} 执行失败: {e}"
                logger.error(log_msg, exc_info=True)
                if _send_response_callback:
                    _send_response_callback("LOG", {"level": "error", "message": log_msg})
                # 记录任务失败状态
                await _record_task_status(db_manager, task_name, "error", f"执行失败: {e}")
            
            finally:
                # 无论成功失败，都从运行列表中移除
                if task_name in _current_running_tasks:
                    _current_running_tasks.remove(task_name)
                # 立即刷新任务状态显示
                await get_all_task_status(db_manager)

    finally:
        logger.info("所有选定任务已处理完毕。")
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "info", "message": "所有任务已处理完毕。"})
        # 清理停止事件
        _global_stop_event = None
        _current_running_tasks = []
        # 最后再刷新一次状态
        await get_all_task_status(db_manager)

def stop_tasks():
    """停止当前运行的任务"""
    global _global_stop_event, _current_running_tasks
    
    if _global_stop_event is None:
        logger.info("当前没有运行的任务可以停止")
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "info", "message": "当前没有运行的任务"})
        return
    
    if _current_running_tasks:
        running_tasks_str = ", ".join(_current_running_tasks)
        log_msg = f"正在停止任务: {running_tasks_str}"
        logger.info(log_msg)
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "warning", "message": log_msg})
    else:
        log_msg = "正在发送停止信号..."
        logger.info(log_msg)
        if _send_response_callback:
            _send_response_callback("LOG", {"level": "warning", "message": log_msg})
    
    # 设置停止事件
    _global_stop_event.set()
    
    logger.info("停止信号已发送")
    if _send_response_callback:
        _send_response_callback("LOG", {"level": "warning", "message": "停止信号已发送，任务将在安全点停止"})


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