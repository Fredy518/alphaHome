import asyncio
import json
import logging  # 仍需导入，但仅用于类型继承
import os
import queue
import threading
import traceback
import urllib.parse  # 需要导入 urllib.parse
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import appdirs  # <-- 导入 appdirs

from ..common.logging_utils import get_logger, setup_logging

# 初始化模块级 logger
logger = get_logger(__name__)  # 使用当前模块名称获取logger

# 使用绝对导入，假设项目根目录在 sys.path 中
from ..fetchers import TaskFactory, base_task

# --- 配置 ---
# 使用 appdirs 获取用户配置目录
APP_NAME = "alphahome"  # <--- 您可以修改应用名称
APP_AUTHOR = "trademaster"  # <--- 建议修改为您的名称或组织名
CONFIG_DIR = appdirs.user_config_dir(APP_NAME, APP_AUTHOR)
CONFIG_FILE_PATH = os.path.join(
    CONFIG_DIR, "config.json"
)  # 配置文件路径现在指向用户目录

# --- 用于线程通信的队列 ---
request_queue = queue.Queue()  # GUI -> 后端线程
response_queue = queue.Queue()  # 后端线程 -> GUI

# --- 内部状态 ---
_task_list_cache: List[Dict[str, Any]] = []  # 任务详情和选择状态的缓存
_running_task_status: Dict[str, Dict[str, Any]] = {}  # 当前运行中任务的状态
_stop_requested = False  # 用于发出停止任务信号的标志 (基础版本)
_backend_thread: Optional[threading.Thread] = None  # 跟踪线程
_backend_running = False  # 指示异步循环是否活动的标志
_current_stop_event: Optional[asyncio.Event] = None

# --- 中文状态映射 ---
STATUS_MAP_CN = {
    "PENDING": "排队中",
    "RUNNING": "运行中",
    "SUCCESS": "成功",
    "FAILED": "失败",
    "CANCELED": "已取消",
    "SKIPPED": "已跳过",
    "WARNING": "部分成功",  # 使用 '部分成功' 替代 '警告'
}


# --- 日志设置 ---
class QueueHandler(logging.Handler):
    """Send log records to the response queue."""

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        log_entry = self.format(record)
        self.log_queue.put(("LOG_ENTRY", log_entry))


# --- 后端异步循环 ---
def _start_async_loop():
    """Target function for the background thread."""
    global _backend_running
    _backend_running = True
    logger.info("后台异步循环启动")
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(_process_requests())
    except Exception as e:
        logger.exception("后台异步循环异常终止")
        response_queue.put(("ERROR", f"后台严重错误: {e}"))
    finally:
        loop.close()
        _backend_running = False
        logger.info("后台异步循环已关闭。")


async def _process_requests():
    """The main async function processing requests from the GUI."""
    global _stop_requested, _backend_running

    # 配置日志处理器
    log_queue = response_queue
    # 配置日志处理器
    queue_handler = QueueHandler(log_queue)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", "%H:%M:%S"
    )
    queue_handler.setFormatter(formatter)
    root_logger = get_logger()  # 获取根 logger，同时捕获来自 data_module 的日志
    root_logger.addHandler(queue_handler)

    # 初始化 TaskFactory
    try:
        await TaskFactory.initialize()
        response_queue.put(("STATUS", "后台初始化完成"))
    except Exception as e:
        response_queue.put(("ERROR", f"TaskFactory 初始化失败: {e}"))
        return

    while _backend_running:  # 检查标志而不是 True
        try:
            # 使用带有短超时的非阻塞 get 以允许检查 _backend_running
            request_type, data = request_queue.get(timeout=0.1)
        except queue.Empty:
            await asyncio.sleep(0.05)  # 防止忙等待
            continue  # 再次检查 _backend_running 标志

        try:
            if request_type == "GET_TASKS":
                await _handle_get_tasks()
            elif request_type == "TOGGLE_SELECT":
                _handle_toggle_select(data)  # data = 行索引
            elif request_type == "EXECUTE_TASKS":
                # 开始前重置停止标志
                _stop_requested = False
                # 在后台执行，不阻塞队列处理
                # Checklist Item 3: Extract smart_increment from data
                start_date = data.get("start_date")
                end_date = data.get("end_date")
                smart_increment = data.get(
                    "smart_increment", False
                )  # Default to False if missing
                selected_task_names = data.get("task_names", [])  # Get selected tasks
                # Checklist Item 3: Pass smart_increment to _handle_execute_tasks
                asyncio.create_task(
                    _handle_execute_tasks(
                        mode=data["mode"],
                        start_date_str=start_date,
                        end_date_str=end_date,
                        task_names=selected_task_names,  # Pass selected task names
                        smart_increment=smart_increment,
                    )
                )
            elif request_type == "REQUEST_STOP":
                if _current_stop_event:
                    _current_stop_event.set()
                    response_queue.put(
                        ("LOG_ENTRY", "收到停止请求，信号已发送给当前任务...")
                    )
                else:
                    response_queue.put(
                        (
                            "LOG_ENTRY",
                            "收到停止请求，但当前没有任务在运行或任务不支持停止。",
                        )
                    )
            elif request_type == "SHUTDOWN":
                logger.info("收到关闭请求，开始关闭...")
                await TaskFactory.shutdown()
                response_queue.put(("LOG_ENTRY", "后台服务已正常关闭。"))
                _backend_running = False  # 设置标志以退出循环
                # 此处不需要 break，循环条件将处理退出
            elif request_type == "SELECT_SPECIFIC":
                _handle_select_specific(data)  # data = 任务名称列表
            elif request_type == "DESELECT_SPECIFIC":
                _handle_deselect_specific(data)  # data = 任务名称列表
            elif request_type == "SAVE_SETTINGS":
                await _handle_save_settings(data)  # data = 来自 GUI 的设置
            else:
                response_queue.put(("LOG_ENTRY", f"未知请求类型: {request_type}"))

        except Exception as e:
            logger.exception(f"处理请求 {request_type} 时出错")  # 记录异常详情
            response_queue.put(("ERROR", f"处理请求 {request_type} 时出错: {e}"))


# --- 请求处理器 (在异步循环中运行) ---
async def _handle_get_tasks():
    """Fetch task list from factory, update cache, and send formatted list to GUI."""
    global _task_list_cache
    success = False  # Track success for final message
    try:
        # 尝试获取任务名称，这可能会因为 TaskFactory 未初始化而失败
        task_names = TaskFactory.get_all_task_names()  # 可能引发 RuntimeError

        # --- 如果成功获取 task_names，继续正常处理 ---
        new_cache = []
        existing_selection = {
            item["name"]: item["selected"] for item in _task_list_cache
        }  # 保留选择状态

        for name in sorted(task_names):  # 首先按字母顺序排序
            try:
                task_instance = await TaskFactory.get_task(name)
                selected = existing_selection.get(name, False)  # 保留选择状态

                # --- 增强的类型提取 ---
                parts = name.split("_")
                task_type = "unknown"  # 默认类型
                if len(parts) > 1:
                    # 映射特定前缀或使用第二部分
                    prefix = parts[0]
                    second_part = parts[1]
                    if prefix == "tushare":
                        if second_part == "fina":
                            task_type = "finance"
                        elif second_part in ["stock", "fund", "index"]:
                            task_type = second_part
                        else:
                            # 如果需要，为其他 tushare 类型提供回退
                            task_type = second_part
                    else:
                        # 对于非 tushare 任务，也许使用前缀？
                        task_type = prefix
                # --- 增强的类型提取结束 ---

                new_cache.append(
                    {
                        "name": name,
                        "type": task_type,
                        "description": getattr(task_instance, "description", ""),
                        "selected": selected,
                        "table_name": getattr(
                            task_instance, "table_name", None
                        ),  # 添加 table_name
                    }
                )
            except Exception as e:
                logger.error(f"获取任务 {name} 详情失败: {e}")
                # 添加错误状态？或跳过？暂时跳过。

        # 按类型然后按名称排序以供显示
        _task_list_cache = sorted(new_cache, key=lambda x: (x["type"], x["name"]))

        # 获取 DBManager 实例
        db_manager = TaskFactory.get_db_manager()
        if not db_manager:
            logger.warning("DBManager not available, cannot fetch update times.")
            for task_detail in _task_list_cache:
                task_detail["latest_update_time"] = "N/A (DB Error)"
            response_queue.put(("TASK_LIST_UPDATE", _task_list_cache))
            response_queue.put(
                ("STATUS", f"任务列表已刷新 (共 {len(_task_list_cache)} 个任务)")
            )
            success = True  # Mark as successful
            return

        # --- 新的并发查询方式 ---
        query_coroutines = []
        tasks_to_query_info = []  # List of (index, table_name) tuples

        # 1. 收集需要查询的任务和协程
        for index, task_detail in enumerate(_task_list_cache):
            table_name = task_detail.get("table_name")
            if table_name:
                # Request the raw datetime object for GUI display
                coro = db_manager.get_latest_date(
                    table_name, "update_time", return_raw_object=True
                )
                query_coroutines.append(coro)
                tasks_to_query_info.append((index, table_name))
            else:
                # 对于没有 table_name 的任务，直接设置默认值
                task_detail["latest_update_time"] = "N/A (No Table)"

        # 2. 并发执行查询
        if query_coroutines:
            logger.info(
                f"Starting concurrent query for {len(query_coroutines)} table timestamps..."
            )
            results = await asyncio.gather(*query_coroutines, return_exceptions=True)
            logger.info("Concurrent timestamp query finished.")

            # 3. 处理查询结果
            for i, result in enumerate(results):
                task_index, table_name = tasks_to_query_info[i]
                latest_timestamp_str = "N/A"  # 重置默认值

                if isinstance(result, Exception):
                    logger.warning(
                        f"Error querying latest timestamp for table {table_name}: {type(result).__name__} - {result}"
                    )
                    latest_timestamp_str = "N/A (Query Error)"
                else:
                    # result 是查询结果 (时间戳) 或 None
                    latest_timestamp = result
                    # --- 日志记录结束 ---

                    if latest_timestamp is not None:
                        # 预期 latest_timestamp 是 datetime 对象或 None
                        if isinstance(latest_timestamp, datetime):
                            # 直接格式化为 YYYY-MM-DD HH:MM:SS
                            latest_timestamp_str = latest_timestamp.strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                        else:
                            # 其他类型，尝试转为字符串
                            logger.warning(
                                f"Expected datetime object for {table_name}, but got {type(latest_timestamp)}. Converting to string."
                            )
                            latest_timestamp_str = str(latest_timestamp)
                    else:
                        latest_timestamp_str = "No Data"

                # 更新缓存中的对应任务
                if 0 <= task_index < len(_task_list_cache):
                    _task_list_cache[task_index][
                        "latest_update_time"
                    ] = latest_timestamp_str
                else:
                    logger.error(
                        f"Task index {task_index} out of bounds while processing timestamp results."
                    )
        # --- 并发查询结束 ---

        logger.info(
            f"Sending updated TASK_LIST with {len(_task_list_cache)} tasks to GUI."
        )
        response_queue.put(("TASK_LIST_UPDATE", _task_list_cache))
        response_queue.put(
            ("STATUS", f"任务列表已刷新 (共 {len(_task_list_cache)} 个任务)")
        )
        success = True  # Mark as successful

    except RuntimeError as e:
        # --- 专门处理 TaskFactory 未初始化的 RuntimeError ---
        if "TaskFactory 尚未初始化" in str(e):
            logger.warning(
                "获取任务列表失败，因为 TaskFactory 尚未初始化。请用户配置数据库。"
            )
            # 发送状态消息，而不是错误弹窗
            response_queue.put(
                ("STATUS", "数据库未配置，无法加载任务。请前往'存储设置'配置并保存。")
            )
            # 清空缓存并更新 GUI 列表为空
            _task_list_cache = []
            response_queue.put(("TASK_LIST_UPDATE", _task_list_cache))
            success = False
        else:
            # 其他类型的 RuntimeError，仍然作为错误处理
            logger.exception("获取任务列表时发生意外的 RuntimeError")
            response_queue.put(("ERROR", f"获取任务列表时发生运行时错误: {e}"))
            success = False

    except Exception as e:
        # --- 处理其他所有异常 ---
        logger.exception("获取任务列表失败")
        # 仍然作为错误发送给 GUI
        response_queue.put(("ERROR", f"获取任务列表失败: {e}"))
        success = False

    finally:
        # Checklist Item 2: Send REFRESH_COMPLETE message regardless of success/failure
        response_queue.put(("REFRESH_COMPLETE", {"success": success}))


def _format_task_list_for_tkinter_treeview() -> List[Dict[str, Any]]:
    """Format the cache for Tkinter Treeview (returns list of dicts)."""
    # 返回所需信息，GUI 端将格式化 'selected' 列
    logger.warning(
        "_format_task_list_for_tkinter_treeview is likely obsolete as full cache is sent."
    )
    return _task_list_cache  # 暂时返回完整缓存


def _handle_toggle_select(row_index: int):
    """Toggle selection state for a task by index and send update."""
    if 0 <= row_index < len(_task_list_cache):
        _task_list_cache[row_index]["selected"] = not _task_list_cache[row_index][
            "selected"
        ]
        # 将更新后的完整缓存列表发送回 GUI
        response_queue.put(("TASK_LIST_UPDATE", _task_list_cache))
    else:
        if "TASK_RUN_PROGRESS" not in item:
            logger.warning(
                f"_handle_toggle_select: 行 {row_index} 数据不完整，缺少进度字段。"
            )


def _handle_select_specific(task_names: List[str]):
    """Set selection state to True for specific tasks and send update."""
    logger.info(f"Controller: Handling SELECT_SPECIFIC for {len(task_names)} tasks.")
    changed = False
    task_name_set = set(task_names)  # 使用集合以加快查找速度
    for task in _task_list_cache:
        if task["name"] in task_name_set:
            if not task["selected"]:  # 仅当状态实际翻转时才标记为已更改
                task["selected"] = True
                changed = True
    if changed:
        response_queue.put(("TASK_LIST_UPDATE", _task_list_cache))
    else:
        logger.info("Controller: No state changed during SELECT_SPECIFIC.")


def _handle_deselect_specific(task_names: List[str]):
    """Set selection state to False for specific tasks and send update."""
    logger.info(f"Controller: Handling DESELECT_SPECIFIC for {len(task_names)} tasks.")
    changed = False
    task_name_set = set(task_names)  # 使用集合以加快查找速度
    for task in _task_list_cache:
        if task["name"] in task_name_set:
            if task["selected"]:  # 仅当状态实际翻转时才标记为已更改
                task["selected"] = False
                changed = True
    if changed:
        response_queue.put(("TASK_LIST_UPDATE", _task_list_cache))
    else:
        logger.info("Controller: No state changed during DESELECT_SPECIFIC.")


# --- Helper function to update running task status ---
def _update_task_status(
    task_name: str,
    status: Optional[str] = None,
    progress: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    details: Optional[str] = None,
):
    """更新单个运行任务的状态并通知GUI。"""
    global _running_task_status

    if task_name in _running_task_status:
        update_payload = {}  # 存储要发送的更新
        task_status = _running_task_status[task_name]

        if status is not None:
            task_status["status"] = status
            update_payload["status"] = status
        if progress is not None:
            task_status["progress"] = progress
            update_payload["progress"] = progress
        if start_time is not None:
            start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
            task_status["start_time"] = start_time_str  # 存储格式化的字符串
            update_payload["start_time"] = start_time_str
        if end_time is not None:
            end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
            task_status["end_time"] = end_time_str  # 存储格式化的字符串
            update_payload["end_time"] = end_time_str
        if details is not None:
            task_status["details"] = details
            update_payload["details"] = details

        # 为特定任务发送更新
        if update_payload:  # 只有当确实有变化时才发送
            # 确保即使在本次调用中未更新的必要字段也存在
            # 这确保GUI接收到一个足够完整的字典
            full_update_for_gui = task_status.copy()
            response_queue.put(("TASK_RUN_UPDATE", {task_name: full_update_for_gui}))
            logger.debug(f"已更新{task_name}的状态: {update_payload}")
    else:
        logger.warning(f"尝试更新未知任务的状态: {task_name}")


async def _handle_execute_tasks(
    mode: str,
    start_date_str: Optional[str],
    end_date_str: Optional[str],
    task_names: List[str],
    smart_increment: bool,
):
    """处理后台选定任务的执行。"""
    global _running_task_status, _current_stop_event

    if not task_names:
        response_queue.put(("LOG_ENTRY", "没有选中的任务可执行。"))
        # 即使没有选择任务，也发送TASKS_FINISHED以更新GUI状态
        response_queue.put(
            ("TASKS_FINISHED", {"completed": 0, "failed": 0, "canceled": 0, "total": 0})
        )
        return

    logger.info(
        f"开始执行 {len(task_names)} 个任务，模式: {mode}, 开始: {start_date_str}, 结束: {end_date_str}, 智能增量: {smart_increment}"
    )  # 记录smart_increment参数
    response_queue.put(
        ("LOG_ENTRY", f"开始执行 {len(task_names)} 个任务 (模式: {mode})...")
    )

    # 创建初始状态列表用于RUN_TABLE_INIT
    initial_statuses = []
    for name in task_names:
        task_info = next((t for t in _task_list_cache if t["name"] == name), None)
        initial_statuses.append(
            {
                "name": name,
                "type": task_info["type"] if task_info else "未知",
                "status": STATUS_MAP_CN["PENDING"],
                "progress": "0%",
                "start_time": None,
                "end_time": None,
                "details": "等待执行",
            }
        )
    _running_task_status = {item["name"]: item for item in initial_statuses}
    response_queue.put(("RUN_TABLE_INIT", initial_statuses))
    response_queue.put(
        ("STATUS", f"正在准备执行 {len(task_names)} 个任务...")
    )  # 更新初始状态

    # 创建停止事件
    _current_stop_event = asyncio.Event()

    total_tasks = len(task_names)
    completed_tasks = 0
    failed_tasks = 0
    canceled_tasks = 0  # 添加已取消任务的计数器

    # --- 顺序执行选中任务 --- #
    for task_name in task_names:
        # <<< 记录开始时间 >>>
        start_time = datetime.now()

        # 更新状态为 '运行中'
        _update_task_status(
            task_name,
            status=STATUS_MAP_CN["RUNNING"],
            start_time=start_time,
            details="开始执行...",
        )

        # 检查停止标志（在开始执行前）
        if _current_stop_event.is_set():
            logger.warning(f"任务 {task_name} 在开始前被取消。")
            _update_task_status(
                task_name, status=STATUS_MAP_CN["CANCELED"], details="执行前被取消"
            )
            canceled_tasks += 1
            continue  # 跳过此任务，执行下一个

        try:
            task = await TaskFactory.get_task(task_name)
            # 检查表项目4: 将smart_increment传递给_execute_single_task
            result = await _execute_single_task(
                task_name,
                task,
                mode,
                start_date_str,
                end_date_str,
                _current_stop_event,
                smart_increment,  # Pass the flag
            )

            # <<< 记录结束时间 >>>
            end_time = datetime.now()

            # 根据执行结果更新状态
            if result.get("status") == "success":
                _update_task_status(
                    task_name,
                    status=STATUS_MAP_CN["SUCCESS"],
                    progress="100%",
                    end_time=end_time,
                    details=f"成功完成 ({result.get('rows', 0)} 行)",
                )
                completed_tasks += 1
            elif result.get("status") == "partial_success":
                _update_task_status(
                    task_name,
                    status=STATUS_MAP_CN["WARNING"],
                    progress="100%",
                    end_time=end_time,
                    details=f"完成但验证失败 ({result.get('rows', 0)} 行)",
                )
                completed_tasks += 1  # 仍然算作完成
            elif (
                result.get("status") == "no_data"
                or result.get("status") == "up_to_date"
            ):
                _update_task_status(
                    task_name,
                    status=STATUS_MAP_CN["SUCCESS"],
                    progress="100%",
                    end_time=end_time,
                    details=f"无需更新 ({result.get('rows', 0)} 行)",
                )
                completed_tasks += 1
            elif result.get("status") == "skipped":
                # 添加对 skipped 状态的处理，视为成功但显示为"已跳过"
                _update_task_status(
                    task_name,
                    status=STATUS_MAP_CN["SKIPPED"],
                    progress="100%",
                    end_time=end_time,
                    details=f"已跳过: {result.get('message', '无详细原因')}",
                )
                completed_tasks += 1  # 跳过也算作完成
            elif result.get("status") == "cancelled":  # 检查特定的已取消状态
                _update_task_status(
                    task_name,
                    status=STATUS_MAP_CN["CANCELED"],
                    end_time=end_time,
                    details="任务被取消",
                )
                canceled_tasks += 1
            else:
                error_msg = result.get("error", "未知错误")
                _update_task_status(
                    task_name,
                    status=STATUS_MAP_CN["FAILED"],
                    end_time=end_time,
                    details=f"失败: {error_msg}",
                )
                failed_tasks += 1
        except asyncio.CancelledError:
            # <<< Record End Time on Cancellation >>>
            end_time = datetime.now()
            logger.warning(f"任务 {task_name} 执行被取消。")
            _update_task_status(
                task_name,
                status=STATUS_MAP_CN["CANCELED"],
                end_time=end_time,
                details="任务被取消",
            )
            canceled_tasks += 1
        except Exception as e:
            # <<< Record End Time on Exception >>>
            end_time = datetime.now()
            error_str = f"执行任务 {task_name} 时出错: {type(e).__name__} - {e}"
            logger.exception(error_str)
            _update_task_status(
                task_name,
                status=STATUS_MAP_CN["FAILED"],
                end_time=end_time,
                details=f"失败: {e}",
            )
            failed_tasks += 1

    # 所有任务完成后
    _current_stop_event = None  # 清除停止事件
    final_status_msg = f"任务执行完毕: {completed_tasks} 成功, {failed_tasks} 失败, {canceled_tasks} 取消"
    logger.info(final_status_msg)
    response_queue.put(("LOG_ENTRY", final_status_msg))
    response_queue.put(
        (
            "TASKS_FINISHED",
            {
                "completed": completed_tasks,
                "failed": failed_tasks,
                "canceled": canceled_tasks,
                "total": total_tasks,
            },
        )
    )


async def _execute_single_task(
    task_name: str,
    task: base_task.Task,
    mode: str,
    start_date_str: Optional[str],
    end_date_str: Optional[str],
    stop_event: Optional[asyncio.Event],
    smart_increment: bool,  # 接收标志
) -> Dict[str, Any]:
    """执行单个任务并返回其结果。"""
    try:
        logger.info(
            f"开始执行单个任务: {task_name}, 模式: {mode}, 智能增量: {smart_increment}"
        )  # 记录smart_increment参数
        # Checklist Item 5: 条件执行逻辑
        if smart_increment:
            logger.info(f"调用 {task_name}.smart_incremental_update")
            # Checklist Item 6: 传递stop_event
            result = await task.smart_incremental_update(stop_event=stop_event)
        else:
            execute_kwargs = {
                "stop_event": stop_event,
                "start_date": start_date_str,
                "end_date": end_date_str,
                # 'smart_increment': smart_increment, # TushareTask.execute使用force_full替代
            }

            # 如果模式为'全量导入'，则设置force_full=True
            if mode == "全量导入":
                execute_kwargs["force_full"] = True
            # 对于'智能增量'，如果start/end日期为None且force_full为False，TushareTask.execute会处理。
            # GUI中的smart_increment标志主要用于区分日期也为None时的'手动增量'。
            # 如果TushareTask的execute特别需要'smart_increment'布尔值，可以添加。
            # 但是，其当前逻辑依赖于start_date、end_date和force_full。

            logger.info(f"调用 {task_name}.execute，参数: {execute_kwargs}")
            result = await task.execute(**execute_kwargs)

        logger.info(f"单个任务 {task_name} 执行结果: {result}")
        return result
    except asyncio.CancelledError:
        # 重要: 重新抛出 CancelledError 以便 _handle_execute_tasks 可以捕获它
        logger.warning(f"单个任务 {task_name} 被取消")
        raise
    except Exception as e:
        error_str = f"执行单个任务 {task_name} 失败: {type(e).__name__} - {e}"
        logger.exception(error_str)
        return {"status": "error", "error": error_str, "task": task_name}


# --- 设置处理 ---
def _load_settings() -> Dict:
    """加载配置文件 (config.json) - 现在从用户配置目录加载"""
    try:
        # 确保在使用前打印或记录最终的 CONFIG_FILE_PATH 以便调试
        logger.info(f"尝试从用户配置路径加载设置: {CONFIG_FILE_PATH}")
        with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
            settings = json.load(f)
            logger.info(f"从 {CONFIG_FILE_PATH} 加载设置成功。")
            return settings
    except FileNotFoundError:
        logger.warning(f"配置文件 {CONFIG_FILE_PATH} 未找到，将使用空设置。")
        return {}
    except json.JSONDecodeError:
        logger.error(f"解析配置文件 {CONFIG_FILE_PATH} 失败。文件可能已损坏。")
        return {}  # 返回空字典而不是抛出异常
    except Exception as e:
        logger.exception(f"加载配置文件时发生未知错误: {CONFIG_FILE_PATH}")
        return {}


def _save_settings(settings: Dict) -> bool:
    """保存设置到配置文件 (config.json) - 现在保存到用户配置目录"""
    try:
        # 确保目录存在 (这是关键步骤)
        logger.info(f"尝试保存设置到用户配置路径: {CONFIG_FILE_PATH}")
        os.makedirs(os.path.dirname(CONFIG_FILE_PATH), exist_ok=True)
        with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=4, ensure_ascii=False)
        logger.info(f"设置已成功保存到 {CONFIG_FILE_PATH}")
        # 保存成功后不再发送 LOG_ENTRY，由 _perform_save_settings 或 _handle_save_settings 发送状态
        return True
    except IOError as e:
        logger.error(f"写入配置文件 {CONFIG_FILE_PATH} 时出错: {e}")
        # 保存失败时也不发送 ERROR，让上层函数处理错误报告
        # response_queue.put(('ERROR', f"保存配置失败: 文件写入错误 ({e})。"))
        return False
    except Exception as e:
        logger.exception(f"保存配置文件时发生未知错误: {CONFIG_FILE_PATH}")
        # response_queue.put(('ERROR', f"保存配置失败: 未知错误 ({e})。"))
        return False


# --- GUI 公共接口 ---


def initialize_controller():
    """启动后台处理线程。"""
    global _backend_thread
    if _backend_thread is None or not _backend_thread.is_alive():
        _backend_thread = threading.Thread(target=_start_async_loop, daemon=True)
        _backend_thread.start()
        logger.info("后台处理线程已启动。")
    else:
        logger.warning("尝试初始化控制器，但后台线程已在运行。")


def request_task_list():
    """向后台请求任务列表。"""
    request_queue.put(("GET_TASKS", None))


def toggle_task_selection(row_index: int):
    """请求切换指定索引任务的选择状态。"""
    request_queue.put(("TOGGLE_SELECT", row_index))


def request_select_specific(task_names: List[str]):
    """请求将指定列表中的任务设置为选中状态。"""
    request_queue.put(("SELECT_SPECIFIC", task_names))


def request_deselect_specific(task_names: List[str]):
    """请求将指定列表中的任务设置为未选中状态。"""
    request_queue.put(("DESELECT_SPECIFIC", task_names))


# --- 新增方法 --- #
def get_cached_task_list() -> List[Dict[str, Any]]:
    """返回当前缓存的任务列表，供 GUI 处理点击事件时查找索引。"""
    # 返回缓存的副本以避免外部修改
    return list(_task_list_cache)


def get_current_settings() -> Dict:
    """从配置文件加载设置或返回默认值。"""
    try:
        settings = _load_settings()
        # 确保返回的字典结构完整，即使文件为空或部分缺失
        # 这样 GUI 端就不需要处理 None 或 KeyError
        default_settings = {"database": {"url": ""}, "api": {"tushare_token": ""}}
        # 合并加载的设置和默认设置，加载的优先
        # 注意：这只是浅层合并
        merged_settings = default_settings.copy()
        if isinstance(settings, dict):
            if "database" in settings and isinstance(settings["database"], dict):
                merged_settings["database"].update(settings["database"])
            if "api" in settings and isinstance(settings["api"], dict):
                merged_settings["api"].update(settings["api"])
        else:
            # 如果 _load_settings 返回的不是字典（例如 None 或异常被捕获返回空），则返回默认
            logger.warning("无法从 config.json 加载有效设置，返回默认空设置。")
            return default_settings

        # 清理 None 值，替换为空字符串，以便 GUI 显示
        if merged_settings["database"].get("url") is None:
            merged_settings["database"]["url"] = ""
        if merged_settings["api"].get("tushare_token") is None:
            merged_settings["api"]["tushare_token"] = ""

        return merged_settings
    except Exception as e:
        logger.error(f"获取当前设置时出错: {e}")
        # 发生任何异常，都返回安全的默认值
        return {"database": {"url": ""}, "api": {"tushare_token": ""}}


def save_settings(settings_from_gui: Dict):
    """请求保存设置（仅Tushare令牌）。"""
    request_queue.put(("SAVE_SETTINGS", settings_from_gui))


def request_task_execution(
    mode: str,
    start_date: Optional[str],
    end_date: Optional[str],
    task_names: List[str],
    smart_increment: bool = False,
):
    """请求后端执行选定的任务。"""
    if not _backend_thread or not _backend_thread.is_alive():
        response_queue.put(("ERROR", "后台服务未运行，无法执行任务"))
        return
    # Checklist Item 2: 在数据负载中包含smart_increment
    request_queue.put(
        (
            "EXECUTE_TASKS",
            {
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                "task_names": task_names,  # 传递选定的任务名称
                "smart_increment": smart_increment,
            },
        )
    )


def request_stop_execution():
    """从GUI线程请求停止当前任务执行。"""
    request_queue.put(("REQUEST_STOP", None))


def request_shutdown():
    """向后台请求关闭服务。"""
    request_queue.put(("SHUTDOWN", None))


def is_backend_running() -> bool:
    """检查后台异步循环是否仍在运行。"""
    return _backend_running


def check_for_updates() -> List:
    """从响应队列中获取所有待处理的更新。"""
    updates = []
    while not response_queue.empty():
        try:
            update = response_queue.get_nowait()
            updates.append(update)
        except queue.Empty:
            break
    return updates


# 重命名内部保存函数
def _perform_save_settings(settings_from_gui: Dict) -> bool:
    """实际执行保存设置到文件系统的操作 (在后台线程调用)。"""
    logger.info(f"Controller (BG): Performing save with data: {settings_from_gui}")
    new_token = settings_from_gui.get("tushare_token")
    new_db_url = settings_from_gui.get("database_url")

    if new_token is None:
        response_queue.put(("ERROR", "保存设置失败：未提供 Tushare Token。"))
        return False

    try:
        full_config = _load_settings()
        if not full_config:
            full_config = {"database": {}, "api": {}, "tasks": {}}
            logger.info("控制器 (BG): 创建新的配置结构。")

        if "api" not in full_config:
            full_config["api"] = {}
        if "database" not in full_config:
            full_config["database"] = {}

        # 直接使用从 GUI 接收的结构来更新
        full_config["api"]["tushare_token"] = new_token
        full_config["database"]["url"] = new_db_url
        logger.info(
            f"控制器 (BG): 已更新配置: api.token={new_token}, db.url={new_db_url}"
        )

        # 调用原始的文件保存函数
        saved_file = _save_settings(
            full_config
        )  # _save_settings 现在只负责写文件和发送LOG
        logger.info(f"控制器 (BG): 文件保存结果: {saved_file}")
        return saved_file
    except Exception as e:
        logger.exception("控制器 (BG): _perform_save_settings 异常")
        response_queue.put(("ERROR", f"保存设置时发生内部错误: {e}"))
        return False


# 添加处理保存和重载的异步 Handler
async def _handle_save_settings(settings_from_gui: Dict):
    """处理 SAVE_SETTINGS 请求：保存配置，如果成功则重载 TaskFactory。"""
    saved_ok = _perform_save_settings(settings_from_gui)

    if saved_ok:
        response_queue.put(("STATUS", "设置已成功保存，正在尝试重新加载配置..."))
        try:
            # !! 重要 !!: TaskFactory 需要有 reload_config 方法
            logger.info("控制器 (BG): 正在调用 TaskFactory.reload_config()...")
            await TaskFactory.reload_config()  # 假设是异步类方法或通过实例调用
            logger.info("控制器 (BG): TaskFactory.reload_config() 已完成。")
            response_queue.put(("LOG_ENTRY", "后台任务配置已根据新设置重新加载。"))
            response_queue.put(("STATUS", "设置已保存并重新加载。"))
        except AttributeError:
            logger.error("TaskFactory 没有 reload_config 方法！无法动态重载配置。")
            response_queue.put(
                (
                    "ERROR",
                    "保存成功，但无法自动重载任务配置 (缺少方法)。请重启应用生效。",
                )
            )
            response_queue.put(("STATUS", "设置已保存，请重启应用生效。"))
        except Exception as e:
            logger.exception("调用 TaskFactory.reload_config() 时出错")
            response_queue.put(("ERROR", f"保存成功，但重载任务配置时出错: {e}"))
            response_queue.put(("STATUS", "设置已保存但重载失败。"))
    # else: # 保存失败，错误消息已由 _perform_save_settings 发送
    #     response_queue.put(('STATUS', '设置保存失败。'))


def _fail_all_running_tasks():
    # 占位实现 - 如果取消逻辑使用它，则需要此功能
    logger.warning("_fail_all_running_tasks 被调用但未完全实现。")
    # 潜在地遍历 _running_task_status 并将 PENDING/RUNNING 更新为 CANCELED
    pass
