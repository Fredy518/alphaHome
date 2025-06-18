"""
数据采集任务处理器

负责处理所有与数据采集任务相关的逻辑，包括：
- 获取和缓存'fetch'类型的任务列表
- 管理任务的选择状态
- 与数据库交互，获取任务的最新更新时间
- 与控制器协调，发送更新后的任务列表给GUI
"""
import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory, get_tasks_by_type

logger = get_logger(__name__)

# --- 缓存和回调 ---
_collection_task_cache: List[Dict[str, Any]] = []
_send_response_callback: Optional[Callable] = None
_update_callback = None


def initialize_data_collection(response_callback: Callable):
    """初始化数据采集处理器，设置回调函数。"""
    global _send_response_callback
    # 初始化数据采集任务缓存
    _send_response_callback = response_callback
    logger.info("数据采集处理器已初始化。")


def get_cached_collection_tasks() -> List[Dict[str, Any]]:
    """获取缓存的数据采集任务列表。"""
    return _collection_task_cache


def toggle_collection_select(row_index: int):
    """切换指定行任务的选中状态。"""
    if 0 <= row_index < len(_collection_task_cache):
        task = _collection_task_cache[row_index]
        task["selected"] = not task.get("selected", False)
        logger.debug(f"切换数据采集任务 '{task['name']}' 的选择状态为: {task['selected']}")
        if _send_response_callback:
            # 发送完整的列表以触发UI更新
            _send_response_callback("COLLECTION_TASK_LIST_UPDATE", _collection_task_cache)


async def handle_get_collection_tasks():
    """处理获取'fetch'类型任务列表的请求。"""
    global _collection_task_cache
    success = False
    try:
        # 获取'fetch'类型的任务
        fetch_tasks = get_tasks_by_type("fetch")
        logger.info(f"发现 {len(fetch_tasks)} 个数据采集任务。")

        new_cache = []
        existing_selection = {item["name"]: item["selected"] for item in _collection_task_cache}

        task_names = sorted(fetch_tasks.keys()) if isinstance(fetch_tasks, dict) else sorted(fetch_tasks)

        for name in task_names:
            try:
                task_instance = await UnifiedTaskFactory.get_task(name)
                # 推断任务子类型
                task_type = getattr(task_instance, 'task_type', 'fetch')
                if task_type == 'fetch':
                    parts = name.split('_')
                    if parts[0] == "tushare" and len(parts) > 1:
                        task_type = parts[1]
                    elif parts[0] != "tushare":
                        task_type = parts[0]

                # 推断数据源
                data_source = getattr(task_instance, 'data_source', None)
                if data_source is None:
                    # 如果任务类未定义data_source，则从名称推断
                    parts = name.split('_')
                    if parts[0] == "tushare":
                        data_source = "tushare"
                    elif parts[0] in ["wind", "jqdata", "baostock", "sina", "yahoo", "ifind"]:
                        data_source = parts[0]
                    else:
                        data_source = "unknown"

                new_cache.append({
                    "name": name,
                    "type": task_type,
                    "data_source": data_source,
                    "description": getattr(task_instance, "description", ""),
                    "selected": existing_selection.get(name, False),
                    "table_name": getattr(task_instance, "table_name", None),
                })
            except Exception as e:
                logger.error(f"获取采集任务 '{name}' 详情失败: {e}")

        _collection_task_cache = sorted(new_cache, key=lambda x: (x["type"], x["name"]))

        await _update_tasks_with_latest_timestamp()

        if _send_response_callback:
            _send_response_callback("COLLECTION_TASK_LIST_UPDATE", _collection_task_cache)
            _send_response_callback("STATUS", f"数据采集任务列表已刷新 (共 {len(_collection_task_cache)} 个任务)")
        success = True

    except Exception as e:
        logger.exception("获取数据采集任务列表时发生严重错误。")
        if _send_response_callback:
            _send_response_callback("ERROR", f"获取数据采集任务列表失败: {e}")
    finally:
        if _send_response_callback:
            _send_response_callback("COLLECTION_REFRESH_COMPLETE", {"success": success})


async def _update_tasks_with_latest_timestamp():
    """使用最新的数据库更新时间来更新任务缓存。"""
    db_manager = UnifiedTaskFactory.get_db_manager()
    if not db_manager:
        for task_detail in _collection_task_cache:
            task_detail["latest_update_time"] = "N/A (DB Error)"
        return

    tasks_to_query = [(i, t['name']) for i, t in enumerate(_collection_task_cache) if t.get('table_name')]
    
    if not tasks_to_query:
        return

    # 获取任务对象
    task_objects = []
    for i, task_name in tasks_to_query:
        try:
            task_obj = await UnifiedTaskFactory.get_task(task_name)
            task_objects.append((i, task_obj))
        except Exception as e:
            logger.warning(f"获取任务对象 {task_name} 失败: {e}")
            # 如果获取任务对象失败，设置错误状态
            _collection_task_cache[i]["latest_update_time"] = "N/A (Task Error)"

    if not task_objects:
        return

    query_coroutines = [db_manager.get_latest_date(task_obj, "update_time") for _, task_obj in task_objects]
    
    results = await asyncio.gather(*query_coroutines, return_exceptions=True)

    for i, result in enumerate(results):
        task_index, _ = task_objects[i]
        
        if isinstance(result, Exception):
            timestamp_str = "N/A (Query Error)"
            logger.warning(f"查询 {_collection_task_cache[task_index]['table_name']} 最新时间失败: {result}")
        elif isinstance(result, datetime):
            # 如果有时区信息，转换为本地时区
            if hasattr(result, 'tzinfo') and result.tzinfo is not None:
                result = result.astimezone()  # 转换为本地时区
            timestamp_str = result.strftime("%Y-%m-%d %H:%M:%S")
        else:
            timestamp_str = "无数据" if result is None else str(result)
        
        _collection_task_cache[task_index]["latest_update_time"] = timestamp_str


def set_update_callback(callback):
    """设置用于更新UI日志消息的回调函数。"""
    global _update_callback
    _update_callback = callback


async def run_tasks_logic(tasks_to_run: List[str], exec_mode: str, max_workers: int):
    """
    运行选定的数据采集任务的核心逻辑。
    
    Args:
        tasks_to_run: 要运行的任务名称列表。
        exec_mode: 执行模式 ('sequential', 'parallel')。
        max_workers: 并行执行时的最大工作线程数。
    """
    def log_message(msg: str):
        """使用UI回调记录消息，或回退到打印。"""
        if _update_callback:
            _update_callback(msg)
        else:
            print(f"Log (no callback): {msg}")

    if not tasks_to_run:
        log_message("没有选择任何要执行的任务。")
        if _send_response_callback:
            _send_response_callback("STATUS", "没有选择任何要执行的任务。")
        return

    if _send_response_callback:
        _send_response_callback("TASK_RUN_START", None)
    
    log_message(f"开始执行 {len(tasks_to_run)} 个任务 (模式: {exec_mode}, 最大并发: {max_workers})...")

    try:
        if exec_mode == 'sequential':
            for task_name in tasks_to_run:
                try:
                    log_message(f"--- 开始顺序执行任务: {task_name} ---")
                    task = await UnifiedTaskFactory.get_task(task_name)
                    result = await task.execute()
                    log_message(f"任务 {task_name} 完成. 结果: {result.get('status', 'unknown')}, "
                                f"影响行数: {result.get('rows', 'N/A')}")
                except Exception as e:
                    error_msg = f"任务 {task_name} 执行失败: {e}"
                    logger.exception(error_msg)
                    log_message(error_msg)
        
        elif exec_mode == 'parallel':
            semaphore = asyncio.Semaphore(max_workers)

            async def run_with_semaphore(task_name):
                async with semaphore:
                    try:
                        log_message(f"--- 开始并行执行任务: {task_name} ---")
                        task = await UnifiedTaskFactory.get_task(task_name)
                        result = await task.execute()
                        log_message(f"任务 {task_name} 完成. 结果: {result.get('status', 'unknown')}, "
                                    f"影响行数: {result.get('rows', 'N/A')}")
                        return result
                    except Exception as e:
                        error_msg = f"任务 {task_name} 执行失败: {e}"
                        logger.exception(error_msg)
                        log_message(error_msg)
                        return {"status": "error", "error": str(e), "task": task_name}

            tasks = [run_with_semaphore(name) for name in tasks_to_run]
            await asyncio.gather(*tasks)

        log_message("所有任务执行完毕。")
        
        # 刷新时间戳
        log_message("正在刷新任务列表的最新更新时间...")
        await handle_get_collection_tasks()
        log_message("任务列表已刷新。")

    except Exception as e:
        error_message = f"任务执行流程中发生严重错误: {e}"
        logger.exception(error_message)
        log_message(error_message)
    finally:
        if _send_response_callback:
            _send_response_callback("TASK_RUN_COMPLETE", None)
            _send_response_callback("STATUS", "任务执行流程结束。") 