"""
任务注册与元数据服务

负责处理所有与任务注册、发现、元数据管理相关的逻辑，包括：
- 任务发现和缓存管理
- 任务元数据提取和格式化
- 任务选择状态管理
- 与数据库交互获取任务的最新更新时间
- 为GUI提供统一的任务信息接口
"""
import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
import inspect

from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory, get_tasks_by_type
from ...gui.utils.common import format_datetime_for_display

logger = get_logger(__name__)

# --- 缓存和回调 ---
_collection_task_cache: List[Dict[str, Any]] = []
_processing_task_cache: List[Dict[str, Any]] = []
_send_response_callback: Optional[Callable] = None


def initialize_task_registry(response_callback: Callable):
    """初始化任务注册服务，设置回调函数。"""
    global _send_response_callback
    _send_response_callback = response_callback
    logger.info("任务注册服务已初始化。")


def get_cached_collection_tasks() -> List[Dict[str, Any]]:
    """获取缓存的数据采集任务列表。"""
    return _collection_task_cache


def get_cached_processing_tasks() -> List[Dict[str, Any]]:
    """获取缓存的数据处理任务列表。"""
    return _processing_task_cache


def toggle_collection_select(row_index: int):
    """切换指定行数据采集任务的选中状态。"""
    if 0 <= row_index < len(_collection_task_cache):
        task = _collection_task_cache[row_index]
        task["selected"] = not task.get("selected", False)
        logger.debug(f"切换数据采集任务 '{task['name']}' 的选择状态为: {task['selected']}")
        if _send_response_callback:
            _send_response_callback("COLLECTION_TASK_LIST_UPDATE", _collection_task_cache)


def toggle_processing_select(row_index: int):
    """切换指定行数据处理任务的选中状态。"""
    if 0 <= row_index < len(_processing_task_cache):
        task = _processing_task_cache[row_index]
        task["selected"] = not task.get("selected", False)
        logger.debug(f"切换数据处理任务 '{task['name']}' 的选择状态为: {task['selected']}")
        if _send_response_callback:
            _send_response_callback("PROCESSING_TASK_LIST_UPDATE", _processing_task_cache)


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

        await _update_tasks_with_latest_timestamp(_collection_task_cache)

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


async def handle_get_processing_tasks():
    """处理获取'processor'类型任务列表的请求。"""
    global _processing_task_cache
    success = False
    try:
        # 使用统一的任务详情获取方法
        task_details = await get_task_details_by_type("processor")
        
        # 保持现有的选择状态
        existing_selection = {item["name"]: item["selected"] for item in _processing_task_cache}
        
        for task in task_details:
            task["selected"] = existing_selection.get(task["name"], False)
        
        _processing_task_cache = sorted(task_details, key=lambda x: x["name"])
        
        logger.info(f"发现 {len(_processing_task_cache)} 个数据处理任务。")
        
        if _send_response_callback:
            _send_response_callback("PROCESSING_TASK_LIST_UPDATE", _processing_task_cache)
            _send_response_callback("STATUS", f"数据处理任务列表已刷新 (共 {len(_processing_task_cache)} 个任务)")
        success = True
        
    except Exception as e:
        logger.exception("获取数据处理任务列表时发生严重错误。")
        if _send_response_callback:
            _send_response_callback("ERROR", f"获取数据处理任务列表失败: {e}")
    finally:
        if _send_response_callback:
            _send_response_callback("PROCESSING_REFRESH_COMPLETE", {"success": success})


async def get_task_details_by_type(task_type: str) -> List[Dict[str, Any]]:
    """
    按类型获取任务的详细信息列表，用于GUI显示。

    此方法是获取任务信息供GUI使用的唯一、标准化的方式。
    它封装了获取任务类、创建实例和提取所需属性的逻辑。

    Args:
        task_type (str): 要获取的任务类型 ('fetch', 'processor', etc.)

    Returns:
        List[Dict[str, Any]]: 一个任务详情字典的列表。
    """
    details_list = []
    try:
        # 1. 从工厂获取指定类型的所有任务类
        task_classes = UnifiedTaskFactory.get_tasks_by_type(task_type)
        if not task_classes:
            logger.warning(f"未找到类型为 '{task_type}' 的任务。")
            return []
        
        # 2. 异步地获取所有任务实例的详情
        tasks_to_gather = [
            _get_single_task_details(name)
            for name in task_classes.keys()
        ]
        results = await asyncio.gather(*tasks_to_gather, return_exceptions=True)

        # 3. 处理结果
        for result in results:
            if isinstance(result, dict):
                details_list.append(result)
            elif isinstance(result, Exception):
                # 错误已在 _get_single_task_details 中记录，这里可以选择性地再次记录
                logger.debug(f"一个任务的详情获取失败，已被跳过: {result}")

    except Exception as e:
        logger.exception(f"获取类型为 '{task_type}' 的任务详情时发生严重错误。")

    return details_list


async def _get_single_task_details(task_name: str) -> Optional[Dict[str, Any]]:
    """
    获取单个任务的详细信息，能安全地处理抽象类。
    """
    try:
        # 正确地从工厂的注册表中获取任务类
        task_class = UnifiedTaskFactory._task_registry.get(task_name)
        if not task_class:
            logger.warning(f"无法在工厂注册表中找到名为 '{task_name}' 的任务类。")
            return None

        # --- 默认从类属性中获取基础信息 ---
        details = {
            "name": task_name,
            "description": getattr(task_class, "description", ""),
            "task_type": getattr(task_class, "task_type", "base"),
            "data_source": getattr(task_class, "data_source", "未知"),
            "dependencies": ", ".join(getattr(task_class, "dependencies", []) or []),
            "table_name": getattr(task_class, "table_name", "未知"),
            "selected": False,
            "error": None
        }

        # --- 检查是否为抽象类 ---
        if inspect.isabstract(task_class):
            details["error"] = "抽象任务无法直接执行"
            logger.info(f"任务 '{task_name}' 是抽象类，仅加载基础信息。")
            return details

        # --- 如果是具体类，则实例化以获取更多信息 ---
        task_instance = await UnifiedTaskFactory.get_task(task_name)
        if not task_instance:
            details["error"] = "任务实例创建失败"
            return details
            
        # 使用实例更新/覆盖信息
        details["primary_keys"] = getattr(task_instance, "primary_keys", [])
        details["date_column"] = getattr(task_instance, "date_column", None)
        
        # 为 'fetch' 任务推断更具体的子类型
        if details["task_type"] == 'fetch':
            parts = task_name.split('_')
            if parts[0] == "tushare" and len(parts) > 1:
                details["task_type"] = parts[1]
            elif parts[0] != "tushare":
                details["task_type"] = parts[0]

        return details

    except Exception as e:
        logger.error(f"获取任务 '{task_name}' 详情时发生严重错误: {e}", exc_info=True)
        return {
            "name": task_name, "description": "获取详情时出错", "error": str(e),
            "task_type": "error", "data_source": "error", "dependencies": "error",
            "table_name": "error", "selected": False
        }


async def _update_tasks_with_latest_timestamp(task_cache: List[Dict[str, Any]]):
    """使用每个任务的最新数据时间戳更新任务缓存列表。"""
    db_manager = UnifiedTaskFactory.get_db_manager()
    if not db_manager:
        for task_detail in task_cache:
            task_detail["latest_update_time"] = "N/A (DB Error)"
        return

    async def update_single_task(task_detail: Dict[str, Any]):
        table_name = task_detail.get("table_name")
        if table_name:
            try:
                # 修复: 传递整个task_detail字典，而不是只有name
                # TableNameResolver可以从字典中正确解析出data_source和table_name
                latest_time = await db_manager.get_latest_update_time(
                    task_detail
                )
                if latest_time:
                    task_detail["latest_update_time"] = format_datetime_for_display(latest_time)
                else:
                    task_detail["latest_update_time"] = "无数据"
            except Exception as e:
                logger.warning(
                    f"查询 {table_name} 最新时间失败: {e}"
                )
                task_detail["latest_update_time"] = "查询失败"
        else:
            task_detail["latest_update_time"] = "无对应表"

    # 使用 asyncio.gather 并发更新所有任务
    tasks = [update_single_task(task) for task in task_cache]
    await asyncio.gather(*tasks)


# --- 用于向后兼容的函数别名 ---
# 这些别名保持与原collection_service的接口兼容

def set_update_callback(callback):
    """设置用于更新UI日志消息的回调函数。(向后兼容)"""
    # 在新架构中，这个功能由task_execution_service处理
    logger.warning("set_update_callback is deprecated. Use task_execution_service instead.")


async def run_tasks_logic(tasks_to_run: List[str], exec_mode: str, max_workers: int):
    """
    向后兼容的任务执行函数。
    实际执行逻辑应该使用task_execution_service。
    """
    logger.warning("run_tasks_logic is deprecated. Use task_execution_service.run_tasks instead.")
    if _send_response_callback:
        _send_response_callback("ERROR", "任务执行功能已迁移到task_execution_service。") 