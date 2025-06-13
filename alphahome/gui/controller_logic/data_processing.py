"""
数据处理任务处理器

负责处理所有与数据处理任务（'processor'类型）相关的逻辑，包括：
- 获取和缓存'processor'类型的任务列表
- 管理任务的选择状态
- 获取任务依赖和最新的更新时间
- 与控制器协调，发送更新后的任务列表给GUI
"""
import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory, get_tasks_by_type
from ...common.task_system.task_decorator import get_registered_tasks_by_type

logger = get_logger(__name__)

# --- 缓存和回调 ---
_processing_task_cache: List[Dict[str, Any]] = []
_send_response_callback: Optional[Callable] = None


def initialize_data_processing(response_callback: Callable):
    """初始化数据处理处理器，设置回调函数。"""
    global _send_response_callback
    _send_response_callback = response_callback
    logger.info("数据处理处理器已初始化。")


def get_cached_processing_tasks() -> List[Dict[str, Any]]:
    """获取缓存的数据处理任务列表。"""
    return _processing_task_cache


def toggle_processing_select(row_index: int):
    """切换指定行任务的选中状态。"""
    if 0 <= row_index < len(_processing_task_cache):
        task = _processing_task_cache[row_index]
        task["selected"] = not task.get("selected", False)
        logger.debug(f"切换数据处理任务 '{task['name']}' 的选择状态为: {task['selected']}")
        if _send_response_callback:
            _send_response_callback("PROCESSING_TASK_LIST_UPDATE", _processing_task_cache)


def toggle_processing_task_selection(task_name: str):
    """根据任务名称切换任务的选中状态。"""
    for task in _processing_task_cache:
        if task["name"] == task_name:
            task["selected"] = not task.get("selected", False)
            logger.debug(f"切换数据处理任务 '{task_name}' 的选择状态为: {task['selected']}")
            if _send_response_callback:
                _send_response_callback("PROCESSING_TASK_LIST_UPDATE", _processing_task_cache)
            return
    logger.warning(f"未找到名为 '{task_name}' 的数据处理任务")


async def handle_get_processing_tasks():
    """处理获取'processor'类型任务列表的请求。"""
    global _processing_task_cache
    success = False
    try:
        # 首先尝试从装饰器注册表获取processor任务
        processor_tasks_dict = get_registered_tasks_by_type("processor")
        if not processor_tasks_dict:
            # 如果装饰器注册表为空，尝试从UnifiedTaskFactory获取
            try:
                processor_tasks_dict = get_tasks_by_type("processor")
            except RuntimeError as e:
                logger.warning(f"UnifiedTaskFactory 未初始化: {e}")
                processor_tasks_dict = {}
        
        logger.info(f"发现 {len(processor_tasks_dict)} 个数据处理任务。")

        new_cache = []
        existing_selection = {item["name"]: item["selected"] for item in _processing_task_cache}
        
        task_names = sorted(processor_tasks_dict.keys()) if isinstance(processor_tasks_dict, dict) else sorted(processor_tasks_dict)

        for name in task_names:
            try:
                # 尝试从UnifiedTaskFactory获取任务实例
                try:
                    task_instance = await UnifiedTaskFactory.get_task(name)
                except Exception:
                    # 如果UnifiedTaskFactory失败，直接从类创建实例
                    task_class = processor_tasks_dict[name]
                    # 创建一个临时实例来获取属性（不需要数据库连接）
                    task_instance = task_class.__new__(task_class)
                    for attr in ['description', 'dependencies', 'table_name']:
                        if not hasattr(task_instance, attr):
                            setattr(task_instance, attr, getattr(task_class, attr, None))
                
                dependencies = getattr(task_instance, 'dependencies', [])
                dependencies_str = ", ".join(dependencies) if dependencies else "无"

                new_cache.append({
                    "name": name,
                    "type": "processor",
                    "description": getattr(task_instance, "description", ""),
                    "dependencies": dependencies_str,
                    "selected": existing_selection.get(name, False),
                    "table_name": getattr(task_instance, "table_name", None),
                })
            except Exception as e:
                logger.error(f"获取处理任务 '{name}' 详情失败: {e}")

        _processing_task_cache = sorted(new_cache, key=lambda x: x["name"])

        await _update_tasks_with_latest_timestamp()

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


async def _update_tasks_with_latest_timestamp():
    """使用最新的数据库更新时间来更新任务缓存。"""
    try:
        db_manager = UnifiedTaskFactory.get_db_manager()
        if not db_manager:
            for task_detail in _processing_task_cache:
                task_detail["latest_update_time"] = "N/A (DB Error)"
            return

        tasks_to_query = [(i, t['table_name']) for i, t in enumerate(_processing_task_cache) if t.get('table_name')]
        
        if not tasks_to_query:
            return

        query_coroutines = [db_manager.get_latest_date(tn, "update_time", return_raw_object=True) for _, tn in tasks_to_query]
        
        results = await asyncio.gather(*query_coroutines, return_exceptions=True)

        for i, result in enumerate(results):
            task_index, _ = tasks_to_query[i]
            
            if isinstance(result, Exception):
                timestamp_str = "N/A (Query Error)"
            elif isinstance(result, datetime):
                timestamp_str = result.strftime("%Y-%m-%d %H:%M:%S")
            else:
                timestamp_str = "无数据" if result is None else str(result)
            
            _processing_task_cache[task_index]["latest_update_time"] = timestamp_str
    except Exception as e:
        logger.warning(f"更新任务时间戳失败: {e}")
        for task_detail in _processing_task_cache:
            task_detail["latest_update_time"] = "N/A (Error)" 