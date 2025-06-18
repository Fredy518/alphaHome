import logging
from functools import wraps
from typing import Type, Dict, Optional, Callable, Union

from .base_task import BaseTask

# 获取logger
logger = logging.getLogger("unified_task_decorator")

# 任务注册表
_task_registry: Dict[str, Type[BaseTask]] = {}

_tasks_registered = False


def task_register(task_name_or_class: Optional[Union[str, Type[BaseTask]]] = None) -> Union[Callable[[Type[BaseTask]], Type[BaseTask]], Type[BaseTask]]:
    """统一任务注册装饰器

    用于自动注册任务类到UnifiedTaskFactory。
    支持fetch、processor等所有类型的任务注册。
    如果未提供任务名称，将使用任务类的name属性作为任务名称。

    Args:
        task_name (str, optional): 任务名称，默认为None表示使用类的name属性

    Returns:
        装饰器函数

    Examples:
        @task_register()
        class MyFetchTask(BaseTask):  # 直接继承BaseTask
            name = "tushare_stock_daily"
            task_type = "fetch"
            # ...

        @task_register()
        class MyProcessorTask(BaseTask):  # 直接继承BaseTask
            name = "stock_adjusted_price"
            task_type = "processor"
            # ...

        @task_register("custom_name")
        class AnotherTask(BaseTask):
            task_type = "derivative"
            # ...
    """

    # 分支1: 作为 @task_register 调用 (task_name_or_class 是类本身)
    if callable(task_name_or_class) and not isinstance(task_name_or_class, str):
        cls_to_register: Type[BaseTask] = task_name_or_class
        
        name_for_registry: str
        if hasattr(cls_to_register, "name") and cls_to_register.name:
            name_for_registry = cls_to_register.name
        else:
            name_for_registry = cls_to_register.__name__
        
        task_type = getattr(cls_to_register, 'task_type', 'unknown')
        if task_type == 'unknown':
            logger.warning(f"任务 '{name_for_registry}' 未设置task_type，将使用'unknown'")
        
        _task_registry[name_for_registry] = cls_to_register
        logger.debug(f"任务 '{name_for_registry}' (类型: {task_type}) 已通过装饰器注册 (@task_register)")
        return cls_to_register

    # 分支2: 作为 @task_register("name") 或 @task_register() 调用 
    # (task_name_or_class 是字符串或 None)
    else:
        provided_task_name: Optional[str]
        if isinstance(task_name_or_class, str):
            provided_task_name = task_name_or_class
        else: # 如果执行到此处，则必须为 None
            provided_task_name = None

        def decorator_factory(cls_param: Type[BaseTask]) -> Type[BaseTask]:
            name_for_registry_inner: str
            if provided_task_name:
                name_for_registry_inner = provided_task_name
            elif hasattr(cls_param, "name") and cls_param.name:
                name_for_registry_inner = cls_param.name
            else:
                name_for_registry_inner = cls_param.__name__

            task_type = getattr(cls_param, 'task_type', 'unknown')
            if task_type == 'unknown':
                logger.warning(f"任务 '{name_for_registry_inner}' 未设置task_type，将使用'unknown'")
            
            _task_registry[name_for_registry_inner] = cls_param
            logger.debug(f"任务 '{name_for_registry_inner}' (类型: {task_type}) 已通过装饰器注册 (@task_register(...))")
            return cls_param
        return decorator_factory


def get_registered_tasks() -> Dict[str, Type[BaseTask]]:
    """获取所有已注册的任务"""
    return _task_registry.copy()


def get_registered_tasks_by_type(task_type: Optional[str] = None) -> Dict[str, Type[BaseTask]]:
    """按类型获取已注册的任务"""
    if task_type is None:
        return _task_registry.copy()
    
    filtered_tasks = {}
    # 将 task_class 重命名为 task_class_item 以避免名称冲突
    for name, task_class_item in _task_registry.items(): 
        if hasattr(task_class_item, 'task_type') and task_class_item.task_type == task_type:
            filtered_tasks[name] = task_class_item
    return filtered_tasks


def register_tasks_to_factory(force=False):
    """将所有已注册的任务注册到UnifiedTaskFactory

    注意：该函数需要在导入所有任务类之后调用
    
    Args:
        force (bool): 强制重新注册，忽略_tasks_registered标志
    """
    global _tasks_registered
    if _tasks_registered and not force:
        logger.debug("任务注册已完成，跳过。")
        return
        
    try:
        from .task_factory import UnifiedTaskFactory
        # 重命名变量以避免名称冲突
        for task_name_key, task_class_val in _task_registry.items(): 
            UnifiedTaskFactory.register_task(task_name_key, task_class_val)
            task_type = getattr(task_class_val, 'task_type', 'unknown')
            logger.debug(f"任务 '{task_name_key}' (类型: {task_type}) 已从装饰器注册表同步到 UnifiedTaskFactory")
        _tasks_registered = True
        logger.info(f"成功注册 {len(_task_registry)} 个任务到 UnifiedTaskFactory")
    except ImportError as e:
        logger.error(f"无法导入 UnifiedTaskFactory: {e}")
        raise


# def register_tasks_to_legacy_factory():
#     """为向后兼容，支持注册到原有的TaskFactory"""
#     try:
#         # 尝试导入并注册到原有的fetchers.TaskFactory
#         from ...fetchers.task_factory import TaskFactory as LegacyTaskFactory
#         
#         # 只注册fetch类型的任务到原有工厂
#         fetch_tasks = get_registered_tasks_by_type('fetch')
#         for task_name, task_class in fetch_tasks.items():
#             LegacyTaskFactory.register_task(task_name, task_class)
#             logger.debug(f"Fetch任务 '{task_name}' 已注册到 LegacyTaskFactory")
#         
#         if fetch_tasks:
#             logger.info(f"成功注册 {len(fetch_tasks)} 个fetch任务到 LegacyTaskFactory")
#             
#     except ImportError:
#         logger.debug("未找到 LegacyTaskFactory，跳过向后兼容注册")


def get_registration_stats():
    """获取注册统计信息"""
    stats = {
        "total_tasks": len(_task_registry),
        "tasks_by_type": {},
        "task_list": list(_task_registry.keys())
    }
    
    # 按类型统计
    # 将 task_class 重命名为 task_class_instance 以避免名称冲突
    for task_class_instance in _task_registry.values(): 
        task_type = getattr(task_class_instance, 'task_type', 'unknown')
        if task_type not in stats["tasks_by_type"]:
            stats["tasks_by_type"][task_type] = 0
        stats["tasks_by_type"][task_type] += 1
    
    return stats


# 为了向后兼容，保留原有的别名
task_decorator = task_register  # 别名
register = task_register        # 简化别名 