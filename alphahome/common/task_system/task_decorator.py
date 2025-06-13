import logging
from functools import wraps
from typing import Type

# 获取logger
logger = logging.getLogger("unified_task_decorator")

# 任务注册表
_task_registry = {}

_tasks_registered = False


def task_register(task_name=None):
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

    def decorator(cls):
        # 确定任务名称
        actual_task_name = task_name

        # 如果未提供任务名称，使用类的name属性
        if actual_task_name is None:
            if hasattr(cls, "name") and cls.name:
                actual_task_name = cls.name
            else:
                actual_task_name = cls.__name__

        # 验证任务类型
        task_type = getattr(cls, 'task_type', 'unknown')
        if task_type == 'unknown':
            logger.warning(f"任务 '{actual_task_name}' 未设置task_type，将使用'unknown'")

        # 存储在本地注册表中
        _task_registry[actual_task_name] = cls

        # 记录日志
        logger.debug(f"任务 '{actual_task_name}' (类型: {task_type}) 已通过装饰器注册")

        # 返回原始类，不做修改
        return cls

    # 处理无参数调用的情况
    if callable(task_name):
        cls = task_name
        task_name = None
        return decorator(cls)

    return decorator


def get_registered_tasks():
    """获取所有已注册的任务"""
    return _task_registry.copy()


def get_registered_tasks_by_type(task_type: str):
    """按类型获取已注册的任务"""
    filtered_tasks = {}
    for name, task_class in _task_registry.items():
        if hasattr(task_class, 'task_type') and task_class.task_type == task_type:
            filtered_tasks[name] = task_class
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

        for task_name, task_class in _task_registry.items():
            UnifiedTaskFactory.register_task(task_name, task_class)
            task_type = getattr(task_class, 'task_type', 'unknown')
            logger.debug(f"任务 '{task_name}' (类型: {task_type}) 已从装饰器注册表同步到 UnifiedTaskFactory")
        _tasks_registered = True
        logger.info(f"成功注册 {len(_task_registry)} 个任务到 UnifiedTaskFactory")
    except ImportError as e:
        logger.error(f"无法导入 UnifiedTaskFactory: {e}")
        raise


def register_tasks_to_legacy_factory():
    """为向后兼容，支持注册到原有的TaskFactory"""
    try:
        # 尝试导入并注册到原有的fetchers.TaskFactory
        from ...fetchers.task_factory import TaskFactory as LegacyTaskFactory
        
        # 只注册fetch类型的任务到原有工厂
        fetch_tasks = get_registered_tasks_by_type('fetch')
        for task_name, task_class in fetch_tasks.items():
            LegacyTaskFactory.register_task(task_name, task_class)
            logger.debug(f"Fetch任务 '{task_name}' 已注册到 LegacyTaskFactory")
        
        if fetch_tasks:
            logger.info(f"成功注册 {len(fetch_tasks)} 个fetch任务到 LegacyTaskFactory")
            
    except ImportError:
        logger.debug("未找到 LegacyTaskFactory，跳过向后兼容注册")


def get_registration_stats():
    """获取注册统计信息"""
    stats = {
        "total_tasks": len(_task_registry),
        "tasks_by_type": {},
        "task_list": list(_task_registry.keys())
    }
    
    # 按类型统计
    for task_class in _task_registry.values():
        task_type = getattr(task_class, 'task_type', 'unknown')
        if task_type not in stats["tasks_by_type"]:
            stats["tasks_by_type"][task_type] = 0
        stats["tasks_by_type"][task_type] += 1
    
    return stats


# 为了向后兼容，保留原有的别名
task_decorator = task_register  # 别名
register = task_register        # 简化别名 