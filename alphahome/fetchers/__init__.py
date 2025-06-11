# 数据模块包
# 包含数据任务系统的核心组件

from . import tools
from .base_task import Task
from .task_factory import TaskFactory, get_task
from .fetch_task import FetchTask, Task as BaseTask

# 导入所有具体的fetch任务以确保它们被注册
from . import tasks

# 重新导出统一任务系统的组件（为fetchers模块提供便捷访问）
from ..common.task_system import (
    UnifiedTaskFactory,
    TaskFactory as UnifiedTaskFactoryAlias,  # 避免名称冲突
    task_register,
    get_task as unified_get_task,
    get_tasks_by_type,
    get_task_types,
    register_tasks_to_factory
)

# 注册所有任务到统一工厂
register_tasks_to_factory()

# 同时将旧装饰器注册的任务也注册到统一系统
def register_legacy_tasks():
    """将旧的task_decorator注册的任务也注册到统一系统中"""
    from .task_decorator import get_registered_tasks
    from ..common.task_system.task_decorator import _task_registry as unified_registry
    from ..common.task_system import UnifiedTaskFactory
    import logging
    
    legacy_tasks = get_registered_tasks()
    logger = logging.getLogger("fetchers.legacy_registration")
    
    registered_count = 0
    for task_name, task_class in legacy_tasks.items():
        # 确保任务类有task_type属性，如果没有则设为fetch
        if not hasattr(task_class, 'task_type'):
            task_class.task_type = 'fetch'
        
        # 添加到统一装饰器注册表
        unified_registry[task_name] = task_class
        
        # 直接注册到UnifiedTaskFactory（不需要初始化）
        UnifiedTaskFactory.register_task(task_name, task_class)
        
        registered_count += 1
        logger.debug(f"旧任务 '{task_name}' (类型: {task_class.task_type}) 已注册到统一系统")
    
    if registered_count > 0:
        logger.info(f"成功将 {registered_count} 个旧任务注册到统一系统和UnifiedTaskFactory")
    else:
        logger.warning("未发现任何旧任务需要注册")

# 调用注册函数
register_legacy_tasks()

# 确保任务也注册到UnifiedTaskFactory
from ..common.task_system.task_decorator import register_tasks_to_factory
register_tasks_to_factory()

__all__ = ["TaskFactory", "Task", "get_task", "FetchTask"]
