import logging
from functools import wraps
from typing import Type, Dict, Optional, Callable, Union

from .base_task import BaseTask
# 导入工厂以直接注册
from .task_factory import UnifiedTaskFactory

# 获取logger
logger = logging.getLogger("unified_task_decorator")

# 任务注册表 (将被废弃)
# _task_registry: Dict[str, Type[BaseTask]] = {}

# _tasks_registered = False (将被废弃)


def task_register(task_name_or_class: Optional[Union[str, Type[BaseTask]]] = None) -> Union[Callable[[Type[BaseTask]], Type[BaseTask]], Type[BaseTask]]:
    """统一任务注册装饰器

    用于自动将任务类注册到UnifiedTaskFactory。
    此装饰器现在直接与UnifiedTaskFactory交互，简化了注册流程。
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
        
        # 直接注册到工厂
        UnifiedTaskFactory.register_task(name_for_registry, cls_to_register)
        logger.debug(f"任务 '{name_for_registry}' (类型: {task_type}) 已直接注册到 UnifiedTaskFactory")
        return cls_to_register

    # 分支2: 作为 @task_register("name") 或 @task_register() 调用 
    else:
        provided_task_name: Optional[str]
        if isinstance(task_name_or_class, str):
            provided_task_name = task_name_or_class
        else:
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
            
            # 直接注册到工厂
            UnifiedTaskFactory.register_task(name_for_registry_inner, cls_param)
            logger.debug(f"任务 '{name_for_registry_inner}' (类型: {task_type}) 已直接注册到 UnifiedTaskFactory")
            return cls_param
        return decorator_factory


# --- 所有旧的、废弃的函数已被移除 ---


# 为了向后兼容，保留原有的别名
task_decorator = task_register  # 别名
register = task_register        # 简化别名 