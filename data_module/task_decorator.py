import logging
from typing import Type
from functools import wraps

# 获取logger
logger = logging.getLogger('task_decorator')

# 任务注册表
_task_registry = {}

def task_register(task_name=None):
    """任务注册装饰器
    
    用于自动注册任务类到TaskFactory。
    如果未提供任务名称，将使用任务类的name属性作为任务名称。
    
    Args:
        task_name (str, optional): 任务名称，默认为None表示使用类的name属性
        
    Returns:
        装饰器函数
    
    Examples:
        @task_register()
        class MyTask(Task):
            name = "my_task"
            # ...
            
        @task_register("custom_name")
        class AnotherTask(Task):
            # ...
    """
    def decorator(cls):
        # 确定任务名称
        actual_task_name = task_name
        
        # 如果未提供任务名称，使用类的name属性
        if actual_task_name is None:
            if hasattr(cls, 'name') and cls.name:
                actual_task_name = cls.name
            else:
                actual_task_name = cls.__name__
        
        # 存储在本地注册表中
        _task_registry[actual_task_name] = cls
        
        # 记录日志
        logger.debug(f"任务 '{actual_task_name}' 已通过装饰器注册")
        
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

def register_tasks_to_factory():
    """将所有已注册的任务注册到TaskFactory
    
    注意：该函数需要在导入所有任务类之后调用
    """
    # 延迟导入以避免循环依赖
    from .task_factory import TaskFactory
    
    # 注册所有任务
    for task_name, task_class in _task_registry.items():
        TaskFactory.register_task(task_name, task_class)
        logger.info(f"任务 '{task_name}' 已从装饰器注册表同步到 TaskFactory") 