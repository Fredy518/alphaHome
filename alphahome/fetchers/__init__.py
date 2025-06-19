# -*- coding: utf-8 -*-
"""
alphahome.fetchers - 数据采集模块

提供统一的数据采集任务系统，基于新的统一任务架构。
"""

# 导入工具模块
from . import tools

# 导入统一任务系统的核心组件
from ..common.task_system.base_task import BaseTask as Task
from ..common.task_system import (
    UnifiedTaskFactory as TaskFactory,
    task_register,
    get_task,
    get_tasks_by_type,
    get_task_types,
)

# 导入所有具体的数据采集任务（这会触发任务注册）
from . import tasks

# 注册所有任务到统一工厂 (此步骤已不再需要，因为注册是即时的)
# register_tasks_to_factory()

# 主要导出
__all__ = [
    "Task",           # BaseTask 的别名
    "TaskFactory",    # UnifiedTaskFactory 的别名  
    "task_register",  # 统一的任务注册装饰器
    "get_task",       # 获取任务的函数
    "get_tasks_by_type",
    "get_task_types",
    "tools"           # 工具模块
]
