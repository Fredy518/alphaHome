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

# 具体任务注册由调用方显式调用 alphahome.fetchers.tasks.discover_tasks() 触发。
# 避免导入任意 fetcher 子模块时连带加载所有任务和外部客户端依赖。

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
