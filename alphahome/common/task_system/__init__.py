#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
alphaHome 统一任务系统

该模块提供统一的任务管理框架，支持数据采集(fetch)和数据处理(processor)两种任务类型。

主要组件:
- BaseTask: 统一的任务基类
- UnifiedTaskFactory: 统一的任务工厂
- task_register: 统一的任务注册装饰器

设计目标:
1. 统一fetchers和processors的任务架构
2. 支持任务类型分类管理
3. 保持向后兼容性
4. 提供扩展性支持未来新的任务类型
"""

__version__ = "1.0.0"
__author__ = "alphaHome Team"

# 导入核心组件
from .base_task import BaseTask
from .task_decorator import task_register
from .task_factory import (
    UnifiedTaskFactory,
    get_task,
    get_tasks_by_type,
    get_task_names_by_type,
    get_task_types,
)

# 统一导出接口
__all__ = [
    "BaseTask",
    "UnifiedTaskFactory",
    "task_register",
    "get_task",
    "get_tasks_by_type",
    "get_task_names_by_type",
    "get_task_types",
]