#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
alphaHome 数据处理模块

该模块提供数据处理的核心功能，包括:
1. 数据清洗和标准化
2. 特征工程
3. 技术指标计算
4. 因子构建的基础处理

主要组件:
- 基础处理任务 (base/processor_task.py)
- 块处理器 (base/block_processor.py)
- 数据操作 (operations/)
- 具体处理任务 (tasks/)
"""

__version__ = "0.1.0"

from .processor_task import ProcessorTask

# 重新导出统一任务系统的组件
from ..common.task_system import (
    BaseTask,
    UnifiedTaskFactory,
    task_register,
    get_task,
    get_tasks_by_type,
    get_task_types,
)

# 导入所有具体的processor任务（这会触发@task_register装饰器）
from . import tasks

# 确保processor任务注册到UnifiedTaskFactory（放在任务导入之后） (此步骤已不再需要)
# from ..common.task_system.task_decorator import register_tasks_to_factory
# register_tasks_to_factory()

__all__ = ["ProcessorTask", "BaseTask"]
