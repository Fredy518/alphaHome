#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
alphaHome 数据处理模块

该模块提供数据处理的核心功能，采用分层架构设计:

## 架构层次
1. **基础层** (base/): 处理器基类和分块处理支持
2. **操作层** (operations/): 原子级数据处理操作
3. **流水线层** (pipelines/): 高级数据处理流水线
4. **任务层** (tasks/): 具体的数据处理任务
5. **引擎层** (engine/): 处理引擎，协调和执行任务

## 主要功能
1. 数据清洗和标准化
2. 特征工程
3. 技术指标计算
4. 因子构建的基础处理
5. 分块处理大数据集
6. 流水线式数据处理
7. 任务调度和执行监控

## 使用示例
```python
# 使用处理引擎执行任务
from alphahome.processors import ProcessorEngine

engine = ProcessorEngine(max_workers=4)
result = await engine.execute_task("stock_adjusted_price_v2")

# 直接使用流水线
from alphahome.processors.pipelines import ProcessingPipeline
from alphahome.processors.operations import Operation

pipeline = MyDataPipeline()
result = await pipeline.execute(data)
```
"""

__version__ = "0.2.0"

# 导入基础层
from .base import BaseProcessor, DataProcessor, BlockProcessor, BlockProcessorMixin

# 导入操作层
from .operations import Operation, OperationPipeline

# 导入流水线层
from .pipelines import ProcessingPipeline

# 导入任务层
from .tasks import ProcessorTaskBase

# 导入引擎层
from .engine import ProcessorEngine

# 保持向后兼容性
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

__all__ = [
    # 基础层
    "BaseProcessor",
    "DataProcessor",
    "BlockProcessor",
    "BlockProcessorMixin",

    # 操作层
    "Operation",
    "OperationPipeline",

    # 流水线层
    "ProcessingPipeline",

    # 任务层
    "ProcessorTaskBase",

    # 引擎层
    "ProcessorEngine",

    # 向后兼容
    "ProcessorTask",
    "BaseTask",

    # 任务系统组件
    "task_register",
    "get_task",
    "get_tasks_by_type",
    "get_task_types",
]
