#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
alphaHome 数据处理模块

该模块提供数据处理的核心功能，采用分层架构设计:

## 架构层次
1. **引擎层 (engine/)**: 任务调度、并发控制和执行监控。
2. **任务层 (tasks/)**: 封装完整的业务处理流程，负责数据IO和操作编排。
3. **操作层 (operations/)**: 可复用的、原子级的数据处理操作。

## 使用示例
```python
# 使用处理引擎执行任务
from alphahome.processors import ProcessorEngine

engine = ProcessorEngine(max_workers=4)
result = await engine.execute_task("stock_adjusted_price_v2")

# 在任务内部直接使用操作
from alphahome.processors import Operation, OperationPipeline

class MyTask(ProcessorTaskBase):
    async def process_data(self, data, **kwargs):
        pipeline = OperationPipeline("MyInternalPipeline")
        pipeline.add_operation(MyOperation1())
        pipeline.add_operation(MyOperation2())
        return await pipeline.apply(data)
```
"""

__version__ = "0.3.0"

# 导入操作层
from .operations import Operation, OperationPipeline

# 导入任务层
from .tasks import ProcessorTaskBase, BlockProcessingTaskMixin

# 导入引擎层
from .engine import ProcessorEngine

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

__all__ = [
    # 操作层
    "Operation",
    "OperationPipeline",

    # 任务层
    "ProcessorTaskBase",
    "BlockProcessingTaskMixin",

    # 引擎层
    "ProcessorEngine",

    # 任务系统组件
    "task_register",
    "get_task",
    "get_tasks_by_type",
    "get_task_types",
]
