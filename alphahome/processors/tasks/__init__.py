#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务层

包含各种具体的数据处理任务实现。
任务层使用流水线层和操作层来组合复杂的处理逻辑。

主要组件:
- ProcessorTaskBase: 数据处理任务基类
- 各种具体的数据处理任务实现
"""

# 导入基类
from .base_task import ProcessorTaskBase
from .block_processing_mixin import BlockProcessingTaskMixin

# 导入具体的处理任务
from .stock_adjusted_price_v2 import StockAdjustedPriceV2Task
from .stock_adjdaily_processor import StockAdjdailyProcessorTask
# 导入旧的 stock_adjusted_price 任务，如果需要保持兼容性
# from .stock_adjusted_price import StockAdjustedPriceTask

__all__ = [
    # 基类
    "ProcessorTaskBase",
    "BlockProcessingTaskMixin",

    # 具体任务
    "StockAdjustedPriceV2Task",
    "StockAdjdailyProcessorTask",
    # "StockAdjustedPriceTask", # 如需兼容，取消此行注释
]
