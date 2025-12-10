#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务层

包含各种具体的数据处理任务实现。
任务层使用流水线层和操作层来组合复杂的处理逻辑。

主要组件:
- ProcessorTaskBase: 数据处理任务基类
- 各种具体的数据处理任务实现

领域子目录:
- market/: 市场级特征（横截面技术特征、市场情绪等）
- index/: 指数级特征（指数波动率、成分股特征等）
- style/: 风格因子（风格动量、风格轮动等）
"""

# 导入基类
from .base_task import ProcessorTaskBase
from .block_processing_mixin import BlockProcessingTaskMixin

# 导入具体的处理任务
# 导入旧的 stock_adjusted_price 任务，如果需要保持兼容性
# from .stock_adjusted_price import StockAdjustedPriceTask

# 导入领域子模块
from . import market
from . import index
from . import style

__all__ = [
    # 基类
    "ProcessorTaskBase",
    "BlockProcessingTaskMixin",

    # 具体任务
    # "StockAdjustedPriceTask", # 如需兼容，取消此行注释
    
    # 领域子模块
    "market",
    "index",
    "style",
]
