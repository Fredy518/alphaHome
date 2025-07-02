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

# 导入所有处理任务（旧版本，保持兼容性）
from .stock_adjusted_price import StockAdjustedPriceTask

# 导入新架构的任务
from .stock_adjusted_price_v2 import StockAdjustedPriceTaskV2

__all__ = [
    "ProcessorTaskBase",
    "StockAdjustedPriceTask",      # 旧版本
    "StockAdjustedPriceTaskV2",    # 新版本
]
