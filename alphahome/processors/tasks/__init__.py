#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理任务模块

包含各种具体的数据处理任务实现。
"""

# 导入所有处理任务
from .stock_adjusted_price import StockAdjustedPriceTask

__all__ = [
    "StockAdjustedPriceTask",
]
