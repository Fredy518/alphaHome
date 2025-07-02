#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理操作层

该模块包含各种原子级数据处理操作，包括:
- Operation: 基础操作抽象类
- OperationPipeline: 操作流水线
- 缺失值处理操作
- 异常值处理操作
- 技术指标计算操作
- 数据转换操作

所有操作都是原子级的，可以独立使用，也可以组合成流水线。
"""

from .base_operation import Operation, OperationPipeline

# 导入具体操作（如果存在）
try:
    from .missing_data import *
except ImportError:
    pass

try:
    from .technical_indicators import *
except ImportError:
    pass

__all__ = [
    "Operation",
    "OperationPipeline"
]
