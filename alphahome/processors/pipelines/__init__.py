#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据处理流水线层

该模块包含高级数据处理流水线，组合多个操作来完成复杂的数据处理任务。

主要组件:
- ProcessingPipeline: 高级数据处理流水线基类
- 各种预定义的处理流水线

流水线层与操作层的区别：
- 操作层：原子级操作，如填充缺失值、计算移动平均等
- 流水线层：组合多个操作，完成特定的业务逻辑，如股票数据预处理、技术指标计算等
"""

from .base_pipeline import ProcessingPipeline

__all__ = [
    "ProcessingPipeline"
]
