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

# 导入变换函数
from .transforms import (
    # 基础标准化函数
    zscore,
    minmax_scale,
    # 滚动计算函数
    rolling_zscore,
    rolling_percentile,
    rolling_sum,
    rolling_rank,
    # 去极值和分箱函数
    winsorize,
    quantile_bins,
    # 收益率计算函数 (Task 3.1)
    diff_pct,
    log_return,
    ema,
    # 滚动斜率函数 (Task 3.2)
    rolling_slope,
    # 高级特征函数 (Task 3.3, 3.5)
    price_acceleration,
    rolling_slope_volatility_adjusted,
    trend_strength_index,
)

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
    # 基类
    "Operation",
    "OperationPipeline",
    # 基础标准化函数
    "zscore",
    "minmax_scale",
    # 滚动计算函数
    "rolling_zscore",
    "rolling_percentile",
    "rolling_sum",
    "rolling_rank",
    # 去极值和分箱函数
    "winsorize",
    "quantile_bins",
    # 收益率计算函数 (Task 3.1)
    "diff_pct",
    "log_return",
    "ema",
    # 滚动斜率函数 (Task 3.2)
    "rolling_slope",
    # 高级特征函数 (Task 3.3, 3.5)
    "price_acceleration",
    "rolling_slope_volatility_adjusted",
    "trend_strength_index",
]
