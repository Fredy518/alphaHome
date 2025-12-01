#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 数据源模块

提供对 akshare 库的封装，包括：
- AkShareAPI: API 封装层，处理请求间隔、重试和错误
- AkShareTask: 任务基类，继承 FetcherTask
- AkShareSingleBatchTask: 单批次任务基类
- AkShareDataTransformer: 数据转换器
"""

from .akshare_api import AkShareAPI, AkShareAPIError, AkShareRateLimitError
from .akshare_task import AkShareTask, AkShareSingleBatchTask
from .akshare_data_transformer import AkShareDataTransformer

__all__ = [
    "AkShareAPI",
    "AkShareAPIError",
    "AkShareRateLimitError",
    "AkShareTask",
    "AkShareSingleBatchTask",
    "AkShareDataTransformer",
]

