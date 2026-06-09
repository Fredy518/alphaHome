#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft 数据源模块

提供对 pyTSL 的封装，包括：
- TinySoftAPI: API 封装层，处理登录、查询和错误
- TinySoftOPIAPI: TS-OPI HTTP API 封装层
- TinySoftTask: 任务基类，继承 FetcherTask
"""

from .tinysoft_api import (
    TinySoftAPI,
    TinySoftAPIError,
    TinySoftAuthError,
    TinySoftDependencyError,
    TinySoftRateLimitError,
)
from .tinysoft_opi_api import TinySoftOPIAPI
from .tinysoft_task import TinySoftTask

__all__ = [
    "TinySoftAPI",
    "TinySoftAPIError",
    "TinySoftAuthError",
    "TinySoftDependencyError",
    "TinySoftRateLimitError",
    "TinySoftOPIAPI",
    "TinySoftTask",
]
