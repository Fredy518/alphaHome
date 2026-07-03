#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
FRED (圣路易斯联邦储备银行) 数据源模块

提供对 FRED 经济数据接口的封装，包括：
- FredAPI: API 封装层，处理请求间隔、重试和错误
- FredTask: 任务基类，继承 FetcherTask

数据接口：FRED fredgraph.csv 端点（无需 API key）
- 端点：https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES>&cosd=<START>&coed=<END>
- 返回两列 CSV：observation_date, <series_id>
- 缺失值用 "." 表示
"""

from .fred_api import FredAPI, FredAPIError
from .fred_task import FredTask

__all__ = [
    "FredAPI",
    "FredAPIError",
    "FredTask",
]
