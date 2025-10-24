# -*- coding: utf-8 -*-

"""
Akshare 数据源模块

提供akshare数据源的实现，包括API封装和数据转换功能。
akshare是一个免费的开源金融数据接口库。
"""

from .akshare_api import AkshareAPI
from .akshare_task import AkshareTask, StockTask

__all__ = [
    'AkshareAPI',
    'AkshareTask',
    'StockTask'
]
