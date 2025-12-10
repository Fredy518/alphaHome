#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
市场级特征处理任务

包含横截面市场技术特征、市场情绪指标等任务。
"""

from .market_technical import MarketTechnicalTask
from .money_flow import MoneyFlowTask

__all__ = [
    "MarketTechnicalTask",
    "MoneyFlowTask",
]
