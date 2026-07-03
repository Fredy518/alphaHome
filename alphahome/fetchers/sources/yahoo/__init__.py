#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Yahoo Finance 数据源模块

通过 Yahoo v8 chart API 直连获取历史行情（绕过 yfinance 库的限流问题）。

用途：作为 FRED 不可达环境下的备用数据源（DXY/VIX）。
- 接口：https://query1.finance.yahoo.com/v8/finance/chart/<SYMBOL>?period1=&period2=&interval=1d
- 返回 JSON：timestamp(unix秒) + indicators.quote[0].{open,high,low,close,volume}
- 需 User-Agent 头，无需 API key
"""

from .yahoo_api import YahooAPI, YahooAPIError

__all__ = [
    "YahooAPI",
    "YahooAPIError",
]
