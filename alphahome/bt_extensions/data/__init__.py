#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Data Layer for Backtesting Module

数据层组件，负责数据源管理和数据访问
"""

from .feeds import PostgreSQLDataFeed, PostgreSQLDataFeedFactory

__all__ = [
    "PostgreSQLDataFeed",
    "PostgreSQLDataFeedFactory",
]
