#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Excel 数据源模块

用于从本地 Excel 文件读取数据并保存到数据库。
适用于补充难以从常规数据源获取的数据。
"""

from .excel_task import ExcelTask

__all__ = [
    "ExcelTask",
]
