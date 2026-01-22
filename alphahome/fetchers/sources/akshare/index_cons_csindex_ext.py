#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare-like extension: CSIndex constituents (fix xls engine issue).

AkShare 1.17.x 的 index_stock_cons_csindex 使用 openpyxl 读取 .xls 文件会报错。
这里提供同名函数以覆盖 AkShareAPI.EXTRA_FUNCS，从而在项目内稳定使用。
"""

from __future__ import annotations

from io import BytesIO

import pandas as pd
import requests


def index_stock_cons_csindex(symbol: str = "000300") -> pd.DataFrame:
    """
    中证指数网站-成份股目录
    https://www.csindex.com.cn/zh-CN/indices/index-detail/000300

    Args:
        symbol: 指数代码
    """
    url = (
        "https://oss-ch.csindex.com.cn/static/"
        f"html/csindex/public/uploads/file/autofile/cons/{symbol}cons.xls"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    # .xls: use xlrd engine (already in dependencies in this environment)
    temp_df = pd.read_excel(BytesIO(r.content), engine="xlrd")
    temp_df.columns = [
        "日期",
        "指数代码",
        "指数名称",
        "指数英文名称",
        "成分券代码",
        "成分券名称",
        "成分券英文名称",
        "交易所",
        "交易所英文名称",
    ]
    temp_df["日期"] = pd.to_datetime(temp_df["日期"], format="%Y%m%d", errors="coerce").dt.date
    temp_df["指数代码"] = temp_df["指数代码"].astype(str).str.zfill(6)
    temp_df["成分券代码"] = temp_df["成分券代码"].astype(str).str.zfill(6)
    return temp_df


__all__ = ["index_stock_cons_csindex"]

