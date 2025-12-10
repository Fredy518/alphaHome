#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
IndexValuationTask 单元测试
"""

import pandas as pd
import numpy as np
import pytest

from alphahome.processors.tasks.index.index_valuation import IndexValuationTask


class _DummyDB:
    """最小 db 占位，避免真实 IO"""
    async def save_dataframe(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_process_data_computes_percentiles_and_erp():
    # 构造 300 天数据，保证滚动窗口能产生分位值
    dates = pd.date_range("2023-01-01", periods=300, freq="B")
    df = pd.DataFrame({
        "trade_date": dates,
        "ts_code": ["000300.SH"] * len(dates),
        "pe_ttm": np.linspace(10, 20, len(dates)),
        "pb": np.linspace(1, 2, len(dates)),
    })

    # 构造国债收益率表
    yield_df = pd.DataFrame({
        "trade_date": dates,
        "yield": [3.0] * len(dates),
    }).set_index("trade_date")

    # 挂在特殊列以便 process_data 复用
    df["_yield_df"] = [yield_df] * len(df)

    task = IndexValuationTask(db_connection=_DummyDB())
    result = await task.process_data(df)

    assert not result.empty
    # 核心列存在
    for col in ["HS300_PE", "HS300_PB", "HS300_PE_Pctl_12M", "HS300_PB_Pctl_12M", "HS300_ERP"]:
        assert col in result.columns
    # 分位与 ERP 不应含 inf
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()
    # 末尾样本的分位应有值（窗口已满足）
    assert not pd.isna(result["HS300_PE_Pctl_12M"].iloc[-1])
    # ERP 计算存在值
    assert not pd.isna(result["HS300_ERP"].iloc[-1])


@pytest.mark.asyncio
async def test_process_data_empty_returns_empty_df():
    task = IndexValuationTask(db_connection=_DummyDB())
    result = await task.process_data(pd.DataFrame())
    assert result.empty

