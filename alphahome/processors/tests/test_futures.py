#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pytest

from alphahome.processors.tasks.index.futures import FuturesBasisTask, MemberPositionTask


class _DummyDB:
    def __init__(self, fut_rows=None, idx_rows=None, hold_rows_map=None):
        self._fut_rows = fut_rows or []
        self._idx_rows = idx_rows or []
        self._hold_rows_map = hold_rows_map or {}

    async def fetch(self, sql):
        if "future_daily" in sql:
            return self._fut_rows
        if "index_factor_pro" in sql:
            return self._idx_rows
        if "future_holding" in sql:
            for key, rows in self._hold_rows_map.items():
                if f"symbol LIKE '{key}%'" in sql:
                    return rows
            return []
        return []

    async def save_dataframe(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_futures_basis():
    fut_rows = [
        {"trade_date": "20240101", "ts_code": "IF2301", "close": 4000, "oi": 1000},
        {"trade_date": "20240101", "ts_code": "IF2302", "close": 4010, "oi": 500},
    ]
    idx_rows = [
        {"trade_date": "20240101", "ts_code": "000300.SH", "close": 4020},
    ]
    task = FuturesBasisTask(db_connection=_DummyDB(fut_rows, idx_rows))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert not result.empty
    for col in ["IF_Basis", "IF_Basis_Ratio", "IF_Basis_ZScore", "IF_Basis_Pctl", "IF_Basis_Ratio_ZScore", "IF_Basis_Ratio_Pctl"]:
        assert col in result.columns
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()


@pytest.mark.asyncio
async def test_member_position():
    hold_rows_map = {
        "IF": [
            {
                "trade_date": "20240101",
                "total_long_hld": 2000,
                "total_short_hld": 1000,
                "total_long_chg": 100,
                "total_short_chg": 50,
            }
        ]
    }
    task = MemberPositionTask(db_connection=_DummyDB(hold_rows_map=hold_rows_map))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert not result.empty
    for col in [
        "IF_NET_LONG",
        "IF_NET_CHG",
        "IF_RATIO",
        "IF_NET_LONG_ZScore",
        "IF_NET_LONG_Pctl",
        "IF_NET_CHG_ZScore",
        "IF_NET_CHG_Pctl",
        "IF_RATIO_ZScore",
        "IF_RATIO_Pctl",
    ]:
        assert col in result.columns
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()


@pytest.mark.asyncio
async def test_futures_empty():
    task = FuturesBasisTask(db_connection=_DummyDB())
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert result.empty

