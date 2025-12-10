#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import pytest
import numpy as np

from alphahome.processors.tasks.index.option_iv import OptionIVTask


class _DummyDB:
    def __init__(self, opt_rows, etf_rows):
        self._opt_rows = opt_rows
        self._etf_rows = etf_rows

    async def fetch(self, sql):
        if "option_daily" in sql:
            return self._opt_rows
        if "fund_daily" in sql:
            return self._etf_rows
        return []

    async def save_dataframe(self, *args, **kwargs):
        return None


def _make_sample():
    # 两个到期（20天、50天），行权价接近现价，便于生成 near/next/30D
    opt_rows = [
        {
            "trade_date": "20240101",
            "ts_code": "OP510300.SH001",
            "opt_price": 2.0,
            "oi": 1000,
            "call_put": "C",
            "exercise_price": 100.0,
            "maturity_date": "20240121",
            "days_to_expiry": 20,
        },
        {
            "trade_date": "20240101",
            "ts_code": "OP510300.SH002",
            "opt_price": 3.5,
            "oi": 1000,
            "call_put": "C",
            "exercise_price": 100.0,
            "maturity_date": "20240220",
            "days_to_expiry": 50,
        },
    ]
    etf_rows = [
        {"trade_date": "20240101", "ts_code": "510300.SH", "close": 100.0},
    ]
    return opt_rows, etf_rows


@pytest.mark.asyncio
async def test_option_iv_columns():
    opt_rows, etf_rows = _make_sample()
    task = OptionIVTask(
        db_connection=_DummyDB(opt_rows, etf_rows),
        config={"option_underlying_map": {"OP510300.SH": ("HS300", "510300.SH")}},
    )
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert not result.empty
    for col in ["HS300_IV_Near", "HS300_IV_Next", "HS300_IV_ShortTerm"]:
        assert col in result.columns
    # 30D 可能因窗口不足缺失，不强制
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()


@pytest.mark.asyncio
async def test_option_iv_empty():
    task = OptionIVTask(db_connection=_DummyDB([], []))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert result.empty

