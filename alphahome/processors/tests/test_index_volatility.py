#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pytest

from alphahome.processors.tasks.index.index_volatility import IndexVolatilityTask


class _DummyDB:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, sql):
        return self._rows

    async def save_dataframe(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_index_volatility_compute():
    dates = pd.date_range("2024-01-01", periods=40, freq="B")
    rows = []
    for d in dates:
        rows.append({"trade_date": d.strftime("%Y%m%d"), "ts_code": "000300.SH", "close": 100 + len(rows)})
    task = IndexVolatilityTask(db_connection=_DummyDB(rows), config={"index_map": {"000300.SH": "HS300"}})
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert not result.empty
    for col in ["HS300_RV_20D", "HS300_RV_60D", "HS300_RV_252D", "HS300_RV_20D_Pctl", "HS300_RV_Ratio_20_60"]:
        assert col in result.columns
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()


@pytest.mark.asyncio
async def test_index_volatility_empty():
    task = IndexVolatilityTask(db_connection=_DummyDB([]))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert result.empty

