#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pytest

from alphahome.processors.tasks.style.style_index import StyleIndexReturnTask


class _DummyDB:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, sql):
        return self._rows

    async def save_dataframe(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_style_index_returns():
    dates = pd.date_range("2024-01-01", periods=10, freq="B")
    rows = []
    for d in dates:
        rows.append({"trade_date": d.strftime("%Y%m%d"), "ts_code": "000300.SH", "close": 100 + len(rows)})
    task = StyleIndexReturnTask(db_connection=_DummyDB(rows), config={"index_map": {"000300.SH": "HS300"}})
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert not result.empty
    for col in ["HS300_Return", "HS300_Return_5D", "HS300_Return_20D", "HS300_Return_60D"]:
        assert col in result.columns
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()


@pytest.mark.asyncio
async def test_style_index_empty():
    task = StyleIndexReturnTask(db_connection=_DummyDB([]), config={"index_map": {"000300.SH": "HS300"}})
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert result.empty

