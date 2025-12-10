#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pytest

from alphahome.processors.tasks.index.industry import IndustryReturnTask, IndustryBreadthTask


class _DummyDB:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, sql):
        return self._rows

    async def save_dataframe(self, *args, **kwargs):
        return None


def _mock_rows():
    dates = pd.date_range("2024-01-01", periods=6, freq="B")
    rows = []
    for d in dates:
        rows.append({"trade_date": d.strftime("%Y%m%d"), "ts_code": "881001.SI", "close": 100 + len(rows)})
        rows.append({"trade_date": d.strftime("%Y%m%d"), "ts_code": "881002.SI", "close": 200 + len(rows)})
    return rows


@pytest.mark.asyncio
async def test_industry_return():
    rows = _mock_rows()
    task = IndustryReturnTask(db_connection=_DummyDB(rows))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert not result.empty
    assert any(col.startswith("SW_") for col in result.columns)
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()


@pytest.mark.asyncio
async def test_industry_breadth():
    rows = _mock_rows()
    task = IndustryBreadthTask(db_connection=_DummyDB(rows))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert not result.empty
    for col in [
        "Industry_Up_Ratio",
        "Industry_Strong_Ratio",
        "Industry_Weak_Ratio",
        "Industry_Return_Std",
        "Industry_Return_Skew",
        "Industry_Up_Ratio_5D",
    ]:
        assert col in result.columns
    assert not np.isinf(result.select_dtypes(include=[float]).to_numpy()).any()


@pytest.mark.asyncio
async def test_industry_empty():
    task = IndustryReturnTask(db_connection=_DummyDB([]))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert result.empty

