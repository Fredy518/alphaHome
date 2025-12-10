#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import pytest

from alphahome.processors.tasks.market.money_flow import MoneyFlowTask


class _DummyDB:
    def __init__(self, flow_rows, mv_rows):
        self._flow_rows = flow_rows
        self._mv_rows = mv_rows

    async def fetch(self, sql):
        if "stock_moneyflow" in sql:
            return self._flow_rows
        if "stock_dailybasic" in sql:
            return self._mv_rows
        return []

    async def save_dataframe(self, *args, **kwargs):
        return None


@pytest.mark.asyncio
async def test_money_flow_basic_calculation():
    flow_rows = [
        {"trade_date": "20240101", "total_net_mf_amount": 1000},
        {"trade_date": "20240102", "total_net_mf_amount": 2000},
        {"trade_date": "20240103", "total_net_mf_amount": 3000},
    ]
    mv_rows = [
        {"trade_date": "20240101", "total_circ_mv": 100},
        {"trade_date": "20240102", "total_circ_mv": 100},
        {"trade_date": "20240103", "total_circ_mv": 100},
    ]

    task = MoneyFlowTask(db_connection=_DummyDB(flow_rows, mv_rows), config={"window": 2, "min_periods": 2})
    data = await task.fetch_data()
    result = await task.process_data(data)

    assert not result.empty
    for col in ["Total_Net_MF", "Net_MF_Rate", "Net_MF_Rate_ZScore", "Net_MF_Rate_Pctl", "Net_MF_ZScore"]:
        assert col in result.columns
    assert not np.isinf(result.to_numpy()).any()
    # 第一行标准化因 min_periods=2 应为 NaN
    assert pd.isna(result["Net_MF_Rate_ZScore"].iloc[0])


@pytest.mark.asyncio
async def test_money_flow_empty():
    task = MoneyFlowTask(db_connection=_DummyDB([], []))
    data = await task.fetch_data()
    result = await task.process_data(data)
    assert result.empty

