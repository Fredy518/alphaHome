#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MarketTechnicalTask.fetch_data 配置化源表测试
"""

import pytest
from unittest.mock import AsyncMock

from alphahome.processors.tasks.market.market_technical import MarketTechnicalTask


class _DummyDB:
    def __init__(self):
        self.fetch = AsyncMock(return_value=[])


@pytest.mark.asyncio
async def test_fetch_data_uses_configured_source_table():
    custom_table = "custom_schema.custom_table"
    db = _DummyDB()
    task = MarketTechnicalTask(db_connection=db, config={"source_table": custom_table})

    await task.fetch_data(start_date="20200101", end_date="20200131")

    db.fetch.assert_awaited()
    query = db.fetch.call_args[0][0]
    assert custom_table in query
    assert task.source_tables == [custom_table]

