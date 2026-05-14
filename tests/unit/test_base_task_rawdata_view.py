# -*- coding: utf-8 -*-
"""Regression: rawdata mapping views must OR REPLACE when source table gains columns."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from alphahome.common.task_system.base_task import BaseTask


class _TinysoftLikeTask(BaseTask):
    task_type = "fetch"
    name = "test_rawdata_view_tinysoft_dummy"
    table_name = "tinysoft_only_table"
    data_source = "tinysoft"
    schema_def = {"ts_code": {"type": "TEXT"}}

    async def _fetch_data(self, stop_event=None, **kwargs):
        return pd.DataFrame()


class _TushareLikeTask(BaseTask):
    task_type = "fetch"
    name = "test_rawdata_view_tushare_dummy"
    table_name = "stock_basic"
    data_source = "tushare"
    schema_def = {"ts_code": {"type": "TEXT"}}

    async def _fetch_data(self, stop_event=None, **kwargs):
        return pd.DataFrame()


class _RawdataLikeTask(BaseTask):
    task_type = "fetch"
    name = "test_rawdata_view_rawdata_dummy"
    table_name = "some_table"
    data_source = "rawdata"
    schema_def = {"ts_code": {"type": "TEXT"}}

    async def _fetch_data(self, stop_event=None, **kwargs):
        return pd.DataFrame()


@pytest.mark.asyncio
async def test_non_tushare_always_or_replaces_rawdata_view_when_no_tushare_table():
    db = AsyncMock()
    db.check_table_exists = AsyncMock(return_value=False)
    db.create_rawdata_view = AsyncMock()

    task = _TinysoftLikeTask(db)
    await task._create_rawdata_view_if_needed()

    db.check_table_exists.assert_awaited_once_with("tushare", "tinysoft_only_table")
    db.create_rawdata_view.assert_awaited_once_with(
        view_name="tinysoft_only_table",
        source_schema="tinysoft",
        source_table="tinysoft_only_table",
        replace=True,
    )


@pytest.mark.asyncio
async def test_non_tushare_skips_rawdata_view_when_tushare_has_same_table():
    db = AsyncMock()
    db.check_table_exists = AsyncMock(return_value=True)
    db.create_rawdata_view = AsyncMock()

    task = _TinysoftLikeTask(db)
    await task._create_rawdata_view_if_needed()

    db.check_table_exists.assert_awaited_once_with("tushare", "tinysoft_only_table")
    db.create_rawdata_view.assert_not_called()


@pytest.mark.asyncio
async def test_tushare_always_or_replaces_rawdata_view():
    db = AsyncMock()
    db.create_rawdata_view = AsyncMock()

    task = _TushareLikeTask(db)
    await task._create_rawdata_view_if_needed()

    db.check_table_exists.assert_not_called()
    db.create_rawdata_view.assert_awaited_once_with(
        view_name="stock_basic",
        source_schema="tushare",
        source_table="stock_basic",
        replace=True,
    )


@pytest.mark.asyncio
async def test_rawdata_data_source_skips_view_logic():
    db = AsyncMock()
    db.create_rawdata_view = AsyncMock()

    task = _RawdataLikeTask(db)
    await task._create_rawdata_view_if_needed()

    db.create_rawdata_view.assert_not_called()
    db.check_table_exists.assert_not_called()
