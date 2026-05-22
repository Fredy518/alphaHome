import asyncio
import time

import pandas as pd
import pytest

from alphahome.fetchers.sources.tinysoft.tinysoft_api import TinySoftAPI, TinySoftAPIError


def test_format_stock_selector_uses_single_stock_literal():
    assert TinySoftAPI._format_stock_selector(["SZ000001"]) == "'SZ000001'"


def test_format_stock_selector_uses_array_for_multiple_stocks():
    assert TinySoftAPI._format_stock_selector(["SZ000001", "SH600000"]) == (
        "array('SZ000001','SH600000')"
    )


def test_format_stock_selector_ignores_empty_values():
    assert TinySoftAPI._format_stock_selector([None, " ", "SZ000001"]) == "'SZ000001'"


def test_format_stock_selector_rejects_empty_selector():
    with pytest.raises(ValueError, match="selector cannot be empty"):
        TinySoftAPI._format_stock_selector([None, " "])


@pytest.mark.asyncio
async def test_call_dataframe_for_stocks_builds_infotable_query():
    api = TinySoftAPI(user="u", password="p")
    captured = {}

    async def fake_exec(tsl_code, *, as_dataframe=True, stop_event=None):
        captured["tsl_code"] = tsl_code
        captured["as_dataframe"] = as_dataframe
        return pd.DataFrame({"StockID": ["SZ000001", "SH600000"]})

    api.exec = fake_exec
    df = await api.call_dataframe_for_stocks(
        "infoarray",
        42,
        stocks=["SZ000001", "SH600000"],
        where_clause='["公布日"]>=20250301',
    )

    assert captured["as_dataframe"] is True
    assert captured["tsl_code"] == (
        "return select * from infotable 42 of array('SZ000001','SH600000') "
        'where ["公布日"]>=20250301 end;'
    )
    assert len(df) == 2


@pytest.mark.asyncio
async def test_call_dataframe_for_stocks_builds_projected_infotable_query():
    api = TinySoftAPI(user="u", password="p")
    captured = {}

    async def fake_exec(tsl_code, *, as_dataframe=True, stop_event=None):
        captured["tsl_code"] = tsl_code
        return pd.DataFrame({"StockID": ["SZ000001"]})

    api.exec = fake_exec
    await api.call_dataframe_for_stocks(
        "infoarray",
        349,
        stocks=["SZ000001"],
        fields=["StockID", "占净值比例(%)"],
        where_clause='["截止日"]>=20250301',
    )

    assert captured["tsl_code"] == (
        "return select [\"StockID\"],[\"占净值比例(%)\"] from infotable 349 of 'SZ000001' "
        'where ["截止日"]>=20250301 end;'
    )


@pytest.mark.asyncio
async def test_call_dataframe_for_stocks_passes_timeout_to_exec():
    api = TinySoftAPI(user="u", password="p")
    captured = {}

    async def fake_exec(tsl_code, *, as_dataframe=True, timeout_ms=None, stop_event=None):
        captured["timeout_ms"] = timeout_ms
        captured["stop_event"] = stop_event
        return pd.DataFrame()

    stop_event = asyncio.Event()
    api.exec = fake_exec
    await api.call_dataframe_for_stocks(
        "infoarray",
        42,
        stocks=["SZ000001", "SH600000"],
        timeout_ms=12345,
        stop_event=stop_event,
    )

    assert captured["timeout_ms"] == 12345
    assert captured["stop_event"] is stop_event


@pytest.mark.asyncio
async def test_call_dataframe_table_builds_full_table_query():
    api = TinySoftAPI(user="u", password="p")
    captured = {}

    async def fake_exec(tsl_code, *, as_dataframe=True, stop_event=None):
        captured["tsl_code"] = tsl_code
        captured["as_dataframe"] = as_dataframe
        return pd.DataFrame({"基金经理代码": ["M001"]})

    api.exec = fake_exec
    df = await api.call_dataframe_table(
        "infoarray",
        625,
        where_clause='["截止日"]>=20260301',
    )

    assert captured["as_dataframe"] is True
    assert captured["tsl_code"] == (
        'return select * from infotable 625 where ["截止日"]>=20260301 end;'
    )
    assert len(df) == 1


@pytest.mark.asyncio
async def test_call_dataframe_table_builds_projected_full_table_query():
    api = TinySoftAPI(user="u", password="p")
    captured = {}

    async def fake_exec(tsl_code, *, as_dataframe=True, stop_event=None):
        captured["tsl_code"] = tsl_code
        return pd.DataFrame({"基金经理代码": ["M001"]})

    api.exec = fake_exec
    await api.call_dataframe_table(
        "infoarray",
        625,
        fields=["基金经理代码", "开始日"],
        where_clause='["截止日"]>=20260301',
    )

    assert captured["tsl_code"] == (
        'return select ["基金经理代码"],["开始日"] from infotable 625 where ["截止日"]>=20260301 end;'
    )


@pytest.mark.asyncio
async def test_call_dataframe_table_passes_timeout_to_exec():
    api = TinySoftAPI(user="u", password="p")
    captured = {}

    async def fake_exec(tsl_code, *, as_dataframe=True, timeout_ms=None, stop_event=None):
        captured["timeout_ms"] = timeout_ms
        return pd.DataFrame()

    api.exec = fake_exec
    await api.call_dataframe_table(
        "infoarray",
        625,
        where_clause='["实际截止日"]>=20260301',
        timeout_ms=54321,
    )

    assert captured["timeout_ms"] == 54321


@pytest.mark.asyncio
async def test_exec_timeout_discards_current_client():
    class BlockingClient:
        def is_logined(self):
            return 1

        def exec(self, tsl_code):
            time.sleep(0.05)
            return None

    api = TinySoftAPI(user="u", password="p")
    client = BlockingClient()
    api._client = client

    with pytest.raises(TinySoftAPIError, match="Tinysoft exec 超时"):
        await api.exec("return 1;", timeout_ms=1)

    assert api._client is None
