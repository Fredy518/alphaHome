import pandas as pd
import pytest

import alphahome.fetchers.sources.tinysoft.tinysoft_opi_api as opi_api_module
from alphahome.fetchers.sources.tinysoft import TinySoftAPIError, TinySoftOPIAPI, TinySoftRateLimitError
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import TinySoftStockMinuteTask


class _CaptureTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class _SequenceTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def __call__(self, **kwargs):
        self.calls.append(kwargs)
        idx = min(len(self.calls) - 1, len(self.responses) - 1)
        return self.responses[idx]


@pytest.mark.asyncio
async def test_opi_exec_posts_run_payload_and_returns_dataframe():
    transport = _CaptureTransport({"body": [{"StockID": "SZ000001", "close": 10.5}]})
    api = TinySoftOPIAPI(
        user="u",
        password="p",
        transport=transport,
        request_interval=0,
    )

    df = await api.exec("return select * from infotable 42 of 'SZ000001' end;")

    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["StockID"] == "SZ000001"
    call = transport.calls[0]
    assert call["path"] == "/Service/Run/"
    assert call["json"] == {"body": "return select * from infotable 42 of 'SZ000001' end;"}
    assert call["headers"]["Authorization"].startswith("Basic ")


@pytest.mark.asyncio
async def test_opi_call_dataframe_for_stocks_builds_infotable_tsl():
    transport = _CaptureTransport({"body": [{"StockID": "SZ000001"}, {"StockID": "SH600000"}]})
    api = TinySoftOPIAPI(
        user="u",
        password="p",
        transport=transport,
        request_interval=0,
    )

    df = await api.call_dataframe_for_stocks(
        "infoarray",
        42,
        stocks=["SZ000001", "SH600000"],
        where_clause='["公布日"]>=20260101',
    )

    assert len(df) == 2
    body = transport.calls[0]["json"]["body"]
    assert body == (
        "return select * from infotable 42 of array('SZ000001','SH600000') "
        'where ["公布日"]>=20260101 end;'
    )


@pytest.mark.asyncio
async def test_opi_call_dataframe_for_stocks_builds_projected_infotable_tsl():
    transport = _CaptureTransport({"body": [{"StockID": "SZ000001"}]})
    api = TinySoftOPIAPI(
        user="u",
        password="p",
        transport=transport,
        request_interval=0,
    )

    await api.call_dataframe_for_stocks(
        "infoarray",
        349,
        stocks=["SZ000001"],
        fields=["StockID", "占净值比例(%)"],
        where_clause='["截止日"]>=20260101',
    )

    body = transport.calls[0]["json"]["body"]
    assert body == (
        "return select [\"StockID\"],[\"占净值比例(%)\"] from infotable 349 of 'SZ000001' "
        'where ["截止日"]>=20260101 end;'
    )


@pytest.mark.asyncio
async def test_opi_call_dataframe_table_builds_projected_infotable_tsl():
    transport = _CaptureTransport({"body": [{"基金经理代码": "M001"}]})
    api = TinySoftOPIAPI(
        user="u",
        password="p",
        transport=transport,
        request_interval=0,
    )

    await api.call_dataframe_table(
        "infoarray",
        625,
        fields=["基金经理代码", "开始日"],
        where_clause='["截止日"]>=20260101',
    )

    body = transport.calls[0]["json"]["body"]
    assert body == (
        'return select ["基金经理代码"],["开始日"] from infotable 625 where ["截止日"]>=20260101 end;'
    )


@pytest.mark.asyncio
async def test_opi_query_translates_to_markettable_tsl():
    transport = _CaptureTransport(
        {
            "body": [
                {
                    "date": "2026-05-21 09:31:00",
                    "StockID": "SZ000001",
                    "close": 10.5,
                }
            ]
        }
    )
    api = TinySoftOPIAPI(
        user="u",
        password="p",
        transport=transport,
        request_interval=0,
    )

    df = await api.query(
        stock="SZ000001",
        cycle="1分钟线",
        begin_time="2026-05-21 09:30:00",
        end_time="2026-05-21 15:00:00",
        fields=["date", "StockID", "close"],
        service="auto",
    )

    assert len(df) == 1
    body = transport.calls[0]["json"]["body"]
    assert "setsysparam(pn_cycle(),cy_1m());" in body
    assert 'datetimetostr(["date"]) as "date"' in body
    assert "from markettable datekey 20260521.093000T to 20260521.150000T" in body
    assert "of 'SZ000001' end;" in body
    assert transport.calls[0]["headers"]["TS-EVENTNAME"] == "auto"


@pytest.mark.asyncio
async def test_opi_query_panel_translates_to_markettable_array_tsl():
    transport = _CaptureTransport(
        {
            "body": [
                {"date": "2026-05-21 09:31:00", "StockID": "SZ000001", "close": 10.5},
                {"date": "2026-05-21 09:31:00", "StockID": "SH600000", "close": 9.8},
            ]
        }
    )
    api = TinySoftOPIAPI(user="u", password="p", transport=transport, request_interval=0)

    df = await api.query_panel(
        stocks=["SZ000001", "SH600000"],
        cycle="1分钟线",
        begin_time="2026-05-21 09:30:00",
        end_time="2026-05-21 15:00:00",
        fields=["date", "StockID", "close"],
    )

    assert len(df) == 2
    body = transport.calls[0]["json"]["body"]
    assert "from markettable datekey 20260521.093000T to 20260521.150000T" in body
    assert "of array('SZ000001','SH600000') end;" in body


@pytest.mark.asyncio
async def test_opi_request_retries_429_with_retry_after():
    transport = _SequenceTransport(
        [
            (429, {"Retry-After": "0"}, {"code": 429, "message": "too many requests"}),
            {"body": [{"StockID": "SZ000001", "close": 10.5}]},
        ]
    )
    api = TinySoftOPIAPI(user="u", password="p", transport=transport, request_interval=0)

    df = await api.exec("return 1;")

    assert len(df) == 1
    assert len(transport.calls) == 2


@pytest.mark.asyncio
async def test_opi_request_raises_rate_limit_after_retries():
    transport = _SequenceTransport(
        [
            (429, {"Retry-After": "0"}, {"code": 429, "message": "too many requests"}),
            (429, {"Retry-After": "0"}, {"code": 429, "message": "too many requests"}),
        ]
    )
    api = TinySoftOPIAPI(user="u", password="p", transport=transport, request_interval=0)
    api.RATE_LIMIT_MAX_ATTEMPTS = 2

    with pytest.raises(TinySoftRateLimitError):
        await api.exec("return 1;")

    assert len(transport.calls) == 2


@pytest.mark.asyncio
async def test_opi_reuses_aiohttp_session(monkeypatch):
    sessions = []

    class _FakeResponse:
        status = 200
        headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def read(self):
            return b'{"body":[]}'

    class _FakeSession:
        def __init__(self):
            self.closed = False
            self.post_calls = []
            sessions.append(self)

        def post(self, url, *, headers=None, json=None, timeout=None):
            self.post_calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
            return _FakeResponse()

        async def close(self):
            self.closed = True

    monkeypatch.setattr(opi_api_module.aiohttp, "ClientSession", _FakeSession)

    api = TinySoftOPIAPI(user="u", password="p", request_interval=0)
    await api.exec("return 1;")
    await api.exec("return 2;")

    assert len(sessions) == 1
    assert len(sessions[0].post_calls) == 2

    await api.close()
    assert sessions[0].closed is True


def test_tinysoft_task_uses_opi_backend_when_configured():
    task = TinySoftStockMinuteTask(
        db_connection=object(),
        tinysoft_config={
            "mode": "opi",
            "user": "u",
            "password": "p",
            "request_interval": 0,
        },
        task_config={"ts_codes": ["000001.SZ"]},
    )

    assert isinstance(task.api, TinySoftOPIAPI)


def test_opi_payload_to_dataframe_supports_columns_rows_shape():
    df = TinySoftOPIAPI._payload_to_dataframe(
        {"columns": ["StockID", "close"], "rows": [["SZ000001", 10.5]]}
    )

    assert df.to_dict("records") == [{"StockID": "SZ000001", "close": 10.5}]


@pytest.mark.asyncio
async def test_opi_exec_rejects_session_key_without_wrapper_function():
    api = TinySoftOPIAPI(
        auth_mode="bearer",
        session_key="SESSION-KEY",
        request_interval=0,
    )

    with pytest.raises(TinySoftAPIError, match="run_func_name"):
        await api.exec("return 1;")
