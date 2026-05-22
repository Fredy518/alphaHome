import pandas as pd
import pytest

from alphahome.fetchers.sources.tinysoft import TinySoftAPIError, TinySoftOPIAPI
from alphahome.fetchers.tasks.stock.tinysoft_stock_minute import TinySoftStockMinuteTask


class _CaptureTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


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
