from __future__ import annotations

import base64

import pandas as pd
import pytest

from tinydata.client import TinyClient
from tinydata.config import TinyDataConfig
from tinydata.errors import TinyDataAuthError, TinyDataQueryError


class CaptureTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def test_opi_exec_posts_run_payload_and_returns_dataframe():
    transport = CaptureTransport({"body": [{"StockID": "SZ000001", "close": 10.5}]})
    client = TinyClient(
        TinyDataConfig(user="u", password="p", request_interval=0),
        transport=transport,
    )

    df = client.exec("return select * from infotable 42 of 'SZ000001' end;")

    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["StockID"] == "SZ000001"
    call = transport.calls[0]
    assert call["path"] == "/Service/Run/"
    assert call["json"] == {"body": "return select * from infotable 42 of 'SZ000001' end;"}
    assert call["headers"]["Authorization"] == "Basic " + base64.b64encode(b"u:p").decode("ascii")


def test_opi_exec_escapes_non_ascii_request_body(monkeypatch):
    captured = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'[{"ok":1}]'

    def fake_urlopen(request, timeout=None):
        captured["data"] = request.data
        return FakeResponse()

    monkeypatch.setattr("tinydata.client.urlopen", fake_urlopen)
    client = TinyClient(TinyDataConfig(user="u", password="p", request_interval=0))

    client.exec('return select ["公司中文简称"] from infotable 10 of \'SH600000\' end;')

    assert b"\\u516c\\u53f8\\u4e2d\\u6587\\u7b80\\u79f0" in captured["data"]
    assert "公司中文简称".encode("utf-8") not in captured["data"]


def test_opi_query_translates_to_markettable_tsl():
    transport = CaptureTransport({"body": [{"date": "2026-05-21 09:31:00", "StockID": "SZ000001"}]})
    client = TinyClient(
        TinyDataConfig(user="u", password="p", request_interval=0, service="auto"),
        transport=transport,
    )

    df = client.query(
        stock="SZ000001",
        cycle="1分钟线",
        begin_time="2026-05-21 09:30:00",
        end_time="2026-05-21 15:00:00",
        fields=["date", "StockID"],
    )

    assert len(df) == 1
    call = transport.calls[0]
    body = call["json"]["body"]
    assert "setsysparam(pn_cycle(),cy_1m());" in body
    assert 'datetimetostr(["date"]) as "date"' in body
    assert "from markettable datekey 20260521.093000T to 20260521.150000T" in body
    assert "of 'SZ000001' end;" in body
    assert call["headers"]["TS-EVENTNAME"] == "auto"


def test_session_key_exec_requires_wrapper_function():
    client = TinyClient(
        TinyDataConfig(opi_auth_mode="session-key", session_key="s", request_interval=0),
        transport=CaptureTransport({"body": []}),
    )

    with pytest.raises(TinyDataQueryError, match="run_func_name"):
        client.exec("return 1;")


def test_session_key_call_uses_session_uri_and_bearer_header():
    transport = CaptureTransport({"body": [{"ok": 1}]})
    client = TinyClient(
        TinyDataConfig(opi_auth_mode="session-key", session_key="s", session_password="p", request_interval=0),
        transport=transport,
    )

    df = client.call("my/wrapper", {"body": "return 1;"})

    assert df.to_dict("records") == [{"ok": 1}]
    call = transport.calls[0]
    assert call["path"] == "/Service/Session/Call/my/wrapper"
    assert call["headers"]["Authorization"] == "Bearer s:p"


def test_missing_basic_credentials_raise_auth_error():
    client = TinyClient(TinyDataConfig(user="", password="", request_interval=0), transport=CaptureTransport({}))
    with pytest.raises(TinyDataAuthError):
        client.exec("return 1;")


def test_payload_to_dataframe_supports_columns_rows_shape():
    df = TinyClient._payload_to_dataframe({"columns": ["StockID", "close"], "rows": [["SZ000001", 10.5]]})
    assert df.to_dict("records") == [{"StockID": "SZ000001", "close": 10.5}]
