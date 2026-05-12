import logging

import aiohttp
import pandas as pd
import pytest

from alphahome.fetchers.sources.tushare import tushare_api as tushare_api_module
from alphahome.fetchers.sources.tushare.tushare_api import TushareAPI


class _FakeResponse:
    def __init__(self, status: int, json_data):
        self.status = status
        self._json_data = json_data

    async def text(self):
        return str(self._json_data)

    async def json(self):
        return self._json_data


class _FakeRequestContext:
    def __init__(self, on_enter):
        self._on_enter = on_enter

    async def __aenter__(self):
        return self._on_enter()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeClientSession:
    def __init__(self, *, fail_times: int, response_json):
        self._fail_times = fail_times
        self._response_json = response_json
        self.post_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def post(self, url, json):
        def _enter():
            self.post_calls += 1
            if self.post_calls <= self._fail_times:
                raise aiohttp.client_exceptions.ServerDisconnectedError()
            return _FakeResponse(200, self._response_json)

        return _FakeRequestContext(_enter)


@pytest.mark.asyncio
async def test_tushare_api_retries_on_server_disconnected(monkeypatch):
    api = TushareAPI(token="test", logger=logging.getLogger("test"))

    async def _no_wait(_api_name: str):
        return None

    monkeypatch.setattr(api, "_wait_for_rate_limit_slot", _no_wait)

    # 让内部的 _sleep_with_stop 快速结束：sleep 不等待 + monotonic 递增
    async def _no_sleep(_seconds: float):
        return None

    counter = {"t": 0.0}

    def _fake_monotonic():
        counter["t"] += 1.0
        return counter["t"]

    monkeypatch.setattr(tushare_api_module.asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(tushare_api_module.time, "monotonic", _fake_monotonic)

    response_json = {
        "code": 0,
        "data": {"fields": ["date"], "items": [["20260122"]]},
    }
    fake_session = _FakeClientSession(fail_times=2, response_json=response_json)

    def _session_factory(*args, **kwargs):
        return fake_session

    monkeypatch.setattr(tushare_api_module.aiohttp, "ClientSession", _session_factory)

    df = await api.query(
        api_name="eco_cal",
        fields="date",
        max_retries=3,
        start_date="20260111",
        end_date="20260122",
        limit=5000,
    )

    assert fake_session.post_calls == 3
    assert list(df.columns) == ["date"]
    assert df.iloc[0]["date"] == "20260122"


@pytest.mark.asyncio
async def test_tushare_api_raises_after_max_retries(monkeypatch):
    api = TushareAPI(token="test", logger=logging.getLogger("test"))

    async def _no_wait(_api_name: str):
        return None

    monkeypatch.setattr(api, "_wait_for_rate_limit_slot", _no_wait)

    async def _no_sleep(_seconds: float):
        return None

    counter = {"t": 0.0}

    def _fake_monotonic():
        counter["t"] += 1.0
        return counter["t"]

    monkeypatch.setattr(tushare_api_module.asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(tushare_api_module.time, "monotonic", _fake_monotonic)

    response_json = {
        "code": 0,
        "data": {"fields": ["date"], "items": [["20260122"]]},
    }
    fake_session = _FakeClientSession(fail_times=10, response_json=response_json)

    def _session_factory(*args, **kwargs):
        return fake_session

    monkeypatch.setattr(tushare_api_module.aiohttp, "ClientSession", _session_factory)

    with pytest.raises(aiohttp.client_exceptions.ServerDisconnectedError):
        await api.query(
            api_name="eco_cal",
            fields="date",
            max_retries=2,
            start_date="20260111",
            end_date="20260122",
            limit=5000,
        )

    assert fake_session.post_calls == 2


@pytest.mark.asyncio
async def test_tushare_api_does_not_split_generic_50101(monkeypatch):
    api = TushareAPI(token="test", logger=logging.getLogger("test"))

    async def _no_wait(_api_name: str):
        return None

    monkeypatch.setattr(api, "_wait_for_rate_limit_slot", _no_wait)

    split_called = False

    async def _unexpected_split(**kwargs):
        nonlocal split_called
        split_called = True
        return pd.DataFrame()

    monkeypatch.setattr(api, "_handle_offset_limit_error", _unexpected_split)

    response_json = {
        "code": 50101,
        "msg": "查询数据失败，请确认参数！可以反馈管理员协助您排查问题",
    }
    fake_session = _FakeClientSession(fail_times=0, response_json=response_json)

    def _session_factory(*args, **kwargs):
        return fake_session

    monkeypatch.setattr(tushare_api_module.aiohttp, "ClientSession", _session_factory)

    with pytest.raises(ValueError, match="Tushare API 返回错误"):
        await api.query(
            api_name="moneyflow_hsgt",
            fields="trade_date",
            max_retries=1,
            trade_date="20191118",
            limit=300,
        )

    assert split_called is False


@pytest.mark.asyncio
async def test_tushare_api_splits_offset_limit_50101(monkeypatch):
    api = TushareAPI(token="test", logger=logging.getLogger("test"))

    async def _no_wait(_api_name: str):
        return None

    monkeypatch.setattr(api, "_wait_for_rate_limit_slot", _no_wait)

    captured = {}

    async def _fake_split(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"trade_date": ["20240506"]})

    monkeypatch.setattr(api, "_handle_offset_limit_error", _fake_split)

    response_json = {
        "code": 50101,
        "msg": "offset不能大于100000",
    }
    fake_session = _FakeClientSession(fail_times=0, response_json=response_json)

    def _session_factory(*args, **kwargs):
        return fake_session

    monkeypatch.setattr(tushare_api_module.aiohttp, "ClientSession", _session_factory)

    df = await api.query(
        api_name="daily",
        fields="trade_date",
        max_retries=1,
        start_date="20240101",
        end_date="20240531",
        limit=5000,
    )

    assert df.iloc[0]["trade_date"] == "20240506"
    assert captured["api_name"] == "daily"
    assert captured["start_date"] == "20240101"
    assert captured["end_date"] == "20240531"


@pytest.mark.asyncio
async def test_offset_split_raises_on_failed_subrange(monkeypatch):
    api = TushareAPI(token="test", logger=logging.getLogger("test"))

    monkeypatch.setattr(
        api,
        "_split_date_range",
        lambda start_date, end_date: [
            {"start_date": "20240101", "end_date": "20240115"},
            {"start_date": "20240116", "end_date": "20240131"},
        ],
    )

    async def _fetch_subrange(**kwargs):
        if kwargs["start_date"] == "20240101":
            raise ValueError("subrange boom")
        return pd.DataFrame({"trade_date": [kwargs["start_date"]]})

    monkeypatch.setattr(api, "_fetch_with_pagination", _fetch_subrange)

    with pytest.raises(RuntimeError, match="子批次 1/2"):
        await api._handle_offset_limit_error(
            api_name="daily",
            fields="trade_date",
            max_retries=1,
            stop_event=None,
            start_date="20240101",
            end_date="20240131",
            limit=5000,
        )
