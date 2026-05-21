from __future__ import annotations

import time

import pandas as pd
import pytest

from tinydata.client import TinyClient
from tinydata.config import TinyDataConfig
from tinydata.errors import TinyDataAuthError, TinyDataTimeoutError


class FakeResult:
    def __init__(self, error=0, message="ok", df=None, value=None):
        self._error = error
        self._message = message
        self._df = df if df is not None else pd.DataFrame({"x": [1]})
        self._value = value

    def error(self):
        return self._error

    def message(self):
        return self._message

    def dataframe(self):
        return self._df

    def value(self):
        return self._value


class FakePyTSLClient:
    login_result = 1
    sleep_seconds = 0

    def __init__(self, *args):
        self.args = args
        self.logged_in = False

    def is_logined(self):
        return 1 if self.logged_in else 0

    def login(self):
        if self.login_result == 1:
            self.logged_in = True
        return self.login_result

    def last_error(self):
        return "bad login"

    def exec(self, code):
        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)
        return FakeResult(df=pd.DataFrame({"code": [code]}))


class FakePyTSLModule:
    Client = FakePyTSLClient


def test_client_exec_success():
    FakePyTSLClient.login_result = 1
    FakePyTSLClient.sleep_seconds = 0
    client = TinyClient(
        TinyDataConfig(user="u", password="p", request_interval=0, timeout_ms=1000),
        pytsl_module=FakePyTSLModule,
    )

    df = client.exec("return 1;")
    assert df.loc[0, "code"] == "return 1;"


def test_client_login_failure():
    FakePyTSLClient.login_result = 0
    client = TinyClient(
        TinyDataConfig(user="u", password="p", request_interval=0, timeout_ms=1000),
        pytsl_module=FakePyTSLModule,
    )

    with pytest.raises(TinyDataAuthError):
        client.exec("return 1;")
    FakePyTSLClient.login_result = 1


def test_client_timeout_discards_client():
    FakePyTSLClient.login_result = 1
    FakePyTSLClient.sleep_seconds = 0.05
    client = TinyClient(
        TinyDataConfig(user="u", password="p", request_interval=0, timeout_ms=1),
        pytsl_module=FakePyTSLModule,
    )

    with pytest.raises(TinyDataTimeoutError):
        client.exec("return slow;", timeout_ms=1)
    assert client._client is None
    FakePyTSLClient.sleep_seconds = 0
