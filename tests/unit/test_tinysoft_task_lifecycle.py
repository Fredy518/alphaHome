import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from alphahome.fetchers.base.fetcher_task import FetcherTask
from alphahome.fetchers.sources.tinysoft.tinysoft_task import TinySoftTask


class _LifecycleTinySoftTask(TinySoftTask):
    name = "test_tinysoft_task_lifecycle"
    table_name = "test_tinysoft_task_lifecycle"

    async def get_batch_list(self, **kwargs):
        return []


class _CloseAPI:
    def __init__(self, *, error=None):
        self.close_calls = 0
        self.error = error

    async def close(self):
        self.close_calls += 1
        if self.error is not None:
            raise self.error


class _LogoutAPI:
    def __init__(self):
        self.logout_calls = 0

    async def logout(self):
        self.logout_calls += 1


@pytest.mark.asyncio
async def test_execute_closes_api_after_success(monkeypatch):
    result = {"status": "success", "rows": 1}
    parent_execute = AsyncMock(return_value=result)
    monkeypatch.setattr(FetcherTask, "execute", parent_execute)
    api = _CloseAPI()
    task = _LifecycleTinySoftTask(db_connection=object(), api=api)

    actual = await task.execute()

    assert actual is result
    assert api.close_calls == 1


@pytest.mark.asyncio
async def test_execute_closes_api_after_error(monkeypatch):
    parent_execute = AsyncMock(side_effect=RuntimeError("fetch failed"))
    monkeypatch.setattr(FetcherTask, "execute", parent_execute)
    api = _CloseAPI()
    task = _LifecycleTinySoftTask(db_connection=object(), api=api)

    with pytest.raises(RuntimeError, match="fetch failed"):
        await task.execute()

    assert api.close_calls == 1


@pytest.mark.asyncio
async def test_execute_closes_api_after_cancellation(monkeypatch):
    parent_execute = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr(FetcherTask, "execute", parent_execute)
    api = _CloseAPI()
    task = _LifecycleTinySoftTask(db_connection=object(), api=api)

    with pytest.raises(asyncio.CancelledError):
        await task.execute()

    assert api.close_calls == 1


@pytest.mark.asyncio
async def test_execute_falls_back_to_logout(monkeypatch):
    result = {"status": "success"}
    monkeypatch.setattr(FetcherTask, "execute", AsyncMock(return_value=result))
    api = _LogoutAPI()
    task = _LifecycleTinySoftTask(db_connection=object(), api=api)

    actual = await task.execute()

    assert actual is result
    assert api.logout_calls == 1


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_replace_task_result(monkeypatch, caplog):
    result = {"status": "success"}
    monkeypatch.setattr(FetcherTask, "execute", AsyncMock(return_value=result))
    api = _CloseAPI(error=RuntimeError("close failed"))
    task = _LifecycleTinySoftTask(db_connection=object(), api=api)

    with caplog.at_level(logging.WARNING):
        actual = await task.execute()

    assert actual is result
    assert api.close_calls == 1
    assert "关闭 Tinysoft API 资源失败: close failed" in caplog.text
