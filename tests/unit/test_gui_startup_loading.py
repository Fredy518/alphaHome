import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from alphahome.gui import controller, main_window


@pytest.mark.asyncio
async def test_initial_async_load_starts_independent_lists_concurrently(monkeypatch):
    commands = {
        "GET_COLLECTION_TASKS",
        "GET_PIT_TASKS",
        "GET_FEATURES",
        "GET_STORAGE_SETTINGS",
    }
    started = set()
    release = asyncio.Event()

    async def _initialize(response_callback):
        return None

    async def _handle_request(command, data=None):
        started.add(command)
        await release.wait()

    monkeypatch.setattr(controller, "initialize_controller", _initialize)
    monkeypatch.setattr(controller, "handle_request", _handle_request)
    window = SimpleNamespace(handle_controller_response=lambda *_: None)

    load_task = asyncio.create_task(main_window.MainWindow.initial_async_load(window))
    await asyncio.sleep(0.05)
    observed_before_release = set(started)
    release.set()
    await load_task

    assert observed_before_release == commands


@pytest.mark.asyncio
async def test_factory_connection_is_reused_when_already_initialized(monkeypatch):
    db_manager = object()
    initialize = AsyncMock()
    monkeypatch.setattr(
        controller.UnifiedTaskFactory,
        "get_db_manager",
        Mock(return_value=db_manager),
    )
    monkeypatch.setattr(controller.UnifiedTaskFactory, "initialize", initialize)

    result = await controller._get_or_initialize_db_manager()

    assert result is db_manager
    initialize.assert_not_awaited()


@pytest.mark.asyncio
async def test_factory_connection_is_initialized_when_missing(monkeypatch):
    db_manager = object()
    get_db_manager = Mock(
        side_effect=[RuntimeError("factory not initialized"), db_manager]
    )
    initialize = AsyncMock()
    monkeypatch.setattr(
        controller.UnifiedTaskFactory,
        "get_db_manager",
        get_db_manager,
    )
    monkeypatch.setattr(controller.UnifiedTaskFactory, "initialize", initialize)

    result = await controller._get_or_initialize_db_manager()

    assert result is db_manager
    initialize.assert_awaited_once_with()
