#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest.mock import AsyncMock

import pytest

from alphahome.common.constants import UpdateTypes
from alphahome.gui.services import task_execution_service


class _TaskWithoutIncrementalCapabilityMethod:
    data_source = "akshare"

    def __init__(self):
        self.executed = False

    def get_display_name(self) -> str:
        return "macro_release_calendar"

    async def execute(self, stop_event=None, **kwargs):
        self.executed = True
        return {"status": "success", "rows": 1}


@pytest.mark.asyncio
async def test_run_tasks_handles_task_without_incremental_capability_method(monkeypatch):
    task = _TaskWithoutIncrementalCapabilityMethod()
    create_task_instance = AsyncMock(return_value=task)

    monkeypatch.setattr(task_execution_service, "_ensure_task_status_table_exists", AsyncMock())
    monkeypatch.setattr(task_execution_service, "_record_task_status", AsyncMock())
    monkeypatch.setattr(task_execution_service, "get_all_task_status", AsyncMock())
    monkeypatch.setattr(
        task_execution_service.UnifiedTaskFactory,
        "create_task_instance",
        create_task_instance,
    )

    await task_execution_service.run_tasks(
        db_manager=object(),
        tasks_to_run=[{"task_name": "macro_release_calendar"}],
        start_date=None,
        end_date=None,
        exec_mode="智能增量",
    )

    create_task_instance.assert_awaited_once_with(
        "macro_release_calendar",
        update_type=UpdateTypes.SMART,
        use_insert_mode=False,
    )
    assert task.executed is True


class _TaskWithoutIncrementalSupport:
    data_source = "akshare"

    def __init__(self):
        self.executed = False
        self.update_type = UpdateTypes.SMART

    def supports_incremental_update(self) -> bool:
        return False

    def get_incremental_skip_reason(self) -> str:
        return "unit test fallback"

    def get_display_name(self) -> str:
        return "non_incremental_task"

    async def execute(self, stop_event=None, **kwargs):
        self.executed = True
        return {"status": "success", "rows": 1}


@pytest.mark.asyncio
async def test_run_tasks_switches_unsupported_smart_task_to_full(monkeypatch):
    task = _TaskWithoutIncrementalSupport()
    create_task_instance = AsyncMock(return_value=task)

    monkeypatch.setattr(task_execution_service, "_ensure_task_status_table_exists", AsyncMock())
    monkeypatch.setattr(task_execution_service, "_record_task_status", AsyncMock())
    monkeypatch.setattr(task_execution_service, "get_all_task_status", AsyncMock())
    monkeypatch.setattr(
        task_execution_service.UnifiedTaskFactory,
        "create_task_instance",
        create_task_instance,
    )

    await task_execution_service.run_tasks(
        db_manager=object(),
        tasks_to_run=[{"task_name": "non_incremental_task"}],
        start_date=None,
        end_date=None,
        exec_mode="智能增量",
    )

    assert task.executed is True
    assert task.update_type == UpdateTypes.FULL
