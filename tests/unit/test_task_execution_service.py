#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from datetime import date, datetime
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


class _BlockingTask:
    data_source = "akshare"

    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def get_display_name(self) -> str:
        return "blocking_task"

    async def execute(self, stop_event=None, **kwargs):
        self.started.set()
        await self.release.wait()
        return {"status": "success", "rows": 1}


def test_order_tasks_by_dependencies_moves_selected_inputs_before_dependent():
    tasks = [
        {"task_name": "pit_balance_quarterly", "dependencies": []},
        {"task_name": "pit_cashflow_quarterly", "dependencies": []},
        {
            "task_name": "pit_financial_indicators",
            "dependencies": ["pit_income_quarterly", "pit_balance_quarterly"],
        },
        {"task_name": "pit_income_quarterly", "dependencies": []},
        {"task_name": "pit_industry_classification", "dependencies": []},
    ]

    ordered = task_execution_service._order_tasks_by_dependencies(tasks)

    assert [item["task_name"] for item in ordered] == [
        "pit_balance_quarterly",
        "pit_cashflow_quarterly",
        "pit_income_quarterly",
        "pit_financial_indicators",
        "pit_industry_classification",
    ]


def test_pit_batch_cutoff_is_frozen_to_last_complete_month():
    tasks = [{"task_name": "pit_stock_fttm_monthly", "task_type": "pit"}]

    before_midnight = task_execution_service._freeze_pit_month_end_cutoff(
        tasks, datetime(2026, 8, 31, 23, 59, 59)
    )
    after_midnight = task_execution_service._freeze_pit_month_end_cutoff(
        tasks, datetime(2026, 9, 1, 0, 0, 1)
    )

    assert before_midnight == date(2026, 7, 31)
    assert after_midnight == date(2026, 8, 31)
    assert (
        task_execution_service._freeze_pit_month_end_cutoff(
            [
                {
                    "task_name": "pit_income_quarterly",
                    "task_type": "pit",
                    "pit_time_key": "ann_date",
                }
            ],
            datetime(2026, 9, 1, 0, 0, 1),
        )
        is None
    )


@pytest.mark.asyncio
async def test_run_tasks_passes_one_frozen_cutoff_to_every_pit_task(monkeypatch):
    first = _TaskWithoutIncrementalCapabilityMethod()
    second = _TaskWithoutIncrementalCapabilityMethod()
    create_task_instance = AsyncMock(side_effect=[first, second])
    cutoff_references = []

    def freeze_cutoff(tasks_to_run, batch_started_at=None):
        cutoff_references.append(batch_started_at)
        return date(2026, 7, 31)

    monkeypatch.setattr(task_execution_service, "_is_running", False)
    monkeypatch.setattr(
        task_execution_service,
        "_freeze_pit_month_end_cutoff",
        freeze_cutoff,
    )
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
        tasks_to_run=[
            {"task_name": "pit_stock_fttm_monthly", "task_type": "pit"},
            {"task_name": "pit_industry_fttm_monthly", "task_type": "pit"},
        ],
        start_date=None,
        end_date=None,
        exec_mode="智能增量",
    )

    expected_config = {"pit_month_end_cutoff": "2026-07-31"}
    assert len(cutoff_references) == 1
    assert isinstance(cutoff_references[0], datetime)
    assert create_task_instance.await_args_list[0].kwargs["task_config"] == expected_config
    assert create_task_instance.await_args_list[1].kwargs["task_config"] == expected_config


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


@pytest.mark.asyncio
async def test_run_tasks_ignores_concurrent_run_request(monkeypatch):
    task = _BlockingTask()
    create_task_instance = AsyncMock(return_value=task)

    monkeypatch.setattr(task_execution_service, "_is_running", False)
    monkeypatch.setattr(task_execution_service, "_send_response_callback", None)
    monkeypatch.setattr(task_execution_service, "_ensure_task_status_table_exists", AsyncMock())
    monkeypatch.setattr(task_execution_service, "_record_task_status", AsyncMock())
    monkeypatch.setattr(task_execution_service, "get_all_task_status", AsyncMock())
    monkeypatch.setattr(
        task_execution_service.UnifiedTaskFactory,
        "create_task_instance",
        create_task_instance,
    )

    first_run = asyncio.create_task(
        task_execution_service.run_tasks(
            db_manager=object(),
            tasks_to_run=[{"task_name": "blocking_task"}],
            start_date=None,
            end_date=None,
            exec_mode="智能增量",
        )
    )
    await task.started.wait()

    await task_execution_service.run_tasks(
        db_manager=object(),
        tasks_to_run=[{"task_name": "second_task"}],
        start_date=None,
        end_date=None,
        exec_mode="智能增量",
    )

    assert create_task_instance.await_count == 1

    task.release.set()
    await first_run


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


@pytest.mark.asyncio
async def test_run_tasks_records_error_when_task_factory_fails(monkeypatch):
    db_manager = object()
    create_task_instance = AsyncMock(side_effect=RuntimeError("factory boom"))
    record_task_status = AsyncMock()

    monkeypatch.setattr(task_execution_service, "_ensure_task_status_table_exists", AsyncMock())
    monkeypatch.setattr(task_execution_service, "_record_task_status", record_task_status)
    monkeypatch.setattr(task_execution_service, "get_all_task_status", AsyncMock())
    monkeypatch.setattr(
        task_execution_service.UnifiedTaskFactory,
        "create_task_instance",
        create_task_instance,
    )

    await task_execution_service.run_tasks(
        db_manager=db_manager,
        tasks_to_run=[{"task_name": "broken_task"}],
        start_date=None,
        end_date=None,
        exec_mode="智能增量",
    )

    record_task_status.assert_any_await(
        db_manager,
        "broken_task",
        "error",
        "任务实例创建失败: factory boom",
    )
