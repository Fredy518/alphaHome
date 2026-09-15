from __future__ import annotations

import asyncio
import inspect
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from alphahome.gui.services import daily_update_service as service


class CalendarDB:
    def __init__(self, is_open=1, *, fail=False):
        self.is_open = is_open
        self.fail = fail
        self.calls = 0

    async def fetch_one(self, *_args):
        self.calls += 1
        if self.fail:
            raise RuntimeError("calendar unavailable")
        return {"is_open": self.is_open}


class DailyFetch:
    smart_refresh_interval_days = 1


class SlowFetch:
    smart_refresh_interval_days = 7


class HiddenFetch:
    hide_from_gui_collection = True


class DailyPit:
    contract = SimpleNamespace(pit_time_key="ann_date", dependencies=())


class MonthlyPit:
    contract = SimpleNamespace(pit_time_key="obs_date", dependencies=())


class DailyPitWithMonthlyDependency:
    contract = SimpleNamespace(
        pit_time_key="ann_date",
        dependencies=("pit_monthly",),
    )


class DailyFeature:
    name = "daily_prices"
    is_python_feature = False
    supported_strategies = ("incremental",)
    source_tables = ()


class MonthlyFeature:
    name = "market_snapshot_monthly"
    is_python_feature = False
    supported_strategies = ("incremental",)
    source_tables = ()


class FullOnlyFeature:
    name = "full_only_python"
    is_python_feature = True
    supported_strategies = ("full",)
    source_tables = ()


class DailyFeatureWithMonthlyDependency:
    name = "daily_with_monthly_dependency"
    is_python_feature = False
    supported_strategies = ("incremental",)
    source_tables = ("features.market_snapshot_monthly",)


def test_collection_updater_loads_without_scripts_package(monkeypatch, tmp_path):
    """The installed GUI must not depend on importing the top-level scripts name."""

    service._load_collection_updater_class.cache_clear()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setitem(sys.modules, "scripts", SimpleNamespace(__path__=[]))

    updater_class = service._load_collection_updater_class()

    source_path = Path(inspect.getsourcefile(updater_class) or "").resolve()
    assert updater_class.__name__ == "DataCollectionProductionUpdater"
    assert source_path.name == "data_collection_smart_update_production.py"
    assert source_path.is_file()
    service._load_collection_updater_class.cache_clear()


@pytest.fixture
def discovered_tasks(monkeypatch):
    monkeypatch.setattr(service.fetcher_tasks, "discover_tasks", lambda: None)
    monkeypatch.setattr(service.pit_tasks, "discover_tasks", lambda: None)
    monkeypatch.setattr(service.factor_tasks, "discover_tasks", lambda: None)
    monkeypatch.setattr(
        service.FeatureRegistry,
        "discover",
        lambda: [
            DailyFeature,
            MonthlyFeature,
            FullOnlyFeature,
            DailyFeatureWithMonthlyDependency,
        ],
    )

    def get_tasks(task_type):
        return {
            "fetch": {
                "fetch_daily": DailyFetch,
                "fetch_manual": HiddenFetch,
                "fetch_weekly": SlowFetch,
            },
            "pit": {
                "pit_daily": DailyPit,
                "pit_daily_dependent": DailyPitWithMonthlyDependency,
                "pit_monthly": MonthlyPit,
            },
            "factor": {"factor_p": object, "factor_g": object},
            "processor": {"legacy_processor": object},
        }[task_type]

    monkeypatch.setattr(service.UnifiedTaskFactory, "get_tasks_by_type", get_tasks)
    monkeypatch.setattr(
        service.fundpos_service,
        "get_fundpos_snapshot",
        lambda: {
            "status": "ready",
            "families": [{"name": "fundpos.enhanced_index"}],
        },
    )


@pytest.mark.asyncio
async def test_workday_plan_skips_low_frequency_tasks(discovered_tasks):
    plan = await service.build_daily_update_plan(CalendarDB(is_open=1), "2026-09-15")
    groups = {group["key"]: group for group in plan["groups"]}

    assert plan["day_type"] == "交易日"
    assert groups["collection"]["task_names"] == ["fetch_daily"]
    assert groups["collection"]["skipped_task_names"] == [
        "fetch_manual",
        "fetch_weekly",
        "legacy_processor",
    ]
    assert groups["collection"]["manual_only_task_names"] == [
        "fetch_manual",
        "legacy_processor",
    ]
    assert groups["pit"]["task_names"] == ["pit_daily"]
    assert groups["pit"]["skipped_task_names"] == [
        "pit_daily_dependent",
        "pit_monthly",
    ]
    assert groups["factors"]["status"] == "skipped_policy"
    assert groups["features"]["task_names"] == ["daily_prices"]
    assert groups["features"]["skipped_task_names"] == [
        "daily_with_monthly_dependency",
        "full_only_python",
        "market_snapshot_monthly",
    ]
    assert groups["fundpos"]["task_names"] == ["fundpos.enhanced_index"]


@pytest.mark.asyncio
async def test_weekend_plan_includes_low_frequency_tasks(discovered_tasks):
    db = CalendarDB(is_open=1)
    plan = await service.build_daily_update_plan(db, "2026-09-13")

    assert plan["day_type"] == "非交易日"
    assert db.calls == 0
    collection = next(group for group in plan["groups"] if group["key"] == "collection")
    assert collection["task_names"] == ["fetch_daily", "fetch_weekly"]
    assert collection["skipped_task_names"] == ["fetch_manual", "legacy_processor"]
    assert all(
        not group["skipped_task_names"]
        for group in plan["groups"]
        if group["key"] != "collection"
    )
    assert sum(group["run_count"] for group in plan["groups"]) == 12


@pytest.mark.asyncio
async def test_calendar_failure_fails_closed_as_workday(discovered_tasks):
    plan = await service.build_daily_update_plan(
        CalendarDB(fail=True),
        "2026-09-15",
    )

    assert plan["is_workday"] is True
    assert "查询失败" in plan["calendar_source"]
    factors = next(group for group in plan["groups"] if group["key"] == "factors")
    assert factors["status"] == "skipped_policy"


@pytest.mark.asyncio
async def test_blocked_group_makes_overall_result_partial(monkeypatch):
    plan = {
        "status": "ready_with_blockers",
        "as_of_date": "2026-09-15",
        "day_type": "交易日",
        "policy_hash": "policy",
        "groups": [
            {
                "key": "collection",
                "label": "数据采集",
                "order": 1,
                "status": "ready",
                "task_names": ["a"],
                "skipped_task_names": [],
                "run_count": 1,
                "skip_count": 0,
            },
            {
                "key": "fundpos",
                "label": "FundPos 估算",
                "order": 5,
                "status": "blocked",
                "task_names": [],
                "skipped_task_names": [],
                "run_count": 0,
                "skip_count": 0,
                "action": "配置阻断",
            },
        ],
    }
    monkeypatch.setattr(service, "build_daily_update_plan", lambda *_args: _async(plan))
    monkeypatch.setattr(
        service,
        "_execute_group",
        lambda *_args: _async({"status": "success", "success_count": 1}),
    )
    monkeypatch.setattr(service, "_send_response_callback", lambda *_args: None)
    service._is_running = False

    result = await service.handle_run_daily_update(object(), "2026-09-15")

    assert result["status"] == "partial_success"
    assert result["completed_groups"] == 1
    assert result["failed_groups"] == 1


@pytest.mark.asyncio
async def test_stop_during_group_is_cancelled_not_failed(monkeypatch):
    events = []
    plan = {
        "status": "ready",
        "as_of_date": "2026-09-15",
        "day_type": "交易日",
        "policy_hash": "policy",
        "groups": [
            {
                "key": "features",
                "label": "Features",
                "order": 4,
                "status": "ready",
                "task_names": ["done", "not_started"],
                "skipped_task_names": [],
                "run_count": 2,
                "skip_count": 0,
            },
            {
                "key": "fundpos",
                "label": "FundPos 估算",
                "order": 5,
                "status": "ready",
                "task_names": ["fundpos.enhanced_index"],
                "skipped_task_names": [],
                "run_count": 1,
                "skip_count": 0,
            },
        ],
    }

    async def execute_group(_group, _db, stop_event, _day):
        stop_event.set()
        return {
            "status": "cancelled",
            "success_count": 1,
            "fail_count": 0,
            "cancelled_count": 1,
        }

    monkeypatch.setattr(service, "build_daily_update_plan", lambda *_args: _async(plan))
    monkeypatch.setattr(service, "_execute_group", execute_group)
    monkeypatch.setattr(service, "_send_response_callback", lambda *args: events.append(args))
    service._is_running = False

    result = await service.handle_run_daily_update(object(), "2026-09-15")

    assert result["status"] == "cancelled"
    assert result["failed_groups"] == 0
    assert plan["groups"][0]["status"] == "cancelled"
    assert "失败" not in plan["groups"][0]["action"]
    assert plan["groups"][1]["status"] == "cancelled"
    assert events[-1][0] == "DAILY_UPDATE_COMPLETE"


@pytest.mark.asyncio
async def test_failed_upstream_blocks_downstream_domains(monkeypatch):
    calls = []
    plan = {
        "status": "ready",
        "as_of_date": "2026-09-15",
        "day_type": "交易日",
        "policy_hash": "policy",
        "groups": [
            {
                "key": key,
                "label": label,
                "order": order,
                "status": "ready",
                "task_names": [key],
                "skipped_task_names": [],
                "run_count": 1,
                "skip_count": 0,
            }
            for order, (key, label) in enumerate(
                (("collection", "数据采集"), ("pit", "PIT"), ("features", "Features")),
                start=1,
            )
        ],
    }

    async def execute_group(group, *_args):
        calls.append(group["key"])
        return {"status": "error", "error": "source unavailable"}

    monkeypatch.setattr(service, "build_daily_update_plan", lambda *_args: _async(plan))
    monkeypatch.setattr(service, "_execute_group", execute_group)
    monkeypatch.setattr(service, "_send_response_callback", lambda *_args: None)
    service._is_running = False

    result = await service.handle_run_daily_update(object(), "2026-09-15")

    assert result["status"] == "error"
    assert calls == ["collection"]
    assert plan["groups"][0]["status"] == "error"
    assert plan["groups"][1]["status"] == "blocked"
    assert plan["groups"][2]["status"] == "blocked"
    assert plan["groups"][1]["blocked_by"] == ["数据采集"]
    assert "未启动" in plan["groups"][2]["action"]


@pytest.mark.asyncio
async def test_pit_cancelled_tasks_are_not_counted_as_failures(monkeypatch):
    plan = SimpleNamespace(plan_hash="pit-plan", units=[object(), object()])
    monkeypatch.setattr(
        service.pit_service,
        "plan_pit_execution",
        lambda *_args, **_kwargs: _async(plan),
    )
    monkeypatch.setattr(
        service.pit_service,
        "execute_pit_plan",
        lambda *_args, **_kwargs: _async(
            [
                {"task": "completed", "status": "success"},
                {"task": "not_started", "status": "cancelled"},
            ]
        ),
    )

    result = await service._run_pit(
        object(),
        ["completed", "not_started"],
        asyncio.Event(),
        date(2026, 9, 15),
    )

    assert result["status"] == "cancelled"
    assert result["success_count"] == 1
    assert result["fail_count"] == 0
    assert result["cancelled_count"] == 1


@pytest.mark.asyncio
async def test_plan_failure_restores_running_state_and_notifies_gui(monkeypatch):
    events = []

    async def fail_plan(*_args):
        raise RuntimeError("plan unavailable")

    monkeypatch.setattr(service, "build_daily_update_plan", fail_plan)
    monkeypatch.setattr(service, "_send_response_callback", lambda *args: events.append(args))
    service._is_running = False

    result = await service.handle_run_daily_update(object(), "2026-09-15")

    assert result["status"] == "error"
    assert service.is_daily_update_running() is False
    assert events[-1][0] == "DAILY_UPDATE_COMPLETE"
    assert events[-1][1]["result"]["error"] == "plan unavailable"


async def _async(value):
    return value
