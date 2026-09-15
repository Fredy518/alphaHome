from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scripts.production.data_updaters.tushare import data_collection_smart_update_production as production


@pytest.fixture
def updater():
    value = production.DataCollectionProductionUpdater(max_workers=1)
    yield value
    value.executor.shutdown(wait=True)


@pytest.mark.parametrize("status", ["failed", "error", "partial_success", "completed_with_warnings", "skipped", "cancelled", "unknown", "no_data"])
def test_ninety_percent_success_does_not_hide_required_failure(updater, status):
    names = [f"input_{i}" for i in range(10)]
    results = [{"task_name": name, "status": "success"} for name in names]
    results[-1]["status"] = status
    assert not updater.evaluate_batch(names, results)
    assert updater.batch_outcome["blocking_tasks"] == ["input_9"]


def test_optional_failure_is_explicit_and_does_not_block_required_inputs(updater):
    updater.optional_tasks = frozenset({"optional"})
    assert updater.evaluate_batch(["required", "optional"], [
        {"task_name": "required", "status": "success"},
        {"task_name": "optional", "status": "error"},
    ])
    assert updater.batch_outcome["status"] == "completed_with_optional_failures"
    assert updater.batch_outcome["optional_failures"] == ["optional"]
    assert not updater.evaluate_batch(["required", "optional"], [{"task_name": "required", "status": "success"}])


@pytest.mark.parametrize("results", [[], [{"task_name": "a", "status": "success", "result": {"failed_batches": 1}}],
                                   [{"task_name": "a", "status": "expected_no_data"}]])
def test_missing_result_or_unjustified_success_fails(updater, results):
    assert not updater.evaluate_batch(["a"], results)


def test_capability_declared_expected_skip_requires_reason(updater):
    assert updater.evaluate_batch(
        ["full_only"],
        [{
            "task_name": "full_only",
            "status": "expected_skip",
            "result": {
                "status": "expected_skip",
                "reason": "task explicitly supports full refresh only",
            },
        }],
    )
    assert not updater.evaluate_batch(
        ["full_only"],
        [{"task_name": "full_only", "status": "expected_skip"}],
    )


def test_protocol_errors_and_unknown_optional_task_fail_closed(updater):
    with pytest.raises(ValueError, match="重复"):
        updater.evaluate_batch(["a"], [{"task_name": "a"}] * 2)
    updater.optional_tasks = frozenset({"typo"})
    with pytest.raises(ValueError, match="未知"):
        updater.evaluate_batch(["a"], [])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,exit_code", [(True, 1), (False, 0)])
async def test_actual_main_propagates_required_batch_status(monkeypatch, updater, failure, exit_code):
    names = [f"task_{i}" for i in range(10)]
    results = [{"task_name": name, "status": "success"} for name in names]
    if failure:
        results[-1]["status"] = "partial_success"
    updater.initialize = AsyncMock(return_value=True)
    updater.get_fetch_tasks = AsyncMock(return_value=names)
    updater.execute_tasks_parallel = AsyncMock(return_value=results)
    monkeypatch.setattr(production, "DataCollectionProductionUpdater", lambda **kwargs: updater)
    monkeypatch.setattr(production.sys, "argv", ["collector"])
    monkeypatch.setattr(production.UnifiedTaskFactory, "_task_registry", {name: SimpleNamespace(data_source="fixture") for name in names})
    monkeypatch.setattr(production.UnifiedTaskFactory, "get_task_info", lambda name: {})
    with pytest.raises(SystemExit) as result:
        await production.main()
    assert result.value.code == exit_code
    assert updater.stats["failed_tasks"] == int(failure)


@pytest.mark.asyncio
async def test_requested_task_subset_is_preserved(monkeypatch):
    classes = {
        "daily_a": SimpleNamespace(data_source="fixture"),
        "daily_b": SimpleNamespace(data_source="fixture"),
    }
    value = production.DataCollectionProductionUpdater(
        max_workers=1,
        task_names=["daily_b"],
    )
    monkeypatch.setattr(
        production.UnifiedTaskFactory,
        "get_all_task_names",
        lambda: list(classes),
    )
    monkeypatch.setattr(
        production.UnifiedTaskFactory,
        "get_task_info",
        lambda name: {"type": "fetch"},
    )
    monkeypatch.setattr(production.UnifiedTaskFactory, "_task_registry", classes)
    try:
        assert await value.get_fetch_tasks() == ["daily_b"]
    finally:
        value.executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_task_progress_callback_is_emitted_as_each_task_finishes(monkeypatch):
    seen = []
    value = production.DataCollectionProductionUpdater(
        max_workers=1,
        progress_callback=seen.append,
    )
    monkeypatch.setattr(
        production.UnifiedTaskFactory,
        "_task_registry",
        {"daily_a": SimpleNamespace(data_source="fixture")},
    )
    value.execute_task_with_retry = AsyncMock(
        return_value={"task_name": "daily_a", "status": "success"}
    )
    try:
        results = await value.execute_tasks_parallel(["daily_a"])
        assert seen == results
    finally:
        value.executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_cancelled_collection_task_is_not_counted_as_failed(monkeypatch):
    value = production.DataCollectionProductionUpdater(max_workers=1)
    value.initialize = AsyncMock(return_value=True)
    value.get_fetch_tasks = AsyncMock(return_value=["daily_a"])
    value.execute_tasks_parallel = AsyncMock(
        return_value=[{"task_name": "daily_a", "status": "cancelled"}]
    )
    monkeypatch.setattr(
        production.UnifiedTaskFactory,
        "_task_registry",
        {"daily_a": SimpleNamespace(data_source="fixture")},
    )
    try:
        assert await value.run_production_update() is False
        assert value.stats["failed_tasks"] == 0
        assert value.stats["cancelled_tasks"] == 1
    finally:
        value.executor.shutdown(wait=True)
