from __future__ import annotations

import asyncio

import pytest

from alphahome.pit.base.pit_task import PITTaskContract
from alphahome.pit.pit_data_update_production import PITDataUpdateCoordinator


class _Manager:
    pass


def _contract(task_name, dependencies=()):
    return PITTaskContract(
        task_name=task_name,
        domain="test",
        source_tables=(),
        output_table=f"pit.{task_name}",
        pit_time_key="obs_date",
        primary_keys=("obs_date",),
        dependencies=dependencies,
        supported_modes=("incremental",),
        manager_class=_Manager,
    )


def _fttm_contracts():
    return {
        "pit_stock_fttm_monthly": _contract("pit_stock_fttm_monthly"),
        "pit_industry_classification": _contract("pit_industry_classification"),
        "pit_industry_fttm_monthly": _contract(
            "pit_industry_fttm_monthly",
            ("pit_stock_fttm_monthly", "pit_industry_classification"),
        ),
    }


def test_explicit_industry_target_expands_transitive_dependencies_into_layers():
    contracts = _fttm_contracts()
    selected = PITDataUpdateCoordinator._expand_dependency_closure(
        ["pit_industry_fttm_monthly"], contracts
    )

    layers = PITDataUpdateCoordinator._topological_layers(selected, contracts)

    assert selected == set(contracts)
    assert layers == [
        ["pit_industry_classification", "pit_stock_fttm_monthly"],
        ["pit_industry_fttm_monthly"],
    ]


def test_dependency_cycle_is_rejected():
    contracts = {
        "a": _contract("a", ("b",)),
        "b": _contract("b", ("a",)),
    }

    with pytest.raises(ValueError, match="循环依赖"):
        PITDataUpdateCoordinator._topological_layers(set(contracts), contracts)


def test_explicit_index_target_expands_stock_dependency_only():
    contracts = {
        "pit_stock_fttm_monthly": _contract("pit_stock_fttm_monthly"),
        "pit_index_fttm_monthly": _contract(
            "pit_index_fttm_monthly", ("pit_stock_fttm_monthly",)
        ),
    }

    selected = PITDataUpdateCoordinator._expand_dependency_closure(
        ["pit_index_fttm_monthly"], contracts
    )
    layers = PITDataUpdateCoordinator._topological_layers(selected, contracts)

    assert layers == [
        ["pit_stock_fttm_monthly"],
        ["pit_index_fttm_monthly"],
    ]


def test_explicit_fapi_target_expands_stock_and_classification_dependencies():
    contracts = _fttm_contracts()
    contracts["pit_industry_fapi_monthly"] = _contract(
        "pit_industry_fapi_monthly",
        ("pit_stock_fttm_monthly", "pit_industry_classification"),
    )

    selected = PITDataUpdateCoordinator._expand_dependency_closure(
        ["pit_industry_fapi_monthly"], contracts
    )
    layers = PITDataUpdateCoordinator._topological_layers(selected, contracts)

    assert selected == {
        "pit_stock_fttm_monthly",
        "pit_industry_classification",
        "pit_industry_fapi_monthly",
    }
    assert layers == [
        ["pit_industry_classification", "pit_stock_fttm_monthly"],
        ["pit_industry_fapi_monthly"],
    ]


def test_earnings_surprise_expands_income_and_consensus_dependencies():
    contracts = {
        "pit_income_quarterly": _contract("pit_income_quarterly"),
        "pit_stock_consensus_fy_monthly": _contract(
            "pit_stock_consensus_fy_monthly"
        ),
        "pit_earnings_surprise_annual": _contract(
            "pit_earnings_surprise_annual",
            ("pit_income_quarterly", "pit_stock_consensus_fy_monthly"),
        ),
    }

    selected = PITDataUpdateCoordinator._expand_dependency_closure(
        ["pit_earnings_surprise_annual"], contracts
    )
    layers = PITDataUpdateCoordinator._topological_layers(selected, contracts)

    assert layers == [
        ["pit_income_quarterly", "pit_stock_consensus_fy_monthly"],
        ["pit_earnings_surprise_annual"],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("parallel", [False, True])
async def test_serial_and_parallel_execute_only_within_topological_layer(
    monkeypatch, parallel
):
    coordinator = PITDataUpdateCoordinator(max_workers=2)
    contracts = _fttm_contracts()
    calls = []
    upstream_done = set()

    monkeypatch.setattr(coordinator, "_registered_contracts", lambda: contracts)

    async def _run_task(task_name, target, update_type, task_config=None):
        calls.append(task_name)
        if task_name == "pit_industry_fttm_monthly":
            assert upstream_done == {
                "pit_stock_fttm_monthly",
                "pit_industry_classification",
            }
        await asyncio.sleep(0)
        upstream_done.add(task_name)
        return {"status": "success", "task": task_name, "target": target}

    monkeypatch.setattr(coordinator, "_run_task", _run_task)

    results = await coordinator.run_updates(
        ["industry_fttm"], mode="incremental", parallel=parallel
    )

    assert [result["task"] for result in results] == [
        "pit_industry_classification",
        "pit_stock_fttm_monthly",
        "pit_industry_fttm_monthly",
    ]
    assert calls[-1] == "pit_industry_fttm_monthly"


@pytest.mark.asyncio
async def test_upstream_failure_marks_industry_skipped(monkeypatch):
    coordinator = PITDataUpdateCoordinator()
    contracts = _fttm_contracts()
    calls = []
    monkeypatch.setattr(coordinator, "_registered_contracts", lambda: contracts)

    async def _run_task(task_name, target, update_type, task_config=None):
        calls.append(task_name)
        status = "error" if task_name == "pit_stock_fttm_monthly" else "success"
        return {"status": status, "task": task_name, "target": target}

    monkeypatch.setattr(coordinator, "_run_task", _run_task)

    results = await coordinator.run_updates(
        ["industry_fttm"], mode="incremental", parallel=True
    )
    industry = next(
        result for result in results if result["task"] == "pit_industry_fttm_monthly"
    )

    assert industry["status"] == "skipped_dependency_failed"
    assert industry["failed_dependencies"] == ["pit_stock_fttm_monthly"]
    assert "pit_industry_fttm_monthly" not in calls
