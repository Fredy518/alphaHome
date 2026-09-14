import pytest

from alphahome.factors.pipelines import cli as factor_cli
from scripts.production.data_updaters.tushare.data_collection_smart_update_production import (
    DataCollectionProductionUpdater,
)


def test_factor_missing_dry_run_uses_governed_coordinator(monkeypatch, capsys):
    seen = {}

    def governed_operation(factor_types, mode, start_date, end_date, **kwargs):
        seen.update(
            {
                "factor_types": factor_types,
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                **kwargs,
            }
        )
        return {
            "status": "ready",
            "task_plans": [{"dates": ["2026-06-05"]}],
        }

    monkeypatch.setattr(factor_cli, "_governed_operation", governed_operation)

    exit_code = factor_cli.run_missing_factors(
        "2026-06-01",
        "2026-06-08",
        dry_run=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert seen["factor_types"] == ("p", "g")
    assert seen["mode"] == "smart"
    assert seen["dry_run"] is True
    assert "这是预览模式" in output


@pytest.mark.asyncio
async def test_data_collection_dry_run_skips_are_success(monkeypatch):
    updater = DataCollectionProductionUpdater(max_workers=1, dry_run=True)

    async def initialize():
        return True

    async def get_fetch_tasks():
        return ["task_a", "task_b"]

    async def execute_tasks_parallel(task_names):
        return [
            {"task_name": task_name, "status": "skipped_dry_run"}
            for task_name in task_names
        ]

    monkeypatch.setattr(updater, "initialize", initialize)
    monkeypatch.setattr(updater, "get_fetch_tasks", get_fetch_tasks)
    monkeypatch.setattr(updater, "execute_tasks_parallel", execute_tasks_parallel)
    monkeypatch.setattr(updater, "print_execution_summary", lambda results: None)

    assert await updater.run_production_update() is True
    assert updater.stats["total_tasks"] == 2
    assert updater.stats["successful_tasks"] == 0
    assert updater.stats["failed_tasks"] == 0
    assert updater.stats["skipped_tasks"] == 2
