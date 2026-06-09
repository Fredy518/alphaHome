import pytest

from alphahome.factors.pipelines import cli as factor_cli
from scripts.production.data_updaters.tushare.data_collection_smart_update_production import (
    DataCollectionProductionUpdater,
)


class _FakeDBManager:
    def __init__(self):
        self.closed = False

    def close_sync(self):
        self.closed = True


class _FakeFactorContext:
    def __init__(self):
        self.db_manager = _FakeDBManager()


def test_factor_missing_dry_run_uses_default_context(monkeypatch, capsys):
    context = _FakeFactorContext()
    seen = {}

    class FakeFactorEngine:
        def __init__(self, config, context=None):
            seen["config"] = config
            seen["context"] = context

        def resolve_dates(self):
            return ["2026-06-05"]

    monkeypatch.setattr(factor_cli, "ensure_factor_context", lambda: context)
    monkeypatch.setattr(factor_cli, "FactorEngine", FakeFactorEngine)

    exit_code = factor_cli.run_missing_factors(
        "2026-06-01",
        "2026-06-08",
        dry_run=True,
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert seen["context"] is context
    assert seen["config"].missing_mode == "batch_missing"
    assert "这是预览模式" in output
    assert context.db_manager.closed is True


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
