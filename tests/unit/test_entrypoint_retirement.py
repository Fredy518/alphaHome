from unittest.mock import AsyncMock

import pytest

from alphahome.cli.main import main
from alphahome.cli.core import exitcodes
from scripts.production.data_updaters.tushare import data_collection_smart_update_production as collector


@pytest.mark.parametrize("args", [["prod", "list"], ["mv", "status", "x"], ["mv", "refresh", "x"], ["gui"]])
def test_retired_commands_never_report_business_success(args, capsys):
    assert main(args) == exitcodes.UNAVAILABLE
    assert "已下线" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_collector_dry_run_never_reads_config_or_initializes_database(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Dry-run must not touch configuration/database")

    monkeypatch.setattr(collector, "get_database_url", forbidden)
    initialize = AsyncMock(side_effect=forbidden)
    monkeypatch.setattr(collector.UnifiedTaskFactory, "initialize", initialize)
    updater = collector.DataCollectionProductionUpdater(dry_run=True)
    try:
        assert await updater.initialize()
        names = await updater.get_fetch_tasks()
        assert names
        assert updater.db_manager is None
        initialize.assert_not_awaited()
    finally:
        updater.executor.shutdown(wait=True)


def test_legacy_features_help_does_not_initialize(monkeypatch):
    from scripts import initialize_materialized_views as legacy
    initialize = AsyncMock(side_effect=AssertionError("help must not execute"))
    monkeypatch.setattr(legacy, "initialize_features_views", initialize)
    with pytest.raises(SystemExit) as result:
        legacy.main(["--help"])
    assert result.value.code == 0
    initialize.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("exists", [True, False])
async def test_features_schema_check_exit_matches_structure(monkeypatch, exists):
    from types import SimpleNamespace
    from scripts import features_init
    db = SimpleNamespace(connect=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(features_init, "get_database_url", lambda: "postgresql://example@127.0.0.1:65432/alphahome_test_fixture")
    monkeypatch.setattr(features_init, "DBManager", lambda url: db)
    monkeypatch.setattr(features_init, "check_initialization_status", AsyncMock(return_value={
        "schema_exists": exists, "mv_metadata_exists": exists, "mv_refresh_log_exists": exists,
        "views": [], "view_count": 0,
    }))
    assert await features_init.main(SimpleNamespace(check=True, create_views=False)) == (0 if exists else 1)
    db.close.assert_awaited_once()
