import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
import psycopg2

from alphahome.common.config_manager import ConfigManager
from alphahome.pit.base.pit_table_manager import PITTableManager
from alphahome.pit.tasks.financials import PITIncomeQuarterlyTask


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


@pytest.mark.asyncio
async def test_pit_worker_uses_injected_target_and_closes_session(monkeypatch, isolated_database_url):
    observed = {}

    def forbidden_default(*args, **kwargs):
        raise AssertionError("PIT task consulted the default target")

    monkeypatch.setattr(ConfigManager, "get_database_url", forbidden_default)

    class Probe(PITTableManager):
        def __init__(self):
            super().__init__("pit_income_quarterly")
            observed["construct_thread"] = threading.get_ident()

        def incremental_update(self, **kwargs):
            observed["run_thread"] = threading.get_ident()
            row = self.context.db_manager.fetch_one_sync(
                "SELECT current_database() AS db, pg_backend_pid() AS pid"
            )
            observed.update(dict(row))
            observed["context"] = self.context
            return {"updated_records": 0}

        full_backfill = incremental_update

    task = PITIncomeQuarterlyTask(SimpleNamespace(connection_string=isolated_database_url))
    task.contract = replace(task.contract, manager_class=Probe)
    result = await task.execute()
    assert result["status"] == "success"
    assert observed["db"].startswith("alphahome_test_")
    assert observed["construct_thread"] == observed["run_thread"] != threading.get_ident()
    assert observed["context"]._closed
    connection = psycopg2.connect(isolated_database_url)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE pid = %s", (observed["pid"],))
            assert cursor.fetchone()[0] == 0
    finally:
        connection.close()
