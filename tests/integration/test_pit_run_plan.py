import asyncio
from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import psycopg2
import pytest

from alphahome.common.db_session import owned_sync_session
from alphahome.common.run_models import RunPlan
from alphahome.pit.pit_data_update_production import PITDataUpdateCoordinator
from alphahome.pit.run_plan import build_pit_plan
from alphahome.pit.schema import render_schema_sql


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


@pytest.fixture
def pit_schema(isolated_database_url):
    connection = psycopg2.connect(isolated_database_url)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regnamespace('pit')")
        assert cursor.fetchone()[0] is None, "Refuse to replace a preexisting PIT schema"
        try:
            cursor.execute(render_schema_sql())
        except BaseException:
            cursor.execute("ROLLBACK")
            connection.close()
            raise
    try:
        yield isolated_database_url, connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA pit CASCADE")
        connection.close()


def test_explicit_schema_matches_every_registered_manager(pit_schema):
    url, _ = pit_schema
    contracts = PITDataUpdateCoordinator._registered_contracts()
    with owned_sync_session(url, readonly=True) as db:
        for contract in contracts.values():
            with contract.resolve_manager_class()().bind_database(db_manager=db) as manager:
                manager._ensure_table_exists()
                manager._require_unique_keys(contract.primary_keys)


async def test_gui_and_cli_share_plan_hash_and_reject_drift(pit_schema, monkeypatch):
    url, connection = pit_schema
    from alphahome.gui.services.pit_service import plan_pit_execution

    contract = PITDataUpdateCoordinator._registered_contracts()["pit_stock_fttm_monthly"]
    contract = replace(contract, source_tables=())
    monkeypatch.setattr(PITDataUpdateCoordinator, "_registered_contracts", staticmethod(lambda: {contract.task_name: contract}))
    manager = SimpleNamespace(connection_string=url)
    coordinator = PITDataUpdateCoordinator(db_manager=manager)
    gui_plan = await plan_pit_execution(manager, [contract.task_name], "incremental", cutoff="2026-09-14")
    cli_plan = await coordinator.plan(["stock_fttm"], "incremental", cutoff="2026-09-14")
    assert gui_plan.plan_hash == cli_plan.plan_hash
    assert RunPlan.from_dict(gui_plan.to_dict()) == gui_plan
    assert gui_plan.units[0].dates[-1] == date(2026,8,31)
    run = AsyncMock()
    coordinator._run_task = run
    with pytest.raises(RuntimeError, match="changed"):
        await coordinator.run_updates(["stock_fttm"], plan=gui_plan, expected_plan_hash="f"*64)
    run.assert_not_awaited()
    with connection.cursor() as cursor:
        cursor.execute("ALTER TABLE pit.pit_stock_fttm_monthly ADD COLUMN refactor_drift integer")
    with pytest.raises(RuntimeError, match="changed"):
        await coordinator.run_updates(["stock_fttm"], plan=gui_plan)
    run.assert_not_awaited()


def test_preview_of_missing_schema_does_not_install_it(isolated_database_url):
    with psycopg2.connect(isolated_database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regnamespace('pit')")
            assert cursor.fetchone()[0] is None
            plan = build_pit_plan(isolated_database_url, ["pit_stock_fttm_monthly"], "incremental", cutoff="2026-09-14")
            assert plan.blockers
            cursor.execute("SELECT to_regnamespace('pit')")
            assert cursor.fetchone()[0] is None


@pytest.mark.parametrize("fault", ["source", "empty", "insert", "commit"])
def test_industry_month_failure_preserves_existing_snapshot(pit_schema, fault):
    from alphahome.pit.pit_industry_classification_manager import PITIndustryClassificationManager

    url, connection = pit_schema
    with connection.cursor() as cursor:
        cursor.execute("INSERT INTO pit.pit_industry_classification (ts_code,obs_date,data_source) VALUES ('OLD','2026-08-31','sw')")
        cursor.execute("SELECT oid FROM pg_class WHERE oid='pit.pit_industry_classification'::regclass")
        oid = cursor.fetchone()[0]
        if fault == "commit":
            cursor.execute("""CREATE FUNCTION pit.reject_industry_commit() RETURNS trigger LANGUAGE plpgsql AS
                $$ BEGIN RAISE EXCEPTION 'commit rejected'; END $$;
                CREATE CONSTRAINT TRIGGER reject_commit AFTER INSERT ON pit.pit_industry_classification
                DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pit.reject_industry_commit();""")
    with owned_sync_session(url) as db:
        with PITIndustryClassificationManager().bind_database(db_manager=db) as manager:
            def source(kind, month):
                if fault == "source":
                    raise RuntimeError("source failed")
                if fault == "empty":
                    return []
                return [{"ts_code": "NEW" if fault != "insert" else "x"*500,
                         "obs_date": month, "data_source": kind}]
            manager._generate_industry_snapshot = source
            with pytest.raises(Exception):
                manager._generate_monthly_snapshot(date(2026,8,1))
    with connection.cursor() as cursor:
        cursor.execute("SELECT ts_code,obs_date,data_source FROM pit.pit_industry_classification")
        assert cursor.fetchall() == [("OLD", date(2026,8,31), "sw")]
        cursor.execute("SELECT oid FROM pg_class WHERE oid='pit.pit_industry_classification'::regclass")
        assert cursor.fetchone()[0] == oid
