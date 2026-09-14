import pytest
from psycopg2.errors import QueryCanceled, ReadOnlySqlTransaction

from alphahome.common.db_session import owned_sync_session, query_timeout
from alphahome.common.run_models import target_fingerprint


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


def test_owned_read_snapshot_rejects_writes_and_closes(isolated_database_url):
    with owned_sync_session(isolated_database_url, expected_target=target_fingerprint(isolated_database_url), readonly=True) as db:
        connection = db._get_sync_connection()
        assert db.fetch_val_sync("SHOW transaction_read_only") == "on"
        with pytest.raises(ReadOnlySqlTransaction):
            db.execute_sync("CREATE TABLE public.should_never_exist (id integer)")
    assert connection.closed


def test_query_budget_timeout_is_an_error_and_restores_setting(isolated_database_url):
    with owned_sync_session(isolated_database_url, readonly=True) as db:
        before = db.fetch_val_sync("SHOW statement_timeout")
        with pytest.raises(QueryCanceled):
            with query_timeout(db, 10):
                db.fetch_val_sync("SELECT pg_sleep(0.1)")
        assert db.fetch_val_sync("SHOW statement_timeout") == before


def test_target_drift_fails_before_opening_session(isolated_database_url):
    with pytest.raises(RuntimeError, match="target changed"):
        with owned_sync_session(isolated_database_url, expected_target="0" * 64):
            pytest.fail("unexpected connection")


@pytest.mark.asyncio
async def test_async_snapshot_stays_consistent_while_another_connection_commits(isolated_database_url):
    from uuid import uuid4
    from alphahome.common.db_manager import DBManager
    from alphahome.common.db_session import readonly_snapshot

    db = DBManager(isolated_database_url, mode="async")
    table = "audit_snapshot_probe_" + uuid4().hex
    await db.connect()
    try:
        await db.execute(f"CREATE TABLE public.{table} (value integer)")
        await db.execute(f"INSERT INTO public.{table} VALUES (1)")
        async with readonly_snapshot(db) as snapshot:
            assert await snapshot.fetch_val(f"SELECT value FROM public.{table}") == 1
            await db.execute(f"UPDATE public.{table} SET value=2")
            assert await snapshot.fetch_val(f"SELECT value FROM public.{table}") == 1
            assert await snapshot.fetch_val("SHOW transaction_read_only") == "on"
        assert await db.fetch_val(f"SELECT value FROM public.{table}") == 2
    finally:
        await db.execute(f"DROP TABLE IF EXISTS public.{table}")
        await db.close()


@pytest.mark.asyncio
async def test_real_audit_previews_leave_missing_schemas_missing(isolated_database_url):
    from alphahome.common.db_manager import DBManager
    from alphahome.factors.audit_service import FactorAuditService
    from alphahome.pit.audit_service import PITAuditService

    db = DBManager(isolated_database_url, mode="async")
    await db.connect()
    try:
        if await db.fetch_val("SELECT to_regnamespace('factors') IS NOT NULL OR to_regnamespace('pit') IS NOT NULL"):
            pytest.skip("audit preview test needs unused pit/factors schemas")
        factors = await FactorAuditService(db).list_factor_tasks()
        assert all(row["status"] == "migration_required" for row in factors)
        pit = await PITAuditService(db).audit_task("pit_income_quarterly", persist=False)
        assert pit["status"] == "missing_table"
        assert not await db.fetch_val("SELECT to_regnamespace('factors') IS NOT NULL OR to_regnamespace('pit') IS NOT NULL")
    finally:
        await db.close()
