from types import SimpleNamespace

import asyncpg
import pytest

from alphahome.features.storage.financial_consumption import certify_financial_snapshot
from alphahome.features.storage import validated_mv

pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


@pytest.fixture
async def financial_mv(isolated_database_url):
    connection = await asyncpg.connect(isolated_database_url)
    assert not await connection.fetchval("SELECT to_regnamespace('closeout_financial_acceptance') IS NOT NULL")
    had_log = await connection.fetchval("SELECT to_regclass('features.mv_refresh_log') IS NOT NULL")
    assert not await connection.fetchval("SELECT to_regclass('features.mv_closeout_financial_acceptance') IS NOT NULL")
    await connection.execute("""CREATE SCHEMA closeout_financial_acceptance;
        CREATE SCHEMA IF NOT EXISTS features;
        CREATE TABLE closeout_financial_acceptance.source (
            ts_code text PRIMARY KEY, value integer, update_time timestamptz DEFAULT now(),
            end_date date DEFAULT '2026-06-30', ann_date date DEFAULT '2026-09-30');
        INSERT INTO closeout_financial_acceptance.source(ts_code,value) VALUES ('A',1);
        CREATE TABLE IF NOT EXISTS features.mv_refresh_log (
            view_name text, schema_name text, refresh_strategy text,
            started_at timestamptz, finished_at timestamptz, duration_seconds float,
            success boolean, error_message text, row_count bigint, details jsonb);
    """)

    class Recipe:
        name = "stock_income_quarterly"
        schema = "features"
        view_name = "mv_closeout_financial_acceptance"
        full_name = schema + "." + view_name
        source_tables = ["closeout_financial_acceptance.source"]
        quality_checks = {}
        _db_manager = SimpleNamespace(connection_string=isolated_database_url, execute=connection.execute)

        def get_create_sql(self):
            return f"CREATE MATERIALIZED VIEW {self.full_name} AS SELECT ts_code,value,ann_date,end_date AS report_period,md5(row_to_json(s)::text) AS source_version_hash,NOW() AS _processed_at FROM {self.source_tables[0]} s WHERE ts_code IS NOT NULL AND end_date IS NOT NULL"

    recipe = Recipe()
    await connection.execute(recipe.get_create_sql())
    try:
        yield connection, recipe
    finally:
        await connection.execute(f"DROP MATERIALIZED VIEW IF EXISTS {recipe.full_name}")
        await connection.execute("DROP SCHEMA closeout_financial_acceptance CASCADE")
        await connection.execute("DELETE FROM features.mv_refresh_log WHERE view_name=$1", recipe.view_name)
        if not had_log:
            await connection.execute("DROP TABLE features.mv_refresh_log")
        await connection.close()


@pytest.mark.asyncio
async def test_real_refresh_commits_full_equality_and_its_receipt(financial_mv):
    connection, recipe = financial_mv
    await connection.execute("UPDATE closeout_financial_acceptance.source SET value=2")
    result = await validated_mv.refresh_validated_mv(recipe, "full")
    assert result["source_consumption"] == "verified"
    assert result["audit_receipt_recorded"] is True
    proof = result["consumption_evidence"]
    assert proof["equality"] == {"expected_rows":1,"actual_rows":1,"missing_rows":0,"extra_rows":0}
    assert proof["system_first_receipt_verified"] is False
    assert proof["execution_limits"]["statement_timeout"] == "1min"
    assert proof["execution_limits"]["transaction_timeout"] == "2min"
    assert proof["execution_limits"]["lock_timeout"] == "1s"
    assert proof["execution_limits"]["max_parallel_workers_per_gather"] == "0"
    assert await connection.fetchval("SELECT value FROM features.mv_closeout_financial_acceptance") == 2
    assert await connection.fetchval("SELECT details->>'source_consumption' FROM features.mv_refresh_log WHERE view_name='mv_closeout_financial_acceptance'") == "verified"


@pytest.mark.asyncio
async def test_concurrent_source_commit_is_outside_certified_snapshot(financial_mv, isolated_database_url):
    connection, recipe = financial_mv
    writer = await asyncpg.connect(isolated_database_url)
    try:
        async with connection.transaction(isolation="repeatable_read"):
            await connection.fetchval("SELECT value FROM closeout_financial_acceptance.source")
            await writer.execute("UPDATE closeout_financial_acceptance.source SET value=9")
            await connection.execute("REFRESH MATERIALIZED VIEW features.mv_closeout_financial_acceptance")
            proof = await certify_financial_snapshot(connection, recipe)
            assert proof["equality"]["missing_rows"] == 0
            assert await connection.fetchval("SELECT value FROM features.mv_closeout_financial_acceptance") == 1
        result = await validated_mv.refresh_validated_mv(recipe, "full")
        assert result["source_consumption"] == "verified"
        assert await connection.fetchval("SELECT value FROM features.mv_closeout_financial_acceptance") == 9
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_changed_or_missing_output_fails_equality(financial_mv):
    connection, recipe = financial_mv
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute("REFRESH MATERIALIZED VIEW features.mv_closeout_financial_acceptance")
        await connection.execute("UPDATE closeout_financial_acceptance.source SET value=8")
        with pytest.raises(RuntimeError, match="equality failed"):
            await certify_financial_snapshot(connection, recipe)


@pytest.mark.asyncio
async def test_receipt_failure_rolls_back_only_the_refresh(financial_mv, monkeypatch):
    connection, recipe = financial_mv
    await connection.execute("UPDATE closeout_financial_acceptance.source SET value=2")

    async def failed_receipt(*_, **__):
        return False

    monkeypatch.setattr(validated_mv, "log_mv_refresh", failed_receipt)
    with pytest.raises(RuntimeError, match="receipt failed"):
        await validated_mv.refresh_validated_mv(recipe, "full")
    assert await connection.fetchval("SELECT value FROM features.mv_closeout_financial_acceptance") == 1
    assert await connection.fetchval("SELECT value FROM closeout_financial_acceptance.source") == 2
    assert await connection.fetchval("SELECT COUNT(*) FROM features.mv_refresh_log WHERE view_name='mv_closeout_financial_acceptance'") == 0


@pytest.mark.asyncio
async def test_exhaustive_batches_cover_old_and_target_only_stock_codes(financial_mv):
    connection, recipe = financial_mv
    await connection.execute("INSERT INTO closeout_financial_acceptance.source(ts_code,value) SELECT 'S'||lpad(n::text,4,'0'),n FROM generate_series(1,405) n")
    result = await validated_mv.refresh_validated_mv(recipe, "full")
    ranges = result["consumption_evidence"]["exhaustive_stock_ranges"]
    assert ranges["stock_count"] == 406
    assert len(ranges["batches"]) == 3
    assert result["consumption_evidence"]["equality"]["expected_rows"] == 406
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute("REFRESH MATERIALIZED VIEW features.mv_closeout_financial_acceptance")
        await connection.execute("DELETE FROM closeout_financial_acceptance.source WHERE ts_code='S0405'")
        with pytest.raises(RuntimeError, match="equality failed"):
            await certify_financial_snapshot(connection, recipe)


@pytest.mark.asyncio
async def test_duplicate_or_null_target_event_is_never_certified(financial_mv):
    connection, recipe = financial_mv
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute("DROP MATERIALIZED VIEW features.mv_closeout_financial_acceptance")
        await connection.execute(recipe.get_create_sql() + " UNION ALL " + recipe.get_create_sql().split(" AS ",1)[1])
        with pytest.raises(RuntimeError, match="equality failed"):
            await certify_financial_snapshot(connection, recipe)
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute("DROP MATERIALIZED VIEW features.mv_closeout_financial_acceptance")
        await connection.execute(recipe.get_create_sql().replace("SELECT ts_code,value", "SELECT NULL::text AS ts_code,value"))
        with pytest.raises(RuntimeError, match="null event keys"):
            await certify_financial_snapshot(connection, recipe)
