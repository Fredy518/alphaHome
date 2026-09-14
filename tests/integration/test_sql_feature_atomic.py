import asyncio
import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from alphahome.features.storage.incremental_view import IncrementalTableView
from alphahome.features.storage.refresh import MaterializedViewRefresh


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


class SQLFeature(IncrementalTableView):
    name = "sql_atomic_test"
    primary_keys = ("ts_code", "trade_date")
    query = "SELECT 'A'::text AS ts_code, DATE '2026-09-11' AS trade_date, 9 AS value"

    def get_create_sql(self):
        raise AssertionError("Refresh may not recreate the live table")

    def get_incremental_sql(self, start_date, end_date):
        return self.query


@pytest_asyncio.fixture
async def sql_feature(isolated_database_url):
    connection = await asyncpg.connect(isolated_database_url)
    name = "mv_sql_atomic_" + uuid4().hex[:12]
    await connection.execute("CREATE SCHEMA IF NOT EXISTS features")
    await connection.execute(f"""
        CREATE TABLE features.{name} (
            ts_code text NOT NULL, trade_date date NOT NULL,
            value integer CHECK (value >= 0), PRIMARY KEY (ts_code, trade_date)
        );
        INSERT INTO features.{name} VALUES ('A','2026-09-11',1),('B','2026-08-01',2);
        CREATE VIEW features.{name}_consumer AS SELECT * FROM features.{name};
        GRANT SELECT ON features.{name} TO PUBLIC;
        CREATE FUNCTION features.{name}_trigger() RETURNS trigger LANGUAGE plpgsql AS
        $$ BEGIN RETURN NEW; END $$;
        CREATE TRIGGER preserve_trigger BEFORE INSERT ON features.{name}
        FOR EACH ROW EXECUTE FUNCTION features.{name}_trigger();
    """)
    feature = SQLFeature(SimpleNamespace(connection_string=isolated_database_url))
    feature.materialized_view_name = name
    feature._log_refresh = AsyncMock()
    try:
        yield connection, feature
    finally:
        await connection.execute(f"DROP VIEW features.{name}_consumer; DROP TABLE features.{name}; DROP FUNCTION features.{name}_trigger()")
        await connection.close()


async def rows(connection, feature):
    return [tuple(row.values()) for row in await connection.fetch(f"SELECT * FROM {feature.full_name} ORDER BY ts_code, trade_date")]


async def identity(connection, feature):
    return await connection.fetchrow("""
        SELECT c.oid, c.relowner, c.relacl::text,
            (SELECT array_agg(indexrelid ORDER BY indexrelid) FROM pg_index WHERE indrelid=c.oid) AS indexes,
            (SELECT array_agg(oid ORDER BY oid) FROM pg_trigger WHERE tgrelid=c.oid) AS triggers
        FROM pg_class c WHERE c.oid=$1::regclass
    """, feature.full_name)


@pytest.mark.parametrize("strategy", ["full", "incremental"])
@pytest.mark.parametrize("fault", ["compute", "type", "duplicate", "empty", "out_of_window", "cancel", "commit"])
async def test_failure_keeps_data_and_object_identity(sql_feature, strategy, fault):
    connection, feature = sql_feature
    before, object_before = await rows(connection, feature), await identity(connection, feature)
    if fault == "compute":
        feature.query = "SELECT * FROM definitely_missing_refactor_table"
    elif fault == "type":
        feature.query = feature.query.replace("9 AS value", "'bad'::text AS value")
    elif fault == "duplicate":
        feature.query += " UNION ALL " + feature.query
    elif fault == "empty":
        feature.query += " WHERE false"
    elif fault == "out_of_window":
        feature.query = feature.query.replace("2026-09-11", "2026-08-01")
    elif fault == "cancel":
        def cancel(*args):
            raise asyncio.CancelledError()
        feature.get_incremental_sql = cancel
    elif fault == "commit":
        await connection.execute(f"""
            CREATE OR REPLACE FUNCTION features.{feature.view_name}_trigger() RETURNS trigger LANGUAGE plpgsql AS
            $$ BEGIN RAISE EXCEPTION 'injected commit failure'; END $$;
            DROP TRIGGER preserve_trigger ON {feature.full_name};
            CREATE CONSTRAINT TRIGGER reject_commit AFTER INSERT ON {feature.full_name}
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION features.{feature.view_name}_trigger();
        """)
        object_before = await identity(connection, feature)
    expected = {
        "compute": asyncpg.UndefinedTableError, "type": asyncpg.DatatypeMismatchError,
        "duplicate": asyncpg.UniqueViolationError, "empty": ValueError,
        "out_of_window": ValueError, "cancel": asyncio.CancelledError,
        "commit": asyncpg.RaiseError,
    }
    with pytest.raises(expected[fault]):
        await feature._refresh_table_window(strategy, "20260901", "20260911")
    assert await rows(connection, feature) == before
    assert await identity(connection, feature) == object_before


@pytest.mark.parametrize("strategy, count", [("full", 1), ("incremental", 2)])
async def test_refresh_preserves_view_index_owner_acl_and_trigger(sql_feature, strategy, count):
    connection, feature = sql_feature
    before = await identity(connection, feature)
    result = await feature._refresh_table_window(strategy, "20260901", "20260911")
    assert result["committed_rows"] == 1
    assert await identity(connection, feature) == before
    assert await connection.fetchval(f"SELECT count(*) FROM {feature.full_name}_consumer") == count
    assert await connection.fetchval(f"SELECT value FROM {feature.full_name}_consumer WHERE ts_code='A'") == 9


async def test_full_and_incremental_compute_under_same_target_lock(sql_feature):
    connection, first = sql_feature
    second = SQLFeature(first._db_manager)
    second.materialized_view_name = first.view_name
    second._log_refresh = AsyncMock()
    first_started, second_started = asyncio.Event(), asyncio.Event()

    def first_sql(*args):
        first_started.set()
        return first.query.replace("9 AS value", "5 AS value") + " FROM pg_sleep(0.35)"

    def second_sql(*args):
        second_started.set()
        return second.query.replace("9 AS value", "7 AS value")

    first.get_incremental_sql, second.get_incremental_sql = first_sql, second_sql
    first_task = asyncio.create_task(first._refresh_table_window("full", "20260901", "20260911"))
    await asyncio.wait_for(first_started.wait(), 5)
    second_task = asyncio.create_task(second._refresh_table_window("incremental", "20260901", "20260911"))
    results = await asyncio.gather(first_task, second_task)
    assert second_started.is_set()
    assert results[1]["lock_wait_seconds"] >= 0.15
    assert await rows(connection, first) == [("A", date(2026, 9, 11), 7)]


async def test_explicit_empty_contract(sql_feature):
    connection, feature = sql_feature
    feature.query += " WHERE false"
    feature.allow_expected_no_data = True
    feature.expected_no_data_reason = AsyncMock(return_value="verified no eligible source rows")
    result = await feature._refresh_table_window("incremental", "20260901", "20260911")
    assert result["status"] == "expected_no_data"
    assert await rows(connection, feature) == [("B", date(2026, 8, 1), 2)]


async def test_isolated_refresh_scale_probe(sql_feature, record_property):
    """Bounded synthetic probe; this is not production capacity certification."""
    import tracemalloc

    connection, feature = sql_feature
    feature.query = "SELECT n::text AS ts_code, DATE '2026-09-11' AS trade_date, n AS value FROM generate_series(1,20000) n"
    tracemalloc.start()
    try:
        result = await feature._refresh_table_window("full", "20260901", "20260911")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    probe = {key: result[key] for key in ("committed_rows", "duration_seconds", "lock_wait_seconds")}
    probe.update(python_peak_bytes=peak, rows_per_second=20000/result["duration_seconds"])
    record_property("isolated_sql_refresh_probe", json.dumps(probe))
    print("isolated_sql_refresh_probe=" + json.dumps(probe))
    assert result["committed_rows"] == 20000
    assert await connection.fetchval(f"SELECT count(*) FROM {feature.full_name}_consumer") == 20000


async def test_concurrent_capability_requires_explicit_fallback(isolated_database_url):
    connection = await asyncpg.connect(isolated_database_url)
    name = "mv_concurrent_" + uuid4().hex[:12]

    class DB:
        async def fetch(self, query, *args, **kwargs):
            return await connection.fetch(query, *args, **kwargs)

        async def execute(self, query, *args, **kwargs):
            return await connection.execute(query, *args, **kwargs)

    await connection.execute("CREATE SCHEMA IF NOT EXISTS features")
    await connection.execute(f"CREATE MATERIALIZED VIEW features.{name} AS SELECT 1 AS id")
    executor = MaterializedViewRefresh(DB())
    executor._log_refresh = AsyncMock()
    try:
        rejected = await executor.refresh(name, "concurrent")
        assert rejected["status"] == "failed"
        assert rejected["effective_strategy"] is None
        accepted = await executor.refresh(name, "concurrent", allow_blocking_fallback=True)
        assert accepted["status"] == "success"
        assert accepted["requested_strategy"] == "concurrent"
        assert accepted["effective_strategy"] == "full"
        await connection.execute(f"CREATE UNIQUE INDEX ON features.{name} (id)")
        normal = await executor.refresh(name, "concurrent")
        assert normal["status"] == "success"
        assert normal["effective_strategy"] == "concurrent"
        assert normal["fallback_reason"] is None
    finally:
        await connection.execute(f"DROP MATERIALIZED VIEW features.{name}")
        await connection.close()
