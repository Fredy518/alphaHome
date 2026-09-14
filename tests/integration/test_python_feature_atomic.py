import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pandas as pd
import pytest
import pytest_asyncio

from alphahome.features.storage.python_feature import PythonFeatureTable


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


class Feature(PythonFeatureTable):
    name = "atomic_test"
    primary_keys = ("ts_code", "trade_date")

    def get_create_sql(self):
        raise AssertionError("Refresh must not create a live table")

    async def compute(self, start_date, end_date):
        return pd.DataFrame({"ts_code": ["A"], "trade_date": [date(2026, 9, 11)], "value": [9]})


@pytest_asyncio.fixture
async def feature_db(isolated_database_url):
    connection = await asyncpg.connect(isolated_database_url)
    name = "mv_atomic_" + uuid4().hex[:12]
    await connection.execute("CREATE SCHEMA IF NOT EXISTS features")
    await connection.execute(f"""
        CREATE TABLE features.{name} (
            ts_code text NOT NULL, trade_date date NOT NULL,
            value integer CHECK (value >= 0), PRIMARY KEY (ts_code, trade_date)
        );
        INSERT INTO features.{name} VALUES ('A','2026-09-11',1),('B','2026-08-01',2);
        CREATE VIEW features.{name}_consumer AS SELECT * FROM features.{name};
    """)
    feature = Feature(SimpleNamespace(connection_string=isolated_database_url))
    feature.materialized_view_name = name
    feature._log_refresh = AsyncMock()
    try:
        yield connection, feature
    finally:
        await connection.execute(f"DROP VIEW features.{name}_consumer; DROP TABLE features.{name}")
        await connection.close()


async def snapshot(connection, feature):
    rows = await connection.fetch(f"SELECT * FROM {feature.full_name} ORDER BY ts_code, trade_date")
    return [tuple(row.values()) for row in rows]


@pytest.mark.parametrize("strategy", ["full", "incremental"])
@pytest.mark.parametrize("fault", ["compute", "copy", "cancel", "empty", "commit"])
async def test_failed_refresh_preserves_old_snapshot(feature_db, strategy, fault):
    connection, feature = feature_db
    before = await snapshot(connection, feature)
    if fault == "compute":
        feature.compute = AsyncMock(side_effect=RuntimeError("compute failed"))
    elif fault == "cancel":
        feature.compute = AsyncMock(side_effect=asyncio.CancelledError())
    elif fault == "empty":
        feature.compute = AsyncMock(return_value=pd.DataFrame())
    elif fault == "copy":
        feature.compute = AsyncMock(return_value=pd.DataFrame({"ts_code": ["A"], "trade_date": [date(2026, 9, 11)], "value": ["invalid integer"]}))
    else:
        # Deferred trigger fails only at COMMIT, after DELETE and INSERT ran.
        function = feature.view_name + "_reject"
        await connection.execute(f"""
            CREATE FUNCTION features.{function}() RETURNS trigger LANGUAGE plpgsql AS
            $$ BEGIN RAISE EXCEPTION 'injected commit failure'; END $$;
            CREATE CONSTRAINT TRIGGER reject_commit AFTER INSERT ON {feature.full_name}
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION features.{function}();
        """)
    try:
        with pytest.raises((Exception, asyncio.CancelledError)):
            await feature._refresh_window(strategy, "20260901", "20260911")
        assert await snapshot(connection, feature) == before
    finally:
        if fault == "commit":
            await connection.execute(f"DROP TRIGGER reject_commit ON {feature.full_name}; DROP FUNCTION features.{function}()")


@pytest.mark.parametrize("strategy, expected_rows", [("incremental", 2), ("full", 1)])
async def test_success_preserves_object_identity_and_consumer_view(feature_db, strategy, expected_rows):
    connection, feature = feature_db
    oid = await connection.fetchval("SELECT $1::regclass::oid", feature.full_name)
    result = await feature._refresh_window(strategy, "20260901", "20260911")
    assert result["committed_rows"] == 1
    assert await connection.fetchval("SELECT $1::regclass::oid", feature.full_name) == oid
    assert await connection.fetchval(f"SELECT COUNT(*) FROM {feature.full_name}_consumer") == expected_rows
    assert await connection.fetchval(f"SELECT value FROM {feature.full_name}_consumer WHERE ts_code='A'") == 9


async def test_explicit_expected_no_data_can_clear_only_requested_window(feature_db):
    connection, feature = feature_db
    frame = pd.DataFrame()
    frame.attrs.update(expected_no_data=True, reason="verified empty eligible universe")
    feature.allow_expected_no_data = True
    feature.compute = AsyncMock(return_value=frame)
    result = await feature._refresh_window("incremental", "20260901", "20260911")
    assert result["status"] == "expected_no_data"
    assert await snapshot(connection, feature) == [("B", date(2026, 8, 1), 2)]


async def test_full_and_incremental_serialize_before_computation(feature_db):
    connection, first = feature_db
    second = Feature(first._db_manager)
    second.materialized_view_name = first.view_name
    second._log_refresh = AsyncMock()
    started, release, second_started = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def first_compute(*_):
        started.set()
        await release.wait()
        return pd.DataFrame({"ts_code": ["A"], "trade_date": [date(2026, 9, 11)], "value": [5]})

    async def second_compute(*_):
        second_started.set()
        return pd.DataFrame({"ts_code": ["A"], "trade_date": [date(2026, 9, 11)], "value": [7]})

    first.compute, second.compute = first_compute, second_compute
    first_task = asyncio.create_task(first._refresh_window("full", "20260901", "20260911"))
    await asyncio.wait_for(started.wait(), timeout=5)
    second_task = asyncio.create_task(second._refresh_window("incremental", "20260901", "20260911"))
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(second_started.wait(), timeout=0.15)
    finally:
        release.set()
        await asyncio.gather(first_task, second_task)
    assert await snapshot(connection, first) == [("A", date(2026, 9, 11), 7)]
