import asyncio
from datetime import date
from threading import Event
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

from alphahome.common.db_manager import DBManager
from alphahome.common.run_models import RunPlan
from alphahome.features import coordinator as module
from alphahome.features.coordinator import FeatureCoordinator, execute_feature_request
from alphahome.features.storage.database_init import CREATE_MV_METADATA_TABLE_SQL, CREATE_MV_REFRESH_LOG_TABLE_SQL
from alphahome.features.storage.incremental_view import IncrementalTableView


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]
CUTOFF = date(2026, 9, 11)


@pytest_asyncio.fixture
async def feature_plan_db(isolated_database_url, monkeypatch):
    connection = await asyncpg.connect(isolated_database_url)
    suffix = uuid4().hex[:10]
    source = "features.plan_source_" + suffix
    await connection.execute("CREATE SCHEMA IF NOT EXISTS features")
    created = []
    for name, sql in (("mv_metadata", CREATE_MV_METADATA_TABLE_SQL), ("mv_refresh_log", CREATE_MV_REFRESH_LOG_TABLE_SQL)):
        if not await connection.fetchval("SELECT to_regclass($1)", "features." + name):
            await connection.execute(sql)
            created.append("features." + name)
    await connection.execute(f"CREATE TABLE {source}(ts_code text, trade_date date, value integer); INSERT INTO {source} VALUES ('A', '2026-09-11', 1)")

    class Parent(IncrementalTableView):
        # These tests exercise dependency orchestration. Incremental recovery is
        # covered separately with explicit sources and a committed baseline.
        refresh_strategy = 'full'
        name = "plan_parent_" + suffix
        source_tables = [source]
        primary_keys = ("ts_code", "trade_date")

        def get_create_sql(self):
            return f"CREATE TABLE {self.full_name}(ts_code text, trade_date date, value integer)"

        def get_incremental_sql(self, start_date, end_date):
            return f"SELECT * FROM {source} WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'"

    class Child(Parent):
        name = "plan_child_" + suffix
        source_tables = [Parent().full_name]

        def get_incremental_sql(self, start_date, end_date):
            return f"SELECT ts_code,trade_date,value+1 AS value FROM {Parent().full_name} WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'"

    monkeypatch.setattr(module, "_recipes", lambda: {cls.name: cls for cls in (Parent, Child)})
    db = DBManager(isolated_database_url, mode="async")
    try:
        yield connection, db, Parent, Child, source
    finally:
        await db.close()
        for cls in (Child, Parent):
            await connection.execute(f"DROP TABLE IF EXISTS {cls().full_name}")
            await connection.execute("DELETE FROM features.mv_metadata WHERE view_name=$1", cls().view_name)
            await connection.execute("DELETE FROM features.mv_refresh_log WHERE view_name=$1", cls().view_name)
        await connection.execute(f"DROP TABLE {source}")
        for relation in reversed(created):
            await connection.execute(f"DROP TABLE {relation}")
        await connection.close()


async def create_chain(db, child):
    result = await execute_feature_request(db, [child.name], operation="create", as_of_date=CUTOFF)
    assert result["status"] == "success"


async def test_preview_readonly_gui_cli_hash_and_dag(feature_plan_db, monkeypatch):
    connection, db, parent, child, source = feature_plan_db
    coordinator = FeatureCoordinator(db)
    missing = await coordinator.plan([child.name], as_of_date=CUTOFF)
    assert missing.blockers
    assert await connection.fetchval("SELECT to_regclass($1)", child().full_name) is None
    plan = await coordinator.plan([child.name], operation="create", as_of_date=CUTOFF)
    assert not plan.blockers
    assert [unit.task_name for unit in plan.units] == [parent.name, child.name]
    assert RunPlan.from_dict(plan.to_dict()) == plan
    from alphahome.gui.services import feature_service
    monkeypatch.setattr(feature_service.UnifiedTaskFactory, "get_db_manager", lambda: db)
    gui = await feature_service.plan_feature_execution([child.name], operation="create", as_of_date=CUTOFF)
    assert plan.plan_hash == gui.plan_hash
    await coordinator.run(gui)
    refreshed = await coordinator.plan([child.name], as_of_date=CUTOFF)
    assert refreshed.units[-1].end_date == CUTOFF
    result = await coordinator.run(refreshed)
    assert result["status"] == "success"
    assert await connection.fetchval(f"SELECT value FROM {child().full_name}") == 2
    assert result["source_consumption"] == "unverified"
    assert (await execute_feature_request(db, [child.name], operation="create", as_of_date=CUTOFF))["results"][child.name]["status"] == "no_op"


async def test_source_drift_rejects_before_creation(feature_plan_db):
    connection, db, parent, child, source = feature_plan_db
    coordinator = FeatureCoordinator(db)
    plan = await coordinator.plan([child.name], operation="create", as_of_date=CUTOFF)
    await connection.execute(f"UPDATE {source} SET value=3")
    with pytest.raises(RuntimeError, match="changed"):
        await coordinator.run(plan)
    assert await connection.fetchval("SELECT to_regclass($1)", parent().full_name) is None


async def test_legacy_create_facade_uses_same_dependency_chain(feature_plan_db):
    connection, db, parent, child, source = feature_plan_db
    from scripts.features_init import create_materialized_views
    result = await create_materialized_views(db, [child])
    assert result["success"] == [parent.name, child.name]
    assert result["failed"] == []
    assert await connection.fetchval("SELECT to_regclass($1)", parent().full_name)


@pytest.mark.parametrize("fault", ["index", "metadata"])
async def test_creation_failure_rolls_back_table_and_metadata(feature_plan_db, monkeypatch, fault):
    connection, db, parent, child, source = feature_plan_db
    if fault == "index":
        monkeypatch.setattr(parent, "get_post_create_sqls", lambda self: [f"CREATE INDEX invalid_test_idx ON {self.full_name}(no_column)"])
    else:
        await connection.execute("ALTER TABLE features.mv_metadata ADD CONSTRAINT reject_plan_test CHECK (view_name NOT LIKE '%plan_parent_%')")
    try:
        result = await execute_feature_request(db, [child.name], operation="create", as_of_date=CUTOFF)
        assert result["status"] == "error"
        assert result["results"][child.name]["status"] == "blocked"
        assert await connection.fetchval("SELECT to_regclass($1)", parent().full_name) is None
        assert not await connection.fetchval("SELECT count(*) FROM features.mv_metadata WHERE view_name=$1", parent().view_name)
    finally:
        if fault == "metadata":
            await connection.execute("ALTER TABLE features.mv_metadata DROP CONSTRAINT reject_plan_test")


async def test_failed_parent_blocks_child_and_preserves_previous(feature_plan_db, monkeypatch):
    connection, db, parent, child, source = feature_plan_db
    await create_chain(db, child)
    assert (await execute_feature_request(db, [child.name], as_of_date=CUTOFF))["status"] == "success"
    monkeypatch.setattr(parent, "get_incremental_sql", lambda *args: "SELECT 1/0")
    result = await execute_feature_request(db, [child.name], as_of_date=CUTOFF)
    assert result["results"][parent.name]["status"] == "error"
    assert result["results"][child.name]["status"] == "blocked"
    assert await connection.fetchval(f"SELECT value FROM {child().full_name}") == 2


async def test_stop_and_unsupported_strategy_do_not_write(feature_plan_db):
    connection, db, parent, child, source = feature_plan_db
    await create_chain(db, child)
    coordinator = FeatureCoordinator(db)
    bad = await coordinator.plan([child.name], "concurrent", as_of_date=CUTOFF)
    assert bad.blockers
    with pytest.raises(RuntimeError, match="blocked"):
        await coordinator.run(bad)
    plan = await coordinator.plan([child.name], as_of_date=CUTOFF)
    stop = Event()
    stop.set()
    result = await coordinator.run(plan, stop_event=stop)
    assert result["status"] == "cancelled"
    assert not await connection.fetchval(f"SELECT count(*) FROM {child().full_name}")


async def test_pipeline_lock_precedes_planning_and_is_released_on_cancel(feature_plan_db, monkeypatch):
    connection, db, parent, child, source = feature_plan_db
    await create_chain(db, child)
    coordinator = FeatureCoordinator(db)
    plan = await coordinator.plan([child.name], as_of_date=CUTOFF)
    started = asyncio.Event()
    original = coordinator.plan

    async def record(*args, **kwargs):
        started.set()
        return await original(*args, **kwargs)

    monkeypatch.setattr(coordinator, "plan", record)
    await connection.execute("SELECT pg_advisory_lock(hashtext('alphahome.features'),hashtext('pipeline'))")
    task = asyncio.create_task(coordinator.run(plan))
    try:
        await asyncio.sleep(0.1)
        assert not started.is_set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await connection.execute("SELECT pg_advisory_unlock(hashtext('alphahome.features'),hashtext('pipeline'))")
    assert (await coordinator.run(plan))["status"] == "success"
