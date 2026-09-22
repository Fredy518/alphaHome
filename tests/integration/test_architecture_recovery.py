"""Cross-module acceptance scenarios, using only the explicitly isolated DB."""

from dataclasses import replace
from datetime import date
import json
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock

import asyncpg
import pandas as pd
import psycopg2
import pytest
import pytest_asyncio

from alphahome.common.db_manager import DBManager
from alphahome.common.db_session import owned_sync_session
from alphahome.features import coordinator as feature_module
from alphahome.features.coordinator import build_feature_plan, execute_feature_request
from alphahome.features.storage.database_init import CREATE_MV_METADATA_TABLE_SQL, CREATE_MV_REFRESH_LOG_TABLE_SQL
from alphahome.features.storage.incremental_view import IncrementalTableView
from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.pit.pit_data_update_production import PITDataUpdateCoordinator
from alphahome.pit.pit_income_quarterly_manager import PITIncomeQuarterlyManager
from alphahome.pit.pit_stock_fttm_manager import PITStockFTTMManager
from alphahome.pit.pit_industry_classification_manager import PITIndustryClassificationManager
from alphahome.pit.schema import render_schema_sql
from alphahome.pit.planning_time import frozen_pit_time


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


@pytest.fixture
def pit_recovery_db(isolated_database_url):
    connection = psycopg2.connect(isolated_database_url)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regnamespace('pit')")
        assert cursor.fetchone()[0] is None
        cursor.execute(render_schema_sql())
    try:
        yield isolated_database_url, connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute('DROP SCHEMA pit CASCADE')
        connection.close()


async def test_pit_entrypoint_records_baseline_and_cancel_does_not_advance_it(pit_recovery_db, monkeypatch):
    url, connection = pit_recovery_db
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE pit.review_source(ann_date date, update_time timestamptz); INSERT INTO pit.review_source VALUES ('2020-01-01',clock_timestamp())")
    contract = PITDataUpdateCoordinator._registered_contracts()['pit_income_quarterly']
    contract = replace(contract, source_tables=('pit.review_source',))
    monkeypatch.setattr(PITDataUpdateCoordinator, '_registered_contracts', staticmethod(lambda: {contract.task_name: contract}))
    monkeypatch.setattr(PITIncomeQuarterlyManager, 'plan_incremental_range', lambda self, days=None:
                        self.resolve_incremental_date_range(days, (('pit.review_source', ('ann_date',), 'update_time'),)))
    coordinator = PITDataUpdateCoordinator(db_manager=SimpleNamespace(connection_string=url))
    coordinator._run_task = AsyncMock(return_value={'task': contract.task_name, 'status': 'success', 'rows': 1})
    missing = await coordinator.plan(['income'], cutoff='2026-09-22')
    assert any('baseline_required' in value for value in missing.blockers)
    full = await coordinator.plan(['income'], 'full_backfill', cutoff='2026-09-22')
    result = await coordinator.run_updates(['income'], 'full_backfill', plan=full)
    assert result[0]['run_id']
    with connection.cursor() as cursor:
        cursor.execute('SELECT status,baseline_ready,started_at,finished_at FROM pit.task_run')
        row = cursor.fetchone()
        assert row[0:2] == ('success', True) and row[2] <= row[3]
    plan = await coordinator.plan(['income'], cutoff='2026-09-22')
    assert not plan.blockers and plan.units[0].start_date == date(2020, 1, 1)
    # A data commit followed by a missing final ledger record is deliberately
    # replayed: a running row is never treated as a recovery watermark.
    with connection.cursor() as cursor:
        cursor.execute("""INSERT INTO pit.task_run(run_id,batch_id,task_name,plan_hash,mode,started_at,
            start_date,end_date,status) VALUES (%s,%s,%s,'crashed','incremental',clock_timestamp(),
            '2026-09-15','2026-09-22','running')""", (str(uuid4()),str(uuid4()),contract.task_name))
    plan = await coordinator.plan(['income'], cutoff='2026-09-22')
    assert not plan.blockers and plan.units[0].start_date == date(2020, 1, 1)
    import asyncio
    coordinator._run_task = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await coordinator.run_updates(['income'], plan=plan)
    with connection.cursor() as cursor:
        cursor.execute("SELECT status,baseline_ready FROM pit.task_run WHERE status<>'running' ORDER BY finished_at")
        assert cursor.fetchall() == [('success', True), ('cancelled', False)]
    # The pipeline lock is released after the cancelled worker has finished.
    lock = await asyncpg.connect(url)
    try:
        assert await lock.fetchval("SELECT pg_try_advisory_lock(hashtext('alphahome.pit'),hashtext('pipeline'))")
    finally:
        await lock.close()


async def test_empty_pit_full_result_does_not_establish_a_baseline(pit_recovery_db, monkeypatch):
    url, connection = pit_recovery_db
    contract = PITDataUpdateCoordinator._registered_contracts()['pit_income_quarterly']
    contract = replace(contract, source_tables=())
    monkeypatch.setattr(PITDataUpdateCoordinator, '_registered_contracts', staticmethod(lambda: {contract.task_name: contract}))
    coordinator = PITDataUpdateCoordinator(db_manager=SimpleNamespace(connection_string=url))
    coordinator._run_task = AsyncMock(return_value={'task': contract.task_name, 'status': 'success', 'rows': 0})
    full = await coordinator.plan(['income'], 'full_backfill', cutoff='2026-09-22')
    result = await coordinator.run_updates(['income'], 'full_backfill', plan=full)
    assert result[0]['status'] == 'error' and 'baseline_unproven' in result[0]['error']
    with connection.cursor() as cursor:
        cursor.execute('SELECT status,baseline_ready FROM pit.task_run')
        assert cursor.fetchone() == ('error', False)


def test_pit_plan_projects_this_batches_new_month(pit_recovery_db, monkeypatch):
    from alphahome.pit.run_plan import build_pit_plan
    url, connection = pit_recovery_db
    with connection.cursor() as cursor:
        cursor.execute("INSERT INTO pit.pit_industry_classification(ts_code,obs_date,data_source,industry_code1,industry_code2) VALUES ('A','2026-07-31','sw','L1','L2')")
    all_contracts = PITDataUpdateCoordinator._registered_contracts()
    names = ['pit_stock_fttm_monthly', 'pit_industry_classification', 'pit_industry_fttm_monthly']
    contracts = {name: replace(all_contracts[name], source_tables=()) for name in names}
    monkeypatch.setattr(PITDataUpdateCoordinator, '_registered_contracts', staticmethod(lambda: contracts))
    monkeypatch.setattr(PITIndustryClassificationManager, '_detect_industry_changes', lambda *a, **k: {'has_changes': True})
    monkeypatch.setattr(PITIndustryClassificationManager, '_get_affected_months', lambda *a, **k: [date(2026,8,31)])
    plan = build_pit_plan(url, ['pit_industry_fttm_monthly'], 'incremental', cutoff='2026-09-22')
    assert not plan.blockers
    assert all(unit.dates[-1] == date(2026, 8, 31) for unit in plan.units)


@pytest.mark.parametrize('partial', [False, True])
def test_unproven_empty_or_missing_month_preserves_old_snapshot(pit_recovery_db, partial):
    url, connection = pit_recovery_db
    with connection.cursor() as cursor:
        cursor.execute("""INSERT INTO pit.pit_stock_fttm_monthly
            (ts_code,org_name,obs_date,selected_report_date,report_quarter,fy1_year,fy2_year,
             fy1_np_used,fy2_np_used,fy1_weight,fy2_weight,fttm_np,estimate_pair_status,
             is_single_year_fallback,source_window_start,source_window_end,formula_version,source_max_report_date)
            VALUES ('A','broker','2026-08-31','2026-08-01',3,2026,2027,
                    100,100,0.5,0.5,100,'both',false,'2026-02-01','2026-08-31','review','2026-08-01')""")
    frame = pd.DataFrame([{'ts_code':'A', 'org_name':'broker', 'obs_date':date(2026,7,31), 'fttm_np':110}]) if partial else pd.DataFrame()
    months = [date(2026,7,31), date(2026,8,31)] if partial else [date(2026,8,31)]
    with owned_sync_session(url) as db, PITStockFTTMManager().bind_database(db_manager=db) as manager:
        with pytest.raises(ValueError, match='incomplete_months'):
            manager._atomic_replace_months(frame, months, ['ts_code','org_name','obs_date','fttm_np'], ['ts_code','org_name','obs_date'])
    with connection.cursor() as cursor:
        cursor.execute('SELECT obs_date,fttm_np FROM pit.pit_stock_fttm_monthly')
        assert cursor.fetchall() == [(date(2026,8,31),100)]


@pytest_asyncio.fixture
async def recovery_feature(isolated_database_url, monkeypatch):
    connection = await asyncpg.connect(isolated_database_url)
    suffix = uuid4().hex[:12]
    source = 'features.review_source_' + suffix
    await connection.execute('CREATE SCHEMA IF NOT EXISTS features')
    await connection.execute(CREATE_MV_METADATA_TABLE_SQL + CREATE_MV_REFRESH_LOG_TABLE_SQL)
    await connection.execute(f"""CREATE TABLE {source}(trade_date date PRIMARY KEY,value integer,update_time timestamptz NOT NULL);
        INSERT INTO {source} SELECT d::date,1,'2026-07-01'::timestamptz
        FROM generate_series('2026-07-01'::date,'2026-09-22'::date,'1 day') d;""")

    class Window(IncrementalTableView):
        name = 'review_window_' + suffix
        source_tables = [source]
        recovery_sources = {source: ('trade_date', 'update_time')}
        primary_keys = ('trade_date',)
        quality_checks = {'null_check': {'columns': ['value'], 'threshold': 0}}

        def get_create_sql(self):
            return f'CREATE TABLE {self.full_name}(trade_date date PRIMARY KEY,value integer)'

        def get_incremental_sql(self, start_date, end_date):
            return f"SELECT trade_date,value FROM {source} WHERE trade_date BETWEEN '{start_date}' AND '{end_date}'"

    await connection.execute(Window().get_create_sql())
    monkeypatch.setattr(feature_module, '_recipes', lambda: {Window.name: Window})
    db = DBManager(isolated_database_url, mode='async')
    try:
        yield connection, db, Window, source
    finally:
        await db.close()
        await connection.execute(f'DROP TABLE {Window().full_name}; DROP TABLE {source}')
        await connection.execute('DELETE FROM features.refresh_checkpoint WHERE target=$1', Window().full_name)
        await connection.execute('DELETE FROM features.mv_refresh_log WHERE view_name=$1', Window().view_name)
        await connection.close()


async def test_feature_outage_recovery_and_old_revision_are_not_silent(recovery_feature):
    connection, db, recipe, source = recovery_feature
    missing = build_feature_plan(db.connection_string, [recipe.name], as_of_date='2026-09-22')
    assert any('baseline_required' in item for item in missing.blockers)
    first = await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-07-01')
    assert first['status'] == 'success'
    plan = build_feature_plan(db.connection_string, [recipe.name], as_of_date='2026-09-22')
    assert plan.units[0].start_date == date(2026,7,2)
    result = await execute_feature_request(db, [recipe.name], submitted_plan=plan, as_of_date='2026-09-22')
    assert result['status'] == 'success'
    assert await connection.fetchval(f'SELECT count(*) FROM {source} s LEFT JOIN {recipe().full_name} t USING(trade_date) WHERE t.trade_date IS NULL') == 0
    await connection.execute(f"UPDATE {source} SET value=7,update_time=clock_timestamp() WHERE trade_date='2026-07-20'")
    plan = build_feature_plan(db.connection_string, [recipe.name], as_of_date='2026-09-22')
    assert plan.units[0].start_date == date(2026,7,20)
    assert (await execute_feature_request(db,[recipe.name],submitted_plan=plan,as_of_date='2026-09-22'))['status'] == 'success'
    assert await connection.fetchval(f"SELECT value FROM {recipe().full_name} WHERE trade_date='2026-07-20'") == 7
    await connection.execute(f"INSERT INTO {source} VALUES ('2020-01-01',3,clock_timestamp())")
    oversized = build_feature_plan(db.connection_string, [recipe.name], as_of_date='2026-09-22')
    assert any('backfill_required' in item and '2020-01-01' in item for item in oversized.blockers)


async def test_quality_failure_rolls_back_output_and_checkpoint(recovery_feature):
    connection, db, recipe, source = recovery_feature
    assert (await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-09-22'))['status'] == 'success'
    checkpoint = await connection.fetchrow('SELECT * FROM features.refresh_checkpoint WHERE target=$1', recipe().full_name)
    recipe.quality_checks = {'row_count_change': {'threshold': .2}}
    await connection.execute(f"DELETE FROM {source} WHERE trade_date <> '2026-09-22'")
    result = await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-09-22')
    assert result['status'] == 'error'
    assert 'quality_failed' in result['results'][recipe.name]['error_message']
    assert await connection.fetchval(f'SELECT count(*) FROM {recipe().full_name}') == 84
    assert await connection.fetchrow('SELECT * FROM features.refresh_checkpoint WHERE target=$1', recipe().full_name) == checkpoint


async def test_explicit_initial_baseline_growth_is_plan_bound_and_one_time(recovery_feature, monkeypatch):
    connection, db, recipe, source = recovery_feature
    await connection.execute(
        f"INSERT INTO {recipe().full_name} "
        f"SELECT trade_date,value FROM {source} WHERE trade_date BETWEEN '2026-07-01' AND '2026-07-09' "
        f"UNION ALL SELECT trade_date,value FROM {source} WHERE trade_date='2026-08-10'"
    )
    monkeypatch.setattr(recipe, 'quality_checks', {'row_count_change': {'threshold': 0.2}})
    monkeypatch.setattr(
        recipe, 'expected_keys_sql',
        lambda self, start, end: f"SELECT trade_date FROM {source} WHERE trade_date BETWEEN '{start}' AND '{end}'",
    )

    rejected = await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-09-22')
    assert rejected['status'] == 'error'
    assert await connection.fetchval(f'SELECT count(*) FROM {recipe().full_name}') == 10
    assert await connection.fetchrow(
        'SELECT * FROM features.refresh_checkpoint WHERE target=$1', recipe().full_name,
    ) is None

    plan = build_feature_plan(
        db.connection_string, [recipe.name], strategy='full', as_of_date='2026-09-22',
        approve_initial_baseline_growth=True,
    )
    assert not plan.blockers
    assert plan.units[0].estimated_rows == 84
    params = json.loads(plan.units[0].parameters_json)
    assert params['initial_baseline_expected_rows'] == 84
    with pytest.raises(ValueError, match='approval differs'):
        await execute_feature_request(
            db, [recipe.name], strategy='full', submitted_plan=plan, as_of_date='2026-09-22',
        )

    accepted = await execute_feature_request(
        db, [recipe.name], strategy='full', submitted_plan=plan, expected_plan_hash=plan.plan_hash,
        as_of_date='2026-09-22', approve_initial_baseline_growth=True,
    )
    assert accepted['status'] == 'success'
    assert await connection.fetchval(f'SELECT count(*) FROM {recipe().full_name}') == 84
    check = accepted['results'][recipe.name]['quality']['executed']['row_count_change']
    assert check['initial_baseline_growth_approved'] is True
    assert check['current'] == 41 and check['previous'] == 10
    assert check['total_rows'] == check['approved_total_rows'] == 84

    repeated = build_feature_plan(
        db.connection_string, [recipe.name], strategy='full', as_of_date='2026-09-22',
        approve_initial_baseline_growth=True,
    )
    assert any('initial baseline already exists' in blocker for blocker in repeated.blockers)


@pytest.mark.parametrize('strategy', ['full', 'concurrent'])
async def test_materialized_view_quality_failure_rolls_back(recovery_feature, strategy):
    connection, db, recipe, source = recovery_feature

    class View(BaseFeatureView):
        name = recipe.name + '_mv'
        quality_checks = {'row_count_change': {'threshold': .2}}

        def get_create_sql(self):
            return f'CREATE MATERIALIZED VIEW {self.full_name} AS SELECT trade_date,value FROM {source}'

    view = View(db_manager=db)
    await connection.execute(view.get_create_sql())
    await connection.execute(f'CREATE UNIQUE INDEX ON {view.full_name}(trade_date)')
    try:
        await connection.execute(f"DELETE FROM {source} WHERE trade_date <> '2026-09-22'")
        with pytest.raises(ValueError, match='quality_failed'):
            await view.refresh(strategy)
        assert await connection.fetchval(f'SELECT count(*) FROM {view.full_name}') == 84
    finally:
        await connection.execute(f'DROP MATERIALIZED VIEW {view.full_name}')


async def test_rawdata_mapping_validation_uses_no_ddl_and_rejects_a_filter(recovery_feature):
    connection, db, recipe, source = recovery_feature
    name = 'review_mapping_' + uuid4().hex[:12]
    await connection.execute('CREATE SCHEMA IF NOT EXISTS rawdata')
    await connection.execute(f'CREATE VIEW rawdata.{name} AS SELECT * FROM {source}')
    schema, table = source.split('.')
    try:
        db.ensure_schema_exists = AsyncMock(side_effect=AssertionError('routine validation must not issue DDL'))
        await db.create_rawdata_view(name, schema, table, replace=True, verify_only=True)
        db.ensure_schema_exists.assert_not_awaited()
        await connection.execute(f'CREATE OR REPLACE VIEW rawdata.{name} AS SELECT * FROM {source} WHERE value>0')
        with pytest.raises(RuntimeError, match='migration_required'):
            await db.create_rawdata_view(name, schema, table, replace=True, verify_only=True)
    finally:
        await connection.execute(f'DROP VIEW rawdata.{name}')


async def test_partial_nonempty_feature_keys_cannot_advance_checkpoint(recovery_feature, monkeypatch):
    connection, db, recipe, source = recovery_feature
    result = await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-09-22')
    assert result['status'] == 'success'
    checkpoint = await connection.fetchrow('SELECT * FROM features.refresh_checkpoint WHERE target=$1', recipe().full_name)
    monkeypatch.setattr(recipe, 'expected_keys_sql', lambda self, start, end:
                        f"SELECT trade_date FROM {source} WHERE trade_date BETWEEN '{start}' AND '{end}'")
    original = recipe.get_incremental_sql
    monkeypatch.setattr(recipe, 'get_incremental_sql', lambda self, start, end:
                        original(self, start, end) + " AND trade_date <> '2026-07-20'")
    result = await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-09-22')
    assert result['status'] == 'error'
    assert await connection.fetchval(f'SELECT count(*) FROM {recipe().full_name}') == 84
    assert await connection.fetchrow('SELECT * FROM features.refresh_checkpoint WHERE target=$1', recipe().full_name) == checkpoint


async def test_feature_recovery_contract_change_requires_new_baseline(recovery_feature, monkeypatch):
    _, db, recipe, _ = recovery_feature
    assert (await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-09-22'))['status'] == 'success'
    monkeypatch.setattr(recipe, 'recovery_contract_version', 'changed-formula')
    plan = build_feature_plan(db.connection_string, [recipe.name], as_of_date='2026-09-22')
    assert any('baseline_required' in item for item in plan.blockers)


async def test_expected_new_dates_during_catchup_do_not_fail_row_count_gate(recovery_feature, monkeypatch):
    connection, db, recipe, _ = recovery_feature
    monkeypatch.setattr(recipe, 'quality_checks', {'row_count_change': {'threshold': 0.2}})
    assert (await execute_feature_request(db, [recipe.name], strategy='full', as_of_date='2026-07-22'))['status'] == 'success'
    result = await execute_feature_request(db, [recipe.name], as_of_date='2026-08-10')
    assert result['status'] == 'success'
    assert await connection.fetchval(f'SELECT count(*) FROM {recipe().full_name}') == 41
    check = result['results'][recipe.name]['quality']['executed']['row_count_change']
    assert check['current'] == check['previous'] == 12 and check['total_rows'] == 31


async def test_materialized_view_empty_requires_a_specific_reason(recovery_feature):
    connection, db, _, source = recovery_feature

    class EmptyView(BaseFeatureView):
        name = 'review_empty_' + uuid4().hex[:12]

        def get_create_sql(self):
            return f'CREATE MATERIALIZED VIEW {self.full_name} AS SELECT * FROM {source} WHERE false'

    view = EmptyView(db)
    await connection.execute(view.get_create_sql())
    try:
        with pytest.raises(ValueError, match='empty result'):
            await view.refresh('full')
        view.expected_empty_view_reason = AsyncMock(return_value='Synthetic eligible universe is empty.')
        result = await view.refresh('full')
        assert result['status'] == 'expected_no_data' and result['empty_reason']
    finally:
        await connection.execute(f'DROP MATERIALIZED VIEW {view.full_name}')
        await connection.execute('DELETE FROM features.mv_refresh_log WHERE view_name=$1', view.view_name)
