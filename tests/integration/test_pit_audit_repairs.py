"""Execute repaired production methods/SQL in the guarded disposable database."""
from datetime import date, datetime
from decimal import Decimal
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

from alphahome.common.db_components.database_operations_mixin import DatabaseOperationsMixin
from alphahome.common.task_system.base_task import BaseTask
from alphahome.features.recipes.mv.stock.stock_shareholder_concentration import StockShareholderConcentrationMV
from alphahome.features.recipes.mv.pit_asof_rank_sql import ah_premium_sql, fund_holdings_sql
from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager

pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


@pytest.fixture
def factor_db(isolated_database_url):
    connection = psycopg2.connect(isolated_database_url)
    with connection.cursor() as cur:
        cur.execute("SELECT current_database()")
        assert cur.fetchone()[0].startswith('alphahome_test_')
        cur.execute("CREATE SCHEMA IF NOT EXISTS rawdata")
        cur.execute("CREATE SCHEMA IF NOT EXISTS features")
        cur.execute("CREATE SCHEMA IF NOT EXISTS pit")
        cur.execute("SELECT to_regclass('rawdata.stock_holdernumber')")
        assert cur.fetchone()[0] is None, 'Refuses to overwrite existing relation'
        cur.execute("""CREATE TABLE rawdata.stock_holdernumber (
            ts_code text, end_date date, ann_date date, holder_num bigint,
            PRIMARY KEY (ts_code, ann_date))""")
        for relation in ('fund_portfolio', 'stock_ahcomparison'):
            cur.execute('SELECT to_regclass(%s)', ('rawdata.' + relation,))
            assert cur.fetchone()[0] is None, 'Refuses to overwrite existing relation'
        cur.execute("""CREATE TABLE rawdata.fund_portfolio (
            ts_code text, ann_date date, end_date date, symbol text, mkv numeric,
            amount numeric, stk_mkv_ratio numeric, update_time timestamp)""")
        cur.execute('CREATE INDEX ON rawdata.fund_portfolio (symbol,end_date,ann_date)')
        cur.execute("""CREATE TABLE rawdata.stock_ahcomparison (
            ts_code text, trade_date date, hk_code text DEFAULT 'X.HK', name text, close numeric,
            hk_close numeric, pct_chg numeric, hk_pct_chg numeric, ah_comparison numeric,
            ah_premium numeric, PRIMARY KEY(trade_date,ts_code,hk_code))""")
        cur.execute("SELECT to_regclass('pit.pit_balance_quarterly')")
        assert cur.fetchone()[0] is None, 'Refuses to overwrite existing relation'
        cur.execute("""CREATE TABLE pit.pit_balance_quarterly (
            ts_code text, end_date date, ann_date date, data_source text,
            tot_liab numeric, total_cur_assets numeric, total_cur_liab numeric, inventories numeric,
            source_ann_date date, source_f_ann_date date, source_update_time timestamp,
            source_version_hash text, pit_contract_version text,
            availability_basis text,
            PRIMARY KEY(ts_code,end_date,ann_date,data_source))""")
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def execute_factor(connection, sql, relation):
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        cur.execute('SELECT * FROM features.' + relation)
        result = [{key: value for key,value in dict(row).items() if not key.startswith('_')}
                  for row in cur.fetchall()]
        cur.execute('DROP MATERIALIZED VIEW features.' + relation)
    return result


def insert_positions(connection, rows):
    with connection.cursor() as cur:
        cur.executemany('INSERT INTO rawdata.fund_portfolio VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', rows)


def positions(period, ann, count, *, value=10):
    return [(f'F{i}', ann, period, 'X', value, value, 1, None) for i in range(count)]


def fund_rows(connection, minimum=2):
    return execute_factor(connection, fund_holdings_sql(min_history_periods=minimum), 'mv_fund_holdings_quarterly')


def business(row):
    # A future event can legitimately close an earlier open validity interval.
    return {key:value for key,value in row.items() if key != 'query_end_date'}


@pytest.mark.parametrize('minimum', [2, 4])
def test_holdings_future_publications_leave_past_factors_unchanged(factor_db, minimum):
    insert_positions(factor_db, positions('2025-03-31', '2025-04-20', 2)
                     + positions('2025-06-30', '2025-07-20', 3))
    before = {row['ann_date']: business(row) for row in fund_rows(factor_db, minimum)}
    insert_positions(factor_db, positions('2025-09-30', '2025-10-20', 1)
                     + positions('2025-06-30', '2025-11-20', 1, value=100))
    after = {row['ann_date']: business(row) for row in fund_rows(factor_db, minimum)}
    assert all(after[ann] == row for ann,row in before.items())


def test_holdings_same_day_periods_have_one_nonnegative_window(factor_db):
    insert_positions(factor_db, positions('2025-03-31', '2025-07-20', 2)
                     + positions('2025-06-30', '2025-07-20', 3)
                     + positions('2025-06-30', '2025-08-01', 1, value=40))
    rows = sorted(fund_rows(factor_db), key=lambda row: row['ann_date'])
    assert len(rows) == 2
    assert all(row['end_date'] == date(2025, 6, 30) for row in rows)
    assert all(row['query_end_date'] >= row['query_start_date'] for row in rows)
    assert rows[0]['query_end_date'] == date(2025, 7, 31)
    assert rows[1]['fund_count_pctl_sample_count'] == 2
    assert rows[1]['total_holding_value'] == 60


def test_position_revisions_do_not_double_count_or_restore_obsolete_positive_values(factor_db):
    insert_positions(factor_db, positions('2025-03-31', '2025-04-20', 2)
                     + positions('2025-03-31', '2025-04-21', 1, value=20)
                     + positions('2025-03-31', '2025-04-22', 1, value=None))
    rows = {row['ann_date']: row for row in fund_rows(factor_db)}
    assert rows[date(2025,4,20)]['total_holding_value'] == 20
    assert rows[date(2025,4,21)]['total_holding_value'] == 30
    assert rows[date(2025,4,22)]['total_holding_value'] == 10
    assert rows[date(2025,4,22)]['fund_count'] == 1
    assert all(row['fund_count_pctl_sample_count'] == 1 for row in rows.values())
    assert all(row['fund_count_pctl'] is None and row['crowd_signal'] == 'NORMAL' for row in rows.values())


def test_holdings_rank_ties_match_percent_rank_on_asof_distinct_periods(factor_db):
    insert_positions(factor_db, positions('2025-03-31', '2025-04-20', 1)
                     + positions('2025-06-30', '2025-07-20', 2)
                     + positions('2025-09-30', '2025-10-20', 2))
    rows = {row['ann_date']: row for row in fund_rows(factor_db)}
    assert rows[date(2025,10,20)]['fund_count_pctl'] == .5


def insert_ah(connection, rows):
    with connection.cursor() as cur:
        cur.executemany("""INSERT INTO rawdata.stock_ahcomparison(ts_code,trade_date,ah_premium)
                           VALUES (%s,%s,%s)""", rows)


@pytest.mark.parametrize('minimum', [2, 60])
def test_ah_future_observations_do_not_change_past_ranks_or_signals(factor_db, minimum):
    dates = pd.bdate_range('2025-01-02', periods=65)
    insert_ah(factor_db, [('X', day.date(), index + 10) for index,day in enumerate(dates)])
    sql = ah_premium_sql(history_interval='1 year', min_observations=minimum)
    before = {row['trade_date']: row for row in execute_factor(factor_db, sql, 'mv_ah_premium_daily')}
    insert_ah(factor_db, [('X', '2025-06-01', -100)])
    after = {row['trade_date']: row for row in execute_factor(factor_db, sql, 'mv_ah_premium_daily')}
    assert all(after[day] == row for day,row in before.items())
    assert all(row['ah_premium_pctl'] is None and row['arbitrage_signal'] == 'NEUTRAL'
               for row in before.values() if row['ah_premium_pctl_sample_count'] < minimum)


def test_ah_calendar_window_boundary_leap_year_and_ties(factor_db):
    insert_ah(factor_db, [('X','2023-02-27',-100), ('X','2023-02-28',10),
                         ('X','2023-03-01',20), ('X','2024-02-29',20)])
    rows = execute_factor(factor_db, ah_premium_sql(history_interval='1 year', min_observations=2), 'mv_ah_premium_daily')
    target = next(row for row in rows if row['trade_date'] == date(2024,2,29))
    assert target['ah_premium_pctl_sample_count'] == 3
    assert target['ah_premium_pctl'] == .5


@pytest.mark.parametrize('minimum', [2, 4])
def test_holdings_exact_warmup_boundary_and_append_revision(factor_db, minimum):
    ends = ['2024-03-31','2024-06-30','2024-09-30','2024-12-31']
    anns = ['2024-04-20','2024-07-20','2024-10-20','2025-01-20']
    for number in range(minimum):
        insert_positions(factor_db, positions(ends[number],anns[number],number+1))
    rows = sorted(fund_rows(factor_db,minimum),key=lambda row:row['ann_date'])
    assert all(row['fund_count_pctl'] is None and row['crowd_signal']=='NORMAL' for row in rows[:-1])
    assert rows[-1]['fund_count_pctl'] == 1 and rows[-1]['fund_count_pctl_sample_count']==minimum
    insert_positions(factor_db,positions(ends[0],'2025-02-01',1,value=100))
    last = max(fund_rows(factor_db,minimum),key=lambda row:row['ann_date'])
    assert last['fund_count_pctl_sample_count']==minimum


@pytest.mark.parametrize('minimum',[2,60])
def test_ah_exact_warmup_threshold_and_null_observation(factor_db,minimum):
    dates=pd.bdate_range('2025-01-02',periods=minimum)
    insert_ah(factor_db,[('X',day.date(),index) for index,day in enumerate(dates)])
    insert_ah(factor_db,[('X','2025-01-01',None)])
    rows=execute_factor(factor_db,ah_premium_sql(history_interval='1 year',min_observations=minimum),'mv_ah_premium_daily')
    assert len(rows)==minimum
    last=max(rows,key=lambda row:row['trade_date'])
    assert last['ah_premium_pctl_sample_count']==minimum and last['ah_premium_pctl']==1
    assert all(row['ah_premium_pctl'] is None for row in rows if row['trade_date']<last['trade_date'])


@pytest.mark.parametrize('minimum',[2,60])
def test_actual_ah_rank_and_future_prefix_invariance(factor_db,minimum):
    path=Path(__file__).parents[1]/'fixtures/pit_audit_repair_ah_sample.json'
    source=json.loads(path.read_text(encoding='utf-8'))['targets']['LOCAL']['sample']['rows']
    assert len({(row['ts_code'],row['trade_date']) for row in source})==len(source), 'Sample must have one AH pair per stock/day'
    sql=ah_premium_sql(history_interval='1 year',min_observations=minimum)
    columns=['ts_code','trade_date','hk_code','name','close','hk_close','pct_chg','hk_pct_chg','ah_comparison','ah_premium']
    def load(rows):
        with factor_db.cursor() as cursor:
            cursor.executemany('INSERT INTO rawdata.stock_ahcomparison ('+','.join(columns)+') VALUES ('
                               +','.join(['%s']*len(columns))+')',
                               [tuple(row[column] for column in columns) for row in rows])
    load(source[:120])
    before={row['trade_date']:row for row in execute_factor(factor_db,sql,'mv_ah_premium_daily')}
    load(source[120:])
    after={row['trade_date']:row for row in execute_factor(factor_db,sql,'mv_ah_premium_daily')}
    assert all(after[day]==row for day,row in before.items())
    from dateutil.relativedelta import relativedelta
    observations=[(date.fromisoformat(row['trade_date']),Decimal(row['ah_premium'])) for row in source if row['ah_premium'] is not None]
    for day,row in after.items():
        history=[value for previous,value in observations if day-relativedelta(years=1)<=previous<=day]
        assert row['ah_premium_pctl_sample_count']==len(history)
        if len(history)<minimum:
            assert row['ah_premium_pctl'] is None and row['arbitrage_signal']=='NEUTRAL'
        else:
            expected=sum(value<row['ah_premium'] for value in history)/(len(history)-1)
            assert row['ah_premium_pctl']==pytest.approx(expected)


def actual_samples():
    return json.loads((Path(__file__).parents[1] / 'fixtures/pit_audit_repair_samples.json').read_text(encoding='utf-8'))


def actual_positions(period):
    return [(row['ts_code'], row['ann_date'], row['end_date'], row['symbol'],
             row['mkv'], row['amount'], row['stk_mkv_ratio'],
             datetime.fromisoformat(row['update_time']) if row['update_time'] else None)
            for row in actual_samples()['queries']['holdings_' + period]['rows']]


def test_actual_duplicate_fund_disclosures_are_deduplicated_at_final_event(factor_db):
    insert_positions(factor_db, actual_positions('2026-06-30'))
    rows = fund_rows(factor_db)
    final = max(rows, key=lambda row: row['ann_date'])
    assert final['fund_count'] == 2005
    assert final['total_holding_value'] == 33261146350
    assert final['ann_date'] == date(2026,8,31)


def test_actual_late_quarter_holdings_do_not_leak_into_july3_snapshot(factor_db):
    insert_positions(factor_db, actual_positions('2026-03-31') + actual_positions('2026-07-01'))
    before = next(row for row in fund_rows(factor_db) if row['ann_date'] == date(2026,7,3))
    insert_positions(factor_db, actual_positions('2026-06-30'))
    rows = fund_rows(factor_db)
    after = next(row for row in rows if row['ann_date'] == date(2026,7,3))
    assert business(before) == business(after)
    assert after['fund_count'] == 1
    assert after['total_holding_value'] == Decimal('7906879.26')
    assert after['fund_count_chg'] != -2004
    assert all(row['query_end_date'] >= row['query_start_date'] for row in rows)
    assert len({(row['ts_code'],row['ann_date']) for row in rows}) == len(rows)


class _ReadContext:
    def __init__(self, connection):
        self.connection = connection

    def query_dataframe(self, sql, params=None):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)
            return pd.DataFrame([dict(row) for row in cursor.fetchall()])


@pytest.mark.parametrize('reverse', [False, True])
def test_actual_balance_tie_is_stable_with_persisted_report_references(factor_db, monkeypatch, reverse):
    source = actual_samples()['queries']['balance_same_day']['rows']
    reports = [row for row in source if row['data_source'] == 'report']
    if reverse:
        reports.reverse()
    with factor_db.cursor() as cursor:
        for row in reports:
            cursor.execute('INSERT INTO pit.pit_balance_quarterly (' + ','.join(row) + ') VALUES ('
                           + ','.join(['%s'] * len(row)) + ')', tuple(row.values()))
    target = dict(next(row for row in source if row['data_source'] == 'express'))
    # Reconstruct missing derived fields for the fill call; never edit the
    # captured/production express row, whose value was already filled earlier.
    for field in ('tot_liab','total_cur_assets','total_cur_liab','inventories'):
        target[field] = None
    obj = PITBalanceQuarterlyManager()
    obj.logger = logging.getLogger('isolated_balance_reference')
    obj.context = _ReadContext(factor_db)
    monkeypatch.setattr(obj, '_exclude_industry_inapplicable_fields', lambda frame: frame)
    result = obj._fill_express_missing_fields(pd.DataFrame([target]))
    assert pd.isna(result.iloc[0].tot_liab)
    assert result.iloc[0].source_version_hash == target['source_version_hash']


def shareholder_rows(connection):
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(StockShareholderConcentrationMV.create_sql)
        cur.execute("""SELECT ts_code, ann_date, end_date, holder_num_prev,
                       holder_num_chg, holder_num_yoy_chg, concentration_signal
                       FROM features.mv_stock_shareholder_concentration
                       ORDER BY ts_code,end_date,ann_date""")
        result = [dict(row) for row in cur.fetchall()]
        cur.execute("DROP MATERIALIZED VIEW features.mv_stock_shareholder_concentration")
    return result


def insert_holders(connection, rows):
    with connection.cursor() as cur:
        cur.executemany("INSERT INTO rawdata.stock_holdernumber VALUES (%s,%s,%s,%s)", rows)


def test_actual_holder_sample_future_announcement_cannot_change_earlier_signal(factor_db):
    insert_holders(factor_db, [
        ('000001.SZ', '2024-02-28', '2024-03-22', 566200),
        ('000001.SZ', '2024-03-31', '2024-04-20', 567902),
    ])
    before = shareholder_rows(factor_db)
    insert_holders(factor_db, [('000001.SZ', '2024-03-29', '2024-05-22', 567902)])
    after = shareholder_rows(factor_db)
    target = next(row for row in after if row['end_date'] == date(2024, 3, 31))
    assert target['holder_num_prev'] == 566200
    assert target['holder_num_chg'] == 1702
    assert target['concentration_signal'] == -1
    assert target == next(row for row in before if row['end_date'] == date(2024, 3, 31))


def test_yoy_matches_same_period_and_latest_public_revision(factor_db):
    insert_holders(factor_db, [
        ('X', '2020-12-31', '2021-01-20', 100),
        ('X', '2020-12-31', '2022-01-10', 110),
        ('X', '2020-12-31', '2022-03-20', 900),
        ('X', '2021-02-26', '2021-03-01', 120),
        ('X', '2021-03-31', '2021-04-20', 130),
        ('X', '2021-06-30', '2021-07-20', 140),
        ('X', '2021-09-30', '2021-10-20', 150),
        ('X', '2021-12-31', '2022-02-01', 200),
    ])
    result = shareholder_rows(factor_db)
    target = next(row for row in result if row['end_date'] == date(2021, 12, 31))
    assert target['holder_num_yoy_chg'] == 90
    assert target['holder_num_prev'] == 150


def test_yoy_missing_period_and_leap_year(factor_db):
    insert_holders(factor_db, [
        ('X', '2023-02-28', '2023-03-01', 100),
        ('X', '2024-02-29', '2024-03-01', 120),
        ('X', '2024-03-31', '2024-04-20', 130),
    ])
    result = shareholder_rows(factor_db)
    assert next(row for row in result if row['end_date'] == date(2024, 2, 29))['holder_num_yoy_chg'] == 20
    assert next(row for row in result if row['end_date'] == date(2024, 3, 31))['holder_num_yoy_chg'] is None


class _UpsertHarness(DatabaseOperationsMixin):
    def __init__(self, pool):
        self.pool = pool
        self.resolver = SimpleNamespace(get_schema_and_table=lambda target: ('public', 'pitfix_update_time'))
        self.logger = logging.getLogger('isolated_upsert')
        self.copy_records_chunk_size = 2

    def _get_date_and_timestamp_columns_from_target(self, target):
        return set(), {'update_time'}


@pytest.mark.asyncio
@pytest.mark.parametrize('skip_unchanged', [False, True])
async def test_actual_base_task_upsert_updates_timestamp_only_on_payload_change(isolated_database_url, skip_unchanged):
    pool = await asyncpg.create_pool(isolated_database_url, min_size=1, max_size=1)
    created = False
    try:
        async with pool.acquire() as connection:
            assert await connection.fetchval("SELECT to_regclass('public.pitfix_update_time')") is None
            await connection.execute("CREATE TABLE public.pitfix_update_time (id int PRIMARY KEY, value numeric, update_time timestamp)")
            created = True
            old = datetime(2020, 1, 1)
            await connection.execute("INSERT INTO public.pitfix_update_time VALUES (1,10,$1)", old)
        harness = _UpsertHarness(pool)
        harness.skip_unchanged_upserts = skip_unchanged
        harness.sort_bulk_conflict_keys = skip_unchanged
        task = SimpleNamespace(db=harness, name='isolated_pitfix', table_name='pitfix_update_time',
                               primary_keys=['id'], use_insert_mode=False, auto_add_update_time=True,
                               logger=harness.logger)
        await BaseTask._save_to_database(task, pd.DataFrame({'id': [1], 'value': [10]}))
        async with pool.acquire() as connection:
            assert await connection.fetchval("SELECT update_time FROM public.pitfix_update_time WHERE id=1") == old
        await BaseTask._save_to_database(task, pd.DataFrame({'id': [1], 'value': [None]}))
        async with pool.acquire() as connection:
            changed = await connection.fetchrow("SELECT value,update_time FROM public.pitfix_update_time WHERE id=1")
            assert changed['value'] is None and changed['update_time'] > old
        await BaseTask._save_to_database(task, pd.DataFrame({'id': [1], 'value': [None]}))
        async with pool.acquire() as connection:
            assert await connection.fetchval("SELECT update_time FROM public.pitfix_update_time WHERE id=1") == changed['update_time']
        await BaseTask._save_to_database(task, pd.DataFrame({'id': [1], 'value': [20]}))
        async with pool.acquire() as connection:
            final = await connection.fetchrow("SELECT value,update_time FROM public.pitfix_update_time WHERE id=1")
            assert final['value'] == 20 and final['update_time'] > changed['update_time']
            before_insert = await connection.fetchval('SELECT LOCALTIMESTAMP')
        await BaseTask._save_to_database(task, pd.DataFrame({'id': [2], 'value': [10]}))
        async with pool.acquire() as connection:
            inserted = await connection.fetchval('SELECT update_time FROM public.pitfix_update_time WHERE id=2')
            after_insert = await connection.fetchval('SELECT LOCALTIMESTAMP')
            assert before_insert <= inserted <= after_insert
        await BaseTask._save_to_database(task, pd.DataFrame({'id': [2], 'value': [20]}))
        async with pool.acquire() as connection:
            assert await connection.fetchval('SELECT update_time FROM public.pitfix_update_time WHERE id=2') > inserted
    finally:
        if created:
            async with pool.acquire() as connection:
                await connection.execute("DROP TABLE public.pitfix_update_time")
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['explicit_requested','explicit_null','timestamp_none','timestamp_only','empty_updates'])
async def test_explicit_timestamp_and_opt_out_contracts(isolated_database_url,mode):
    pool=await asyncpg.create_pool(isolated_database_url,min_size=1,max_size=1)
    created=False
    old=datetime(2020,1,1)
    supplied=datetime(2021,1,1)
    try:
        async with pool.acquire() as connection:
            assert await connection.fetchval("SELECT to_regclass('public.pitfix_update_time')") is None
            await connection.execute('CREATE TABLE public.pitfix_update_time (id int PRIMARY KEY,value numeric,update_time timestamp)')
            created=True
            await connection.execute('INSERT INTO public.pitfix_update_time VALUES(1,10,$1)',old)
        harness=_UpsertHarness(pool)
        insert_stamp=None if mode=='explicit_null' else supplied
        data=pd.DataFrame({'id':[1,2],'value':[10,10],'update_time':[insert_stamp,insert_stamp]})
        updates={'explicit_requested':['value'],'explicit_null':['value'],'timestamp_none':['value','update_time'],
                 'timestamp_only':['update_time'],'empty_updates':[]}[mode]
        await harness.upsert(data,target='test',conflict_columns=['id'],update_columns=updates,
                             timestamp_column=None if mode=='timestamp_none' else 'update_time')
        async with pool.acquire() as connection:
            assert await connection.fetchval('SELECT update_time FROM public.pitfix_update_time WHERE id=2')==insert_stamp
            expected=supplied if mode=='timestamp_none' else old
            assert await connection.fetchval('SELECT update_time FROM public.pitfix_update_time WHERE id=1')==expected
        if mode=='explicit_requested':
            await harness.upsert(pd.DataFrame({'id':[1],'value':[20],'update_time':[supplied]}),target='test',
                                 conflict_columns=['id'],update_columns=['value'],timestamp_column='update_time')
            async with pool.acquire() as connection:
                changed=await connection.fetchrow('SELECT value,update_time FROM public.pitfix_update_time WHERE id=1')
                assert changed['value']==20 and changed['update_time']>supplied
    finally:
        if created:
            async with pool.acquire() as connection:
                await connection.execute('DROP TABLE public.pitfix_update_time')
        await pool.close()
