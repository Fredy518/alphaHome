import logging
from datetime import datetime
from types import SimpleNamespace
import asyncpg
import pandas as pd
import psycopg2
import pytest
from alphahome.common.db_components.database_operations_mixin import DatabaseOperationsMixin
from alphahome.common.source_observations import CREATE_SQL
from alphahome.features.recipes.mv.market.etf_flow_daily import ETFFlowDailyMV
from alphahome.features.recipes.mv.stock.stock_industry_monthly_snapshot import StockIndustryMonthlySnapshotMV
from alphahome.features.recipes.mv.index.index_fundamental_daily import IndexFundamentalDailyMV

pytestmark=[pytest.mark.integration,pytest.mark.requires_db]


class Harness(DatabaseOperationsMixin):
    def __init__(self,pool):
        self.pool=pool
        self.resolver=SimpleNamespace(get_schema_and_table=lambda target:('public','source_archive_test'))
        self.logger=logging.getLogger('source_archive_test')


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['changed','missing_archive','failed_upsert'])
async def test_source_observations_and_raw_update_are_atomic(isolated_database_url,mode):
    pool=await asyncpg.create_pool(isolated_database_url,min_size=1,max_size=1)
    created_schema = created_source = created_archive = False
    try:
        async with pool.acquire() as c:
            created_schema = await c.fetchval("SELECT to_regnamespace('tushare')") is None
            await c.execute('CREATE SCHEMA IF NOT EXISTS tushare')
            assert await c.fetchval("SELECT to_regclass('public.source_archive_test')") is None
            assert await c.fetchval("SELECT to_regclass('tushare.financial_source_observation')") is None
            await c.execute('CREATE TABLE public.source_archive_test(id int PRIMARY KEY,value numeric CHECK(value>=0),update_time timestamp)')
            created_source = True
            await c.execute("INSERT INTO public.source_archive_test VALUES(1,10,'2020-01-01')")
            if mode!='missing_archive':
                await c.execute(CREATE_SQL)
                created_archive = True
        harness=Harness(pool)
        target=SimpleNamespace(archive_source_versions=True,schema_def={})
        if mode=='changed':
            await harness.upsert(pd.DataFrame({'id':[1],'value':[20]}),target=target,conflict_columns=['id'],update_columns=['value'],timestamp_column='update_time')
            await harness.upsert(pd.DataFrame({'id':[1],'value':[20]}),target=target,conflict_columns=['id'],update_columns=['value'],timestamp_column='update_time')
            async with pool.acquire() as c:
                rows=await c.fetch('SELECT observation_role,payload::text AS payload,received_at,availability_basis FROM tushare.financial_source_observation ORDER BY observation_id')
                assert len(rows)==2 and {r['observation_role'] for r in rows}=={'incoming_received','pre_change_retained_snapshot'}
                assert all(r['received_at'].year>=2026 and r['availability_basis']=='system_observed_not_publication' for r in rows)
                assert await c.fetchval('SELECT value FROM public.source_archive_test WHERE id=1')==20
        else:
            with pytest.raises(asyncpg.PostgresError):
                await harness.upsert(pd.DataFrame({'id':[1],'value':[-1 if mode=='failed_upsert' else 20]}),target=target,conflict_columns=['id'],update_columns=['value'],timestamp_column='update_time')
            async with pool.acquire() as c:
                assert await c.fetchval('SELECT value FROM public.source_archive_test WHERE id=1')==10
                if mode=='failed_upsert': assert await c.fetchval('SELECT count(*) FROM tushare.financial_source_observation')==0
    finally:
        async with pool.acquire() as c:
            if created_source:
                await c.execute('DROP TABLE public.source_archive_test')
            if created_archive:
                await c.execute('DROP TABLE tushare.financial_source_observation')
            if created_schema:
                await c.execute('DROP SCHEMA tushare')
        await pool.close()


def test_etf_nav_publication_boundary_no_unit_nav_invention_and_delisting_history(isolated_database_url):
    c=psycopg2.connect(isolated_database_url)
    try:
        with c.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS tushare;CREATE SCHEMA IF NOT EXISTS features')
            for t in ('fund_share','fund_nav','fund_etf_basic'):
                cur.execute('SELECT to_regclass(%s)',('tushare.'+t,));assert cur.fetchone()[0] is None
            cur.execute('CREATE TABLE tushare.fund_etf_basic(ts_code text,index_code text,name text,list_date date,status text)')
            cur.execute('CREATE TABLE tushare.fund_share(ts_code text,trade_date date,fd_share numeric)')
            cur.execute('CREATE TABLE tushare.fund_nav(ts_code text,nav_date date,ann_date date,unit_nav numeric)')
            cur.execute("INSERT INTO tushare.fund_etf_basic VALUES('X','000016.SH','X','2024-01-01','D')")
            cur.execute("INSERT INTO tushare.fund_share VALUES('X','2024-08-01',100),('X','2024-08-02',120),('X','2024-08-05',130)")
            cur.execute("INSERT INTO tushare.fund_nav VALUES('X','2024-07-31','2024-08-01',2.4641),('X','2024-08-01','2024-08-02',2.4485)")
            cur.execute(ETFFlowDailyMV().get_create_sql().replace('WITH NO DATA','WITH DATA'))
            cur.execute('SELECT trade_date,total_aum,total_net_flow,etf_count,_pit_eligible FROM features.mv_etf_flow_daily ORDER BY trade_date')
            rows=cur.fetchall()
            assert rows[0][1] is None and rows[0][2] is None
            assert rows[1][1]==120*__import__('decimal').Decimal('2.4641')/10000
            assert rows[2][1]==130*__import__('decimal').Decimal('2.4485')/10000
            assert all(r[3]==1 and r[4] is False for r in rows)
            cur.execute('SELECT net_flow_5d,net_flow_20d,net_flow_avg20 FROM features.mv_etf_flow_daily')
            assert all(all(v is None for v in row) for row in cur.fetchall())
    finally:
        c.rollback();c.close()


@pytest.mark.parametrize('day,expected',[('2026-10-06','2026-09-30'),('2026-10-31','2026-10-31'),('2026-03-01','2026-02-28')])
def test_month_series_contains_only_ended_months(isolated_database_url,day,expected):
    sql=StockIndustryMonthlySnapshotMV().get_create_sql()
    series=sql[sql.index('month_series AS (')+len('month_series AS ('):sql.index('),',sql.index('month_series AS ('))]
    with psycopg2.connect(isolated_database_url) as c:
        with c.cursor() as cur:
            cur.execute('SELECT max(obs_date)::text FROM ('+series.replace('CURRENT_DATE',"DATE '"+day+"'")+') month_series')
            assert cur.fetchone()[0]==expected


def test_index_statistical_weight_output_is_explicitly_uncertified(isolated_database_url):
    with psycopg2.connect(isolated_database_url) as c:
        try:
            with c.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS tushare;CREATE SCHEMA IF NOT EXISTS features')
                for name in ('tushare.index_weight','tushare.stock_dailybasic','features.mv_index_fundamental_daily'):
                    cur.execute('SELECT to_regclass(%s)',(name,));assert cur.fetchone()[0] is None
                cur.execute('CREATE TABLE tushare.index_weight(index_code text,con_code text,trade_date date,weight numeric)')
                cur.execute('CREATE TABLE tushare.stock_dailybasic(ts_code text,trade_date date,pe_ttm numeric,pb numeric,dv_ratio numeric)')
                cur.execute("INSERT INTO tushare.index_weight VALUES('000300.SH','A','2024-01-31',60),('000300.SH','B','2024-01-31',40),('000300.SH','FUTURE','2024-03-31',100)")
                cur.execute("INSERT INTO tushare.stock_dailybasic VALUES('A','2024-02-01',10,2,3),('B','2024-02-01',20,4,1)")
                cur.execute(IndexFundamentalDailyMV().get_create_sql().replace('WITH NO DATA','WITH DATA'))
                cur.execute('SELECT constituent_count,_pit_eligible,_pit_limitations FROM features.mv_index_fundamental_daily')
                row=cur.fetchone();assert row[0]==2 and row[1] is False and 'publication_and_vintages_unverified' in row[2]
        finally:c.rollback()
