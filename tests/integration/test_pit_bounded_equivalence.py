"""Native equivalence on revisions, withdrawals, delayed NAVs and weight gaps."""
from datetime import date,datetime,timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values
import pytest
from alphahome.features.recipes.mv.fund.fund_holdings_quarterly import FundHoldingsQuarterlyMV
from alphahome.features.recipes.mv.index.index_fundamental_daily import IndexFundamentalDailyMV
from alphahome.features.recipes.mv.market.etf_flow_daily import ETFFlowDailyMV
from alphahome.features.recipes.mv.pit_asof_rank_sql import fund_holdings_sql

pytestmark=[pytest.mark.integration,pytest.mark.requires_db]

def query(sql):
    return sql.split(' AS',1)[1].replace('WITH NO DATA','').strip().rstrip(';')

def compare(cur,reference,bounded):
    cur.execute('WITH reference AS ('+query(reference)+'), bounded AS ('+query(bounded)+') SELECT count(*) FROM ((SELECT * FROM reference EXCEPT ALL SELECT * FROM bounded) UNION ALL (SELECT * FROM bounded EXCEPT ALL SELECT * FROM reference)) differences')
    assert cur.fetchone()[0]==0

def schemas(cur,tables):
    cur.execute("SET LOCAL statement_timeout='45s';CREATE SCHEMA IF NOT EXISTS tushare;CREATE SCHEMA IF NOT EXISTS rawdata")
    for table in tables:
        cur.execute('SELECT to_regclass(%s)',(table,));assert cur.fetchone()[0] is None

def test_bounded_index_preserves_weights_missing_values_and_pre_snapshot_gaps(isolated_database_url):
    c=psycopg2.connect(isolated_database_url)
    try:
        with c.cursor() as cur:
            schemas(cur,['tushare.stock_dailybasic','tushare.index_weight'])
            cur.execute('CREATE TABLE tushare.stock_dailybasic(ts_code text,trade_date date,pe_ttm numeric,pb numeric,dv_ratio numeric)')
            values=[(f'S{s}',date(2024,1,1)+timedelta(days=day),None if s%9==0 else s+day%10,Decimal(s)/10,Decimal(s)/20) for s in range(25) for day in range(90)]
            execute_values(cur,'INSERT INTO tushare.stock_dailybasic VALUES %s',values)
            cur.execute('CREATE TABLE tushare.index_weight(index_code text,con_code text,trade_date date,weight numeric,PRIMARY KEY(index_code,con_code,trade_date))')
            weights=[(code,f'S{s}',date(2024,1,15)+timedelta(days=k*22),None if s%7==0 else s+1) for code in ('000300.SH','000016.SH','OTHER') for k in range(3) for s in range(25)]
            execute_values(cur,'INSERT INTO tushare.index_weight VALUES %s',weights)
            recipe=IndexFundamentalDailyMV();compare(cur,recipe.create_sql,recipe.get_create_sql())
    finally:c.rollback();c.close()

def test_bounded_nav_preserves_delayed_old_announcements_and_missing_windows(isolated_database_url):
    c=psycopg2.connect(isolated_database_url)
    try:
        with c.cursor() as cur:
            schemas(cur,['tushare.fund_etf_basic','tushare.fund_nav','tushare.fund_share'])
            cur.execute('CREATE TABLE tushare.fund_etf_basic(ts_code text PRIMARY KEY,index_code text,name text,list_date date,status text)')
            cur.execute('CREATE TABLE tushare.fund_nav(ts_code text,nav_date date,ann_date date,unit_nav numeric,PRIMARY KEY(ts_code,nav_date))')
            cur.execute('CREATE TABLE tushare.fund_share(ts_code text,trade_date date,fd_share numeric,PRIMARY KEY(ts_code,trade_date))')
            cur.execute("INSERT INTO tushare.fund_etf_basic VALUES('F1','000016.SH','F1','2024-01-01','D'),('F2','000300.SH','F2','2024-01-05','L'),('F3','OTHER','F3','2024-01-01','L')")
            nav=[(f'F{f}',date(2024,1,1)+timedelta(days=k),None if k%13==0 else date(2024,1,1)+timedelta(days=k+(15 if k%5==0 else -2 if k%7==0 else 1)),None if k%11==0 else -1 if k%17==0 else Decimal(100+k)/100) for f in (1,2,3) for k in range(65)]
            shares=[(f'F{f}',date(2024,1,1)+timedelta(days=k),None if k%11==0 else 0 if k%17==0 else 100+k) for f in (1,2,3) for k in range(65)]
            execute_values(cur,'INSERT INTO tushare.fund_nav VALUES %s',nav);execute_values(cur,'INSERT INTO tushare.fund_share VALUES %s',shares)
            recipe=ETFFlowDailyMV();compare(cur,recipe.create_sql,recipe.get_create_sql())
    finally:c.rollback();c.close()

@pytest.mark.parametrize('warmup',[2,4])
def test_bounded_holdings_preserves_duplicate_key_winners_nulls_and_withdrawals(isolated_database_url,warmup):
    c=psycopg2.connect(isolated_database_url)
    try:
        with c.cursor() as cur:
            schemas(cur,['rawdata.fund_portfolio'])
            # Deliberately no PK: check generic recipe's deterministic tie
            # policy rather than relying on production's verified unique key.
            cur.execute('CREATE TABLE rawdata.fund_portfolio(ts_code text,ann_date date,end_date date,symbol text,mkv numeric,amount numeric,stk_mkv_ratio numeric,update_time timestamp)')
            positions=[]
            for stock in range(3):
                for period in range(8):
                    end=date(2020,1,1)+timedelta(days=90*period)
                    for fund in range(8):
                        positions.extend([
                            (f'F{fund}',end+timedelta(days=20+fund%3),end,f'S{stock}',100+fund,None if fund%3==0 else fund,None if fund%4==0 else Decimal(fund)/10,None),
                            (f'F{fund}',end+timedelta(days=33),end,f'S{stock}',None if fund==0 else 0 if fund==1 else -1 if fund==2 else 200+fund,None if fund%2==0 else fund*2,None if fund%3==0 else Decimal(8-fund)/10,None),
                            (f'F{fund}',end+timedelta(days=41),end,f'S{stock}',200+fund,fund,None if fund%2==0 else Decimal(fund)/20,None)])
                    # A later-received same-day withdrawal must beat a
                    # positive older observation. A NULL latest payload must
                    # suppress a positive payload too; same-clock ties use MD5.
                    positions.extend([
                        ('F7',end+timedelta(days=41),end,f'S{stock}',0,0,0,datetime(2024,1,2)),
                        ('F6',end+timedelta(days=41),end,f'S{stock}',None,None,None,datetime(2024,1,2)),
                        ('F5',end+timedelta(days=41),end,f'S{stock}',400,4,Decimal('9.8'),datetime(2024,1,2)),
                        ('F5',end+timedelta(days=41),end,f'S{stock}',450,5,Decimal('9.9'),datetime(2024,1,2)),
                        ('F4',end+timedelta(days=41),end,f'S{stock}',0,0,0,None)])
            execute_values(cur,'INSERT INTO rawdata.fund_portfolio VALUES %s',positions)
            compare(cur,fund_holdings_sql(min_history_periods=warmup),FundHoldingsQuarterlyMV(min_history_periods=warmup).get_create_sql())
    finally:c.rollback();c.close()
