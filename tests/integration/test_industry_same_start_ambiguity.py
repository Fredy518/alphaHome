"""Native interval ambiguity must be independent of source insertion order."""
import psycopg2
import pytest
import logging
from datetime import date
from pathlib import Path
import pandas as pd
from psycopg2.extras import RealDictCursor, execute_values
from alphahome.features.recipes.mv.stock.stock_industry_monthly_snapshot import StockIndustryMonthlySnapshotMV
from alphahome.factors.core.data_repository import IndustryDataUnavailable, PFactorDataRepository
from alphahome.pit.pit_industry_classification_manager import PITIndustryClassificationManager
from alphahome.pit.calculators.industry_fapi_calculator import IndustryFAPICalculator
from alphahome.pit.calculators.industry_fttm_calculator import IndustryFTTMCalculator
pytestmark=[pytest.mark.integration,pytest.mark.requires_db]

@pytest.mark.parametrize('reverse',[False,True])
def test_same_start_conflict_is_flagged_until_a_unique_vintage_is_active(isolated_database_url,reverse):
    c=psycopg2.connect(isolated_database_url)
    try:
        with c.cursor() as q:
            q.execute('CREATE SCHEMA IF NOT EXISTS rawdata;CREATE SCHEMA IF NOT EXISTS features')
            for name in ('rawdata.index_swmember','rawdata.index_cimember','features.mv_stock_industry_monthly_snapshot'):
                q.execute('SELECT to_regclass(%s)',(name,));assert q.fetchone()[0] is None
            definition='(ts_code text,l1_name text,l2_name text,l3_name text,l1_code text,l2_code text,l3_code text,in_date date,out_date date)'
            q.execute('CREATE TABLE rawdata.index_swmember '+definition);q.execute('CREATE TABLE rawdata.index_cimember '+definition)
            rows=[('X','A','A2','A3','A','A2','A3','2017-01-03','2019-12-01'),('X','B','B2','B3','B','B2','B3','2017-01-03',None),('Y','S','S2','S3','S','S2','S3','2017-01-03',None),('Y','S','S2','S3','S','S2','S3','2017-01-03',None)]
            for row in reversed(rows) if reverse else rows:q.execute('INSERT INTO rawdata.index_cimember VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',row)
            q.execute(StockIndustryMonthlySnapshotMV().get_create_sql())
            q.execute("SELECT attname,format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid='features.mv_stock_industry_monthly_snapshot'::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum")
            types=dict(q.fetchall())
            assert types['ts_code']=='character varying(30)'
            assert types['industry_level1']=='character varying(50)'
            assert types['industry_level3']=='character varying(100)'
            assert types['industry_code1']=='character varying(20)'
            q.execute("SELECT industry_level1,industry_code1,data_quality,requires_special_gpa_handling,gpa_calculation_method,special_handling_reason FROM features.mv_stock_industry_monthly_snapshot WHERE ts_code='X' AND obs_date=DATE '2019-11-30'")
            row=q.fetchone();assert row[:5]==(None,None,'ambiguous_same_start_classification',True,'null') and 'public_vintage_unverified' in row[5]
            q.execute("SELECT industry_level1,data_quality FROM features.mv_stock_industry_monthly_snapshot WHERE ts_code='X' AND obs_date=DATE '2019-12-31'");assert q.fetchone()==('B','normal')
            q.execute("SELECT industry_level1,data_quality FROM features.mv_stock_industry_monthly_snapshot WHERE ts_code='Y' AND obs_date=DATE '2019-11-30'");assert q.fetchone()==('S','normal')
    finally:c.rollback();c.close()


@pytest.mark.parametrize('source', ['sw', 'ci'])
@pytest.mark.parametrize('reverse', [False, True])
def test_pit_quarantine_function_and_membership_consumer_closed_loop(isolated_database_url, source, reverse):
    c=psycopg2.connect(isolated_database_url)
    try:
        with c.cursor() as q:
            q.execute('CREATE SCHEMA IF NOT EXISTS tushare;CREATE SCHEMA IF NOT EXISTS pit;CREATE SCHEMA consumer_f16_test')
            for table in ('tushare.index_swmember','tushare.index_cimember','pit.pit_industry_classification'):
                q.execute('SELECT to_regclass(%s)',(table,));assert q.fetchone()[0] is None
            definition='(ts_code varchar(20),l1_name varchar(128),l2_name varchar(128),l3_name varchar(128),l1_code varchar(32),l2_code varchar(32),l3_code varchar(32),in_date date,out_date date)'
            q.execute('CREATE TABLE tushare.index_swmember '+definition)
            q.execute('CREATE TABLE tushare.index_cimember '+definition)
            rows=[('X','A','A2','A3','A','A2','A3','2017-01-03','2019-12-01'),('X','B','B2','B3','B','B2','B3','2017-01-03',None),('Y','S','S2','S3','S','S2','S3','2017-01-03',None)]
            for row in reversed(rows) if reverse else rows:
                q.execute('INSERT INTO tushare.index_'+('sw' if source=='sw' else 'ci')+'member VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',row)
            ddl=Path(__file__).resolve().parents[2]/'alphahome/pit/database/create_pit_industry_classification_table.sql'
            q.execute(ddl.read_text(encoding='utf-8'))
            q.execute("ALTER TABLE pit.pit_industry_classification ADD CONSTRAINT native_quality_values CHECK(data_quality IN ('high','normal','low','invalid'))")
            q.execute("""CREATE FUNCTION consumer_f16_test.get_industry_classification_batch_pit_optimized(text[],date,varchar)
            RETURNS TABLE(ts_code varchar,obs_date date,data_source varchar,industry_level1 varchar,industry_level2 varchar,industry_level3 varchar,requires_special_gpa_handling boolean,gpa_calculation_method varchar,special_handling_reason text)
            LANGUAGE SQL AS $$ SELECT DISTINCT ON(c.ts_code) c.ts_code,c.obs_date,c.data_source,c.industry_level1,c.industry_level2,c.industry_level3,c.requires_special_gpa_handling,c.gpa_calculation_method,c.special_handling_reason
            FROM pit.pit_industry_classification c WHERE c.ts_code=ANY($1) AND c.obs_date<=$2 AND c.data_source=$3 ORDER BY c.ts_code,c.obs_date DESC $$""")
            q.execute('SET LOCAL search_path=consumer_f16_test,public')

        class Context:
            db_manager=object()
            def query_dataframe(self,sql,params):
                with c.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(sql,params)
                    return pd.DataFrame([dict(x) for x in cursor.fetchall()])

        context=Context()
        manager=PITIndustryClassificationManager.__new__(PITIndustryClassificationManager)
        manager.context=context;manager.logger=logging.getLogger('isolated_consumer_f16')
        records=manager._generate_industry_snapshot(source,date(2019,11,30))
        fields=list(records[0])
        with c.cursor() as q:
            execute_values(q,'INSERT INTO pit.pit_industry_classification ('+','.join(fields)+') VALUES %s',[tuple(row[k] for k in fields) for row in records])
        frame=context.query_dataframe('SELECT * FROM pit.pit_industry_classification ORDER BY ts_code',())
        assert frame.loc[frame.ts_code.eq('X'),'data_quality'].iloc[0]=='invalid'
        repository=PFactorDataRepository(context,manager.logger)
        with pytest.raises(IndustryDataUnavailable,match='industry_quality_invalid'):
            repository.industry_classification(['X'],'2019-11-30')
        valid=repository.industry_classification(['Y'],'2019-11-30').iloc[0]
        assert valid['data_source']==source
        assert valid['industry_level1']=='S'
        for calculator in (IndustryFAPICalculator(),IndustryFTTMCalculator()):
            members=calculator._prepare_members(frame)
            assert 'X' not in set(members.ts_code)
            assert (set(members.ts_code)=={'Y'}) if source=='sw' else members.empty
    finally:
        c.rollback();c.close()
