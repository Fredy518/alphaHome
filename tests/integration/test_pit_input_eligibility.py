from dataclasses import replace
from datetime import date

import pytest

from alphahome.common.db_manager import DBManager
from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.repository import FactorRepository


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]
DAY = date(2026, 9, 11)


@pytest.mark.parametrize("missing_eligible", [False, True])
def test_only_current_eligible_input_gaps_block_p(isolated_database_url, missing_eligible):
    db = DBManager(isolated_database_url, mode="sync")
    if db.fetch_val_sync("SELECT to_regnamespace('pit') IS NOT NULL OR to_regnamespace('tushare') IS NOT NULL"):
        db.close_sync()
        pytest.skip("eligibility test needs unused pit/tushare schemas")
    db.execute_sync("""
        CREATE SCHEMA pit; CREATE SCHEMA tushare;
        CREATE TABLE pit.pit_income_quarterly (
            ts_code text, end_date date, ann_date date, data_source text, conversion_status text
        );
        CREATE TABLE pit.pit_balance_quarterly (
            ts_code text, end_date date, ann_date date, data_source text, tot_assets numeric
        );
        CREATE TABLE pit.pit_financial_indicators (
            ts_code text, end_date date, ann_date date, data_source text, calculation_status text, data_quality text
        );
        CREATE TABLE tushare.stock_basic (ts_code text PRIMARY KEY, list_date date, delist_date date);
        INSERT INTO pit.pit_income_quarterly
          SELECT code, '2026-06-30'::date, '2026-08-30'::date, 'report',
                 CASE WHEN code='U' THEN 'RPT_ORIG' ELSE 'converted' END
          FROM unnest(ARRAY['F','D','M','U','B','N','X']) code;
        INSERT INTO pit.pit_balance_quarterly
          SELECT ts_code, end_date, ann_date, data_source,
                 CASE WHEN ts_code='X' THEN 'NaN'::numeric ELSE 100 END
          FROM pit.pit_income_quarterly WHERE ts_code<>'B';
        INSERT INTO tushare.stock_basic
          SELECT code, CASE WHEN code='N' THEN '2026-10-01'::date ELSE '2020-01-01'::date END,
                 CASE WHEN code='D' THEN '2026-08-01'::date END
          FROM unnest(ARRAY['F','D','U','B','N','X']) code;
        INSERT INTO pit.pit_financial_indicators VALUES ('F','2026-06-30','2026-08-30','report','success','high');
    """)
    try:
        if missing_eligible:
            db.execute_sync("""
                INSERT INTO tushare.stock_basic VALUES ('A','2020-01-01',NULL);
                INSERT INTO pit.pit_income_quarterly VALUES ('A','2026-06-30','2026-08-30','report','converted');
                INSERT INTO pit.pit_balance_quarterly VALUES ('A','2026-06-30','2026-08-30','report',100);
            """)
        repository = FactorRepository(db)
        report = repository.financial_input_gaps(DAY)
        assert report["eligible_missing"] == int(missing_eligible)
        assert report["missing_balance"] == 2  # B has no row; X has NaN assets.
        assert report["delisted"] == report["missing_master"] == report["unconverted"] == report["not_yet_listed"] == 1
        contract = replace(FactorCoordinator.contracts()["factor_p"], source_tables=("pit.pit_financial_indicators",))
        blockers = repository.readiness(contract, DAY, [DAY], {})
        assert blockers == ([f"pit_input_eligibility:missing=1:{DAY.isoformat()}"] if missing_eligible else [])
    finally:
        db.execute_sync("DROP SCHEMA pit CASCADE; DROP SCHEMA tushare CASCADE")
        db.close_sync()
