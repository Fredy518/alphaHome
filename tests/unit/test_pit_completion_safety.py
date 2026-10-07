from datetime import date,datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo
import logging
import pandas as pd
import pytest

from alphahome.common.price_quality import validate_ohlc,PriceDataQualityError
from alphahome.common.task_system.base_task import BaseTask
from alphahome.providers.availability import publication_cutoff,require_historical_evidence,HistoricalEvidenceError
from alphahome.providers.data_access import AlphaDataTool,DataAccessError
from alphahome.features.recipes.mv.fund.fund_holdings_quarterly import FundHoldingsQuarterlyMV
from alphahome.features.recipes.mv.market.ah_premium_daily import AHPremiumDailyMV


@pytest.mark.parametrize('values',[(100,200),(200,100),(None,100),(100,None)])
@pytest.mark.asyncio
async def test_conflicting_financial_response_aborts_before_any_write(values):
    save=AsyncMock()
    task=SimpleNamespace(primary_keys=['id'],reject_conflicting_primary_keys=True,
                         timestamp_column_name='update_time',logger=logging.getLogger('test'),_save_to_database=save)
    with pytest.raises(ValueError,match='conflicting_versions_unverified'):
        await BaseTask._save_data(task,pd.DataFrame({'id':[1,1],'value':values}),ensure_table=False)
    save.assert_not_called()


def test_recipe_entrypoints_adopt_configurable_recommended_policy():
    assert 'sample_count >= 4' in FundHoldingsQuarterlyMV().get_create_sql()
    assert 'sample_count >= 6' in FundHoldingsQuarterlyMV(min_history_periods=6).get_create_sql()
    ah=AHPremiumDailyMV().get_create_sql()
    assert "INTERVAL '1 year'" in ah and 'sample_count >= 60' in ah
    assert "INTERVAL '2 years'" in AHPremiumDailyMV(history_interval='2 years',min_observations=80).get_create_sql()


@pytest.mark.parametrize('ohlc',[(19706.11,19879.86,19706.11,761),(10,9,8,8.5),(10,12,11,11),(10,12,9,None),(0,12,9,10),(float('inf'),12,9,10)])
def test_bad_ohlc_is_unavailable_not_repaired(ohlc):
    frame=pd.DataFrame([dict(zip(['open','high','low','close'],ohlc))])
    before=frame.copy(deep=True)
    with pytest.raises(PriceDataQualityError): validate_ohlc(frame)
    pd.testing.assert_frame_equal(frame,before)


def test_valid_prices_and_documented_rounding_allowance():
    validate_ohlc(pd.DataFrame([{'open':10,'high':12,'low':9,'close':11},
                               {'open':10,'high':10,'low':9,'close':10.00001}]))


@pytest.mark.parametrize('source',['akshare.macro_fixed_asset_investment','macro_industrial_value_added','macro_retail_sales',
                                  'features.mv_etf_flow_daily','tushare.stock_monthly','stock_weekly',
                                  'pit.pit_etf_index_members_monthly','pit_etf_index_fapi_monthly','index_fundamental_daily'])
def test_incomplete_history_is_refused(source):
    with pytest.raises(HistoricalEvidenceError): require_historical_evidence(source)


def test_date_only_release_waits_until_end_of_day_and_verified_timestamp_is_respected():
    cutoff=publication_cutoff(date(2024,8,1))
    assert cutoff==datetime(2024,8,2,tzinfo=ZoneInfo('Asia/Shanghai'))
    stamped=datetime(2024,8,1,20,tzinfo=ZoneInfo('Asia/Shanghai'))
    assert publication_cutoff(date(2024,8,1),stamped)==stamped
    with pytest.raises(HistoricalEvidenceError): publication_cutoff(date(2024,8,1),stamped.replace(tzinfo=None))


def test_historical_feature_api_blocks_unverified_sources_before_query():
    db=SimpleNamespace(fetch_sync=lambda *args: pytest.fail('must not query an unverified history'))
    with pytest.raises(HistoricalEvidenceError): AlphaDataTool(db).get_feature_data('etf_flow_daily','2024-08-01')


def test_historical_event_api_does_not_expose_date_only_same_day_announcement():
    calls=[]
    db=SimpleNamespace(fetch_sync=lambda q,p: calls.append((q,p)) or [])
    frame=AlphaDataTool(db).get_feature_data('fund_holdings_quarterly','2024-08-01',['X'])
    assert 'ann_date < %s' in calls[0][0] and calls[0][1][0]==date(2024,8,1)
    assert frame.attrs['system_first_receipt_verified'] is False


def test_close_only_provider_still_rejects_inconsistent_other_ohlc():
    row={'ts_code':'X','trade_date':'2024-03-18','open':19706.11,'high':19879.86,'low':19706.11,'close':761}
    db=SimpleNamespace(fetch_sync=lambda *args:[row])
    tool=AlphaDataTool(db);tool._get_stock_table=lambda:'rawdata.stock_daily'
    with pytest.raises(DataAccessError,match='ohlc_unavailable_rows'): tool.get_stock_data('X','2024-03-18','2024-03-18',fields=['close'])
