from unittest.mock import Mock

import pandas as pd
import pytest

from alphahome.pit.disclosure import DISCLOSURE_COLUMNS
from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager

FIELDS = ['tot_liab', 'total_cur_assets', 'total_cur_liab', 'inventories']


def reference(end, ann, value, *, source='report', stamp=None, version=None):
    return dict(ts_code='002883.SZ', end_date=end, ann_date=ann,
                data_source=source, source_update_time=stamp, source_version_hash=version,
                **{field: value for field in FIELDS})


def manager(monkeypatch, history=None):
    obj = PITBalanceQuarterlyManager()
    obj.logger = Mock()
    obj.context = Mock()
    obj.context.query_dataframe.return_value = pd.DataFrame(history or [])
    monkeypatch.setattr(obj, '_exclude_industry_inapplicable_fields', lambda frame: frame)
    monkeypatch.setattr(obj, '_get_table_columns', lambda *args: set(FIELDS + list(DISCLOSURE_COLUMNS)))
    return obj


@pytest.mark.parametrize('reverse', [False, True])
def test_actual_same_day_report_period_tie_keeps_latest_period_null(monkeypatch, reverse):
    reports = [reference('2016-12-31', '2017-06-06', 82449226.98),
               reference('2017-03-31', '2017-06-06', None)]
    if reverse:
        reports.reverse()
    obj = manager(monkeypatch)
    result = obj._fill_express_missing_fields(pd.DataFrame(
        reports + [reference('2017-06-30', '2017-07-21', None, source='express')]
    ))
    assert result is not None
    assert pd.isna(result.loc[result.data_source.eq('express'), 'tot_liab']).all()
    sql = obj.context.query_dataframe.call_args.args[0]
    assert 'end_date' in sql and 'source_version_hash' in sql
    assert "pit_contract_version='public_disclosure_v2'" in sql


def test_persisted_and_batch_references_compete_without_batch_preference(monkeypatch):
    newest = reference('2026-03-31', '2026-04-20', 200)
    obj = manager(monkeypatch, [newest])
    target = reference('2026-06-30', '2026-07-20', None, source='express')
    with_batch = obj._fill_express_missing_fields(pd.DataFrame([
        reference('2025-12-31', '2026-03-20', 100), target,
    ]))
    without_batch = obj._fill_express_missing_fields(pd.DataFrame([target]))
    assert with_batch.iloc[-1].tot_liab == without_batch.iloc[-1].tot_liab == 200


def test_fill_excludes_later_announcement_and_later_report_period(monkeypatch):
    obj = manager(monkeypatch, [
        reference('2026-03-31', '2026-04-20', 100),
        reference('2026-03-31', '2026-08-01', 200),
        reference('2026-09-30', '2026-07-01', 300),
    ])
    result = obj._fill_express_missing_fields(pd.DataFrame([
        reference('2026-06-30', '2026-07-20', None, source='express')
    ]))
    assert result.iloc[0].tot_liab == 100


@pytest.mark.parametrize('reverse', [False, True])
def test_same_event_versions_are_stable_under_input_order(monkeypatch, reverse):
    reports = [reference('2026-03-31', '2026-04-20', 100, stamp='2026-04-21', version='a'),
               reference('2026-03-31', '2026-04-20', 200, stamp='2026-04-22', version='b')]
    if reverse:
        reports.reverse()
    obj = manager(monkeypatch)
    result = obj._fill_express_missing_fields(pd.DataFrame(reports + [
        reference('2026-06-30', '2026-07-20', None, source='express')
    ]))
    assert result.iloc[-1].tot_liab == 200


def test_batch_and_database_use_same_configured_lookback(monkeypatch):
    obj = manager(monkeypatch)
    obj.table_config = dict(obj.table_config, BALANCE_EXPRESS_FILL_LOOKBACK_MONTHS=1)
    result = obj._fill_express_missing_fields(pd.DataFrame([
        reference('2026-03-31', '2026-04-20', 100),
        reference('2026-06-30', '2026-07-20', None, source='express'),
    ]))
    assert pd.isna(result.iloc[-1].tot_liab)


def test_complete_batch_fill_returns_dataframe_and_preserves_existing_field(monkeypatch):
    obj = manager(monkeypatch)
    target = reference('2026-06-30', '2026-07-20', None, source='express')
    target['inventories'] = 7
    result = obj._fill_express_missing_fields(pd.DataFrame([
        reference('2026-03-31', '2026-04-20', 100), target,
    ]))
    assert result is not None
    assert result.iloc[-1].tot_liab == 100
    assert result.iloc[-1].inventories == 7


def test_legacy_null_repair_uses_same_period_tie_rule_and_field_protection(monkeypatch):
    obj = manager(monkeypatch)
    target = reference('2017-06-30', '2017-07-21', None, source='express')
    target['inventories'] = 7
    keys = pd.DataFrame([{k:target[k] for k in ('ts_code','end_date','ann_date')}])
    baseline = pd.DataFrame([{'report_total':2,'report_nulls':1}])
    references = [reference('2016-12-31', '2017-06-06', 82449226.98),
                  reference('2017-03-31', '2017-06-06', None)]
    obj.context.query_dataframe.side_effect = [baseline,keys,pd.DataFrame([target]),pd.DataFrame(references),baseline]
    result = obj.fix_missing_express_fields(start_date='2017-01-01',end_date='2017-12-31')
    assert result == {'scanned':1,'updated':0}
    obj.context.db_manager.execute_sync.assert_not_called()


def test_legacy_null_repair_never_overwrites_an_existing_express_field(monkeypatch):
    obj = manager(monkeypatch)
    target = reference('2026-06-30','2026-07-20',None,source='express')
    target['inventories'] = 7
    keys = pd.DataFrame([{k:target[k] for k in ('ts_code','end_date','ann_date')}])
    baseline = pd.DataFrame([{'report_total':1,'report_nulls':0}])
    obj.context.query_dataframe.side_effect = [baseline,keys,pd.DataFrame([target]),
        pd.DataFrame([reference('2026-03-31','2026-04-20',100)]),baseline]
    result = obj.fix_missing_express_fields(start_date='2026-01-01',end_date='2026-12-31')
    assert result == {'scanned':1,'updated':1}
    sql,params = obj.context.db_manager.execute_sync.call_args.args
    assert 'tot_liab=COALESCE(tot_liab, %s)' in sql and 'inventories=' not in sql
    assert params[-3:] == ('002883.SZ','2026-06-30','2026-07-20')
