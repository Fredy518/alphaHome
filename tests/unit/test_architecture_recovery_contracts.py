"""Regression contracts from the September 2026 architecture review."""

import asyncio
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from alphahome.pit.planning_time import frozen_pit_time
from alphahome.pit.pit_income_quarterly_manager import PITIncomeQuarterlyManager
from alphahome.pit.run_ledger import PITBaselineRequired, baseline_eligible
from alphahome.providers import AlphaDataTool, ValidationError


def test_maintenance_sql_does_not_force_drop_dependencies():
    from alphahome.common.maintenance_sql import rawdata_mapping_sql, recovery_schema_sql

    sql = rawdata_mapping_sql('stock_daily', 'tushare', 'stock_daily', ['ts_code', 'trade_date'])
    assert 'CREATE OR REPLACE VIEW rawdata."stock_daily"' in sql and 'DROP' not in sql
    assert 'SELECT "ts_code", "trade_date" FROM "tushare"."stock_daily"' in sql
    with pytest.raises(ValueError):
        rawdata_mapping_sql('unsafe; DROP TABLE', 'tushare', 'stock_daily')
    ledgers = recovery_schema_sql()
    assert 'pit.task_run' in ledgers and 'features.refresh_checkpoint' in ledgers
    assert 'UPDATE ' not in ledgers and 'DELETE ' not in ledgers


def test_pit_without_baseline_blocks_instead_of_silently_ignoring_history():
    manager = PITIncomeQuarterlyManager()
    calls = []

    def query(sql, params=None):
        calls.append(sql)
        return pd.DataFrame()

    manager.context = SimpleNamespace(query_dataframe=query)
    with frozen_pit_time(date(2026, 9, 22)), pytest.raises(PITBaselineRequired, match='baseline_required'):
        manager.resolve_incremental_date_range(7, (('tushare.fina_income', ('ann_date',), 'update_time'),))
    assert len(calls) == 1 and 'pit.task_run' in calls[0]


def test_pit_source_change_check_error_is_not_no_changes():
    manager = PITIncomeQuarterlyManager()

    def query(sql, params=None):
        if 'pit.task_run' in sql:
            return pd.DataFrame([{'last_success_local': datetime(2026, 9, 21), 'coverage_end': date(2026, 9, 21)}])
        raise RuntimeError('source unavailable')

    manager.context = SimpleNamespace(query_dataframe=query)
    with frozen_pit_time(date(2026, 9, 22)), pytest.raises(RuntimeError, match='source_change_check_failed'):
        manager.resolve_incremental_date_range(7, (('tushare.fina_income', ('ann_date',), 'update_time'),))


def test_manual_slice_does_not_initialize_a_complete_pit_baseline():
    assert baseline_eligible('full_backfill', date(2000, 1, 1), '2000-01-01')
    assert not baseline_eligible('manual_range', date(2000, 1, 1), '2000-01-01')
    assert not baseline_eligible('full_backfill', date(2026, 1, 1), '2000-01-01')


def test_public_provider_rejects_unimplemented_financial_semantics_before_query():
    class NoQuery:
        def fetch_sync(self, *args, **kwargs):
            raise AssertionError('Unsupported semantics must fail before querying')

    tool = AlphaDataTool(NoQuery())
    with pytest.raises(ValidationError, match='adjust=False'):
        tool.get_stock_data('A', '2026-09-01', '2026-09-22', adjust=True)
    with pytest.raises(ValidationError, match='current labels'):
        tool.get_industry_data('A', industry_type='SW2021')
    with pytest.raises(ValidationError, match='fields'):
        tool.get_stock_data('A', '2026-09-01', '2026-09-22', fields=['invented'])


@pytest.mark.parametrize('has_rows', [False, True])
def test_provider_fields_and_price_semantics_are_consistent_for_empty_results(has_rows):
    rows = [{'ts_code': 'A', 'trade_date': '2026-09-21', 'close': '10', 'open': '9'}] if has_rows else []
    tool = AlphaDataTool(SimpleNamespace(fetch_sync=lambda *args: rows))
    result = tool.get_stock_data('A', '2026-09-01', '2026-09-22', fields=['close'])
    assert list(result.columns) == ['ts_code', 'trade_date', 'close']
    assert result.attrs['price_adjustment'] == 'unadjusted'
    assert len(result) == int(has_rows)


async def test_mapping_failure_cannot_be_a_successful_collection():
    from alphahome.common.task_system.base_task import BaseTask

    class MappingTask(BaseTask):
        name = 'architecture_mapping_test'
        task_type = 'fetch'
        table_name = 'architecture_mapping_test'
        data_source = 'tushare'
        primary_keys = ['code']
        schema_def = {'code': {'type': 'TEXT'}}

        async def _fetch_data(self, **kwargs):
            return pd.DataFrame({'code': ['A']})

        def process_data(self, data, **kwargs):
            return data

    db = AsyncMock()
    db.table_exists.return_value = True
    db.create_rawdata_view.side_effect = RuntimeError('mapping mismatch')
    result = await MappingTask(db).execute()
    assert result['status'] == 'error'
    db.copy_from_dataframe.assert_not_awaited()
    db.create_rawdata_view.assert_awaited_once_with(
        view_name=MappingTask.table_name, source_schema='tushare', source_table=MappingTask.table_name,
        replace=True, verify_only=True,
    )


@pytest.mark.parametrize('raise_error', [False, True])
async def test_candidate_product_failure_is_visible_after_candidate_commit(monkeypatch, raise_error):
    from alphahome.gui.services import daily_update_service as service

    order = []

    def update(*args, **kwargs):
        order.append('candidate_committed')
        return {'status': 'succeeded', 'output_snapshot_id': 'new-version'}

    async def refresh(names, **kwargs):
        order.append('dependent_product')
        if raise_error:
            raise RuntimeError('dependent product failed')
        return {'status': 'error', 'fail_count': 1}

    monkeypatch.setattr(service.candidate_maintenance, 'execute_candidate_monthly_maintenance', update)
    monkeypatch.setattr(service.feature_service, 'handle_refresh_features', refresh)
    monkeypatch.setattr(service, '_database_url', lambda *_: 'postgresql://unused')
    result = await service._run_candidate_monthly(None, asyncio.Event(), date(2026, 9, 22))
    assert order == ['candidate_committed', 'dependent_product']
    assert result['status'] == 'partial_success' and result['candidate_status'] == 'succeeded'
