from io import BytesIO
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from alphahome.fetchers.tasks.fund import szse_share_reference as ref
from alphahome.fetchers.tasks.fund import tushare_fund_share as task_module

DAYS = pd.bdate_range('2026-09-10', '2026-09-23')


def frame(rows):
    return pd.DataFrame(rows, columns=['ts_code', 'trade_date', 'fd_share'])


def reconcile(data, reference, start='20260914', end='20260918'):
    return ref.reconcile_share_dates(data, reference, DAYS, start, end)


def test_mixed_date_offsets_are_resolved_per_record():
    source = frame([
        ('159205.SZ', '20260915', 69305.6064), # already correctly labeled
        ('159205.SZ', '20260917', 69905.6064), # previous business day
        ('159205.SZ', '20260918', 67905.6064),
        ('159205.SZ', '20260921', 66305.6064), # Friday across weekend
        ('510300.SH', '20260918', 8),
    ])
    official = frame([
        ('159205.SZ', '20260915', 69305.6064),
        ('159205.SZ', '20260916', 69905.6064),
        ('159205.SZ', '20260917', 67905.6064),
        ('159205.SZ', '20260918', 66305.6064),
    ])
    result = reconcile(source, official)
    assert list(result[result.ts_code == '159205.SZ'].fd_share) == [Decimal('69305.61'), Decimal('69905.61'), Decimal('67905.61'), Decimal('66305.61')]
    assert result[result.ts_code == '510300.SH'].trade_date.iloc[0] == pd.Timestamp('20260918')


@pytest.mark.parametrize('seed', range(4))
def test_shuffle_batch_and_future_append_do_not_change_old_results(seed):
    source = frame([('159205.SZ', day, value) for day, value in [('20260915', 1), ('20260916', 2), ('20260917', 3), ('20260918', 4), ('20260921', 5)]])
    official = frame([('159205.SZ', day, value) for day, value in [('20260914', 1), ('20260915', 2), ('20260916', 3), ('20260917', 4), ('20260918', 5)]])
    expected = reconcile(source, official)
    pd.testing.assert_frame_equal(expected, reconcile(source.sample(frac=1, random_state=seed), official.sample(frac=1, random_state=seed)))
    future_source = pd.concat([source, frame([('159205.SZ', '20260923', 99999)])], ignore_index=True)
    future_ref = pd.concat([official, frame([('159205.SZ', '20260922', 99999)])], ignore_index=True)
    pd.testing.assert_frame_equal(expected, reconcile(future_source, future_ref))
    batches = [reconcile(source, official, str(day.date()), str(day.date())) for day in pd.bdate_range('20260914', '20260918')]
    pd.testing.assert_frame_equal(expected, ref.normalize_share_rows(pd.concat(batches)))


def test_constant_values_are_certified_by_each_reference_date():
    source = frame([('159205.SZ', '20260915', 4), ('159205.SZ', '20260916', 4)])
    official = frame([('159205.SZ', '20260914', 4), ('159205.SZ', '20260915', 4)])
    assert len(reconcile(source, official, '20260914', '20260915')) == 2


@pytest.mark.parametrize('column', ['source', 'reference'])
@pytest.mark.parametrize('reverse', [False, True])
def test_same_key_conflicts_never_pick_input_order(column, reverse):
    source = frame([('159205.SZ', '20260915', 1)])
    official = source.copy()
    conflicting = frame([('159205.SZ', '20260915', 1), ('159205.SZ', '20260915', 2)])
    if reverse: conflicting = conflicting.iloc[::-1]
    if column == 'source': source = conflicting
    else: official = conflicting
    with pytest.raises(ValueError, match='conflicting_same_key'):
        reconcile(source, official)


def test_identical_duplicates_are_safe_and_round_to_database_precision():
    rows = frame([('159205.SZ', '20260915', '1.125'), ('159205.SZ', '20260915', '1.13')])
    result = reconcile(rows, rows)
    assert len(result) == 1 and result.fd_share.iloc[0] == Decimal('1.13')


def test_zero_withdrawal_is_retained_instead_of_previous_positive_value():
    source = frame([('159205.SZ', '20260915', 10), ('159205.SZ', '20260916', 0)])
    official = frame([('159205.SZ', '20260914', 10), ('159205.SZ', '20260915', 0)])
    result = reconcile(source, official, '20260914', '20260915')
    assert result.fd_share.iloc[-1] == Decimal('0.00')


@pytest.mark.parametrize('amount', [None, float('nan'), float('inf'), -1])
def test_missing_or_invalid_amount_cannot_certify_or_withdraw(amount):
    with pytest.raises(ValueError, match='amount'):
        reconcile(frame([('159205.SZ', '20260915', amount)]), frame([('159205.SZ', '20260915', 0)]))


def test_delayed_reference_does_not_accept_unverified_latest_date():
    source = frame([('159205.SZ', '20260915', 1), ('159205.SZ', '20260916', 2)])
    official = frame([('159205.SZ', '20260915', 1)])
    with pytest.raises(ValueError, match='reference_gap'):
        reconcile(source, official)


def test_missing_reference_fails_closed():
    with pytest.raises(ValueError, match='reference_missing'):
        reconcile(frame([('159205.SZ', '20260915', 1)]), frame([]))


def test_calendar_missing_cannot_fall_back_to_natural_days():
    rows = frame([('159205.SZ', '20260915', 1)])
    with pytest.raises(ValueError, match='calendar_missing'):
        ref.reconcile_share_dates(rows, rows, [], '20260914', '20260918')


def test_distant_matching_value_is_not_evidence_of_one_day_shift():
    source = frame([('159205.SZ', '20260921', 1)])
    official = frame([('159205.SZ', '20260914', 1)])
    with pytest.raises(ValueError, match='value_unmatched'):
        reconcile(source, official)


@pytest.mark.parametrize('exchange_code', ['159205', '159205  ', ' 159205\t'])
def test_exchange_loader_units_range_and_no_code_leak(monkeypatch, exchange_code):
    calls = []
    def get(url, **kwargs):
        calls.append(kwargs['params'])
        day = kwargs['params']['txtStart']
        buffer = BytesIO()
        pd.DataFrame({'日期': [day, day], '基金代码': [exchange_code, '159999'], '基金规模(份)': ['10,123.5', '90000']}).to_excel(buffer, index=False)
        return Mock(content=buffer.getvalue(), raise_for_status=Mock())
    monkeypatch.setattr(ref.requests, 'get', get)
    rows = ref.load_szse_reference('20260830', '20260902', ['159205.SZ'])
    assert [(c['txtStart'], c['txtEnd']) for c in calls] == [('2026-08-30', '2026-08-31'), ('2026-09-01', '2026-09-02')]
    assert set(rows.ts_code) == {'159205.SZ'}
    assert list(rows.fd_share) == [Decimal('1.01'), Decimal('1.01')]


def test_exchange_http_failure_does_not_try_other_routes(monkeypatch):
    get = Mock(side_effect=ref.requests.Timeout())
    monkeypatch.setattr(ref.requests, 'get', get)
    with pytest.raises(ref.requests.Timeout):
        ref.load_szse_reference('20260914', '20260918', ['159205.SZ'])
    assert get.call_count == 1


@pytest.mark.asyncio
async def test_fetch_batch_uses_exchange_dates_with_boundary_expansion(monkeypatch):
    task = object.__new__(task_module.TushareFundShareTask)
    task.logger = Mock()
    raw = frame([('159205.SZ', '20260921', 1)])
    official = frame([('159205.SZ', '20260918', 1)])
    fetch = AsyncMock(return_value=raw)
    monkeypatch.setattr(task_module.TushareTask, 'fetch_batch', fetch)
    monkeypatch.setattr(task_module, 'get_last_trade_day', AsyncMock(return_value='20260917'))
    monkeypatch.setattr(task_module, 'get_next_trade_day', AsyncMock(return_value='20260921'))
    monkeypatch.setattr(task_module, 'get_trade_cal', AsyncMock(return_value=pd.DataFrame({'cal_date': ['20260917', '20260918', '20260921'], 'is_open': [1, 1, 1]})))
    monkeypatch.setattr(task_module, 'load_szse_reference', Mock(return_value=official))
    result = await task.fetch_batch({'trade_date': '20260918'})
    assert fetch.call_args.args[0] == {'start_date': '20260917', 'end_date': '20260921'}
    assert result.trade_date.iloc[0] == pd.Timestamp('20260918')
    assert task.archive_source_versions is True


@pytest.mark.asyncio
async def test_sh_only_collection_does_not_depend_on_szse(monkeypatch):
    task = object.__new__(task_module.TushareFundShareTask)
    fetch = AsyncMock(return_value=frame([('510300.SH', '20260918', 1)]))
    monkeypatch.setattr(task_module.TushareTask, 'fetch_batch', fetch)
    loader = Mock(side_effect=AssertionError('must not query SZSE'))
    monkeypatch.setattr(task_module, 'load_szse_reference', loader)
    result = await task.fetch_batch({'ts_code': '510300.SH', 'trade_date': '20260918'})
    assert len(result) == 1 and not loader.called


@pytest.mark.asyncio
async def test_reit_collection_does_not_apply_etf_date_or_value_contract(monkeypatch):
    task = object.__new__(task_module.TushareFundShareTask)
    raw = frame([('180402.SZ', '20260918', 20000)])
    fetch = AsyncMock(return_value=raw)
    monkeypatch.setattr(task_module.TushareTask, 'fetch_batch', fetch)
    loader = Mock(side_effect=AssertionError('ETF contract does not apply to REITs'))
    monkeypatch.setattr(task_module, 'load_szse_reference', loader)
    result = await task.fetch_batch({'ts_code': '180402.SZ', 'trade_date': '20260918'})
    assert result.trade_date.iloc[0] == pd.Timestamp('20260918')
    assert result.fd_share.iloc[0] == 20000 and not loader.called


def test_mixed_products_preserve_non_etf_dates_and_values():
    source = frame([('159205.SZ', '20260921', 1), ('180402.SZ', '20260918', 20000)])
    official = frame([('159205.SZ', '20260918', 1)])
    result = reconcile(source, official, '20260918', '20260918')
    reit = result[result.ts_code == '180402.SZ'].iloc[0]
    assert reit.trade_date == pd.Timestamp('20260918') and reit.fd_share == 20000


def test_unlisted_product_is_preserved_explicitly_unverified_not_shifted():
    source = frame([('159205.SZ', '20260921', 1), ('158025.SZ', '20260918', 2)])
    official = frame([('159205.SZ', '20260918', 1)])
    result = ref.reconcile_share_dates(source, official, DAYS, '20260918', '20260918', retain_unreferenced=True)
    raw = result[result.ts_code == '158025.SZ'].iloc[0]
    assert raw.trade_date == pd.Timestamp('20260918') and raw.fd_share == 2
    assert result.attrs['unverified_sz_etf_codes'] == ['158025.SZ']


def test_empty_exchange_response_cannot_masquerade_as_all_unlisted():
    with pytest.raises(ValueError, match='reference_missing'):
        ref.reconcile_share_dates(frame([('159205.SZ', '20260918', 1)]), frame([]), DAYS, '20260918', '20260918', retain_unreferenced=True)
