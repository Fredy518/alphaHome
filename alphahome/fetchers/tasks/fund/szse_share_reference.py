"""Reconcile fund-share dates against the exchange's dated records.

The latest SH/SZ dates cannot identify a historical date offset: exchanges have
different publication times and a supplier can change its labeling mid-history.
Use each SZSE record's date and value instead. This is a current historical
reference, not a certificate of its original publication or first receipt.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from io import BytesIO
from typing import Iterable

import pandas as pd
import requests

REPORT_URL = 'https://www.szse.cn/api/report/ShowReport'
REPORT_TYPES = {'15': 'ETF'}
KEYS = ['ts_code', 'trade_date']


def _amount(value):
    if pd.isna(value):
        raise ValueError('fund_share_reference_missing_amount')
    try:
        number = Decimal(str(value).replace(',', ''))
    except InvalidOperation as exc:
        raise ValueError('fund_share_reference_invalid_amount') from exc
    if not number.is_finite() or number < 0:
        raise ValueError('fund_share_reference_invalid_amount')
    return number.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def normalize_share_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate identical rows; conflicting same-key revisions fail closed."""
    if frame.empty:
        return pd.DataFrame(columns=[*KEYS, 'fd_share'])
    if not {*KEYS, 'fd_share'}.issubset(frame.columns):
        raise ValueError('fund_share_reference_missing_columns')
    out = frame[[*KEYS, 'fd_share']].copy()
    out['ts_code'] = out['ts_code'].astype('string').str.strip().str.upper()
    out['trade_date'] = pd.to_datetime(out['trade_date'], errors='coerce').dt.normalize()
    if out[KEYS].isna().any().any():
        raise ValueError('fund_share_reference_invalid_key')
    out['fd_share'] = out['fd_share'].map(_amount)
    out = out.drop_duplicates()
    if out.duplicated(KEYS).any():
        raise ValueError('fund_share_conflicting_same_key')
    return out.sort_values(KEYS).reset_index(drop=True)


def load_szse_reference(start_date, end_date, codes: Iterable[str]) -> pd.DataFrame:
    """Read complete exchange exports in <= 1-month windows, never latest-only.

    No writes, credentials, fallback hosts, or automatic date inference. A
    failed/truncated/changed export must fail the batch instead of reverting to
    the provider's unverified date labels.
    """
    codes = set(codes)
    prefixes = {code[:2] for code in codes}
    if not prefixes.issubset(REPORT_TYPES):
        raise ValueError('fund_share_unsupported_szse_product')
    start, end = pd.Timestamp(start_date).normalize(), pd.Timestamp(end_date).normalize()
    frames = []
    for prefix in sorted(prefixes):
        current = start
        while current <= end:
            stop = min(end, current + pd.offsets.MonthEnd(0))
            response = requests.get(
                REPORT_URL,
                params={'SHOWTYPE': 'xlsx', 'CATALOGID': 'scsj_fund_jjgm',
                        'TABKEY': 'tab1', 'txtStart': current.strftime('%Y-%m-%d'),
                        'txtEnd': stop.strftime('%Y-%m-%d'), 'jjlb': REPORT_TYPES[prefix]},
                headers={'Referer': 'https://www.szse.cn/market/fund/volume/etf/index.html',
                         'User-Agent': 'Mozilla/5.0'}, timeout=(10, 20),
            )
            response.raise_for_status()
            report = pd.read_excel(BytesIO(response.content), dtype=str)
            required = {'日期', '基金代码', '基金规模(份)'}
            if not required.issubset(report.columns):
                raise ValueError('fund_share_exchange_export_schema_changed')
            report = report.dropna(how='all')
            if not report.empty:
                # Do not coerce malformed date/value rows away as missing history.
                if report[list(required)].isna().any().any():
                    raise ValueError('fund_share_exchange_export_incomplete')
                report['ts_code'] = report['基金代码'].str.strip().str.zfill(6) + '.SZ'
                report = report[report['ts_code'].isin(codes)].copy()
                report['trade_date'] = report['日期']
                report['fd_share'] = report['基金规模(份)'].map(
                    lambda value: Decimal(value.replace(',', '')) / Decimal(10000))
                frames.append(report[[*KEYS, 'fd_share']])
            current = stop + pd.Timedelta(days=1)
    if not frames:
        return pd.DataFrame(columns=[*KEYS, 'fd_share'])
    return normalize_share_rows(pd.concat(frames, ignore_index=True))


def reconcile_share_dates(data, reference, open_days, start_date, end_date, *, retain_unreferenced=False):
    """Certify each returned SZ date/value using its own exchange record.

    A supplier value on the same or an adjacent trading day corroborates the
    record. This verification covers SZ ETFs (15xxxx.SZ); REITs/LOFs have
    different share definitions and retain their provider dates unchanged.
    No observed shift is extrapolated to a different date or fund. A
    constant value is unambiguous because the reference certifies each date.
    Other products retain their provider dates. Unknown/missing ETF reference or
    unmatched values abort the batch; neither input order nor latest dates pick
    a winner. Zero is a real value, while NULL cannot certify a withdrawal.
    """
    source = normalize_share_rows(data)
    ref = normalize_share_rows(reference)
    start, end = pd.Timestamp(start_date).normalize(), pd.Timestamp(end_date).normalize()
    calendar = sorted({pd.Timestamp(day).normalize() for day in open_days})
    position = {day: i for i, day in enumerate(calendar)}
    etf_mask = source.ts_code.str.endswith('.SZ') & source.ts_code.str.startswith('15')
    sz = source[etf_mask]
    codes = set(sz.ts_code)
    ref = ref[ref.ts_code.isin(codes) & ref.trade_date.between(start, end)]
    in_range = sz[sz.trade_date.between(start, end)]
    unreferenced = set(in_range.ts_code) - set(ref.ts_code)
    if unreferenced and (not retain_unreferenced or ref.empty):
        raise ValueError('fund_share_exchange_reference_missing')
    lookup = {(r.ts_code, r.trade_date): r.fd_share for r in sz.itertuples()}
    for row in ref.itertuples():
        index = position.get(row.trade_date)
        if index is None:
            raise ValueError('fund_share_exchange_calendar_missing')
        candidates = calendar[max(0, index-1):index+2]
        if not any(lookup.get((row.ts_code, day)) == row.fd_share for day in candidates):
            raise ValueError('fund_share_exchange_value_unmatched')
    # Missing reference for an interior supplier date is a gap, except a new
    # listing's leading shifted row. Never silently accept a partial export.
    for code, group in ref.groupby('ts_code'):
        observed = in_range[in_range.ts_code == code]
        interior = observed[observed.trade_date >= group.trade_date.min()]
        if not set(interior.trade_date).issubset(set(group.trade_date)):
            raise ValueError('fund_share_exchange_reference_gap')
    other = source[~etf_mask & source.trade_date.between(start, end)]
    # The provider can carry pre-listing subscription shares which do not yet
    # appear in exchange trading reports. Preserve those raw labels explicitly
    # unverified rather than extrapolating the offset of listed ETFs to them.
    retained = in_range[in_range.ts_code.isin(unreferenced)]
    parts = [part for part in (other, ref, retained) if not part.empty]
    result = normalize_share_rows(pd.concat(parts, ignore_index=True) if parts else frame_empty())
    result.attrs['unverified_sz_etf_codes'] = sorted(unreferenced)
    return result


def frame_empty():
    return pd.DataFrame(columns=[*KEYS, 'fd_share'])
