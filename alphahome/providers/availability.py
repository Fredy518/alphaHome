"""Explicit historical-use gates for retained data with incomplete evidence."""
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


class HistoricalEvidenceError(ValueError):
    pass


UNVERIFIED_HISTORY = {
    'macro_fixed_asset_investment': 'release/revision payload history missing',
    'macro_industrial_value_added': 'release/revision payload history missing',
    'macro_retail_sales': 'release/revision payload history missing',
    'etf_flow_daily': 'historical mapping, share publication and NAV vintages missing',
    'index_fundamental_daily': 'weight statistical dates do not prove public release or retained vintages',
    'stock_weekly': 'retained unfinished/overwritten period versions missing',
    'stock_monthly': 'retained unfinished/overwritten period versions missing',
    'pit_etf_index_members_monthly': 'official publication precision and historical universe missing',
    'pit_etf_index_fapi_monthly': 'member publication and historical universe certification missing',
}


def require_historical_evidence(source: str) -> None:
    name=source.rsplit('.',1)[-1].removeprefix('mv_')
    if name in UNVERIFIED_HISTORY:
        raise HistoricalEvidenceError(f'historical_evidence_required:{name}:{UNVERIFIED_HISTORY[name]}')


def publication_cutoff(public_date: date, published_at: datetime | None = None) -> datetime:
    """A date-only announcement is usable AFTER that Shanghai day finishes.

    A later caller-verified time takes precedence. This returns an information
    boundary, not a fill price or a guarantee the next session is tradable.
    """
    boundary=datetime.combine(public_date+timedelta(days=1),time.min,ZoneInfo('Asia/Shanghai'))
    if published_at is None:
        return boundary
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        raise HistoricalEvidenceError('publication_timestamp_timezone_required')
    return max(datetime.combine(public_date,time.min,ZoneInfo('Asia/Shanghai')),published_at)
