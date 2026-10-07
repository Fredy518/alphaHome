"""Shared exchange LOF inventory and reported-size contract.

Exchange codes are discovery evidence, not proof of a valid listing date. Split
funds and OTC aliases are excluded; lifecycle and price gates remain mandatory.
Current benchmark text is classification context, never an index identity.
"""

import calendar
from datetime import date


def listing_months(list_date: date, cutoff: date) -> int:
    """Complete calendar months, using month-end for shorter anniversaries."""
    months = (cutoff.year - list_date.year) * 12 + cutoff.month - list_date.month
    anniversary_day = min(
        list_date.day, calendar.monthrange(cutoff.year, cutoff.month)[1]
    )
    return months - (cutoff.day < anniversary_day)


LOF_MAX_AUM_AGE_DAYS = 183

LOF_UNIVERSE_SQL = r"""
SELECT b.ts_code AS fund_code, b.name AS fund_name_reference,
       b.list_date, b.found_date, b.delist_date,
       b.status AS current_status_reference,
       NULL::text AS current_index_reference,
       'fund_basic_exchange_lof'::text AS inventory_source,
       'LOF'::text AS product_type
FROM rawdata.fund_basic b
WHERE b.market='E'
  AND b.ts_code ~ '^(16[0-9]{4}[.]SZ|50[12][0-9]{3}[.]SH)$'
  AND b.name NOT LIKE '%%分级%%'
"""


# Parameters are SQL expressions supplied by our own callers, never user text.
# ann_date == nav_date often describes daily NAV rather than later asset reports;
# it cannot establish when those subsequently filled asset fields became known.
# Overview snapshots establish a conservative observation date, not a retroactive
# publication date. Tinysoft asset allocation has missing ann_date and is unused.
def lof_aum_sql(
    code: str, cutoff: str, known_cutoff: str, *, include_report_archive: bool = False
) -> str:
    archive = ""
    if include_report_archive:
        archive = f"""
        UNION ALL
        SELECT net_asset/100000000.0, report_date, ann_date,
               'tushare_fund_nav_report_archive', 'reported_share_class_net_assets', 0
        FROM (
            SELECT DISTINCT ON (fund_code, report_date) *
            FROM fund_pool_on.lof_aum_report_evidence
            WHERE fund_code={code} AND net_asset>0 AND ann_date>report_date
              AND report_date BETWEEN {cutoff}::date-{LOF_MAX_AUM_AGE_DAYS} AND {cutoff}::date
              AND ann_date<={known_cutoff}::date
            ORDER BY fund_code,report_date,ann_date DESC,recorded_at DESC,source_hash DESC
        ) latest_report
        """
    return f"""
    SELECT a.* FROM (
        SELECT n.net_asset/100000000.0 AS aum_100m, n.nav_date AS aum_date,
               n.ann_date AS aum_known_date, 'fund_nav_reported_net_asset'::text AS aum_source,
               'reported_share_class_net_assets'::text AS aum_scope, 1 AS priority
        FROM rawdata.fund_nav n
        WHERE n.ts_code={code} AND n.net_asset>0
          AND n.nav_date BETWEEN {cutoff}::date-{LOF_MAX_AUM_AGE_DAYS} AND {cutoff}::date
          AND n.ann_date>n.nav_date AND n.ann_date<={known_cutoff}::date
        UNION ALL
        SELECT (m.parts[1])::numeric, to_date(m.parts[2]||m.parts[3]||m.parts[4],'YYYYMMDD'),
               o.snapshot_date, 'fund_overview_observed_net_asset',
               'reported_share_class_net_assets', 0
        FROM rawdata.fund_overview_em o
        CROSS JOIN LATERAL (
            SELECT regexp_match(o.net_asset_size_text,
              '([0-9]+[.]?[0-9]*)亿元.*([0-9]{{4}})年([0-9]{{2}})月([0-9]{{2}})日') AS parts
        ) m
        WHERE o.fund_code=left({code},6) AND o.snapshot_date<={known_cutoff}::date
          AND m.parts IS NOT NULL AND (m.parts[1])::numeric>0
          AND to_date(m.parts[2]||m.parts[3]||m.parts[4],'YYYYMMDD')
              BETWEEN {cutoff}::date-{LOF_MAX_AUM_AGE_DAYS} AND {cutoff}::date
          AND o.snapshot_date>=to_date(m.parts[2]||m.parts[3]||m.parts[4],'YYYYMMDD')
        {archive}
    ) a ORDER BY a.aum_date DESC, a.priority, a.aum_known_date DESC LIMIT 1
    """


ETF_FACT_COLUMNS = (
    "as_of_date",
    "fund_code",
    "fund_name",
    "market",
    "etf_type",
    "tracking_index_code",
    "found_date",
    "list_date",
    "status",
    "price_date",
    "latest_close",
    "amount_20d_100m",
    "amount_20d_days",
    "nav_date",
    "nav_ann_date",
    "unit_nav",
    "share_date",
    "fd_share",
    "aum_100m",
    "aum_source",
    "age_months",
    "total_fee_pct",
    "mean_abs_premium_60d",
    "premium_matched_days",
    "premium_latest_match_date",
    "core_facts_complete",
    "_source_table",
    "_processed_at",
    "_data_version",
)

UNIFIED_FACTS_SQL = f"""
CREATE OR REPLACE VIEW features.exchange_fund_product_facts_current AS
SELECT {', '.join('f.'+c for c in ETF_FACT_COLUMNS)},
       'ETF'::text AS product_type, b.benchmark, b.invest_type, b.fund_type,
       f.nav_date AS aum_date, f.nav_ann_date AS aum_known_date,
       'legacy_etf_aum_contract'::text AS aum_scope
FROM features.mv_etf_product_facts_current f
LEFT JOIN rawdata.fund_basic b ON b.ts_code=f.fund_code
UNION ALL
SELECT * FROM features.mv_lof_product_facts_current;
"""
