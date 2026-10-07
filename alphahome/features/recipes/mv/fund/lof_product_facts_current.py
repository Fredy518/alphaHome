"""Current listed LOF facts; size is a dated report, not stale shares × new NAV."""

from alphahome.curation.exchange_fund_sources import LOF_UNIVERSE_SQL, lof_aum_sql
from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class LOFProductFactsCurrentMV(BaseFeatureView):
    name = "lof_product_facts_current"
    description = "LOF当前规模披露、20个交易日成交、费率、上市时长与折溢价"
    source_tables = [
        "rawdata.fund_basic",
        "rawdata.fund_daily",
        "rawdata.fund_nav",
        "rawdata.fund_overview_em",
        "rawdata.others_calendar",
    ]
    refresh_strategy = "full"
    quality_checks = {
        "grain": "one row per exchange LOF",
        "required_keys": ["fund_code", "as_of_date"],
        "non_pit_current_snapshot": True,
    }
    create_sql = f"""
    CREATE MATERIALIZED VIEW features.mv_lof_product_facts_current AS
    WITH inventory AS ({LOF_UNIVERSE_SQL}),
    cutoff AS (SELECT max(trade_date)::date AS as_of_date FROM rawdata.fund_daily),
    days AS (SELECT DISTINCT cal_date::date AS day FROM rawdata.others_calendar,cutoff
             WHERE exchange='SSE' AND is_open=1 AND cal_date<=as_of_date ORDER BY day DESC LIMIT 20)
    SELECT c.as_of_date, i.fund_code, b.name AS fund_name,
           right(i.fund_code,2)::text AS market, 'LOF'::text AS etf_type,
           NULL::text AS tracking_index_code, b.found_date,b.list_date,b.status,
           d.price_date,d.latest_close,d.amount_20d_100m,d.amount_20d_days,
           n.nav_date,n.ann_date AS nav_ann_date,n.unit_nav,
           NULL::date AS share_date,NULL::numeric AS fd_share,
           a.aum_100m,a.aum_source,
           (extract(year from age(c.as_of_date,b.list_date))*12+
            extract(month from age(c.as_of_date,b.list_date)))::numeric AS age_months,
           b.m_fee+b.c_fee AS total_fee_pct,
           p.mean_abs_premium_60d,p.premium_matched_days,p.premium_latest_match_date,
           (d.price_date=c.as_of_date AND d.amount_20d_days=20 AND d.invalid_days=0
             AND n.nav_date IS NOT NULL AND a.aum_100m>0 AND b.list_date IS NOT NULL)
             AS core_facts_complete,
           'rawdata.fund_basic,rawdata.fund_daily,rawdata.fund_nav,rawdata.fund_overview_em'::text AS _source_table,
           now() AS _processed_at,c.as_of_date AS _data_version,
           'LOF'::text AS product_type,b.benchmark,b.invest_type,b.fund_type,
           a.aum_date,a.aum_known_date,a.aum_scope
    FROM inventory i JOIN rawdata.fund_basic b ON b.ts_code=i.fund_code CROSS JOIN cutoff c
    LEFT JOIN LATERAL (
      SELECT max(trade_date) AS price_date,
             max(close) FILTER(WHERE trade_date=c.as_of_date) AS latest_close,
             avg(amount)/100000.0 AS amount_20d_100m,
             count(DISTINCT trade_date) FILTER(WHERE amount IS NOT NULL) AS amount_20d_days,
             count(*) FILTER(WHERE close<=0 OR close IS NULL OR amount<0
                 OR amount='NaN'::numeric OR close='NaN'::numeric) AS invalid_days
      FROM rawdata.fund_daily WHERE ts_code=i.fund_code AND trade_date IN (SELECT day FROM days)
    ) d ON true
    LEFT JOIN LATERAL (
      SELECT nav_date,ann_date,unit_nav FROM rawdata.fund_nav
      WHERE ts_code=i.fund_code AND nav_date<=c.as_of_date AND ann_date<=c.as_of_date
        AND ann_date>=nav_date AND unit_nav>0 ORDER BY nav_date DESC LIMIT 1
    ) n ON true
    LEFT JOIN LATERAL ({lof_aum_sql('i.fund_code','c.as_of_date','c.as_of_date')}) a ON true
    LEFT JOIN LATERAL (
      SELECT avg(abs(close/unit_nav-1)) AS mean_abs_premium_60d,
             count(*) AS premium_matched_days,max(trade_date) AS premium_latest_match_date
      FROM (SELECT d.trade_date,d.close,n.unit_nav FROM rawdata.fund_daily d
            JOIN rawdata.fund_nav n ON n.ts_code=d.ts_code AND n.nav_date=d.trade_date
            WHERE d.ts_code=i.fund_code AND d.trade_date<=c.as_of_date
              AND d.trade_date>=c.as_of_date-180 AND n.ann_date<=c.as_of_date
              AND d.close>0 AND n.unit_nav>0 ORDER BY d.trade_date DESC LIMIT 60) x
    ) p ON true
    WHERE b.status='L' AND (b.delist_date IS NULL OR b.delist_date>c.as_of_date)
      AND (b.list_date IS NULL OR b.list_date<=c.as_of_date)
    WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_lof_product_facts_current_code ON features.mv_lof_product_facts_current(fund_code)"
        ]
