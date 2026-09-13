"""ETF 产品动态事实（当前快照）。

该视图只计算候选池维护所需的可复用产品事实，不表达候选身份、
资金权限或下单资格。历史研究不应使用本 ``current`` 视图回填过去。
"""

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class ETFProductFactsCurrentMV(BaseFeatureView):
    """ETF 规模、成交、费率、存续和收盘折溢价当前快照。"""

    name = "etf_product_facts_current"
    description = (
        "ETF 产品动态事实当前快照：AUM、20日均成交额、存续月数、"
        "管理加托管费率和60个可匹配日收盘折溢价"
    )
    source_tables = [
        "rawdata.fund_etf_basic",
        "rawdata.fund_basic",
        "rawdata.fund_daily",
        "rawdata.fund_nav",
        "rawdata.fund_share",
    ]
    refresh_strategy = "full"
    quality_checks = {
        "grain": "one row per listed ETF",
        "required_keys": ["fund_code", "as_of_date"],
        "non_pit_current_snapshot": True,
    }

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_etf_product_facts_current AS
        WITH cutoff AS (
            SELECT MAX(trade_date)::date AS as_of_date
            FROM rawdata.fund_daily
        ),
        etf_basic AS (
            SELECT DISTINCT ON (e.ts_code)
                e.ts_code AS fund_code,
                e.name AS fund_name,
                e.market,
                e.etf_type,
                e.index_code AS tracking_index_code,
                e.found_date,
                e.list_date,
                e.status,
                e.m_fee AS etf_management_fee_pct
            FROM rawdata.fund_etf_basic e
            WHERE e.ts_code IS NOT NULL
              AND (e.status IS NULL OR e.status = 'L')
            ORDER BY e.ts_code, e.update_time DESC NULLS LAST
        ),
        fund_basic AS (
            SELECT DISTINCT ON (b.ts_code)
                b.ts_code AS fund_code,
                b.m_fee AS management_fee_pct,
                b.c_fee AS custodian_fee_pct
            FROM rawdata.fund_basic b
            WHERE b.ts_code IS NOT NULL
            ORDER BY b.ts_code, b.update_time DESC NULLS LAST
        ),
        daily_ranked AS (
            SELECT
                d.ts_code AS fund_code,
                d.trade_date,
                d.close,
                d.amount,
                ROW_NUMBER() OVER (
                    PARTITION BY d.ts_code ORDER BY d.trade_date DESC
                ) AS recency_rank
            FROM rawdata.fund_daily d
            JOIN etf_basic e ON e.fund_code = d.ts_code
            CROSS JOIN cutoff c
            WHERE d.trade_date <= c.as_of_date
              AND d.trade_date >= c.as_of_date - INTERVAL '180 days'
        ),
        daily_agg AS (
            SELECT
                fund_code,
                MAX(trade_date) FILTER (WHERE recency_rank = 1) AS price_date,
                MAX(close) FILTER (WHERE recency_rank = 1) AS latest_close,
                AVG(amount) FILTER (
                    WHERE recency_rank <= 20 AND amount IS NOT NULL
                ) / 100000.0 AS amount_20d_100m,
                COUNT(amount) FILTER (WHERE recency_rank <= 20) AS amount_20d_days
            FROM daily_ranked
            WHERE recency_rank <= 20
            GROUP BY fund_code
        ),
        nav_dedup AS (
            SELECT DISTINCT ON (n.ts_code, n.nav_date)
                n.ts_code AS fund_code,
                n.nav_date,
                n.ann_date,
                n.unit_nav,
                n.net_asset,
                n.total_netasset,
                n.update_time
            FROM rawdata.fund_nav n
            JOIN etf_basic e ON e.fund_code = n.ts_code
            CROSS JOIN cutoff c
            WHERE n.nav_date <= c.as_of_date
              AND n.nav_date >= c.as_of_date - INTERVAL '180 days'
            ORDER BY n.ts_code, n.nav_date, n.ann_date DESC NULLS LAST,
                     n.update_time DESC NULLS LAST
        ),
        latest_nav AS (
            SELECT DISTINCT ON (n.fund_code)
                n.fund_code,
                n.nav_date,
                n.ann_date,
                n.unit_nav,
                n.net_asset,
                n.total_netasset
            FROM nav_dedup n
            ORDER BY n.fund_code, n.nav_date DESC, n.ann_date DESC NULLS LAST
        ),
        latest_share AS (
            SELECT DISTINCT ON (s.ts_code)
                s.ts_code AS fund_code,
                s.trade_date AS share_date,
                s.fd_share
            FROM rawdata.fund_share s
            JOIN etf_basic e ON e.fund_code = s.ts_code
            CROSS JOIN cutoff c
            WHERE s.trade_date <= c.as_of_date
              AND s.trade_date >= c.as_of_date - INTERVAL '180 days'
            ORDER BY s.ts_code, s.trade_date DESC, s.update_time DESC NULLS LAST
        ),
        premium_ranked AS (
            SELECT
                d.ts_code AS fund_code,
                d.trade_date,
                ABS(d.close / NULLIF(n.unit_nav, 0) - 1) AS abs_close_nav_premium,
                ROW_NUMBER() OVER (
                    PARTITION BY d.ts_code ORDER BY d.trade_date DESC
                ) AS matched_recency_rank
            FROM rawdata.fund_daily d
            JOIN etf_basic e ON e.fund_code = d.ts_code
            JOIN nav_dedup n
              ON n.fund_code = d.ts_code
             AND n.nav_date = d.trade_date
            CROSS JOIN cutoff c
            WHERE d.trade_date <= c.as_of_date
              AND d.trade_date >= c.as_of_date - INTERVAL '180 days'
              AND d.close > 0
              AND n.unit_nav > 0
        ),
        premium_agg AS (
            SELECT
                fund_code,
                AVG(abs_close_nav_premium) FILTER (
                    WHERE matched_recency_rank <= 60
                ) AS mean_abs_premium_60d,
                COUNT(*) FILTER (
                    WHERE matched_recency_rank <= 60
                ) AS premium_matched_days,
                MAX(trade_date) FILTER (
                    WHERE matched_recency_rank = 1
                ) AS premium_latest_match_date
            FROM premium_ranked
            WHERE matched_recency_rank <= 60
            GROUP BY fund_code
        )
        SELECT
            c.as_of_date,
            e.fund_code,
            e.fund_name,
            e.market,
            e.etf_type,
            e.tracking_index_code,
            e.found_date,
            e.list_date,
            e.status,
            d.price_date,
            d.latest_close,
            d.amount_20d_100m,
            d.amount_20d_days,
            n.nav_date,
            n.ann_date AS nav_ann_date,
            n.unit_nav,
            s.share_date,
            s.fd_share,
            CASE
                WHEN n.total_netasset > 0 THEN n.total_netasset / 100000000.0
                WHEN n.net_asset > 0 THEN n.net_asset / 100000000.0
                WHEN n.unit_nav > 0 AND s.fd_share > 0
                    THEN n.unit_nav * s.fd_share / 10000.0
                ELSE NULL
            END AS aum_100m,
            CASE
                WHEN n.total_netasset > 0 THEN 'total_netasset'
                WHEN n.net_asset > 0 THEN 'net_asset'
                WHEN n.unit_nav > 0 AND s.fd_share > 0 THEN 'unit_nav_x_fd_share'
                ELSE NULL
            END AS aum_source,
            ROUND(
                EXTRACT(EPOCH FROM (c.as_of_date::timestamp - e.list_date::timestamp))
                / 86400.0 / 30.4375,
                1
            ) AS age_months,
            COALESCE(b.management_fee_pct, e.etf_management_fee_pct)
                + COALESCE(b.custodian_fee_pct, 0) AS total_fee_pct,
            p.mean_abs_premium_60d,
            p.premium_matched_days,
            p.premium_latest_match_date,
            (d.price_date IS NOT NULL
                AND n.nav_date IS NOT NULL
                AND s.share_date IS NOT NULL
                AND d.amount_20d_days = 20) AS core_facts_complete,
            -- 血缘
            'rawdata.fund_etf_basic,rawdata.fund_basic,rawdata.fund_daily,rawdata.fund_nav,rawdata.fund_share'
                AS _source_table,
            NOW() AS _processed_at,
            c.as_of_date AS _data_version
        FROM etf_basic e
        CROSS JOIN cutoff c
        LEFT JOIN fund_basic b ON b.fund_code = e.fund_code
        LEFT JOIN daily_agg d ON d.fund_code = e.fund_code
        LEFT JOIN latest_nav n ON n.fund_code = e.fund_code
        LEFT JOIN latest_share s ON s.fund_code = e.fund_code
        LEFT JOIN premium_agg p ON p.fund_code = e.fund_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            "idx_mv_etf_product_facts_current_fund_code "
            "ON features.mv_etf_product_facts_current (fund_code)",
            "CREATE INDEX IF NOT EXISTS idx_mv_etf_product_facts_current_as_of "
            "ON features.mv_etf_product_facts_current (as_of_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_etf_product_facts_current_index_code "
            "ON features.mv_etf_product_facts_current (tracking_index_code)",
        ]
