"""
AH 溢价特征（日频）

AH 溢价指数与个股 AH 溢价分位，用于跨市场估值比较与套利。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class AHPremiumDailyMV(BaseFeatureView):
    """AH 溢价特征"""

    name = "ah_premium_daily"
    description = "AH 溢价指数、个股 AH 溢价及历史分位"
    source_tables = ["rawdata.stock_ahcomparison"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_ah_premium_daily AS
        WITH stock_level AS (
            -- 个股 AH 溢价
            SELECT
                trade_date,
                ts_code,
                hk_code,
                name,
                hk_name,
                close AS a_close,
                hk_close,
                pct_chg AS a_pct_chg,
                hk_pct_chg,
                ah_comparison,      -- AH 比价
                ah_premium          -- AH 溢价率
            FROM rawdata.stock_ahcomparison
            WHERE trade_date IS NOT NULL
              AND ah_premium IS NOT NULL
        ),
        with_pctl AS (
            SELECT
                trade_date,
                ts_code,
                hk_code,
                name,
                a_close,
                hk_close,
                a_pct_chg,
                hk_pct_chg,
                ah_comparison,
                ah_premium,
                -- 个股 AH 溢价历史分位（1 年）
                PERCENT_RANK() OVER (
                    PARTITION BY ts_code
                    ORDER BY ah_premium
                ) AS ah_premium_pctl,
                -- AH 溢价 vs 20 日均值
                ah_premium - AVG(ah_premium) OVER (
                    PARTITION BY ts_code
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ah_premium_dev_ma20
            FROM stock_level
        ),
        market_agg AS (
            -- 市场层面 AH 溢价指数
            SELECT
                trade_date,
                COUNT(*) AS ah_stock_count,
                AVG(ah_premium) AS ah_premium_avg,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ah_premium) AS ah_premium_median,
                MIN(ah_premium) AS ah_premium_min,
                MAX(ah_premium) AS ah_premium_max,
                STDDEV(ah_premium) AS ah_premium_std,
                -- 溢价 > 50% 的股票占比
                COUNT(*) FILTER (WHERE ah_premium > 50) * 100.0 / COUNT(*) AS high_premium_pct,
                -- 溢价 < 0（折价）的股票占比
                COUNT(*) FILTER (WHERE ah_premium < 0) * 100.0 / COUNT(*) AS discount_pct
            FROM stock_level
            GROUP BY trade_date
        )
        SELECT
            s.trade_date,
            s.ts_code,
            s.hk_code,
            s.name,
            s.a_close,
            s.hk_close,
            s.a_pct_chg,
            s.hk_pct_chg,
            s.ah_comparison,
            s.ah_premium,
            s.ah_premium_pctl,
            s.ah_premium_dev_ma20,
            -- 市场层面指标
            m.ah_stock_count,
            m.ah_premium_avg AS market_ah_premium_avg,
            m.ah_premium_median AS market_ah_premium_median,
            m.ah_premium_std AS market_ah_premium_std,
            m.high_premium_pct,
            m.discount_pct,
            -- 套利信号
            CASE
                WHEN s.ah_premium_pctl > 0.9 THEN 'A_EXPENSIVE'
                WHEN s.ah_premium_pctl < 0.1 THEN 'A_CHEAP'
                ELSE 'NEUTRAL'
            END AS arbitrage_signal,
            -- 血缘字段
            'rawdata.stock_ahcomparison' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_pctl s
        JOIN market_agg m ON s.trade_date = m.trade_date
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_ah_premium_daily_trade_date "
            "ON features.mv_ah_premium_daily (trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_ah_premium_daily_ts_code "
            "ON features.mv_ah_premium_daily (ts_code, trade_date)",
        ]
