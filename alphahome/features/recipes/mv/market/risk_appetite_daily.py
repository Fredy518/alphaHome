"""
风险偏好代理特征（日频）

涵盖：
- ST 股票情绪（成交额占比、涨跌比、涨停数）
- 北交所情绪（成交额占比、涨跌比、极端涨跌）
- 可转债情绪（成交额占比、涨跌比、溢价率分布）
- 微盘股情绪（收益差、成交额占比）

数据来源：
- tushare.stock_factor_pro
- tushare.stock_st（动态 ST 列表，避免幸存者偏差）
- tushare.stock_basic（北交所筛选）
- tushare.cbond_daily（可转债）

时间语义：日终，T 日收盘后可计算 T 日指标

注意：
- ST 数据从 2016-08-09 开始
- 北交所 2021-11-15 开市
- 可转债数据从 2010-01-04 开始
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class RiskAppetiteDailyMV(BaseFeatureView):
    """风险偏好代理特征（日频）"""

    name = "risk_appetite_daily"
    description = "ST、北交所、可转债、微盘股等边缘市场的风险偏好代理指标（日频）"
    source_tables = [
        "tushare.stock_factor_pro",
        "tushare.stock_st",
        "tushare.stock_basic",
        "tushare.cbond_daily",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_risk_appetite_daily AS
        WITH
        -- 交易日集合（以全市场交易日为基准）
        trade_dates AS (
            SELECT DISTINCT trade_date
            FROM tushare.stock_factor_pro
            WHERE trade_date IS NOT NULL
        ),

        -- 1. 全市场成交额（用于计算占比）
        market_total AS (
            SELECT
                trade_date,
                SUM(amount) / 100000000 AS market_amount_total  -- 亿元
            FROM tushare.stock_factor_pro
            WHERE amount IS NOT NULL
            GROUP BY trade_date
        ),

        -- 2. ST 股票特征（使用 stock_st 动态列表，避免幸存者偏差）
        st_raw AS (
            SELECT
                f.trade_date,
                f.pct_chg,
                f.amount
            FROM tushare.stock_factor_pro f
            INNER JOIN tushare.stock_st s
                ON f.ts_code = s.ts_code AND f.trade_date = s.trade_date
        ),
        st_agg AS (
            SELECT
                trade_date,
                COUNT(*) AS st_count,
                SUM(amount) / 100000000 AS st_amount_total,
                AVG(pct_chg) AS st_return_mean,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_chg) AS st_return_median,
                SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS st_up_count,
                -- ST 股票通常为 5% 涨跌停（极少数板块规则例外，这里保持与 fetcher 一致的近似）
                SUM(CASE WHEN pct_chg >= 4.9 THEN 1 ELSE 0 END) AS st_limit_up_count,
                SUM(CASE WHEN pct_chg <= -4.9 THEN 1 ELSE 0 END) AS st_limit_down_count
            FROM st_raw
            GROUP BY trade_date
        ),

        -- 3. 北交所股票特征
        bse_stocks AS (
            SELECT DISTINCT ts_code
            FROM tushare.stock_basic
            WHERE exchange = 'BSE' OR ts_code LIKE '%%.BJ'
        ),
        bse_raw AS (
            SELECT
                f.trade_date,
                f.pct_chg,
                f.amount
            FROM tushare.stock_factor_pro f
            INNER JOIN bse_stocks b ON f.ts_code = b.ts_code
        ),
        bse_agg AS (
            SELECT
                trade_date,
                COUNT(*) AS bse_count,
                SUM(amount) / 100000000 AS bse_amount_total,
                AVG(pct_chg) AS bse_return_mean,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_chg) AS bse_return_median,
                SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS bse_up_count,
                -- “极端涨跌”口径（与 fetcher 一致，非严格涨跌停）
                SUM(CASE WHEN pct_chg >= 10 THEN 1 ELSE 0 END) AS bse_surge_count,
                SUM(CASE WHEN pct_chg <= -10 THEN 1 ELSE 0 END) AS bse_plunge_count
            FROM bse_raw
            GROUP BY trade_date
        ),

        -- 4. 可转债特征
        cb_agg AS (
            SELECT
                trade_date,
                COUNT(*) AS cb_count,
                SUM(amount) / 100000000 AS cb_amount_total,
                AVG(pct_chg) AS cb_return_mean,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_chg) AS cb_return_median,
                SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS cb_up_count,
                SUM(CASE WHEN pct_chg >= 5 THEN 1 ELSE 0 END) AS cb_surge_count,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (cb_value - bond_value))
                    FILTER (WHERE cb_value IS NOT NULL AND bond_value IS NOT NULL) AS cb_premium_median
            FROM tushare.cbond_daily
            GROUP BY trade_date
        ),

        -- 5. 微盘股特征（剔除 ST 后按市值排名）
        micro_ranked AS (
            SELECT
                f.trade_date,
                f.ts_code,
                f.pct_chg,
                f.amount,
                f.total_mv,
                ROW_NUMBER() OVER (PARTITION BY f.trade_date ORDER BY f.total_mv ASC) AS mv_rank_asc,
                ROW_NUMBER() OVER (PARTITION BY f.trade_date ORDER BY f.total_mv DESC) AS mv_rank_desc
            FROM tushare.stock_factor_pro f
            LEFT JOIN tushare.stock_st s
                ON f.ts_code = s.ts_code AND f.trade_date = s.trade_date
            WHERE s.ts_code IS NULL  -- 排除 ST
              AND f.total_mv IS NOT NULL
              AND f.total_mv > 0
        ),
        micro_agg AS (
            SELECT
                trade_date,
                -- 微盘股 = 最小市值 400 只
                SUM(CASE WHEN mv_rank_asc <= 400 THEN 1 ELSE 0 END) AS micro_count,
                AVG(pct_chg) FILTER (WHERE mv_rank_asc <= 400) AS micro_return_mean,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_chg)
                    FILTER (WHERE mv_rank_asc <= 400) AS micro_return_median,
                SUM(amount) FILTER (WHERE mv_rank_asc <= 400) / 100000000 AS micro_amount_total,
                SUM(CASE WHEN mv_rank_asc <= 400 AND pct_chg > 0 THEN 1 ELSE 0 END) AS micro_up_count,
                -- 大盘股 = 最大市值 300 只
                AVG(pct_chg) FILTER (WHERE mv_rank_desc <= 300) AS large_return_mean
            FROM micro_ranked
            GROUP BY trade_date
        )

        -- 6. 最终输出
        SELECT
            d.trade_date,
            -- ST 特征
            COALESCE(st.st_count, 0) AS st_count,
            COALESCE(st.st_amount_total, 0) AS st_amount_total,
            st.st_amount_total / NULLIF(m.market_amount_total, 0) AS st_amount_ratio,
            st.st_return_mean,
            st.st_return_median,
            st.st_up_count::float / NULLIF(st.st_count, 0) AS st_up_ratio,
            COALESCE(st.st_limit_up_count, 0) AS st_limit_up_count,
            COALESCE(st.st_limit_down_count, 0) AS st_limit_down_count,
            COALESCE(st.st_limit_up_count, 0) - COALESCE(st.st_limit_down_count, 0) AS st_limit_net,
            -- 北交所特征
            COALESCE(bse.bse_count, 0) AS bse_count,
            COALESCE(bse.bse_amount_total, 0) AS bse_amount_total,
            bse.bse_amount_total / NULLIF(m.market_amount_total, 0) AS bse_amount_ratio,
            bse.bse_return_mean,
            bse.bse_return_median,
            bse.bse_up_count::float / NULLIF(bse.bse_count, 0) AS bse_up_ratio,
            COALESCE(bse.bse_surge_count, 0) AS bse_surge_count,
            COALESCE(bse.bse_plunge_count, 0) AS bse_plunge_count,
            -- 可转债特征
            COALESCE(cb.cb_count, 0) AS cb_count,
            COALESCE(cb.cb_amount_total, 0) AS cb_amount_total,
            cb.cb_amount_total / NULLIF(m.market_amount_total, 0) AS cb_amount_ratio,
            cb.cb_return_mean,
            cb.cb_return_median,
            cb.cb_up_count::float / NULLIF(cb.cb_count, 0) AS cb_up_ratio,
            cb.cb_surge_count::float / NULLIF(cb.cb_count, 0) AS cb_surge_ratio,
            cb.cb_premium_median,
            -- 微盘股特征
            COALESCE(mic.micro_count, 0) AS micro_count,
            mic.micro_return_mean,
            mic.micro_return_median,
            mic.micro_amount_total / NULLIF(m.market_amount_total, 0) AS micro_amount_ratio,
            mic.micro_up_count::float / NULLIF(mic.micro_count, 0) AS micro_up_ratio,
            mic.micro_return_mean - mic.large_return_mean AS micro_large_spread,
            -- 血缘
            'tushare.stock_factor_pro,tushare.stock_st,tushare.stock_basic,tushare.cbond_daily' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM trade_dates d
        LEFT JOIN market_total m ON d.trade_date = m.trade_date
        LEFT JOIN st_agg st ON d.trade_date = st.trade_date
        LEFT JOIN bse_agg bse ON d.trade_date = bse.trade_date
        LEFT JOIN cb_agg cb ON d.trade_date = cb.trade_date
        LEFT JOIN micro_agg mic ON d.trade_date = mic.trade_date
        ORDER BY d.trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_risk_appetite_daily_trade_date "
            "ON features.mv_risk_appetite_daily (trade_date)",
            "COMMENT ON MATERIALIZED VIEW features.mv_risk_appetite_daily IS "
            "'ST/北交所/可转债/微盘股风险偏好代理指标（日频）';",
        ]


# 兼容旧类名（外部 import 不断）
RiskAppetiteDaily = RiskAppetiteDailyMV
