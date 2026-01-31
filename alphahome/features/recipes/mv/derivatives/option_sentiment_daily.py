"""
期权情绪特征（日频）物化视图定义

设计思路：
- 计算 ETF 期权 Put/Call Ratio（成交额与持仓量口径）
- 日频输出，便于与其他日频 MV 直接按 trade_date 对齐
- 分标的输出：按“指数口径”聚合（例如 HS300 将 OP510300.SH 与 OP159919.SZ 合并）

数据来源：
- tushare.option_basic: ETF 期权合约基础信息（包含 call_put、opt_code、exchange、opt_type）
- tushare.option_daily: ETF 期权日行情（trade_date、amount、oi）

输出指标（粒度：trade_date × underlying_group）：
- 成交额 PCR：put_amount / call_amount
- 持仓量 PCR：put_oi / call_oi

命名规范：
- 文件名: option_sentiment_daily.py
- 类名: OptionSentimentDailyMV
- recipe.name: option_sentiment_daily
- 输出表名: features.mv_option_sentiment_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class OptionSentimentDailyMV(BaseFeatureView):
    """期权情绪特征物化视图（日频，按指数口径分标的聚合）"""

    name = "option_sentiment_daily"
    description = "ETF 期权 Put/Call Ratio（成交额/持仓量，日频，按指数口径分标的）"
    source_tables = [
        "tushare.option_basic",
        "tushare.option_daily",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_option_sentiment_daily AS
        WITH
        -- ========== ETF 期权合约列表（SSE/SZSE）==========
        etf_options AS (
            SELECT
                ts_code,
                call_put,
                opt_code,
                -- 按 opt_code 映射到“指数口径”的聚合标的
                CASE opt_code
                    WHEN 'OP510050.SH' THEN 'SZ50'
                    WHEN 'OP510300.SH' THEN 'HS300'
                    WHEN 'OP159919.SZ' THEN 'HS300'
                    WHEN 'OP510500.SH' THEN 'ZZ500'
                    WHEN 'OP159922.SZ' THEN 'ZZ500'
                    WHEN 'OP588000.SH' THEN 'KC50'
                    WHEN 'OP588080.SH' THEN 'KC50'
                    WHEN 'OP159901.SZ' THEN 'SZ100'
                    WHEN 'OP159915.SZ' THEN 'CYB'
                    ELSE 'OTHER'
                END AS underlying_group
            FROM tushare.option_basic
            WHERE exchange IN ('SSE', 'SZSE')
              AND opt_type = 'ETF期权'
        ),

        -- ========== 期权日行情：按 trade_date × underlying_group × call_put 聚合 ==========
        daily_agg AS (
            SELECT
                d.trade_date,
                e.underlying_group,
                e.call_put,
                SUM(d.amount) AS day_amount,
                SUM(d.oi) AS day_oi
            FROM tushare.option_daily d
            INNER JOIN etf_options e ON d.ts_code = e.ts_code
            WHERE d.trade_date IS NOT NULL
            GROUP BY d.trade_date, e.underlying_group, e.call_put
        ),

        -- ========== 透视为宽表（同一标的的 Call/Put 两行合并）==========
        pivoted AS (
            SELECT
                trade_date,
                underlying_group,
                MAX(CASE WHEN call_put = 'C' THEN day_amount END) AS call_amount,
                MAX(CASE WHEN call_put = 'P' THEN day_amount END) AS put_amount,
                MAX(CASE WHEN call_put = 'C' THEN day_oi END) AS call_oi,
                MAX(CASE WHEN call_put = 'P' THEN day_oi END) AS put_oi
            FROM daily_agg
            GROUP BY trade_date, underlying_group
        )

        -- ========== 最终输出 ==========
        SELECT
            trade_date,
            underlying_group,
            call_amount,
            COALESCE(put_amount, 0) AS put_amount,
            put_amount / NULLIF(call_amount, 0) AS pcr_turnover,
            call_oi,
            COALESCE(put_oi, 0) AS put_oi,
            put_oi / NULLIF(call_oi, 0) AS pcr_oi,
            -- 血缘
            'tushare.option_basic,tushare.option_daily' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM pivoted
        WHERE underlying_group <> 'OTHER'
        ORDER BY trade_date, underlying_group
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_option_sentiment_daily_trade_date_underlying_group "
            "ON features.mv_option_sentiment_daily (trade_date, underlying_group)",
            "COMMENT ON MATERIALIZED VIEW features.mv_option_sentiment_daily IS "
            "'ETF 期权 Put/Call Ratio（日频，按指数口径分标的聚合）';",
        ]
