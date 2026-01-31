"""
大小盘分化特征（日频）

涵盖：
- 大盘股（前 10% 市值）平均收益
- 小盘股（后 10% 市值）平均收益
- 大小盘收益差（size spread）
- 大盘股成交额占比
- 市值中位数

数据来源：
- tushare.stock_factor_pro（total_mv, pct_chg, amount）

时间语义：日终，T 日收盘后可计算 T 日指标
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class MarketSizeDailyMV(BaseFeatureView):
    """大小盘分化特征（日频）"""

    name = "market_size_daily"
    description = "大小盘收益差、成交额集中度等特征（日频）"
    source_tables = [
        "tushare.stock_factor_pro",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_market_size_daily AS
        WITH
        ranked AS (
            SELECT
                trade_date,
                ts_code,
                pct_chg,
                total_mv,
                amount,
                NTILE(10) OVER (PARTITION BY trade_date ORDER BY total_mv DESC) AS mv_decile
            FROM tushare.stock_factor_pro
            WHERE total_mv IS NOT NULL AND total_mv > 0
        )

        SELECT
            trade_date,
            -- 大盘股（前 10%）平均收益
            AVG(pct_chg) FILTER (WHERE mv_decile = 1) AS large_cap_return,
            -- 小盘股（后 10%）平均收益
            AVG(pct_chg) FILTER (WHERE mv_decile = 10) AS small_cap_return,
            -- 大小盘收益差
            AVG(pct_chg) FILTER (WHERE mv_decile = 1)
                - AVG(pct_chg) FILTER (WHERE mv_decile = 10) AS size_return_spread,
            -- 大盘股成交额占比
            SUM(amount) FILTER (WHERE mv_decile = 1) / NULLIF(SUM(amount), 0) AS large_cap_amount_ratio,
            -- 前 10% 市值占比
            SUM(total_mv) FILTER (WHERE mv_decile = 1) / NULLIF(SUM(total_mv), 0) AS top10_mv_ratio,
            -- 市值中位数（亿元）
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_mv) / 100000000 AS mv_median_billion,
            -- 血缘
            'tushare.stock_factor_pro' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM ranked
        GROUP BY trade_date
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_market_size_daily_trade_date "
            "ON features.mv_market_size_daily (trade_date)",
            "COMMENT ON MATERIALIZED VIEW features.mv_market_size_daily IS "
            "'大小盘收益差与成交额集中度（日频）';",
        ]


# 兼容旧类名（外部 import 不断）
MarketSizeDaily = MarketSizeDailyMV
