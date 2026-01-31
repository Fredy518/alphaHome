""" 
行业特征（日频）物化视图定义

设计思路：
- 将申万一级行业收益、行业宽度等整合到一张 MV
- 这些特征刻画"行业轮动"与"行业分散度"
- 避免下游多次 join tushare.index_swdaily

数据来源：
- tushare.index_swdaily: 申万行业指数日线
- tushare.index_swmember: 申万行业成分（用于识别二级行业）

输出指标：
- 行业宽度：上涨行业占比、强势/弱势行业占比
- 行业分散度：行业收益标准差、偏度、峰度
- 滚动指标：5 日滚动上涨行业占比

命名规范：
- 文件名: industry_features_daily.py
- 类名: IndustryFeaturesDailyMV
- recipe.name: industry_features_daily
- 输出表名: features.mv_industry_features_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class IndustryFeaturesDailyMV(BaseFeatureView):
    """行业特征物化视图（宽度 + 分散度 + 收益分布）"""

    name = "industry_features_daily"
    description = "申万二级行业宽度、分散度、收益分布（日频）"
    source_tables = [
        "tushare.index_swdaily",
        "tushare.index_swmember",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_industry_features_daily AS
        WITH 
        -- ========== 二级行业代码清单 ==========
        l2_codes AS (
            SELECT DISTINCT l2_code AS ts_code
            FROM tushare.index_swmember
            WHERE l2_code IS NOT NULL
        ),
        
        -- ========== 行业日线数据 ==========
        industry_daily AS (
            SELECT 
                d.trade_date,
                d.ts_code,
                d.close
            FROM tushare.index_swdaily d
            JOIN l2_codes l ON d.ts_code = l.ts_code
            WHERE d.close IS NOT NULL AND d.close > 0
        ),
        
        -- ========== 计算日收益率 ==========
        industry_returns AS (
            SELECT 
                trade_date,
                ts_code,
                close,
                (close / NULLIF(LAG(close) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1) AS daily_return
            FROM industry_daily
        ),
        
        -- ========== 按日聚合行业宽度指标 ==========
        daily_breadth AS (
            SELECT 
                trade_date,
                -- 行业数量
                COUNT(*) AS industry_count,
                -- 上涨行业数量与占比
                SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END) AS industry_up_count,
                SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END)::FLOAT 
                    / NULLIF(COUNT(*), 0) AS industry_up_ratio,
                -- 强势行业（涨幅 > 1%）
                SUM(CASE WHEN daily_return > 0.01 THEN 1 ELSE 0 END) AS industry_strong_count,
                SUM(CASE WHEN daily_return > 0.01 THEN 1 ELSE 0 END)::FLOAT 
                    / NULLIF(COUNT(*), 0) AS industry_strong_ratio,
                -- 弱势行业（跌幅 > 1%）
                SUM(CASE WHEN daily_return < -0.01 THEN 1 ELSE 0 END) AS industry_weak_count,
                SUM(CASE WHEN daily_return < -0.01 THEN 1 ELSE 0 END)::FLOAT 
                    / NULLIF(COUNT(*), 0) AS industry_weak_ratio,
                -- 行业收益统计
                AVG(daily_return) AS industry_return_mean,
                STDDEV(daily_return) AS industry_return_std,
                MAX(daily_return) AS industry_return_max,
                MIN(daily_return) AS industry_return_min,
                MAX(daily_return) - MIN(daily_return) AS industry_return_range
            FROM industry_returns
            WHERE daily_return IS NOT NULL
            GROUP BY trade_date
        ),

        -- ========== 行业收益分布形态：偏度/峰度（excess kurtosis）==========
        daily_moments AS (
            SELECT
                r.trade_date,
                CASE
                    WHEN b.industry_return_std IS NULL OR b.industry_return_std = 0 THEN NULL
                    ELSE AVG(POWER(r.daily_return - b.industry_return_mean, 3))
                        / NULLIF(POWER(b.industry_return_std, 3), 0)
                END AS industry_return_skew,
                CASE
                    WHEN b.industry_return_std IS NULL OR b.industry_return_std = 0 THEN NULL
                    ELSE AVG(POWER(r.daily_return - b.industry_return_mean, 4))
                        / NULLIF(POWER(b.industry_return_std, 4), 0) - 3
                END AS industry_return_kurtosis_excess
            FROM industry_returns r
            JOIN daily_breadth b ON r.trade_date = b.trade_date
            WHERE r.daily_return IS NOT NULL
            GROUP BY r.trade_date, b.industry_return_mean, b.industry_return_std
        ),

        daily_enriched AS (
            SELECT
                b.*,
                m.industry_return_skew,
                m.industry_return_kurtosis_excess
            FROM daily_breadth b
            LEFT JOIN daily_moments m ON b.trade_date = m.trade_date
        ),
        
        -- ========== 滚动指标 ==========
        with_rolling AS (
            SELECT 
                b.*,
                -- 5 日滚动上涨行业占比
                AVG(b.industry_up_ratio) OVER (
                    ORDER BY b.trade_date 
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS industry_up_ratio_5d,
                -- 5 日滚动行业分散度
                AVG(b.industry_return_std) OVER (
                    ORDER BY b.trade_date 
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS industry_return_std_5d,
                -- 20 日滚动上涨行业占比
                AVG(b.industry_up_ratio) OVER (
                    ORDER BY b.trade_date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS industry_up_ratio_20d
            FROM daily_enriched b
        )
        
        SELECT 
            trade_date,
            -- 基础计数
            industry_count,
            industry_up_count,
            industry_strong_count,
            industry_weak_count,
            -- 比例
            industry_up_ratio,
            industry_strong_ratio,
            industry_weak_ratio,
            -- 收益统计
            industry_return_mean,
            industry_return_std,
            industry_return_skew,
            industry_return_kurtosis_excess,
            industry_return_max,
            industry_return_min,
            industry_return_range,
            -- 滚动指标
            industry_up_ratio_5d,
            industry_return_std_5d,
            industry_up_ratio_20d,
            -- 血缘
            'tushare.index_swdaily,tushare.index_swmember' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_rolling
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_industry_features_daily_trade_date "
            "ON features.mv_industry_features_daily (trade_date)",
        ]
