"""
市场技术特征（日频）物化视图定义

设计思路：
- 将市场动量分布、波动分布、量价特征等整合到一张 MV
- 这些特征刻画"全市场技术面状态"
- 合并 market_technical + market_return_distribution
- 合并 MarketTurnoverDistributionFetcher（换手率分布）
- 合并 MarketMomentumDistributionFetcher（RSI/MACD/均线分布）
- 合并 MarketVolatilityDistributionFetcher（ATR/日内振幅分布）

数据来源：
- tushare.stock_factor_pro: 个股技术指标

输出指标：
- 动量分布：5/10/20/60 日动量中位数、正动量比例
- 波动分布：20/60 日波动率中位数、高低波动比例
- 量价特征：量比分布、放缩量比例、价量背离
- 收益分布：涨跌比例、强势/弱势比例
- 换手率分布：换手率中位数、高换手比例、成交集中度
- RSI/MACD/均线：RSI 中位数、均线占比、MACD 金叉比例
- ATR/振幅：ATR 中位数、日内振幅分布、高波动比例

命名规范：
- 文件名: market_technical_daily.py
- 类名: MarketTechnicalDailyMV
- recipe.name: market_technical_daily
- 输出表名: features.mv_market_technical_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class MarketTechnicalDailyMV(BaseFeatureView):
    """市场技术特征物化视图（动量 + 波动 + 量价 + 换手 + RSI/MACD + ATR）"""

    name = "market_technical_daily"
    description = "全市场技术面特征：动量/波动/量价/换手/RSI/MACD/ATR 分布（日频）"
    source_tables = [
        "tushare.stock_factor_pro",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_market_technical_daily AS
        WITH 
        -- ========== 基础数据（个股技术指标）==========
        base AS (
            SELECT 
                trade_date,
                ts_code,
                close_hfq,
                high_hfq,
                low_hfq,
                pct_chg,
                amount,
                volume,
                turnover_rate_f,
                atr_hfq,
                rsi_hfq_6,
                macd_dif_hfq,
                macd_dea_hfq,
                ma_hfq_5,
                ma_hfq_10,
                ma_hfq_20,
                ma_hfq_60,
                ma_hfq_250,
                -- 动量
                (close_hfq / NULLIF(LAG(close_hfq, 5) OVER w, 0) - 1) * 100 AS mom_5d,
                (close_hfq / NULLIF(LAG(close_hfq, 10) OVER w, 0) - 1) * 100 AS mom_10d,
                (close_hfq / NULLIF(LAG(close_hfq, 20) OVER w, 0) - 1) * 100 AS mom_20d,
                (close_hfq / NULLIF(LAG(close_hfq, 60) OVER w, 0) - 1) * 100 AS mom_60d,
                -- 量比
                volume / NULLIF(AVG(volume) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), 0) AS vol_ratio_5d,
                volume / NULLIF(AVG(volume) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING), 0) AS vol_ratio_20d,
                -- 波动率
                STDDEV(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_20d,
                STDDEV(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) * SQRT(252) AS vol_60d,
                -- 日内振幅（%）
                (high_hfq - low_hfq) / NULLIF(close_hfq, 0) * 100 AS intraday_range_pct,
                -- ATR 占比（%）
                atr_hfq / NULLIF(close_hfq, 0) * 100 AS atr_pct,
                -- 成交额排名（用于集中度）
                ROW_NUMBER() OVER (PARTITION BY trade_date ORDER BY amount DESC) AS amount_rank
            FROM tushare.stock_factor_pro
            WHERE close_hfq IS NOT NULL AND close_hfq > 0
            WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
        )
        
        -- ========== 按日聚合 ==========
        SELECT 
            trade_date,
            COUNT(*) AS total_count,
            
            -- ===== 动量分布 =====
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_5d) FILTER (WHERE mom_5d IS NOT NULL) AS mom_5d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_10d) FILTER (WHERE mom_10d IS NOT NULL) AS mom_10d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_20d) FILTER (WHERE mom_20d IS NOT NULL) AS mom_20d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_60d) FILTER (WHERE mom_60d IS NOT NULL) AS mom_60d_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mom_20d) FILTER (WHERE mom_20d IS NOT NULL) AS mom_20d_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mom_20d) FILTER (WHERE mom_20d IS NOT NULL) AS mom_20d_q75,
            STDDEV(mom_20d) FILTER (WHERE mom_20d IS NOT NULL) AS mom_20d_std,
            
            -- 动量强度
            SUM(CASE WHEN mom_5d > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE mom_5d IS NOT NULL), 0) AS mom_5d_pos_ratio,
            SUM(CASE WHEN mom_20d > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE mom_20d IS NOT NULL), 0) AS mom_20d_pos_ratio,
            SUM(CASE WHEN mom_60d > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE mom_60d IS NOT NULL), 0) AS mom_60d_pos_ratio,
            SUM(CASE WHEN mom_20d > 10 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE mom_20d IS NOT NULL), 0) AS strong_mom_ratio,
            SUM(CASE WHEN mom_20d < -10 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE mom_20d IS NOT NULL), 0) AS weak_mom_ratio,
            
            -- ===== 波动分布 =====
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_20d) FILTER (WHERE vol_20d IS NOT NULL AND vol_20d > 0) AS vol_20d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_60d) FILTER (WHERE vol_60d IS NOT NULL AND vol_60d > 0) AS vol_60d_median,
            AVG(vol_20d) FILTER (WHERE vol_20d IS NOT NULL AND vol_20d > 0) AS vol_20d_mean,
            SUM(CASE WHEN vol_20d > 50 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE vol_20d IS NOT NULL), 0) AS high_vol_ratio,
            SUM(CASE WHEN vol_20d > 0 AND vol_20d < 20 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE vol_20d IS NOT NULL), 0) AS low_vol_ratio,
            
            -- ===== 量比分布 =====
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_ratio_5d) FILTER (WHERE vol_ratio_5d IS NOT NULL AND vol_ratio_5d > 0) AS vol_ratio_5d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_ratio_20d) FILTER (WHERE vol_ratio_20d IS NOT NULL AND vol_ratio_20d > 0) AS vol_ratio_20d_median,
            SUM(CASE WHEN vol_ratio_5d > 1.5 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) AS vol_expand_ratio,
            SUM(CASE WHEN vol_ratio_5d < 0.7 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) AS vol_shrink_ratio,
            
            -- ===== 价量背离 =====
            SUM(CASE WHEN pct_chg > 0 AND vol_ratio_5d < 0.8 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) AS price_up_vol_down_ratio,
            SUM(CASE WHEN pct_chg < 0 AND vol_ratio_5d > 1.2 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) AS price_down_vol_up_ratio,
            SUM(CASE WHEN (pct_chg > 0 AND vol_ratio_5d > 1) OR (pct_chg < 0 AND vol_ratio_5d < 1) THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) AS vol_price_aligned_ratio,
            
            -- ===== 收益分布 =====
            SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
            SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count,
            SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS up_ratio,
            SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS down_ratio,
            SUM(CASE WHEN pct_chg > 5 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS strong_up_ratio,
            SUM(CASE WHEN pct_chg < -5 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS strong_down_ratio,
            AVG(pct_chg) AS mean_return,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_chg) AS median_return,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pct_chg) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pct_chg) AS return_iqr,
            STDDEV(pct_chg) AS return_std,
            AVG(pct_chg) FILTER (WHERE pct_chg > 0) AS avg_up_return,
            AVG(pct_chg) FILTER (WHERE pct_chg < 0) AS avg_down_return,
            
            -- ===== 换手率分布（来自 MarketTurnoverDistributionFetcher）=====
            AVG(turnover_rate_f) FILTER (WHERE turnover_rate_f IS NOT NULL) AS turnover_mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_rate_f) FILTER (WHERE turnover_rate_f IS NOT NULL) AS turnover_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY turnover_rate_f) FILTER (WHERE turnover_rate_f IS NOT NULL) AS turnover_q75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY turnover_rate_f) FILTER (WHERE turnover_rate_f IS NOT NULL) AS turnover_q90,
            SUM(CASE WHEN turnover_rate_f > 5 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE turnover_rate_f IS NOT NULL), 0) AS high_turnover_ratio,
            SUM(CASE WHEN turnover_rate_f > 10 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE turnover_rate_f IS NOT NULL), 0) AS very_high_turnover_ratio,
            SUM(CASE WHEN turnover_rate_f < 1 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE turnover_rate_f IS NOT NULL), 0) AS low_turnover_ratio,
            -- 成交集中度
            SUM(amount) / 100000000 AS total_amount_billion,
            SUM(CASE WHEN amount_rank <= 10 THEN amount ELSE 0 END) / NULLIF(SUM(amount), 0) AS top10_amount_ratio,
            SUM(CASE WHEN amount_rank <= 50 THEN amount ELSE 0 END) / NULLIF(SUM(amount), 0) AS top50_amount_ratio,
            
            -- ===== RSI/MACD/均线分布（来自 MarketMomentumDistributionFetcher）=====
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rsi_hfq_6) FILTER (WHERE rsi_hfq_6 IS NOT NULL) AS rsi6_median,
            SUM(CASE WHEN rsi_hfq_6 > 80 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE rsi_hfq_6 IS NOT NULL), 0) AS rsi_overbought_ratio,
            SUM(CASE WHEN rsi_hfq_6 < 20 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE rsi_hfq_6 IS NOT NULL), 0) AS rsi_oversold_ratio,
            SUM(CASE WHEN macd_dif_hfq > macd_dea_hfq THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE macd_dif_hfq IS NOT NULL AND macd_dea_hfq IS NOT NULL), 0) AS macd_golden_ratio,
            SUM(CASE WHEN macd_dif_hfq > 0 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE macd_dif_hfq IS NOT NULL), 0) AS macd_above_zero_ratio,
            SUM(CASE WHEN close_hfq > ma_hfq_5 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE ma_hfq_5 IS NOT NULL), 0) AS above_ma5_ratio,
            SUM(CASE WHEN close_hfq > ma_hfq_10 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE ma_hfq_10 IS NOT NULL), 0) AS above_ma10_ratio,
            SUM(CASE WHEN close_hfq > ma_hfq_20 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE ma_hfq_20 IS NOT NULL), 0) AS above_ma20_ratio,
            SUM(CASE WHEN close_hfq > ma_hfq_60 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE ma_hfq_60 IS NOT NULL), 0) AS above_ma60_ratio,
            SUM(CASE WHEN close_hfq > ma_hfq_250 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE ma_hfq_250 IS NOT NULL), 0) AS above_ma250_ratio,
            
            -- ===== ATR/振幅分布（来自 MarketVolatilityDistributionFetcher）=====
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY atr_pct) FILTER (WHERE atr_pct IS NOT NULL AND atr_pct > 0) AS atr_pct_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY atr_pct) FILTER (WHERE atr_pct IS NOT NULL AND atr_pct > 0) AS atr_pct_q75,
            AVG(intraday_range_pct) FILTER (WHERE intraday_range_pct IS NOT NULL) AS intraday_range_mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY intraday_range_pct) FILTER (WHERE intraday_range_pct IS NOT NULL) AS intraday_range_median,
            SUM(CASE WHEN intraday_range_pct > 5 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*) FILTER (WHERE intraday_range_pct IS NOT NULL), 0) AS high_intraday_range_ratio,
            
            -- 血缘
            'tushare.stock_factor_pro' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM base
        GROUP BY trade_date
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_market_technical_daily_trade_date "
            "ON features.mv_market_technical_daily (trade_date)",
        ]
