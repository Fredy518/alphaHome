"""
市场情绪综合特征（日频）物化视图定义

设计思路：
- 将市场宽度、涨跌停、短线情绪、融资融券等日频截面特征整合到一张 MV
- 这些特征共同刻画"市场整体情绪状态"
- 避免多个小 MV 的碎片化问题

数据来源：
- tushare.stock_factor_pro: MA 占比、新高新低
- tushare.stock_limitlist: 涨跌停明细
- tushare.stock_st: ST 状态（精确识别）
- tushare.stock_margin: 融资融券余额与交易（日频）
- tushare.stock_dailybasic: 流通市值（计算融资占比）

输出指标：
- 市场宽度：MA60/MA90 占比
- 涨跌停：家数、比值、连板、炸板
- 新高新低：52 周新高新低比
- 融资融券：融资余额、融资占流通市值比、融资净买入

命名规范：
- 文件名: market_sentiment_daily.py
- 类名: MarketSentimentDailyMV
- recipe.name: market_sentiment_daily
- 输出表名: features.mv_market_sentiment_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class MarketSentimentDailyMV(BaseFeatureView):
    """市场情绪综合特征物化视图（宽度 + 涨跌停 + 新高新低 + 融资融券）"""

    name = "market_sentiment_daily"
    description = "市场情绪综合特征：MA 占比、涨跌停、新高新低、融资融券（日频）"
    source_tables = [
        "tushare.stock_factor_pro",
        "tushare.stock_limitlist",
        "tushare.stock_st",
        "tushare.stock_margin",
        "tushare.stock_dailybasic",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_market_sentiment_daily AS
        WITH 
        -- ========== 市场宽度（MA 占比）==========
        ma_stats AS (
            SELECT 
                trade_date,
                COUNT(*) AS total_stock_count,
                SUM(CASE WHEN close_hfq > ma_hfq_60 THEN 1 ELSE 0 END) AS above_ma60_count,
                SUM(CASE WHEN close_hfq > ma_hfq_90 THEN 1 ELSE 0 END) AS above_ma90_count,
                -- 上涨/下跌家数
                SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
                SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count
            FROM tushare.stock_factor_pro
            WHERE close_hfq IS NOT NULL
            GROUP BY trade_date
        ),

        -- ========== 新高新低（52周，按个股）==========
        prices AS (
            SELECT ts_code, trade_date, close_hfq
            FROM tushare.stock_factor_pro
            WHERE close_hfq IS NOT NULL
        ),
        rolling_extremes AS (
            SELECT
                ts_code,
                trade_date,
                close_hfq,
                COUNT(*) OVER (
                    PARTITION BY ts_code ORDER BY trade_date
                    ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                ) AS rolling_cnt_prev_252,
                MAX(close_hfq) OVER (
                    PARTITION BY ts_code ORDER BY trade_date
                    ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                ) AS rolling_max_prev_252,
                MIN(close_hfq) OVER (
                    PARTITION BY ts_code ORDER BY trade_date
                    ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING
                ) AS rolling_min_prev_252
            FROM prices
        ),
        new_high_low AS (
            SELECT
                trade_date,
                -- 约束：至少有 126 根历史K线后才开始计算新高新低（不足252根也允许计算，但需有最小历史长度）
                SUM(
                    CASE
                        WHEN rolling_cnt_prev_252 >= 126
                         AND rolling_max_prev_252 IS NOT NULL
                         AND close_hfq > rolling_max_prev_252
                        THEN 1 ELSE 0
                    END
                ) AS new_high_count,
                SUM(
                    CASE
                        WHEN rolling_cnt_prev_252 >= 126
                         AND rolling_min_prev_252 IS NOT NULL
                         AND close_hfq < rolling_min_prev_252
                        THEN 1 ELSE 0
                    END
                ) AS new_low_count
            FROM rolling_extremes
            GROUP BY trade_date
        ),
        
        -- ========== 涨跌停统计（精确识别 ST）==========
        limit_with_st AS (
            SELECT 
                l.trade_date,
                l.ts_code,
                l."limit",
                l.amount,
                l.open_times,
                l.limit_times,
                l.first_time,
                CASE WHEN s.ts_code IS NOT NULL THEN 1 ELSE 0 END AS is_st,
                -- 板块分类
                CASE 
                    -- 北交所：统一使用 .BJ 后缀识别（避免漏掉 43xxxx.BJ 等代码段）
                    WHEN l.ts_code LIKE '%.BJ' THEN 'bse'
                    WHEN l.ts_code LIKE '688%.SH' OR l.ts_code LIKE '689%.SH'
                        OR l.ts_code LIKE '300%.SZ' OR l.ts_code LIKE '301%.SZ'
                    THEN '20cm'
                    ELSE 'main'
                END AS board_type
            FROM tushare.stock_limitlist l
            LEFT JOIN tushare.stock_st s 
                ON l.ts_code = s.ts_code AND l.trade_date = s.trade_date
        ),
        limit_stats AS (
            SELECT 
                trade_date,
                -- 涨跌停家数
                SUM(CASE WHEN "limit" = 'U' THEN 1 ELSE 0 END) AS limit_up_count,
                SUM(CASE WHEN "limit" = 'D' THEN 1 ELSE 0 END) AS limit_down_count,
                -- 非 ST 涨跌停
                SUM(CASE WHEN "limit" = 'U' AND is_st = 0 THEN 1 ELSE 0 END) AS limit_up_non_st,
                SUM(CASE WHEN "limit" = 'D' AND is_st = 0 THEN 1 ELSE 0 END) AS limit_down_non_st,
                -- 20cm 板涨停
                SUM(CASE WHEN "limit" = 'U' AND is_st = 0 AND board_type = '20cm' THEN 1 ELSE 0 END) AS limit_up_20cm,
                -- 北交所涨停（30cm）
                SUM(CASE WHEN "limit" = 'U' AND is_st = 0 AND board_type = 'bse' THEN 1 ELSE 0 END) AS limit_up_bse,
                -- 涨停成交额（亿元）
                SUM(CASE WHEN "limit" = 'U' THEN amount ELSE 0 END) / 1e8 AS limit_up_amount,
                -- 炸板统计
                SUM(CASE WHEN "limit" = 'U' AND open_times > 0 THEN 1 ELSE 0 END) AS broken_limit_count,
                SUM(CASE WHEN "limit" = 'U' AND is_st = 0 AND open_times > 0 THEN 1 ELSE 0 END) AS broken_limit_count_non_st,
                AVG(CASE WHEN "limit" = 'U' THEN open_times ELSE NULL END) AS avg_open_times,
                AVG(CASE WHEN "limit" = 'U' AND is_st = 0 THEN open_times ELSE NULL END) AS avg_open_times_non_st,
                -- 连板分布
                SUM(CASE WHEN "limit" = 'U' AND is_st = 0 AND limit_times >= 2 THEN 1 ELSE 0 END) AS consec_2_count,
                SUM(CASE WHEN "limit" = 'U' AND is_st = 0 AND limit_times >= 3 THEN 1 ELSE 0 END) AS consec_3_count,
                SUM(CASE WHEN "limit" = 'U' AND is_st = 0 AND limit_times >= 5 THEN 1 ELSE 0 END) AS consec_5_count,
                MAX(CASE WHEN "limit" = 'U' AND is_st = 0 THEN limit_times ELSE 0 END) AS max_consec,
                -- 早盘封板（10:00 前）
                SUM(
                    CASE
                        WHEN "limit" = 'U'
                         AND (
                            CASE
                                -- tushare 任务 schema 约定 first_time 为 'HH:MM:SS'
                                WHEN first_time ~ '^[0-9]{2}:[0-9]{2}:[0-9]{2}$'
                                THEN REPLACE(SUBSTRING(first_time FROM 1 FOR 5), ':', '')
                                -- 兼容旧数据 'HHMMSS'
                                WHEN first_time ~ '^[0-9]{6}$'
                                THEN SUBSTRING(first_time FROM 1 FOR 4)
                                ELSE NULL
                            END
                         )::INTEGER < 1000
                        THEN 1 ELSE 0
                    END
                ) AS early_limit_count
                ,
                SUM(
                    CASE
                        WHEN "limit" = 'U'
                         AND is_st = 0
                         AND (
                            CASE
                                WHEN first_time ~ '^[0-9]{2}:[0-9]{2}:[0-9]{2}$'
                                THEN REPLACE(SUBSTRING(first_time FROM 1 FOR 5), ':', '')
                                WHEN first_time ~ '^[0-9]{6}$'
                                THEN SUBSTRING(first_time FROM 1 FOR 4)
                                ELSE NULL
                            END
                         )::INTEGER < 1000
                        THEN 1 ELSE 0
                    END
                ) AS early_limit_count_non_st
            FROM limit_with_st
            GROUP BY trade_date
        ),
        
        -- ========== 融资融券（日频）==========
        margin_daily AS (
            SELECT 
                trade_date,
                SUM(rzye) AS total_margin_balance,      -- 融资余额
                SUM(rzmre) AS total_margin_buy,         -- 融资买入额
                SUM(rzche) AS total_margin_repay,       -- 融资偿还额
                SUM(rqye) AS total_short_balance        -- 融券余额
            FROM tushare.stock_margin
            GROUP BY trade_date
        ),
        market_cap AS (
            SELECT 
                trade_date,
                SUM(circ_mv) AS total_circ_mv   -- 流通市值（万元）
            FROM tushare.stock_dailybasic
            GROUP BY trade_date
        ),
        margin_stats AS (
            SELECT 
                mg.trade_date,
                mg.total_margin_balance,
                mg.total_margin_buy,
                mg.total_margin_repay,
                mg.total_short_balance,
                mc.total_circ_mv,
                -- 单位转换（元 → 亿元），用于下游展示/对齐
                mg.total_margin_balance / 1e8 AS total_margin_balance_billion,
                mg.total_margin_buy / 1e8 AS total_margin_buy_billion,
                mg.total_margin_repay / 1e8 AS total_margin_repay_billion,
                mg.total_short_balance / 1e8 AS total_short_balance_billion,
                -- 融资余额占流通市值比（rzye: 元, circ_mv: 万元）
                mg.total_margin_balance / NULLIF(mc.total_circ_mv * 10000, 0) AS margin_circ_ratio,
                -- 融资净买入（亿元）
                (COALESCE(mg.total_margin_buy, 0) - COALESCE(mg.total_margin_repay, 0)) / 1e8 AS margin_net_buy_billion,
                -- 融资/融券比
                mg.total_margin_balance / NULLIF(mg.total_short_balance, 0) AS margin_short_ratio
            FROM margin_daily mg
            LEFT JOIN market_cap mc ON mg.trade_date = mc.trade_date
        )
        
        -- ========== 合并输出 ==========
        SELECT 
            m.trade_date,
            -- 市场宽度
            m.total_stock_count,
            m.above_ma60_count,
            m.above_ma90_count,
            m.above_ma60_count::FLOAT / NULLIF(m.total_stock_count, 0) AS above_ma60_ratio,
            m.above_ma90_count::FLOAT / NULLIF(m.total_stock_count, 0) AS above_ma90_ratio,
            -- 涨跌比
            m.up_count,
            m.down_count,
            m.up_count::FLOAT / NULLIF(m.down_count, 0) AS up_down_ratio,
            m.up_count::FLOAT / NULLIF(m.down_count + 1, 0) AS up_down_ratio_smooth,
            -- 新高新低
            COALESCE(nh.new_high_count, 0) AS new_high_count,
            COALESCE(nh.new_low_count, 0) AS new_low_count,
            COALESCE(nh.new_high_count, 0)::FLOAT / NULLIF(COALESCE(nh.new_low_count, 0), 0) AS new_high_low_ratio,
            COALESCE(nh.new_high_count, 0)::FLOAT / NULLIF(COALESCE(nh.new_low_count, 0) + 1, 0) AS new_high_low_ratio_smooth,
            -- 涨跌停
            COALESCE(l.limit_up_count, 0) AS limit_up_count,
            COALESCE(l.limit_down_count, 0) AS limit_down_count,
            COALESCE(l.limit_up_non_st, 0) AS limit_up_non_st,
            COALESCE(l.limit_down_non_st, 0) AS limit_down_non_st,
            COALESCE(l.limit_up_20cm, 0) AS limit_up_20cm,
            COALESCE(l.limit_up_bse, 0) AS limit_up_bse,
            COALESCE(l.limit_up_count, 0)::FLOAT / NULLIF(COALESCE(l.limit_down_count, 0), 0) AS limit_up_down_ratio,
            COALESCE(l.limit_up_count, 0)::FLOAT / (COALESCE(l.limit_down_count, 0) + 1) AS limit_up_down_ratio_smooth,
            COALESCE(l.limit_up_non_st, 0)::FLOAT / NULLIF(COALESCE(l.limit_down_non_st, 0), 0) AS limit_up_down_ratio_non_st,
            COALESCE(l.limit_up_non_st, 0)::FLOAT / (COALESCE(l.limit_down_non_st, 0) + 1) AS limit_up_down_ratio_non_st_smooth,
            -- 涨停成交与炸板
            COALESCE(l.limit_up_amount, 0) AS limit_up_amount,
            COALESCE(l.broken_limit_count, 0) AS broken_limit_count,
            l.broken_limit_count::FLOAT / NULLIF(l.limit_up_count, 0) AS broken_limit_ratio,
            COALESCE(l.broken_limit_count_non_st, 0) AS broken_limit_count_non_st,
            l.broken_limit_count_non_st::FLOAT / NULLIF(l.limit_up_non_st, 0) AS broken_limit_ratio_non_st,
            l.avg_open_times,
            l.avg_open_times_non_st,
            -- 连板
            COALESCE(l.consec_2_count, 0) AS consec_2_count,
            COALESCE(l.consec_3_count, 0) AS consec_3_count,
            COALESCE(l.consec_5_count, 0) AS consec_5_count,
            COALESCE(l.max_consec, 0) AS max_consec,
            COALESCE(l.early_limit_count, 0) AS early_limit_count,
            COALESCE(l.early_limit_count_non_st, 0) AS early_limit_count_non_st,
            l.early_limit_count::FLOAT / NULLIF(l.limit_up_count, 0) AS early_limit_ratio,
            l.early_limit_count_non_st::FLOAT / NULLIF(l.limit_up_non_st, 0) AS early_limit_ratio_non_st,
            -- 融资融券
            COALESCE(mg.total_margin_balance, 0) AS total_margin_balance,
            COALESCE(mg.total_margin_buy, 0) AS total_margin_buy,
            COALESCE(mg.total_margin_repay, 0) AS total_margin_repay,
            COALESCE(mg.total_short_balance, 0) AS total_short_balance,
            COALESCE(mg.total_margin_balance_billion, 0) AS total_margin_balance_billion,
            COALESCE(mg.total_margin_buy_billion, 0) AS total_margin_buy_billion,
            COALESCE(mg.total_margin_repay_billion, 0) AS total_margin_repay_billion,
            COALESCE(mg.total_short_balance_billion, 0) AS total_short_balance_billion,
            mg.margin_circ_ratio,
            mg.margin_net_buy_billion,
            mg.margin_short_ratio,
            -- 血缘
            'tushare.stock_factor_pro,tushare.stock_limitlist,tushare.stock_st,tushare.stock_margin,tushare.stock_dailybasic' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM ma_stats m
        LEFT JOIN limit_stats l ON m.trade_date = l.trade_date
        LEFT JOIN new_high_low nh ON m.trade_date = nh.trade_date
        LEFT JOIN margin_stats mg ON m.trade_date = mg.trade_date
        ORDER BY m.trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        # 常见用法：按 trade_date 拉取/关联市场情绪特征
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_market_sentiment_daily_trade_date "
            "ON features.mv_market_sentiment_daily (trade_date)",
        ]
