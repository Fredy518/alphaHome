"""
两融成交占比特征（日频）物化视图定义

设计思路：
- 将两融成交占比特征独立为日频 MV
- 与已废弃的 market_margin_monthly（月末快照，现已并入 market_sentiment_daily）分开，因为：
  1. 粒度不同（日频 vs 月频）
  2. 语义不同（成交占比 vs 余额规模）

数据来源：
- tushare.stock_margin: 融资买入/偿还、融券卖出/偿还
- tushare.stock_daily: 市场成交额（计算占比分母）

输出指标：
- 融资买入占比 = 融资买入 / 市场成交额
- 融资偿还占比 = 融资偿还 / 市场成交额
- 净融资占比 = (融资买入 - 融资偿还) / 市场成交额
- 融券卖出占比 = 融券卖出 / 市场成交额
- 净融券占比 = (融券卖出 - 融券偿还) / 市场成交额
- 占比异常突增信号

命名规范：
- 文件名: margin_turnover_daily.py
- 类名: MarginTurnoverDailyMV
- recipe.name: margin_turnover_daily
- 输出表名: features.mv_margin_turnover_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class MarginTurnoverDailyMV(BaseFeatureView):
    """两融成交占比物化视图（日频）"""

    name = "margin_turnover_daily"
    description = "两融成交占比与异常突增信号（日频）"
    source_tables = [
        "tushare.stock_margin",
        "tushare.stock_daily",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_margin_turnover_daily AS
        WITH 
        -- 市场当日总成交额/成交量（分别用于金额口径与数量口径的占比）
        market_turnover AS (
            SELECT 
                trade_date,
                -- stock_daily.amount 单位：千元，转换为元
                SUM(amount) FILTER (WHERE amount IS NOT NULL AND amount > 0) * 1000 AS market_amount_yuan,
                -- stock_daily.vol：成交量（用于融券卖出量占比的分母）
                SUM(vol) FILTER (WHERE vol IS NOT NULL AND vol > 0) AS market_vol
            FROM tushare.stock_daily
            WHERE trade_date IS NOT NULL
            GROUP BY trade_date
        ),
        
        -- 全市场融资融券数据汇总
        margin_agg AS (
            SELECT 
                trade_date,
                SUM(rzye) AS rzye_total,      -- 融资余额
                SUM(rzmre) AS rzmre_total,    -- 融资买入
                SUM(rzche) AS rzche_total,    -- 融资偿还
                SUM(rqye) AS rqye_total,      -- 融券余额
                SUM(rqmcl) AS rqmcl_total     -- 融券卖出量（注意：任务 schema 中通常无 rqchl 字段）
            FROM tushare.stock_margin
            GROUP BY trade_date
        ),
        
        -- 计算占比
        merged AS (
            SELECT 
                m.trade_date,
                m.rzye_total,
                m.rzmre_total,
                m.rzche_total,
                m.rqye_total,
                m.rqmcl_total,
                t.market_amount_yuan,
                t.market_vol,
                -- 两融成交（融资买入 + 融资偿还）占比：对齐 data_infra.fetchers.margin.MarginTurnoverFetcher 的口径
                (COALESCE(m.rzmre_total, 0) + COALESCE(m.rzche_total, 0)) / NULLIF(t.market_amount_yuan, 0) * 100 AS margin_turnover_ratio,
                -- 融资买入占比（%）
                m.rzmre_total / NULLIF(t.market_amount_yuan, 0) * 100 AS rz_buy_ratio,
                -- 融资偿还占比（%）
                m.rzche_total / NULLIF(t.market_amount_yuan, 0) * 100 AS rz_repay_ratio,
                -- 净融资占比（%）
                (COALESCE(m.rzmre_total, 0) - COALESCE(m.rzche_total, 0)) / NULLIF(t.market_amount_yuan, 0) * 100 AS net_rz_ratio,
                -- 融券卖出占比（%）：融券卖出量（数量口径）/ 市场成交量（数量口径）
                COALESCE(m.rqmcl_total, 0) / NULLIF(t.market_vol, 0) * 100 AS rq_sell_ratio
            FROM margin_agg m
            JOIN market_turnover t ON m.trade_date = t.trade_date
        ),
        
        -- 添加滚动统计和突增信号
        with_signal AS (
            SELECT 
                trade_date,
                rzye_total,
                rzmre_total,
                rzche_total,
                rqye_total,
                rqmcl_total,
                market_amount_yuan,
                market_vol,
                margin_turnover_ratio,
                rz_buy_ratio,
                rz_repay_ratio,
                net_rz_ratio,
                rq_sell_ratio,
                -- 20 日滚动均值
                AVG(rz_buy_ratio) OVER (
                    ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS rz_buy_ratio_avg20,
                AVG(net_rz_ratio) OVER (
                    ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS net_rz_ratio_avg20,
                AVG(margin_turnover_ratio) OVER (
                    ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS margin_turnover_ratio_avg20,
                -- 20 日滚动标准差
                STDDEV(rz_buy_ratio) OVER (
                    ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS rz_buy_ratio_std20,
                STDDEV(net_rz_ratio) OVER (
                    ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS net_rz_ratio_std20
            FROM merged
        )
        
        SELECT 
            trade_date,
            rzye_total,
            rzmre_total,
            rzche_total,
            rqye_total,
            rqmcl_total,
            market_amount_yuan,
            market_vol,
            margin_turnover_ratio,
            rz_buy_ratio,
            rz_repay_ratio,
            net_rz_ratio,
            rq_sell_ratio,
            rz_buy_ratio_avg20,
            net_rz_ratio_avg20,
            -- 突增信号（超过 2 倍标准差）
            CASE 
                WHEN rz_buy_ratio_std20 > 0 AND 
                     (rz_buy_ratio - rz_buy_ratio_avg20) / rz_buy_ratio_std20 > 2 
                THEN 1 
                ELSE 0 
            END AS rz_buy_spike,
            CASE 
                WHEN net_rz_ratio_std20 > 0 AND 
                     (net_rz_ratio - net_rz_ratio_avg20) / net_rz_ratio_std20 > 2 
                THEN 1 
                ELSE 0 
            END AS net_rz_spike,
            CASE
                WHEN margin_turnover_ratio_avg20 IS NOT NULL AND margin_turnover_ratio_avg20 > 0
                THEN margin_turnover_ratio / margin_turnover_ratio_avg20 - 1
                ELSE NULL
            END AS margin_turnover_spike,
            -- 血缘
            'tushare.stock_margin,tushare.stock_daily' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_signal
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_margin_turnover_daily_trade_date "
            "ON features.mv_margin_turnover_daily (trade_date)",
        ]
