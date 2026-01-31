"""
龙虎榜事件信号特征（日频，稀疏事件表）

说明：
- 本表是“事件表”语义：仅在股票当日上榜时才有记录，缺失代表 0 事件而非缺数
- 滚动窗口按“交易日”口径计算（而非按事件行数），避免稀疏导致语义漂移
"""

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class StockToplistEventDailyMV(BaseFeatureView):
    """龙虎榜事件信号（日频，稀疏事件表）。"""

    name = "stock_toplist_event_daily"
    description = "龙虎榜事件信号：机构净买入、成交占比、上榜频次（稀疏事件表，滚动按交易日）"
    source_tables = [
        "rawdata.stock_toplist",
        "rawdata.stock_daily",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_toplist_event_daily AS
        WITH
        trade_calendar AS (
            -- 全市场交易日序号：用于“真·交易日滚动”
            SELECT
                trade_date,
                DENSE_RANK() OVER (ORDER BY trade_date) AS trade_rank
            FROM (
                SELECT DISTINCT trade_date
                FROM rawdata.stock_daily
                WHERE trade_date IS NOT NULL
            ) t
        ),

        daily_agg AS (
            -- 按日聚合（一只股票可能多次上榜/多理由）
            SELECT
                trade_date,
                ts_code,
                MAX(name) AS name,
                MAX(close) AS close,
                MAX(pct_change) AS pct_change,
                MAX(turnover_rate) AS turnover_rate,
                SUM(l_buy) AS total_buy,
                SUM(l_sell) AS total_sell,
                SUM(net_amount) AS net_amount,
                MAX(amount) AS stock_amount,
                COUNT(*) AS toplist_count,
                STRING_AGG(DISTINCT reason, '|') AS reasons
            FROM rawdata.stock_toplist
            WHERE trade_date IS NOT NULL AND ts_code IS NOT NULL
            GROUP BY trade_date, ts_code
        ),

        ranked AS (
            SELECT
                a.*,
                c.trade_rank
            FROM daily_agg a
            JOIN trade_calendar c ON a.trade_date = c.trade_date
        ),

        with_rolling AS (
            SELECT
                trade_date,
                ts_code,
                name,
                close,
                pct_change,
                turnover_rate,
                total_buy,
                total_sell,
                net_amount,
                stock_amount,
                toplist_count,
                reasons,

                -- 净买入占成交额比例
                CASE
                    WHEN stock_amount > 0 THEN net_amount * 100.0 / stock_amount
                    ELSE NULL
                END AS net_amount_ratio,

                -- 近 5 个交易日累计净买入（缺失交易日默认为 0）
                SUM(net_amount) OVER (
                    PARTITION BY ts_code
                    ORDER BY trade_rank
                    RANGE BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS net_amount_5td,

                -- 近 20 个交易日上榜“天数”（缺失交易日默认为 0）
                COUNT(*) OVER (
                    PARTITION BY ts_code
                    ORDER BY trade_rank
                    RANGE BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS toplist_days_20td,

                -- 近 20 个交易日上榜“次数”（当日多理由/多记录会计入）
                SUM(toplist_count) OVER (
                    PARTITION BY ts_code
                    ORDER BY trade_rank
                    RANGE BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS toplist_count_20td
            FROM ranked
        )

        SELECT
            trade_date,
            ts_code,
            name,
            close,
            pct_change,
            turnover_rate,
            total_buy,
            total_sell,
            net_amount,
            net_amount_ratio,
            net_amount_5td,
            toplist_count,
            toplist_days_20td,
            toplist_count_20td,
            reasons,
            -- 机构信号（根据 reasons 判断）
            CASE WHEN reasons LIKE '%%机构%%' THEN 1 ELSE 0 END AS has_institution,
            -- 血缘字段
            'rawdata.stock_toplist,rawdata.stock_daily' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_rolling
        ORDER BY ts_code, trade_date DESC;
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_toplist_event_daily_trade_date "
            "ON features.mv_stock_toplist_event_daily (trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_toplist_event_daily_ts_code_trade_date "
            "ON features.mv_stock_toplist_event_daily (ts_code, trade_date)",
        ]


# 兼容旧类名（外部 import 不断）
StockToplistSignalMV = StockToplistEventDailyMV

__all__ = ["StockToplistEventDailyMV", "StockToplistSignalMV"]

