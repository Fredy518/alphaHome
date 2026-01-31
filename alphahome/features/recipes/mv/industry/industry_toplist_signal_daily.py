"""
行业龙虎榜聚合信号（日频）

将 rawdata.stock_toplist 的个股龙虎榜事件，按申万行业（一级/二级）聚合为日频行业信号，
便于研究“热钱/机构偏好”的行业轮动。

设计要点：
- 行业映射使用 rawdata.index_swmember 的 in_date/out_date 进行 as-of 关联，避免未来函数
- 输出为“全量日历 × 行业维度”的稠密表：没有上榜事件的行业当日输出 0
"""

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class IndustryToplistSignalDailyMV(BaseFeatureView):
    """行业龙虎榜聚合信号（日频，申万一级/二级）。"""

    name = "industry_toplist_signal_daily"
    description = "申万一级/二级行业龙虎榜聚合信号（日频）"
    source_tables = [
        "rawdata.stock_toplist",
        "rawdata.index_swmember",
        "rawdata.stock_daily",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_industry_toplist_signal_daily AS
        WITH
        trade_dates AS (
            -- 以全市场交易日为基准，保证行业表稠密
            SELECT DISTINCT trade_date
            FROM rawdata.stock_daily
            WHERE trade_date IS NOT NULL
        ),

        industry_dim AS (
            -- 申万一级行业维度
            SELECT
                1 AS industry_level,
                l1_code AS industry_code,
                MAX(l1_name) AS industry_name,
                NULL::text AS parent_code,
                NULL::text AS parent_name
            FROM rawdata.index_swmember
            WHERE l1_code IS NOT NULL AND l1_name IS NOT NULL
            GROUP BY l1_code

            UNION ALL

            -- 申万二级行业维度（同时保留所属一级）
            SELECT
                2 AS industry_level,
                l2_code AS industry_code,
                MAX(l2_name) AS industry_name,
                MAX(l1_code) AS parent_code,
                MAX(l1_name) AS parent_name
            FROM rawdata.index_swmember
            WHERE l2_code IS NOT NULL AND l2_name IS NOT NULL
            GROUP BY l2_code
        ),

        daily_stock_toplist AS (
            -- 按股票按日聚合（一只股票可能多次上榜/多理由）
            SELECT
                trade_date,
                ts_code,
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

        stock_industry_asof AS (
            -- 以 trade_date 做 as-of 映射，避免使用月末快照导致的未来函数
            SELECT
                d.trade_date,
                d.ts_code,
                i.l1_code,
                i.l1_name,
                i.l2_code,
                i.l2_name,
                ROW_NUMBER() OVER (
                    PARTITION BY d.trade_date, d.ts_code
                    ORDER BY i.in_date DESC
                ) AS rn
            FROM daily_stock_toplist d
            JOIN rawdata.index_swmember i
              ON i.ts_code = d.ts_code
             AND i.in_date <= d.trade_date
             AND COALESCE(i.out_date, '2099-12-31'::date) > d.trade_date
        ),

        stock_industry AS (
            SELECT
                trade_date,
                ts_code,
                l1_code,
                l1_name,
                l2_code,
                l2_name
            FROM stock_industry_asof
            WHERE rn = 1
        ),

        industry_agg AS (
            -- 一级行业聚合
            SELECT
                d.trade_date,
                1 AS industry_level,
                s.l1_code AS industry_code,
                MAX(s.l1_name) AS industry_name,
                NULL::text AS parent_code,
                NULL::text AS parent_name,
                COUNT(DISTINCT d.ts_code) AS toplist_stock_count,
                SUM(d.toplist_count) AS toplist_event_count,
                SUM(d.total_buy) AS total_buy,
                SUM(d.total_sell) AS total_sell,
                SUM(d.net_amount) AS net_amount,
                SUM(d.stock_amount) AS stock_amount_sum
            FROM daily_stock_toplist d
            JOIN stock_industry s
              ON d.trade_date = s.trade_date AND d.ts_code = s.ts_code
            WHERE s.l1_code IS NOT NULL
            GROUP BY d.trade_date, s.l1_code

            UNION ALL

            -- 二级行业聚合
            SELECT
                d.trade_date,
                2 AS industry_level,
                s.l2_code AS industry_code,
                MAX(s.l2_name) AS industry_name,
                MAX(s.l1_code) AS parent_code,
                MAX(s.l1_name) AS parent_name,
                COUNT(DISTINCT d.ts_code) AS toplist_stock_count,
                SUM(d.toplist_count) AS toplist_event_count,
                SUM(d.total_buy) AS total_buy,
                SUM(d.total_sell) AS total_sell,
                SUM(d.net_amount) AS net_amount,
                SUM(d.stock_amount) AS stock_amount_sum
            FROM daily_stock_toplist d
            JOIN stock_industry s
              ON d.trade_date = s.trade_date AND d.ts_code = s.ts_code
            WHERE s.l2_code IS NOT NULL
            GROUP BY d.trade_date, s.l2_code
        )

        SELECT
            t.trade_date,
            dim.industry_level,
            dim.industry_code,
            dim.industry_name,
            dim.parent_code,
            dim.parent_name,
            COALESCE(a.toplist_stock_count, 0) AS toplist_stock_count,
            COALESCE(a.toplist_event_count, 0) AS toplist_event_count,
            COALESCE(a.total_buy, 0) AS total_buy,
            COALESCE(a.total_sell, 0) AS total_sell,
            COALESCE(a.net_amount, 0) AS net_amount,
            COALESCE(a.stock_amount_sum, 0) AS stock_amount_sum,
            CASE
                WHEN COALESCE(a.stock_amount_sum, 0) > 0
                THEN a.net_amount / a.stock_amount_sum
                ELSE NULL
            END AS net_amount_ratio,
            -- 血缘字段
            'rawdata.stock_toplist,rawdata.index_swmember,rawdata.stock_daily' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM trade_dates t
        CROSS JOIN industry_dim dim
        LEFT JOIN industry_agg a
          ON a.trade_date = t.trade_date
         AND a.industry_level = dim.industry_level
         AND a.industry_code = dim.industry_code
        ORDER BY t.trade_date, dim.industry_level, dim.industry_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_industry_toplist_signal_daily_trade_date "
            "ON features.mv_industry_toplist_signal_daily (trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_industry_toplist_signal_daily_industry "
            "ON features.mv_industry_toplist_signal_daily (industry_level, industry_code, trade_date)",
        ]


__all__ = ["IndustryToplistSignalDailyMV"]
