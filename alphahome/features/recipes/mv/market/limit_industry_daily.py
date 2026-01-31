"""涨跌停行业分布特征 - 日频物化视图

数据来源：
- tushare.stock_limitlist
- rawdata.index_swmember（申万行业成分变更记录，in_date/out_date as-of 映射）

行业口径：
- 申万行业（SW）
- 同时输出两套聚合：分别按 sw_l1（申万一级）与 sw_l2（申万二级）聚合

设计理念：
- 涨跌停行业分布反映热点板块轮动
- 行业集中度（HHI）反映市场风格（抱团 vs 分散）

注意：
- 不使用“当月月末快照”回填月内行业，避免 future leakage（未来函数风险）
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class LimitIndustryDailyMV(BaseFeatureView):
    """涨跌停行业分布日频物化视图
    
    核心指标：
    - 申万一级（sw_l1）聚合：行业集中度（HHI）、最热门行业占比（Top1/Top3）、涨停行业数量等
    - 申万二级（sw_l2）聚合：同上
    """

    name = "limit_industry_daily"
    description = "涨跌停行业分布日频聚合（行业集中度/热门行业占比）"
    source_tables = [
        "tushare.stock_limitlist",
        "rawdata.index_swmember",
    ]
    refresh_strategy = "full"

    def get_create_sql(self) -> str:
        return """
        CREATE MATERIALIZED VIEW features.mv_limit_industry_daily AS
        WITH
        limit_base AS (
            SELECT
                trade_date,
                ts_code,
                "limit"
            FROM tushare.stock_limitlist
            WHERE ts_code IS NOT NULL
              AND trade_date IS NOT NULL
        ),

        sw_member_asof AS (
            SELECT
                l.trade_date,
                l.ts_code,
                i.l1_code AS sw_l1_code,
                i.l1_name AS sw_l1,
                i.l2_code AS sw_l2_code,
                i.l2_name AS sw_l2,
                ROW_NUMBER() OVER (
                    PARTITION BY l.trade_date, l.ts_code
                    ORDER BY i.in_date DESC
                ) AS rn
            FROM limit_base l
            JOIN rawdata.index_swmember i
              ON i.ts_code = l.ts_code
             AND i.in_date <= l.trade_date
             AND COALESCE(i.out_date, '2099-12-31'::date) > l.trade_date
        ),

        limit_with_sw AS (
            SELECT
                l.trade_date,
                l.ts_code,
                l."limit",
                s.sw_l1_code,
                s.sw_l1,
                s.sw_l2_code,
                s.sw_l2
            FROM limit_base l
            LEFT JOIN sw_member_asof s
              ON s.trade_date = l.trade_date
             AND s.ts_code = l.ts_code
             AND s.rn = 1
        ),

        -- ========== sw_l1 聚合 ==========
        l1_daily AS (
            SELECT
                trade_date,
                sw_l1_code AS industry,
                SUM(CASE WHEN "limit" = 'U' THEN 1 ELSE 0 END) AS limit_up_count,
                SUM(CASE WHEN "limit" = 'D' THEN 1 ELSE 0 END) AS limit_down_count
            FROM limit_with_sw
            WHERE sw_l1_code IS NOT NULL
            GROUP BY trade_date, sw_l1_code
        ),
        l1_totals AS (
            SELECT
                trade_date,
                SUM(limit_up_count) AS total_up,
                SUM(limit_down_count) AS total_down,
                COUNT(DISTINCT industry) AS industry_count
            FROM l1_daily
            GROUP BY trade_date
        ),
        l1_shares AS (
            SELECT
                d.trade_date,
                d.industry,
                d.limit_up_count,
                d.limit_down_count,
                CASE
                    WHEN t.total_up > 0 THEN d.limit_up_count::float / t.total_up
                    ELSE 0
                END AS up_share,
                ROW_NUMBER() OVER (PARTITION BY d.trade_date ORDER BY d.limit_up_count DESC) AS up_rank
            FROM l1_daily d
            JOIN l1_totals t ON d.trade_date = t.trade_date
        ),
        l1_hhi AS (
            SELECT
                trade_date,
                SUM(up_share * up_share) AS limit_up_hhi
            FROM l1_shares
            GROUP BY trade_date
        ),
        l1_top AS (
            SELECT
                trade_date,
                SUM(CASE WHEN up_rank = 1 THEN up_share ELSE 0 END) AS top1_industry_up_ratio,
                SUM(CASE WHEN up_rank <= 3 THEN up_share ELSE 0 END) AS top3_industry_up_ratio,
                COUNT(DISTINCT CASE WHEN limit_up_count > 0 THEN industry END) AS up_industry_count
            FROM l1_shares
            GROUP BY trade_date
        ),

        -- ========== sw_l2 聚合 ==========
        l2_daily AS (
            SELECT
                trade_date,
                sw_l2_code AS industry,
                SUM(CASE WHEN "limit" = 'U' THEN 1 ELSE 0 END) AS limit_up_count,
                SUM(CASE WHEN "limit" = 'D' THEN 1 ELSE 0 END) AS limit_down_count
            FROM limit_with_sw
            WHERE sw_l2_code IS NOT NULL
            GROUP BY trade_date, sw_l2_code
        ),
        l2_totals AS (
            SELECT
                trade_date,
                SUM(limit_up_count) AS total_up,
                SUM(limit_down_count) AS total_down,
                COUNT(DISTINCT industry) AS industry_count
            FROM l2_daily
            GROUP BY trade_date
        ),
        l2_shares AS (
            SELECT
                d.trade_date,
                d.industry,
                d.limit_up_count,
                d.limit_down_count,
                CASE
                    WHEN t.total_up > 0 THEN d.limit_up_count::float / t.total_up
                    ELSE 0
                END AS up_share,
                ROW_NUMBER() OVER (PARTITION BY d.trade_date ORDER BY d.limit_up_count DESC) AS up_rank
            FROM l2_daily d
            JOIN l2_totals t ON d.trade_date = t.trade_date
        ),
        l2_hhi AS (
            SELECT
                trade_date,
                SUM(up_share * up_share) AS limit_up_hhi
            FROM l2_shares
            GROUP BY trade_date
        ),
        l2_top AS (
            SELECT
                trade_date,
                SUM(CASE WHEN up_rank = 1 THEN up_share ELSE 0 END) AS top1_industry_up_ratio,
                SUM(CASE WHEN up_rank <= 3 THEN up_share ELSE 0 END) AS top3_industry_up_ratio,
                COUNT(DISTINCT CASE WHEN limit_up_count > 0 THEN industry END) AS up_industry_count
            FROM l2_shares
            GROUP BY trade_date
        )

        SELECT
            COALESCE(l1.trade_date, l2.trade_date) AS trade_date,

            -- sw_l1 结果
            l1_h.limit_up_hhi AS limit_up_hhi_sw_l1,
            l1.top1_industry_up_ratio AS top1_industry_up_ratio_sw_l1,
            l1.top3_industry_up_ratio AS top3_industry_up_ratio_sw_l1,
            l1.up_industry_count AS up_industry_count_sw_l1,
            l1_t.industry_count AS industry_count_sw_l1,
            l1_t.total_up AS total_limit_up_sw_l1,
            l1_t.total_down AS total_limit_down_sw_l1,

            -- sw_l2 结果
            l2_h.limit_up_hhi AS limit_up_hhi_sw_l2,
            l2.top1_industry_up_ratio AS top1_industry_up_ratio_sw_l2,
            l2.top3_industry_up_ratio AS top3_industry_up_ratio_sw_l2,
            l2.up_industry_count AS up_industry_count_sw_l2,
            l2_t.industry_count AS industry_count_sw_l2,
            l2_t.total_up AS total_limit_up_sw_l2,
            l2_t.total_down AS total_limit_down_sw_l2,

            -- 血缘
            'tushare.stock_limitlist,rawdata.index_swmember' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version

        FROM l1_top l1
        JOIN l1_hhi l1_h ON l1.trade_date = l1_h.trade_date
        JOIN l1_totals l1_t ON l1.trade_date = l1_t.trade_date
        FULL OUTER JOIN l2_top l2 ON l1.trade_date = l2.trade_date
        LEFT JOIN l2_hhi l2_h ON l2.trade_date = l2_h.trade_date
        LEFT JOIN l2_totals l2_t ON l2.trade_date = l2_t.trade_date
        ORDER BY trade_date
        WITH DATA
        """

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_limit_industry_daily_trade_date ON features.mv_limit_industry_daily (trade_date)",
        ]
