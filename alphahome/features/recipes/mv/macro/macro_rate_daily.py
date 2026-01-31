"""
宏观利率特征（日频）物化视图定义

设计思路：
- 将中美国债收益率、期限利差、中美利差等整合到一张 MV
- 这些特征是"宏观因子"的核心输入
- 避免下游多次 join akshare.macro_bond_rate

数据来源：
- akshare.macro_bond_rate: 中美国债收益率（长表：date,country,term,yield）

输出指标：
- 中美各期限收益率 (2y, 5y, 10y, 30y)
- 期限利差 (10y-2y, 30y-10y)
- 中美利差 (CN-US)
- 收益率变化 (1D, 5D, 20D)
- 历史分位数 (1Y, 3Y, 5Y)

命名规范：
- 文件名: macro_rate_daily.py
- 类名: MacroRateDailyMV
- recipe.name: macro_rate_daily
- 输出表名: features.mv_macro_rate_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class MacroRateDailyMV(BaseFeatureView):
    """宏观利率物化视图（收益率 + 利差 + 分位数）"""

    name = "macro_rate_daily"
    description = "中美国债收益率、期限利差、中美利差及历史分位数（日频）"
    source_tables = [
        "akshare.macro_bond_rate",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_macro_rate_daily AS
        WITH 
        -- ========== 原始数据透视 ==========
        raw_pivot AS (
            SELECT 
                date AS trade_date,
                MAX(CASE WHEN country = 'CN' AND term = '2y' THEN yield END) AS cn_2y,
                MAX(CASE WHEN country = 'CN' AND term = '5y' THEN yield END) AS cn_5y,
                MAX(CASE WHEN country = 'CN' AND term = '10y' THEN yield END) AS cn_10y,
                MAX(CASE WHEN country = 'CN' AND term = '30y' THEN yield END) AS cn_30y,
                MAX(CASE WHEN country = 'US' AND term = '2y' THEN yield END) AS us_2y,
                MAX(CASE WHEN country = 'US' AND term = '5y' THEN yield END) AS us_5y,
                MAX(CASE WHEN country = 'US' AND term = '10y' THEN yield END) AS us_10y,
                MAX(CASE WHEN country = 'US' AND term = '30y' THEN yield END) AS us_30y
            FROM akshare.macro_bond_rate
            GROUP BY date
        ),
        
        -- ========== 利差计算 ==========
        with_spreads AS (
            SELECT 
                trade_date,
                -- 原始收益率
                cn_2y, cn_5y, cn_10y, cn_30y,
                us_2y, us_5y, us_10y, us_30y,
                -- 中国期限利差
                cn_10y - cn_2y AS cn_term_spread_10y2y,
                cn_30y - cn_10y AS cn_term_spread_30y10y,
                -- 美国期限利差
                us_10y - us_2y AS us_term_spread_10y2y,
                us_30y - us_10y AS us_term_spread_30y10y,
                -- 中美利差
                cn_2y - us_2y AS cnus_spread_2y,
                cn_5y - us_5y AS cnus_spread_5y,
                cn_10y - us_10y AS cnus_spread_10y,
                cn_30y - us_30y AS cnus_spread_30y
            FROM raw_pivot
        ),
        
        -- ========== 收益率变化（滞后 diff，单位 bp）==========
        with_changes AS (
            SELECT 
                s.*,
                -- 中国 10y 变化
                (s.cn_10y - LAG(s.cn_10y, 1) OVER w) * 100 AS cn_10y_chg_1d,
                (s.cn_10y - LAG(s.cn_10y, 5) OVER w) * 100 AS cn_10y_chg_5d,
                (s.cn_10y - LAG(s.cn_10y, 20) OVER w) * 100 AS cn_10y_chg_20d,
                -- 美国 10y 变化
                (s.us_10y - LAG(s.us_10y, 1) OVER w) * 100 AS us_10y_chg_1d,
                (s.us_10y - LAG(s.us_10y, 5) OVER w) * 100 AS us_10y_chg_5d,
                (s.us_10y - LAG(s.us_10y, 20) OVER w) * 100 AS us_10y_chg_20d,
                -- 中美利差 10y 变化
                (s.cnus_spread_10y - LAG(s.cnus_spread_10y, 5) OVER w) * 100 AS cnus_spread_10y_chg_5d,
                (s.cnus_spread_10y - LAG(s.cnus_spread_10y, 20) OVER w) * 100 AS cnus_spread_10y_chg_20d,
                -- 期限利差变化
                (s.cn_term_spread_10y2y - LAG(s.cn_term_spread_10y2y, 20) OVER w) * 100 AS cn_term_spread_10y2y_chg_20d,
                (s.us_term_spread_10y2y - LAG(s.us_term_spread_10y2y, 20) OVER w) * 100 AS us_term_spread_10y2y_chg_20d
            FROM with_spreads s
            WINDOW w AS (ORDER BY s.trade_date)
        )
        
        SELECT 
            trade_date,
            -- 原始收益率
            cn_2y, cn_5y, cn_10y, cn_30y,
            us_2y, us_5y, us_10y, us_30y,
            -- 期限利差
            cn_term_spread_10y2y,
            cn_term_spread_30y10y,
            us_term_spread_10y2y,
            us_term_spread_30y10y,
            -- 中美利差
            cnus_spread_2y,
            cnus_spread_5y,
            cnus_spread_10y,
            cnus_spread_30y,
            -- 收益率变化（bp）
            cn_10y_chg_1d,
            cn_10y_chg_5d,
            cn_10y_chg_20d,
            us_10y_chg_1d,
            us_10y_chg_5d,
            us_10y_chg_20d,
            cnus_spread_10y_chg_5d,
            cnus_spread_10y_chg_20d,
            cn_term_spread_10y2y_chg_20d,
            us_term_spread_10y2y_chg_20d,
            -- 历史分位数（使用过去窗口观测，避免前视；percent_rank 不支持滚动 frame）
            CASE WHEN cn_hist.cnt_1y >= 252 THEN cn_hist.le_cnt_1y::FLOAT / NULLIF(cn_hist.cnt_1y, 0) ELSE NULL END AS cn_10y_pctl_1y,
            CASE WHEN cn_hist.cnt_3y >= 756 THEN cn_hist.le_cnt_3y::FLOAT / NULLIF(cn_hist.cnt_3y, 0) ELSE NULL END AS cn_10y_pctl_3y,
            CASE WHEN cn_hist.cnt_5y >= 1260 THEN cn_hist.le_cnt_5y::FLOAT / NULLIF(cn_hist.cnt_5y, 0) ELSE NULL END AS cn_10y_pctl_5y,
            CASE WHEN us_hist.cnt_1y >= 252 THEN us_hist.le_cnt_1y::FLOAT / NULLIF(us_hist.cnt_1y, 0) ELSE NULL END AS us_10y_pctl_1y,
            CASE WHEN us_hist.cnt_3y >= 756 THEN us_hist.le_cnt_3y::FLOAT / NULLIF(us_hist.cnt_3y, 0) ELSE NULL END AS us_10y_pctl_3y,
            CASE WHEN us_hist.cnt_5y >= 1260 THEN us_hist.le_cnt_5y::FLOAT / NULLIF(us_hist.cnt_5y, 0) ELSE NULL END AS us_10y_pctl_5y,
            CASE WHEN sp_hist.cnt_1y >= 252 THEN sp_hist.le_cnt_1y::FLOAT / NULLIF(sp_hist.cnt_1y, 0) ELSE NULL END AS cnus_spread_10y_pctl_1y,
            CASE WHEN sp_hist.cnt_3y >= 756 THEN sp_hist.le_cnt_3y::FLOAT / NULLIF(sp_hist.cnt_3y, 0) ELSE NULL END AS cnus_spread_10y_pctl_3y,
            -- 血缘
            'akshare.macro_bond_rate' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_changes c
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE t.rn <= 252) AS cnt_1y,
                SUM(CASE WHEN t.rn <= 252 AND t.val <= c.cn_10y THEN 1 ELSE 0 END) AS le_cnt_1y,
                COUNT(*) FILTER (WHERE t.rn <= 756) AS cnt_3y,
                SUM(CASE WHEN t.rn <= 756 AND t.val <= c.cn_10y THEN 1 ELSE 0 END) AS le_cnt_3y,
                COUNT(*) AS cnt_5y,
                SUM(CASE WHEN t.val <= c.cn_10y THEN 1 ELSE 0 END) AS le_cnt_5y
            FROM (
                SELECT
                    cn_10y AS val,
                    ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                FROM with_changes
                WHERE trade_date < c.trade_date
                  AND cn_10y IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT 1260
            ) t
        ) cn_hist ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE t.rn <= 252) AS cnt_1y,
                SUM(CASE WHEN t.rn <= 252 AND t.val <= c.us_10y THEN 1 ELSE 0 END) AS le_cnt_1y,
                COUNT(*) FILTER (WHERE t.rn <= 756) AS cnt_3y,
                SUM(CASE WHEN t.rn <= 756 AND t.val <= c.us_10y THEN 1 ELSE 0 END) AS le_cnt_3y,
                COUNT(*) AS cnt_5y,
                SUM(CASE WHEN t.val <= c.us_10y THEN 1 ELSE 0 END) AS le_cnt_5y
            FROM (
                SELECT
                    us_10y AS val,
                    ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                FROM with_changes
                WHERE trade_date < c.trade_date
                  AND us_10y IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT 1260
            ) t
        ) us_hist ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) FILTER (WHERE t.rn <= 252) AS cnt_1y,
                SUM(CASE WHEN t.rn <= 252 AND t.val <= c.cnus_spread_10y THEN 1 ELSE 0 END) AS le_cnt_1y,
                COUNT(*) FILTER (WHERE t.rn <= 756) AS cnt_3y,
                SUM(CASE WHEN t.rn <= 756 AND t.val <= c.cnus_spread_10y THEN 1 ELSE 0 END) AS le_cnt_3y
            FROM (
                SELECT
                    cnus_spread_10y AS val,
                    ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                FROM with_changes
                WHERE trade_date < c.trade_date
                  AND cnus_spread_10y IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT 756
            ) t
        ) sp_hist ON TRUE
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_macro_rate_daily_trade_date "
            "ON features.mv_macro_rate_daily (trade_date)",
        ]
