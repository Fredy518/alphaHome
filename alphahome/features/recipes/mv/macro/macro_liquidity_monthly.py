"""
宏观流动性指标（月频）

社融/M1/M2 增速是大类资产配置的关键变量。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class MacroLiquidityMonthlyMV(BaseFeatureView):
    """宏观流动性指标"""

    name = "macro_liquidity_monthly"
    description = "社融/M1/M2 增速、社融脉冲、货币结构"
    source_tables = ["rawdata.macro_sf_month", "rawdata.macro_cn_m"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_macro_liquidity_monthly AS
        WITH sf AS (
            SELECT
                month,
                month_end_date,
                inc_month AS sf_month_inc,           -- 当月新增社融
                inc_cumval AS sf_cum_inc,            -- 累计新增社融
                stk_endval AS sf_stock               -- 社融存量
            FROM rawdata.macro_sf_month
        ),
        money AS (
            SELECT
                month,
                month_end_date,
                m0,
                m0_yoy,
                m1,
                m1_yoy,
                m2,
                m2_yoy
            FROM rawdata.macro_cn_m
        ),
        combined AS (
            SELECT
                COALESCE(sf.month, money.month) AS month,
                COALESCE(sf.month_end_date, money.month_end_date) AS month_end_date,
                sf.sf_month_inc,
                sf.sf_cum_inc,
                sf.sf_stock,
                money.m0,
                money.m0_yoy,
                money.m1,
                money.m1_yoy,
                money.m2,
                money.m2_yoy
            FROM sf
            FULL OUTER JOIN money ON sf.month = money.month
        ),
        with_derived AS (
            SELECT
                month,
                month_end_date,
                sf_month_inc,
                sf_cum_inc,
                sf_stock,
                m0,
                m0_yoy,
                m1,
                m1_yoy,
                m2,
                m2_yoy,
                -- M1-M2 剪刀差
                m1_yoy - m2_yoy AS m1_m2_spread,
                -- 社融同比增速（近似）
                CASE WHEN LAG(sf_stock, 12) OVER (ORDER BY month) > 0
                    THEN (sf_stock - LAG(sf_stock, 12) OVER (ORDER BY month)) * 100.0
                         / LAG(sf_stock, 12) OVER (ORDER BY month)
                    ELSE NULL
                END AS sf_stock_yoy,
                -- 社融脉冲（当月增量 vs 12 个月均值）
                sf_month_inc / NULLIF(
                    AVG(sf_month_inc) OVER (ORDER BY month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING),
                    0
                ) AS sf_pulse,
                -- M1 增速变化
                m1_yoy - LAG(m1_yoy) OVER (ORDER BY month) AS m1_yoy_chg,
                -- M2 增速变化
                m2_yoy - LAG(m2_yoy) OVER (ORDER BY month) AS m2_yoy_chg
            FROM combined
        )
        SELECT
            month,
            month_end_date,
            sf_month_inc,
            sf_stock,
            sf_stock_yoy,
            sf_pulse,
            m0,
            m0_yoy,
            m1,
            m1_yoy,
            m1_yoy_chg,
            m2,
            m2_yoy,
            m2_yoy_chg,
            m1_m2_spread,
            -- 流动性信号（M1-M2 剪刀差上升 + 社融脉冲 > 1）
            CASE
                WHEN m1_m2_spread > 0 AND sf_pulse > 1 THEN 1
                WHEN m1_m2_spread < -2 AND sf_pulse < 0.8 THEN -1
                ELSE 0
            END AS liquidity_signal,
            -- 血缘字段
            'rawdata.macro_sf_month,rawdata.macro_cn_m' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_derived
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_macro_liquidity_monthly_month "
            "ON features.mv_macro_liquidity_monthly (month_end_date)",
        ]
