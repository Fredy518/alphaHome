"""
股东户数变化（筹码集中度指标）

核心 alpha 因子：股东户数下降通常意味着筹码集中，可能是主力建仓信号。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class StockShareholderConcentrationMV(BaseFeatureView):
    """股东户数与筹码集中度"""

    name = "stock_shareholder_concentration"
    description = "股东户数变化、环比/同比增速，筹码集中度指标"
    source_tables = ["rawdata.stock_holdernumber"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_shareholder_concentration AS
        WITH base AS (
            SELECT
                ts_code,
                ann_date,
                end_date,
                holder_num,
                -- 环比（上一期）
                LAG(holder_num) OVER (
                    PARTITION BY ts_code ORDER BY end_date
                ) AS holder_num_prev,
                -- 同比（去年同期）
                LAG(holder_num, 4) OVER (
                    PARTITION BY ts_code ORDER BY end_date
                ) AS holder_num_yoy
            FROM rawdata.stock_holdernumber
            WHERE holder_num IS NOT NULL AND holder_num > 0
        )
        SELECT
            ts_code,
            ann_date,
            end_date,
            holder_num,
            holder_num_prev,
            -- 环比变化
            holder_num - holder_num_prev AS holder_num_chg,
            CASE WHEN holder_num_prev > 0
                THEN (holder_num - holder_num_prev) * 100.0 / holder_num_prev
                ELSE NULL
            END AS holder_num_chg_pct,
            -- 同比变化
            holder_num - holder_num_yoy AS holder_num_yoy_chg,
            CASE WHEN holder_num_yoy > 0
                THEN (holder_num - holder_num_yoy) * 100.0 / holder_num_yoy
                ELSE NULL
            END AS holder_num_yoy_chg_pct,
            -- 筹码集中度信号（户数下降为正信号）
            CASE
                WHEN holder_num_prev IS NOT NULL AND holder_num < holder_num_prev THEN 1
                WHEN holder_num_prev IS NOT NULL AND holder_num > holder_num_prev THEN -1
                ELSE 0
            END AS concentration_signal,
            -- 血缘字段
            'rawdata.stock_holdernumber' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM base
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_shareholder_concentration_ts_code "
            "ON features.mv_stock_shareholder_concentration (ts_code, end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_shareholder_concentration_ann_date "
            "ON features.mv_stock_shareholder_concentration (ann_date)",
        ]
