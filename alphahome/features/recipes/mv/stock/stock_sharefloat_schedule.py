"""
限售股解禁日程

解禁压力是中期风险因子，对股价有显著影响。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class StockSharefloatScheduleMV(BaseFeatureView):
    """限售股解禁日程"""

    name = "stock_sharefloat_schedule"
    description = "未来 30/60/90 天解禁市值占比"
    source_tables = ["rawdata.stock_sharefloat", "rawdata.stock_dailybasic"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_sharefloat_schedule AS
        WITH float_schedule AS (
            -- 获取未来解禁计划
            SELECT
                ts_code,
                float_date,
                float_share,
                float_ratio,
                holder_name,
                share_type
            FROM rawdata.stock_sharefloat
            WHERE float_date >= CURRENT_DATE
        ),
        latest_mv AS (
            -- 获取最新流通市值
            SELECT DISTINCT ON (ts_code)
                ts_code,
                circ_mv,
                total_mv,
                trade_date
            FROM rawdata.stock_dailybasic
            WHERE circ_mv IS NOT NULL AND circ_mv > 0
            ORDER BY ts_code, trade_date DESC
        ),
        float_agg AS (
            SELECT
                f.ts_code,
                m.circ_mv,
                m.total_mv,
                -- 未来 30 天解禁
                SUM(f.float_share) FILTER (
                    WHERE f.float_date <= CURRENT_DATE + INTERVAL '30 days'
                ) AS float_share_30d,
                COUNT(*) FILTER (
                    WHERE f.float_date <= CURRENT_DATE + INTERVAL '30 days'
                ) AS float_count_30d,
                -- 未来 60 天解禁
                SUM(f.float_share) FILTER (
                    WHERE f.float_date <= CURRENT_DATE + INTERVAL '60 days'
                ) AS float_share_60d,
                -- 未来 90 天解禁
                SUM(f.float_share) FILTER (
                    WHERE f.float_date <= CURRENT_DATE + INTERVAL '90 days'
                ) AS float_share_90d,
                -- 最近一次解禁日期
                MIN(f.float_date) AS next_float_date,
                -- 最近一次解禁股数
                (ARRAY_AGG(f.float_share ORDER BY f.float_date))[1] AS next_float_share
            FROM float_schedule f
            LEFT JOIN latest_mv m ON f.ts_code = m.ts_code
            GROUP BY f.ts_code, m.circ_mv, m.total_mv
        )
        SELECT
            ts_code,
            circ_mv,
            total_mv,
            float_share_30d,
            float_share_60d,
            float_share_90d,
            float_count_30d,
            next_float_date,
            next_float_share,
            -- 解禁占流通市值比例（假设 float_share 单位为万股，circ_mv 单位为万元）
            -- 这里做简化处理，实际需要乘以股价
            CASE WHEN circ_mv > 0
                THEN float_share_30d / circ_mv * 100
                ELSE NULL
            END AS float_ratio_30d_approx,
            CASE WHEN circ_mv > 0
                THEN float_share_60d / circ_mv * 100
                ELSE NULL
            END AS float_ratio_60d_approx,
            CASE WHEN circ_mv > 0
                THEN float_share_90d / circ_mv * 100
                ELSE NULL
            END AS float_ratio_90d_approx,
            -- 距离下次解禁天数
            next_float_date - CURRENT_DATE AS days_to_next_float,
            -- 解禁压力信号
            CASE
                WHEN float_share_30d IS NOT NULL AND float_share_30d > 0 THEN 'HIGH'
                WHEN float_share_60d IS NOT NULL AND float_share_60d > 0 THEN 'MEDIUM'
                WHEN float_share_90d IS NOT NULL AND float_share_90d > 0 THEN 'LOW'
                ELSE 'NONE'
            END AS float_pressure_level,
            -- 血缘字段
            'rawdata.stock_sharefloat,rawdata.stock_dailybasic' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM float_agg
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_sharefloat_schedule_ts_code "
            "ON features.mv_stock_sharefloat_schedule (ts_code)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_sharefloat_schedule_next_date "
            "ON features.mv_stock_sharefloat_schedule (next_float_date)",
        ]
