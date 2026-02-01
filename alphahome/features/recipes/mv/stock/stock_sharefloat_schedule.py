"""
限售股解禁日程

解禁压力是中期风险因子，对股价有显著影响。

设计说明：
- 当前版本只计算最新时点的解禁数据（性能优先）
- 如需历史快照，可考虑改用 IncrementalTableView + Python 计算
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class StockSharefloatScheduleMV(BaseFeatureView):
    """限售股解禁日程"""

    name = "stock_sharefloat_schedule"
    description = "限售股解禁日程（未来 30/60/90 天解禁市值占比）"
    source_tables = ["rawdata.stock_sharefloat", "rawdata.stock_dailybasic"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_sharefloat_schedule AS
        WITH params AS (
            -- 使用当前日期作为计算基准
            SELECT CURRENT_DATE AS as_of_date
        ),
        latest_mv AS (
            -- 获取最新的流通市值
            SELECT DISTINCT ON (ts_code)
                ts_code,
                circ_mv,
                total_mv,
                trade_date
            FROM rawdata.stock_dailybasic
            WHERE circ_mv IS NOT NULL 
                AND circ_mv > 0
            ORDER BY ts_code, trade_date DESC
        ),
        float_agg AS (
            SELECT
                (SELECT as_of_date FROM params) AS as_of_date,
                m.ts_code,
                m.circ_mv,
                m.total_mv,
                -- 未来 30 天解禁
                COALESCE(SUM(f.float_share) FILTER (
                    WHERE f.float_date > (SELECT as_of_date FROM params) 
                      AND f.float_date <= (SELECT as_of_date FROM params) + 30
                ), 0) AS float_share_30d,
                COUNT(*) FILTER (
                    WHERE f.float_date > (SELECT as_of_date FROM params) 
                      AND f.float_date <= (SELECT as_of_date FROM params) + 30
                ) AS float_count_30d,
                -- 未来 60 天解禁
                COALESCE(SUM(f.float_share) FILTER (
                    WHERE f.float_date > (SELECT as_of_date FROM params) 
                      AND f.float_date <= (SELECT as_of_date FROM params) + 60
                ), 0) AS float_share_60d,
                -- 未来 90 天解禁
                COALESCE(SUM(f.float_share) FILTER (
                    WHERE f.float_date > (SELECT as_of_date FROM params) 
                      AND f.float_date <= (SELECT as_of_date FROM params) + 90
                ), 0) AS float_share_90d,
                -- 最近一次解禁日期 / 股数
                MIN(f.float_date) FILTER (WHERE f.float_date > (SELECT as_of_date FROM params)) AS next_float_date,
                (ARRAY_AGG(f.float_share ORDER BY f.float_date) FILTER (WHERE f.float_date > (SELECT as_of_date FROM params)))[1]
                    AS next_float_share
            FROM latest_mv m
            LEFT JOIN rawdata.stock_sharefloat f ON f.ts_code = m.ts_code
            GROUP BY m.ts_code, m.circ_mv, m.total_mv
        )
        SELECT
            as_of_date,
            ts_code,
            circ_mv,
            total_mv,
            float_share_30d,
            float_share_60d,
            float_share_90d,
            float_count_30d,
            next_float_date,
            next_float_share,
            -- 解禁占流通市值比例
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
            next_float_date - as_of_date AS days_to_next_float,
            -- 解禁压力信号
            CASE
                WHEN float_share_30d > 0 THEN 'HIGH'
                WHEN float_share_60d > 0 THEN 'MEDIUM'
                WHEN float_share_90d > 0 THEN 'LOW'
                ELSE 'NONE'
            END AS float_pressure_level,
            -- 血缘字段
            'rawdata.stock_sharefloat,rawdata.stock_dailybasic'::TEXT AS _source_table,
            NOW() AS _processed_at,
            as_of_date AS _data_version
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
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_sharefloat_schedule_pressure "
            "ON features.mv_stock_sharefloat_schedule (float_pressure_level)",
        ]
