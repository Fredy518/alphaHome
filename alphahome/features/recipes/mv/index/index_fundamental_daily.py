"""Descriptive index valuation from retained statistical-date weights.

Weight trade_date is NOT verified publication time. Historical membership and
revision vintages are not certified by this recipe; do not use its output as a
PIT backtest input without separate official publication evidence.
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register
from alphahome.features.recipes.mv.bounded_pit_sql import index_daily_sql


@feature_register
class IndexFundamentalDailyMV(BaseFeatureView):
    """Descriptive index valuations using retained statistical-date weights."""

    name = "index_fundamental_daily"
    description = "指数加权 PE/PB/股息率（日频；统计日权重，历史发布与修订未认证）"
    source_tables = [
        "tushare.index_weight",
        "tushare.stock_dailybasic",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_index_fundamental_daily AS
        WITH 
        -- 核心宽基指数（与 index_technical_daily / index_features_daily 保持一致）
        core_indexes AS (
            SELECT idx_code FROM (VALUES 
                ('000300.SH'),
                ('000905.SH'),
                ('000852.SH'),
                ('000016.SH'),
                ('399006.SZ'),
                ('000001.SH')                  -- 上证指数（传统市场指数）
            ) AS t(idx_code)
        ),
        
        -- 所有交易日
        trading_days AS (
            SELECT DISTINCT trade_date
            FROM tushare.stock_dailybasic
            WHERE trade_date IS NOT NULL
        ),
        
        -- 权重快照日期
        weight_dates AS (
            SELECT DISTINCT index_code, trade_date AS weight_date
            FROM tushare.index_weight
            WHERE index_code IN (SELECT idx_code FROM core_indexes)
        ),
        
        -- 每个交易日找 PIT 权重日期（当日或之前最近的权重）
        pit_weight_lookup AS (
            SELECT 
                t.trade_date,
                w.index_code,
                MAX(wd.weight_date) AS pit_weight_date
            FROM trading_days t
            CROSS JOIN (SELECT DISTINCT index_code FROM weight_dates) w
            LEFT JOIN weight_dates wd ON w.index_code = wd.index_code AND wd.weight_date <= t.trade_date
            WHERE wd.weight_date IS NOT NULL
            GROUP BY t.trade_date, w.index_code
        ),
        
        -- PIT 权重
        pit_weights AS (
            SELECT 
                p.trade_date,
                p.index_code,
                w.con_code,
                w.weight
            FROM pit_weight_lookup p
            JOIN tushare.index_weight w 
                ON p.index_code = w.index_code 
                AND p.pit_weight_date = w.trade_date
        ),
        
        -- 个股估值
        stock_valuation AS (
            SELECT 
                trade_date,
                ts_code,
                pe_ttm,
                pb,
                dv_ratio
            FROM tushare.stock_dailybasic
            WHERE pe_ttm IS NOT NULL OR pb IS NOT NULL OR dv_ratio IS NOT NULL
        ),
        
        -- 合并权重和估值
        merged AS (
            SELECT 
                w.trade_date,
                w.index_code,
                w.con_code,
                w.weight,
                s.pe_ttm,
                s.pb,
                s.dv_ratio
            FROM pit_weights w
            LEFT JOIN stock_valuation s 
                ON w.con_code = s.ts_code 
                AND w.trade_date = s.trade_date
        ),

        weighted AS (
            SELECT
                trade_date,
                index_code,
                con_code,
                pe_ttm,
                pb,
                dv_ratio,
                weight / NULLIF(SUM(weight) OVER (PARTITION BY trade_date, index_code), 0) AS weight_norm
            FROM merged
            WHERE weight IS NOT NULL
        )
        
        -- 按指数、日期加权聚合
        SELECT 
            trade_date,
            index_code,
            -- 加权 PE（倒数加权：E/P 加权后取倒数，更接近“指数整体 PE”）
            CASE
                WHEN SUM(CASE WHEN pe_ttm > 0 AND pe_ttm < 1000 THEN weight_norm / pe_ttm END) > 0
                THEN 1 / SUM(CASE WHEN pe_ttm > 0 AND pe_ttm < 1000 THEN weight_norm / pe_ttm END)
                ELSE NULL
            END AS weighted_pe_ttm,
            -- 加权 PB（倒数加权：B/P 加权后取倒数）
            CASE
                WHEN SUM(CASE WHEN pb > 0 AND pb < 100 THEN weight_norm / pb END) > 0
                THEN 1 / SUM(CASE WHEN pb > 0 AND pb < 100 THEN weight_norm / pb END)
                ELSE NULL
            END AS weighted_pb,
            -- 加权股息率
            SUM(
                CASE 
                    WHEN dv_ratio > 0 AND dv_ratio < 20 
                    THEN weight_norm * dv_ratio 
                END
            ) / NULLIF(SUM(CASE WHEN dv_ratio > 0 AND dv_ratio < 20 THEN weight_norm END), 0) AS weighted_dv_ratio,
            -- 有效权重比例（用于质量监控）
            SUM(CASE WHEN pe_ttm > 0 AND pe_ttm < 1000 THEN weight_norm END) / NULLIF(SUM(weight_norm), 0) AS pe_coverage,
            SUM(CASE WHEN pb > 0 AND pb < 100 THEN weight_norm END) / NULLIF(SUM(weight_norm), 0) AS pb_coverage,
            COUNT(*) AS constituent_count,
            -- 血缘
            'tushare.index_weight,tushare.stock_dailybasic' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version,
            FALSE AS _pit_eligible,
            'weight_statistical_date_only;publication_and_vintages_unverified'::text AS _pit_limitations
        FROM weighted
        GROUP BY trade_date, index_code
        ORDER BY trade_date, index_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return index_daily_sql(self.create_sql)

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_index_fundamental_daily_trade_date_index_code "
            "ON features.mv_index_fundamental_daily (trade_date, index_code)",
        ]
