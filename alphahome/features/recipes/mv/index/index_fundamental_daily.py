"""
指数加权基本面（日频）物化视图定义

设计思路：
- 用 PIT 风格处理权重，正确计算"当时应该看到的权重"
- 使用个股 PE/PB/股息率，按权重加权得到指数层面估值

数据来源：
- tushare.index_weight: 指数权重（月末披露，需 PIT 处理）
- tushare.stock_dailybasic: 个股 PE/PB/股息率

输出指标：
- 加权 PE (ttm)
- 加权 PB
- 加权股息率

命名规范：
- 文件名: index_fundamental_daily.py
- 类名: IndexFundamentalDailyMV
- recipe.name: index_fundamental_daily
- 输出表名: features.mv_index_fundamental_daily

注意：
- 权重 PIT 处理逻辑：
  - index_weight 按月末发布，但实际披露有延迟
  - 采用"用发布日期之前最近一期权重"的 PIT 方式
  - 这里简化为：取 trade_date 当日或之前最近的权重
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class IndexFundamentalDailyMV(BaseFeatureView):
    """指数加权基本面物化视图（日频，PIT 权重）"""

    name = "index_fundamental_daily"
    description = "指数加权 PE/PB/股息率（PIT 权重）（日频）"
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
            CURRENT_DATE AS _data_version
        FROM weighted
        GROUP BY trade_date, index_code
        ORDER BY trade_date, index_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_index_fundamental_daily_trade_date_index_code "
            "ON features.mv_index_fundamental_daily (trade_date, index_code)",
        ]
