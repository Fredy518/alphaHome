"""
指数技术特征（日频）物化视图定义

设计思路：
- 将布林带信号、MA 偏离度等指数技术指标整合到一张 MV
- 合并 index_boll_signals + index_ma120_distance
- 与 index_features_daily（估值+波动率）分开，因为技术信号相对"可选"

数据来源：
- tushare.index_factor_pro: 指数技术指标

输出指标：
- 布林带突破信号（上轨/下轨）
- MA120 偏离度
- MA60 偏离度

命名规范：
- 文件名: index_technical_daily.py
- 类名: IndexTechnicalDailyMV
- recipe.name: index_technical_daily
- 输出表名: features.mv_index_technical_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class IndexTechnicalDailyMV(BaseFeatureView):
    """指数技术特征物化视图（布林带 + MA 偏离度）"""

    name = "index_technical_daily"
    description = "核心指数技术信号：布林带突破、MA 偏离度（日频）"
    source_tables = [
        "tushare.index_factor_pro",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_index_technical_daily AS
        WITH 
        -- 核心指数清单（与 index_fundamental_daily / index_features_daily 保持一致）
        core_indexes AS (
            SELECT ts_code, alias FROM (VALUES 
                ('000300.SH', 'HS300'),
                ('000905.SH', 'ZZ500'),
                ('000852.SH', 'ZZ1000'),
                ('000016.SH', 'SZ50'),
                ('399006.SZ', 'CYB'),
                ('000001.SH', 'SZZZ')          -- 上证指数（传统市场指数）
            ) AS t(ts_code, alias)
        ),
        
        raw_data AS (
            SELECT 
                p.trade_date,
                p.ts_code,
                c.alias,
                p.close,
                p.boll_upper_bfq,
                p.boll_lower_bfq,
                -- MA120
                AVG(p.close) OVER (
                    PARTITION BY p.ts_code ORDER BY p.trade_date
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS ma_120,
                -- MA60
                AVG(p.close) OVER (
                    PARTITION BY p.ts_code ORDER BY p.trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma_60
            FROM tushare.index_factor_pro p
            JOIN core_indexes c ON p.ts_code = c.ts_code
            WHERE p.close IS NOT NULL AND p.close > 0
        )
        
        SELECT 
            trade_date,
            ts_code,
            alias,
            close,
            -- 布林带
            boll_upper_bfq,
            boll_lower_bfq,
            CASE WHEN close > boll_upper_bfq THEN 1 ELSE 0 END AS upper_boll_break,
            CASE WHEN close < boll_lower_bfq THEN 1 ELSE 0 END AS lower_boll_break,
            -- MA 偏离度（%）
            ma_120,
            ma_60,
            (close - ma_120) / NULLIF(ma_120, 0) * 100 AS ma120_distance,
            (close - ma_60) / NULLIF(ma_60, 0) * 100 AS ma60_distance,
            -- 血缘
            'tushare.index_factor_pro' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM raw_data
        ORDER BY trade_date, ts_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_index_technical_daily_trade_date_ts_code "
            "ON features.mv_index_technical_daily (trade_date, ts_code)",
        ]
