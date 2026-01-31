"""
风格特征（日频）物化视图定义

设计思路：
- 将风格指数收益、风格动量（相对强弱）整合到一张 MV
- 合并 style_index_return + style_momentum
- 用于"风格轮动"择时/选股

数据来源：
- tushare.index_factor_pro: 风格指数价格

输出指标：
- 风格指数收益（日/周/月/季）
- 大盘-小盘动量
- 价值-成长动量
- 红利超额

命名规范：
- 文件名: style_features_daily.py
- 类名: StyleFeaturesDailyMV
- recipe.name: style_features_daily
- 输出表名: features.mv_style_features_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class StyleFeaturesDailyMV(BaseFeatureView):
    """风格特征物化视图（收益 + 动量）"""

    name = "style_features_daily"
    description = "风格指数收益与相对强弱（日频）"
    source_tables = [
        "tushare.index_factor_pro",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_style_features_daily AS
        WITH 
        -- 风格指数定义
        style_indexes AS (
            SELECT ts_code, alias FROM (VALUES 
                ('000300.SH', 'HS300'),
                ('000905.SH', 'ZZ500'),
                ('000852.SH', 'ZZ1000'),
                ('000922.CSI', 'DIV'),         -- 红利
                ('000919.CSI', 'HS300_VAL'),   -- 沪深300价值
                ('000918.CSI', 'HS300_GRO'),   -- 沪深300成长
                ('H30351.CSI', 'ZZ500_VAL'),   -- 中证500价值（正确代码）
                ('H30352.CSI', 'ZZ500_GRO')    -- 中证500成长（正确代码）
            ) AS t(ts_code, alias)
        ),
        
        -- 原始价格数据
        prices AS (
            SELECT 
                p.trade_date,
                p.ts_code,
                s.alias,
                p.close
            FROM tushare.index_factor_pro p
            JOIN style_indexes s ON p.ts_code = s.ts_code
            WHERE p.close IS NOT NULL AND p.close > 0
        ),
        
        -- 计算收益率
        returns AS (
            SELECT 
                trade_date,
                ts_code,
                alias,
                close,
                (close / NULLIF(LAG(close, 1) OVER w, 0) - 1) AS ret_1d,
                (close / NULLIF(LAG(close, 5) OVER w, 0) - 1) AS ret_5d,
                (close / NULLIF(LAG(close, 20) OVER w, 0) - 1) AS ret_20d,
                (close / NULLIF(LAG(close, 60) OVER w, 0) - 1) AS ret_60d
            FROM prices
            WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
        ),
        
        -- 透视为宽表（每个风格一列）
        pivoted AS (
            SELECT 
                trade_date,
                MAX(CASE WHEN alias = 'HS300' THEN ret_5d END) AS hs300_ret_5d,
                MAX(CASE WHEN alias = 'ZZ500' THEN ret_5d END) AS zz500_ret_5d,
                MAX(CASE WHEN alias = 'ZZ1000' THEN ret_5d END) AS zz1000_ret_5d,
                MAX(CASE WHEN alias = 'DIV' THEN ret_5d END) AS div_ret_5d,
                MAX(CASE WHEN alias = 'HS300_VAL' THEN ret_5d END) AS hs300_val_ret_5d,
                MAX(CASE WHEN alias = 'HS300_GRO' THEN ret_5d END) AS hs300_gro_ret_5d,
                -- 20d
                MAX(CASE WHEN alias = 'HS300' THEN ret_20d END) AS hs300_ret_20d,
                MAX(CASE WHEN alias = 'ZZ500' THEN ret_20d END) AS zz500_ret_20d,
                MAX(CASE WHEN alias = 'ZZ1000' THEN ret_20d END) AS zz1000_ret_20d,
                MAX(CASE WHEN alias = 'DIV' THEN ret_20d END) AS div_ret_20d,
                MAX(CASE WHEN alias = 'HS300_VAL' THEN ret_20d END) AS hs300_val_ret_20d,
                MAX(CASE WHEN alias = 'HS300_GRO' THEN ret_20d END) AS hs300_gro_ret_20d,
                -- 60d
                MAX(CASE WHEN alias = 'HS300' THEN ret_60d END) AS hs300_ret_60d,
                MAX(CASE WHEN alias = 'ZZ500' THEN ret_60d END) AS zz500_ret_60d,
                MAX(CASE WHEN alias = 'ZZ1000' THEN ret_60d END) AS zz1000_ret_60d,
                MAX(CASE WHEN alias = 'DIV' THEN ret_60d END) AS div_ret_60d,
                MAX(CASE WHEN alias = 'HS300_VAL' THEN ret_60d END) AS hs300_val_ret_60d,
                MAX(CASE WHEN alias = 'HS300_GRO' THEN ret_60d END) AS hs300_gro_ret_60d
            FROM returns
            GROUP BY trade_date
        )
        
        SELECT 
            trade_date,
            -- 风格收益（5d）
            hs300_ret_5d,
            zz500_ret_5d,
            zz1000_ret_5d,
            div_ret_5d,
            hs300_val_ret_5d,
            hs300_gro_ret_5d,
            -- 风格收益（20d）
            hs300_ret_20d,
            zz500_ret_20d,
            zz1000_ret_20d,
            div_ret_20d,
            hs300_val_ret_20d,
            hs300_gro_ret_20d,
            -- 风格收益（60d）
            hs300_ret_60d,
            zz500_ret_60d,
            zz1000_ret_60d,
            div_ret_60d,
            hs300_val_ret_60d,
            hs300_gro_ret_60d,
            -- 风格动量（相对强弱）
            hs300_ret_5d - zz1000_ret_5d AS large_small_mom_5d,
            hs300_ret_20d - zz1000_ret_20d AS large_small_mom_20d,
            hs300_ret_60d - zz1000_ret_60d AS large_small_mom_60d,
            hs300_val_ret_5d - hs300_gro_ret_5d AS value_growth_mom_5d,
            hs300_val_ret_20d - hs300_gro_ret_20d AS value_growth_mom_20d,
            hs300_val_ret_60d - hs300_gro_ret_60d AS value_growth_mom_60d,
            div_ret_5d - hs300_ret_5d AS dividend_excess_5d,
            div_ret_20d - hs300_ret_20d AS dividend_excess_20d,
            div_ret_60d - hs300_ret_60d AS dividend_excess_60d,
            -- 血缘
            'tushare.index_factor_pro' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM pivoted
        ORDER BY trade_date
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_style_features_daily_trade_date "
            "ON features.mv_style_features_daily (trade_date)",
        ]
