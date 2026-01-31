"""
指数综合特征（日频）物化视图定义

设计思路：
- 将估值、波动率等指数级别特征整合到一张 MV
- 避免多个小 MV 的碎片化问题
- 便于下游一次 join 获取所有指数特征

数据来源：
- tushare.index_dailybasic: PE_TTM / PB 等估值
- tushare.index_factor_pro: 收盘价（用于计算波动率）
- akshare.macro_bond_rate: 国债收益率（用于 ERP）

输出指标：
- 估值：PE/PB 及历史分位数
- ERP：股权风险溢价
- 波动率：20/60/252 日实现波动率
- 波动率比值：短期/长期

命名规范：
- 文件名: index_features_daily.py
- 类名: IndexFeaturesDailyMV
- recipe.name: index_features_daily
- 输出表名: features.mv_index_features_daily
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class IndexFeaturesDailyMV(BaseFeatureView):
    """指数综合特征物化视图（估值 + 波动率 + ERP）"""

    name = "index_features_daily"
    description = "核心指数综合特征：PE/PB 分位数、ERP、实现波动率（日频）"
    source_tables = [
        "tushare.index_dailybasic",
        "tushare.index_factor_pro",
        "akshare.macro_bond_rate",
    ]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_index_features_daily AS
        WITH core_indexes AS (
            -- 核心指数清单（统一定义，避免分散）
            SELECT ts_code, alias FROM (VALUES 
                ('000300.SH', 'HS300'),
                ('000905.SH', 'ZZ500'),
                ('000852.SH', 'ZZ1000'),
                ('000016.SH', 'SZ50'),
                ('399006.SZ', 'CYB'),
                ('000001.SH', 'SZZZ')
            ) AS t(ts_code, alias)
        ),
        
        -- ========== 估值数据 ==========
        valuation AS (
            SELECT 
                v.trade_date,
                v.ts_code,
                c.alias,
                v.pe_ttm,
                v.pb,
                -- 滚动历史分位数：对每个 (ts_code, trade_date) 统计最近 N 个观测中 <= 当前值的比例
                CASE
                    WHEN pe_hist.cnt_10y >= 252 THEN pe_hist.le_cnt_10y::FLOAT / NULLIF(pe_hist.cnt_10y, 0)
                    ELSE NULL
                END AS pe_pctl_10y,
                CASE
                    WHEN pe_hist.cnt_1y >= 60 THEN pe_hist.le_cnt_1y::FLOAT / NULLIF(pe_hist.cnt_1y, 0)
                    ELSE NULL
                END AS pe_pctl_1y,
                CASE
                    WHEN pb_hist.cnt_10y >= 252 THEN pb_hist.le_cnt_10y::FLOAT / NULLIF(pb_hist.cnt_10y, 0)
                    ELSE NULL
                END AS pb_pctl_10y,
                CASE
                    WHEN pb_hist.cnt_1y >= 60 THEN pb_hist.le_cnt_1y::FLOAT / NULLIF(pb_hist.cnt_1y, 0)
                    ELSE NULL
                END AS pb_pctl_1y
            FROM tushare.index_dailybasic v
            JOIN core_indexes c ON v.ts_code = c.ts_code
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) AS cnt_10y,
                    SUM(CASE WHEN t.pe_ttm <= v.pe_ttm THEN 1 ELSE 0 END) AS le_cnt_10y,
                    COUNT(*) FILTER (WHERE t.rn <= 252) AS cnt_1y,
                    SUM(CASE WHEN t.rn <= 252 AND t.pe_ttm <= v.pe_ttm THEN 1 ELSE 0 END) AS le_cnt_1y
                FROM (
                    SELECT
                        pe_ttm,
                        ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                    FROM tushare.index_dailybasic
                    WHERE ts_code = v.ts_code
                      AND trade_date < v.trade_date
                      AND pe_ttm IS NOT NULL AND pe_ttm > 0
                    ORDER BY trade_date DESC
                    LIMIT 2520
                ) t
            ) pe_hist ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) AS cnt_10y,
                    SUM(CASE WHEN t.pb <= v.pb THEN 1 ELSE 0 END) AS le_cnt_10y,
                    COUNT(*) FILTER (WHERE t.rn <= 252) AS cnt_1y,
                    SUM(CASE WHEN t.rn <= 252 AND t.pb <= v.pb THEN 1 ELSE 0 END) AS le_cnt_1y
                FROM (
                    SELECT
                        pb,
                        ROW_NUMBER() OVER (ORDER BY trade_date DESC) AS rn
                    FROM tushare.index_dailybasic
                    WHERE ts_code = v.ts_code
                      AND trade_date < v.trade_date
                      AND pb IS NOT NULL AND pb > 0
                    ORDER BY trade_date DESC
                    LIMIT 2520
                ) t
            ) pb_hist ON TRUE
            WHERE v.pe_ttm IS NOT NULL AND v.pe_ttm > 0
              AND v.pb IS NOT NULL AND v.pb > 0
        ),
        
        -- ========== 波动率数据 ==========
        price_with_return AS (
            SELECT 
                p.trade_date,
                p.ts_code,
                c.alias,
                p.close,
                (p.close / NULLIF(LAG(p.close) OVER (
                    PARTITION BY p.ts_code ORDER BY p.trade_date
                ), 0) - 1) AS daily_return
            FROM tushare.index_factor_pro p
            JOIN core_indexes c ON p.ts_code = c.ts_code
            WHERE p.close IS NOT NULL AND p.close > 0
        ),
        volatility AS (
            SELECT 
                trade_date,
                ts_code,
                -- 实现波动率（年化）
                STDDEV(daily_return) OVER w20 * SQRT(252) AS rv_20d,
                STDDEV(daily_return) OVER w60 * SQRT(252) AS rv_60d,
                STDDEV(daily_return) OVER w252 * SQRT(252) AS rv_252d
            FROM price_with_return
            WINDOW 
                w20 AS (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                w60 AS (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                w252 AS (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
        ),
        
        -- ========== 合并 ==========
        combined AS (
            SELECT 
                v.trade_date,
                v.ts_code,
                v.alias,
                -- 估值
                v.pe_ttm,
                v.pb,
                v.pe_pctl_10y,
                v.pe_pctl_1y,
                v.pb_pctl_10y,
                v.pb_pctl_1y,
                -- ERP
                b.cn_10y_yield,
                CASE WHEN v.pe_ttm > 0 
                     THEN (1.0 / v.pe_ttm) - (COALESCE(b.cn_10y_yield, 0) / 100.0)
                     ELSE NULL 
                END AS erp,
                -- 波动率
                vol.rv_20d,
                vol.rv_60d,
                vol.rv_252d,
                CASE WHEN vol.rv_60d > 0 THEN vol.rv_20d / vol.rv_60d ELSE NULL END AS rv_ratio_20_60
            FROM valuation v
            -- 使用 as-of join：将最近一个 <= trade_date 的 10Y 国债收益率对齐到交易日
            LEFT JOIN LATERAL (
                SELECT b."yield" AS cn_10y_yield
                FROM akshare.macro_bond_rate b
                WHERE b.country = 'CN'
                  AND b.term = '10y'
                  AND b.date <= v.trade_date
                ORDER BY b.date DESC
                LIMIT 1
            ) b ON TRUE
            LEFT JOIN volatility vol ON v.trade_date = vol.trade_date AND v.ts_code = vol.ts_code
        )
        SELECT 
            trade_date,
            ts_code,
            alias,
            -- 估值
            pe_ttm,
            pb,
            pe_pctl_10y,
            pe_pctl_1y,
            pb_pctl_10y,
            pb_pctl_1y,
            -- ERP
            cn_10y_yield,
            erp,
            -- 波动率
            rv_20d,
            rv_60d,
            rv_252d,
            rv_ratio_20_60,
            -- 血缘
            'tushare.index_dailybasic,tushare.index_factor_pro,akshare.macro_bond_rate' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM combined
        ORDER BY trade_date, ts_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        # 常见查询/关联模式：按 trade_date 取全市场指数特征，并以 (trade_date, ts_code) join
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_index_features_daily_trade_date_ts_code "
            "ON features.mv_index_features_daily (trade_date, ts_code)",
        ]
