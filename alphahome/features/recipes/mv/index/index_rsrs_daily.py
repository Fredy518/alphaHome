"""
RSRS 指标组件特征（日频）物化视图定义

设计思路：
- RSRS（阻力支撑相对强度）是一种基于价格结构的趋势强度指标
- 原始论文使用 High/Low 进行 OLS 回归，斜率 β 反映价格趋势强度
- 择时信号本身不入库，但其组件（β、R²、标准化值等）可作为特征

算法逻辑（光大证券 RSRS 择时策略）：
1. 对每日 High/Low 进行 N 日滚动 OLS 回归：High = α + β × Low
2. β > 1 表示上涨趋势，β < 1 表示下跌趋势
3. 标准化：β_std = (β - β_mean_M) / β_stddev_M
4. R² 修正：β_adj = β_std × R²（过滤低置信度信号）
5. 右偏修正：β_rightskew = β_adj × β（放大强趋势信号）

数据来源：
- tushare.index_daily: 指数日线（high, low, close）

输出指标：
- rsrs_beta: 原始 β 斜率（N=18 日回归）
- rsrs_r2: 回归 R²
- rsrs_beta_mean: β 的 M=600 日滚动均值
- rsrs_beta_std: β 的 M=600 日滚动标准差
- rsrs_zscore: 标准化 β（Z-score）
- rsrs_zscore_adj: R² 修正后的标准化 β
- rsrs_zscore_rightskew: 右偏修正值

命名规范：
- 文件名: index_rsrs_daily.py
- 类名: IndexRSRSDailyMV
- recipe.name: index_rsrs_daily
- 输出表名: features.mv_index_rsrs_daily

参考文献：
- 光大证券《基于阻力支撑相对强度的市场择时策略》
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class IndexRSRSDailyMV(BaseFeatureView):
    """RSRS 指标组件物化视图（β/R²/标准化/修正）"""

    name = "index_rsrs_daily"
    description = "RSRS 择时指标组件：β斜率、R²、标准化值、右偏修正（日频）"
    source_tables = [
        "rawdata.index_factor_pro",
    ]
    refresh_strategy = "full"

    # RSRS 参数（可根据需要调整）
    REGRESSION_WINDOW = 18  # 回归窗口 N
    ZSCORE_WINDOW = 600     # 标准化窗口 M

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_index_rsrs_daily AS
        WITH 
        -- ========== 核心指数清单 ==========
        core_indexes AS (
            SELECT ts_code, alias FROM (VALUES 
                ('000300.SH', 'HS300'),
                ('000905.SH', 'ZZ500'),
                ('000852.SH', 'ZZ1000'),
                ('000016.SH', 'SZ50'),
                ('399006.SZ', 'CYB'),
                ('000001.SH', 'SZZZ')
            ) AS t(ts_code, alias)
        ),
        
        -- ========== 原始数据准备 ==========
        raw_data AS (
            SELECT 
                d.trade_date,
                d.ts_code,
                c.alias,
                d.high,
                d.low,
                d.close
            FROM rawdata.index_factor_pro d
            JOIN core_indexes c ON d.ts_code = c.ts_code
            WHERE d.high IS NOT NULL AND d.low IS NOT NULL
              AND d.high > 0 AND d.low > 0
        ),
        
        -- ========== 滚动 OLS 回归：High = α + β × Low ==========
        -- 使用 LATERAL 子查询计算 18 日滚动回归
        -- β = Cov(High, Low) / Var(Low)
        -- R² = Cov(High, Low)² / (Var(High) × Var(Low))
        rsrs_regression AS (
            SELECT 
                r.trade_date,
                r.ts_code,
                r.alias,
                r.close,
                reg.rsrs_beta,
                reg.rsrs_r2,
                reg.obs_count
            FROM raw_data r
            LEFT JOIN LATERAL (
                SELECT 
                    -- β = Cov(High, Low) / Var(Low)
                    CASE 
                        WHEN COUNT(*) >= 18 AND VAR_SAMP(sub.low) > 0 
                        THEN COVAR_SAMP(sub.high, sub.low) / VAR_SAMP(sub.low)
                        ELSE NULL 
                    END AS rsrs_beta,
                    -- R² = Corr(High, Low)²
                    CASE 
                        WHEN COUNT(*) >= 18 
                        THEN POWER(CORR(sub.high, sub.low), 2)
                        ELSE NULL 
                    END AS rsrs_r2,
                    COUNT(*) AS obs_count
                FROM (
                    SELECT high, low
                    FROM raw_data
                    WHERE ts_code = r.ts_code
                      AND trade_date <= r.trade_date
                    ORDER BY trade_date DESC
                    LIMIT 18
                ) sub
            ) reg ON TRUE
        ),
        
        -- ========== β 的滚动均值和标准差（M=600 日）==========
        rsrs_with_stats AS (
            SELECT 
                r.trade_date,
                r.ts_code,
                r.alias,
                r.close,
                r.rsrs_beta,
                r.rsrs_r2,
                -- 滚动统计
                AVG(r.rsrs_beta) OVER w600 AS rsrs_beta_mean,
                STDDEV(r.rsrs_beta) OVER w600 AS rsrs_beta_std,
                COUNT(r.rsrs_beta) OVER w600 AS stats_count
            FROM rsrs_regression r
            WHERE r.rsrs_beta IS NOT NULL
            WINDOW w600 AS (
                PARTITION BY r.ts_code 
                ORDER BY r.trade_date 
                ROWS BETWEEN 599 PRECEDING AND CURRENT ROW
            )
        ),
        
        -- ========== 计算标准化和修正指标 ==========
        rsrs_final AS (
            SELECT 
                trade_date,
                ts_code,
                alias,
                close,
                rsrs_beta,
                rsrs_r2,
                rsrs_beta_mean,
                rsrs_beta_std,
                -- Z-score 标准化（需要足够样本）
                CASE 
                    WHEN stats_count >= 252 AND rsrs_beta_std > 0 
                    THEN (rsrs_beta - rsrs_beta_mean) / rsrs_beta_std
                    ELSE NULL 
                END AS rsrs_zscore,
                -- R² 修正
                CASE 
                    WHEN stats_count >= 252 AND rsrs_beta_std > 0 
                    THEN ((rsrs_beta - rsrs_beta_mean) / rsrs_beta_std) * rsrs_r2
                    ELSE NULL 
                END AS rsrs_zscore_adj,
                -- 右偏修正
                CASE 
                    WHEN stats_count >= 252 AND rsrs_beta_std > 0 
                    THEN ((rsrs_beta - rsrs_beta_mean) / rsrs_beta_std) * rsrs_r2 * rsrs_beta
                    ELSE NULL 
                END AS rsrs_zscore_rightskew,
                stats_count
            FROM rsrs_with_stats
        )
        
        SELECT 
            trade_date,
            ts_code,
            alias,
            close,
            -- RSRS 核心指标
            ROUND(rsrs_beta::NUMERIC, 6) AS rsrs_beta,
            ROUND(rsrs_r2::NUMERIC, 6) AS rsrs_r2,
            -- 滚动统计
            ROUND(rsrs_beta_mean::NUMERIC, 6) AS rsrs_beta_mean,
            ROUND(rsrs_beta_std::NUMERIC, 6) AS rsrs_beta_std,
            -- 标准化与修正
            ROUND(rsrs_zscore::NUMERIC, 4) AS rsrs_zscore,
            ROUND(rsrs_zscore_adj::NUMERIC, 4) AS rsrs_zscore_adj,
            ROUND(rsrs_zscore_rightskew::NUMERIC, 4) AS rsrs_zscore_rightskew,
            -- 血缘
            'rawdata.index_factor_pro' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM rsrs_final
        WHERE rsrs_beta IS NOT NULL
        ORDER BY trade_date, ts_code
        WITH NO DATA
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_index_rsrs_daily_trade_date_ts_code "
            "ON features.mv_index_rsrs_daily (trade_date, ts_code)",
        ]
