"""
市场横截面统计物化视图定义（日频）

从 rawdata.stock_dailybasic 读取数据，计算每日市场横截面统计，
输出到 features.mv_market_stats_daily。

数据流:
    rawdata.stock_dailybasic → features.mv_market_stats_daily

涵盖特征（合并自 market_valuation_distribution）：
- 估值分布：PE/PB/PS 中位数、四分位数、均值、IQR
- 高低估值比例：低估（PE<15）/高估（PE>100）股票占比
- 换手率分布：中位数、四分位数、均值
- 市值分布：总市值、流通市值、中位数

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: market_stats_daily.py
- 类名: MarketStatsDailyMV
- recipe.name: market_stats_daily
- 输出表名: features.mv_market_stats_daily
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class MarketStatsDailyMV(BaseFeatureView):
    """
    市场横截面统计物化视图（日频）。

    计算每日全市场的估值、换手率等统计指标，
    包括中位数、四分位数、均值、高低估值比例等。

    源表列说明 (rawdata.stock_dailybasic / tushare.stock_factor_pro):
    - trade_date: 交易日期
    - pe_ttm, pb, ps: 估值指标
    - turnover_rate: 换手率
    - total_mv, circ_mv: 市值
    """

    # 命名三件套
    name = "market_stats_daily"
    description = "市场横截面统计物化视图（日频，含估值分布）"
    
    # 配置
    refresh_strategy = "full"
    source_tables: List[str] = ["rawdata.stock_dailybasic"]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["trade_date", "total_stock_count", "valid_stock_count"],
            "threshold": 0.01,
        },
        "row_count_change": {
            "threshold": 0.2,
        },
    }

    def get_create_sql(self) -> str:
        """
        返回创建物化视图的 SQL。

        处理逻辑:
        1. 按 trade_date 分组
        2. 计算各指标的统计值 (中位数、四分位数、均值等)
        3. 过滤异常值 (PE 范围限制)
        4. 添加血缘元数据
        5. 同时输出全市场股票数与有效估值样本数，便于数据质量监控
        """
        # 输出表名遵循规范: mv_{recipe.name}
        sql = """
        CREATE MATERIALIZED VIEW features.mv_market_stats_daily AS
        WITH 
        -- 全市场股票（仅过滤 trade_date IS NOT NULL）
        all_stocks AS (
            SELECT trade_date, COUNT(*) AS total_stock_count
            FROM rawdata.stock_dailybasic
            WHERE trade_date IS NOT NULL
            GROUP BY trade_date
        ),
        -- 有效估值样本（过滤极端值后）
        filtered AS (
            SELECT
                trade_date,
                pe_ttm,
                pb,
                ps_ttm,
                dv_ttm,
                turnover_rate,
                total_mv,
                circ_mv
            FROM rawdata.stock_dailybasic
            WHERE
                trade_date IS NOT NULL
                AND pe_ttm IS NOT NULL
                AND pe_ttm > 0
                AND pe_ttm < 1000  -- 过滤极端值
                AND pb IS NOT NULL
                AND pb > 0
                AND pb < 100
                AND total_mv IS NOT NULL
                AND total_mv > 0
        ),
        stats AS (
        SELECT
            trade_date,
            
            -- 有效估值样本数
            COUNT(*) AS valid_stock_count,
            
            -- PE 统计
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ttm) AS pe_ttm_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pe_ttm) AS pe_ttm_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pe_ttm) AS pe_ttm_q75,
            AVG(pe_ttm) AS pe_ttm_mean,
            
            -- PB 统计
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pb) AS pb_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pb) AS pb_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pb) AS pb_q75,
            AVG(pb) AS pb_mean,
            
            -- PS 统计
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ps_ttm)
                FILTER (WHERE ps_ttm IS NOT NULL AND ps_ttm > 0 AND ps_ttm < 1000) AS ps_ttm_median,
            AVG(ps_ttm)
                FILTER (WHERE ps_ttm IS NOT NULL AND ps_ttm > 0 AND ps_ttm < 1000) AS ps_ttm_mean,
            
            -- 股息率统计
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dv_ttm)
                FILTER (WHERE dv_ttm IS NOT NULL AND dv_ttm >= 0 AND dv_ttm < 100) AS dv_ttm_median,
            AVG(dv_ttm)
                FILTER (WHERE dv_ttm IS NOT NULL AND dv_ttm >= 0 AND dv_ttm < 100) AS dv_ttm_mean,
            
            -- 换手率统计
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_rate)
                FILTER (WHERE turnover_rate IS NOT NULL AND turnover_rate >= 0 AND turnover_rate <= 100) AS turnover_rate_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY turnover_rate)
                FILTER (WHERE turnover_rate IS NOT NULL AND turnover_rate >= 0 AND turnover_rate <= 100) AS turnover_rate_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY turnover_rate)
                FILTER (WHERE turnover_rate IS NOT NULL AND turnover_rate >= 0 AND turnover_rate <= 100) AS turnover_rate_q75,
            AVG(turnover_rate)
                FILTER (WHERE turnover_rate IS NOT NULL AND turnover_rate >= 0 AND turnover_rate <= 100) AS turnover_rate_mean,
            
            -- 市值统计
            SUM(total_mv) AS total_mv_sum,
            SUM(circ_mv) FILTER (WHERE circ_mv IS NOT NULL AND circ_mv > 0) AS circ_mv_sum,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_mv)
                FILTER (WHERE total_mv IS NOT NULL AND total_mv > 0) AS total_mv_median,
            AVG(total_mv)
                FILTER (WHERE total_mv IS NOT NULL AND total_mv > 0) AS total_mv_mean,
            
            -- ===== 新增：估值分布特征（合并自 market_valuation_distribution）=====
            -- 低估值股票数量 (PE < 15 或 PB < 1)
            SUM(CASE WHEN pe_ttm > 0 AND pe_ttm < 15 THEN 1 ELSE 0 END) AS low_pe_count,
            SUM(CASE WHEN pb > 0 AND pb < 1 THEN 1 ELSE 0 END) AS low_pb_count,
            -- 高估值股票数量 (PE > 100 或 PB > 10)
            SUM(CASE WHEN pe_ttm > 100 AND pe_ttm < 500 THEN 1 ELSE 0 END) AS high_pe_count,
            SUM(CASE WHEN pb > 10 AND pb < 50 THEN 1 ELSE 0 END) AS high_pb_count
        FROM filtered
        GROUP BY trade_date
        )
        SELECT
            s.trade_date,
            -- 全市场股票数（不过滤）
            a.total_stock_count,
            -- 有效估值样本数（过滤后）
            s.valid_stock_count,
            -- 数据质量监控：有效覆盖率
            s.valid_stock_count::FLOAT / NULLIF(a.total_stock_count, 0) AS valid_coverage_ratio,
            -- PE 统计
            s.pe_ttm_median,
            s.pe_ttm_q25,
            s.pe_ttm_q75,
            s.pe_ttm_mean,
            s.pe_ttm_q75 - s.pe_ttm_q25 AS pe_ttm_iqr,  -- PE IQR（估值离散度）
            -- PB 统计
            s.pb_median,
            s.pb_q25,
            s.pb_q75,
            s.pb_mean,
            s.pb_q75 - s.pb_q25 AS pb_iqr,  -- PB IQR（估值离散度）
            -- PS 统计
            s.ps_ttm_median,
            s.ps_ttm_mean,
            -- 股息率统计
            s.dv_ttm_median,
            s.dv_ttm_mean,
            -- 换手率统计
            s.turnover_rate_median,
            s.turnover_rate_q25,
            s.turnover_rate_q75,
            s.turnover_rate_mean,
            -- 市值统计
            s.total_mv_sum,
            s.circ_mv_sum,
            s.total_mv_median,
            s.total_mv_mean,
            -- ===== 新增：估值分布特征（合并自 market_valuation_distribution）=====
            s.low_pe_count,
            s.low_pb_count,
            s.high_pe_count,
            s.high_pb_count,
            s.low_pe_count::FLOAT / NULLIF(s.valid_stock_count, 0) AS low_pe_ratio,
            s.low_pb_count::FLOAT / NULLIF(s.valid_stock_count, 0) AS low_pb_ratio,
            s.high_pe_count::FLOAT / NULLIF(s.valid_stock_count, 0) AS high_pe_ratio,
            s.high_pb_count::FLOAT / NULLIF(s.valid_stock_count, 0) AS high_pb_ratio,
            -- 数据质量监控：与前日对比
            LAG(a.total_stock_count) OVER (ORDER BY s.trade_date) AS prev_total_stock_count,
            LAG(s.pe_ttm_median) OVER (ORDER BY s.trade_date) AS prev_pe_ttm_median,
            -- 血缘元数据
            'rawdata.stock_dailybasic' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM stats s
        JOIN all_stocks a ON s.trade_date = a.trade_date
        ORDER BY s.trade_date DESC;
        """
        return sql.strip()

    def get_post_create_sqls(self) -> list[str]:
        # 常见用法：按 trade_date 查询/与其他日频特征 join
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_market_stats_daily_trade_date "
            "ON features.mv_market_stats_daily (trade_date)",
        ]
