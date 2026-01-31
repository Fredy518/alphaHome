"""
分析师覆盖与评级特征

分析师覆盖度、评级分布、目标价一致预期是基本面量化核心输入。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class StockAnalystCoverageMV(BaseFeatureView):
    """分析师覆盖与评级"""

    name = "stock_analyst_coverage"
    description = "分析师覆盖数、评级分布、目标价预期"
    source_tables = ["rawdata.stock_report_rc"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_analyst_coverage AS
        WITH report_90d AS (
            -- 近 90 天研报
            SELECT
                ts_code,
                report_date,
                org_name,
                author_name,
                classify,           -- 评级（买入/增持/中性/减持/卖出）
                tp AS target_price, -- 目标价
                eps,                -- 预测 EPS
                pe                  -- 预测 PE
            FROM rawdata.stock_report_rc
            WHERE report_date >= CURRENT_DATE - INTERVAL '90 days'
        ),
        coverage_stats AS (
            SELECT
                ts_code,
                -- 覆盖度指标
                COUNT(DISTINCT org_name) AS analyst_org_count,      -- 覆盖机构数
                COUNT(DISTINCT author_name) AS analyst_count,       -- 分析师数
                COUNT(*) AS report_count_90d,                       -- 研报数量
                -- 评级分布
                COUNT(*) FILTER (WHERE classify IN ('买入', '强烈推荐')) AS buy_count,
                COUNT(*) FILTER (WHERE classify IN ('增持', '推荐')) AS outperform_count,
                COUNT(*) FILTER (WHERE classify IN ('中性', '持有')) AS neutral_count,
                COUNT(*) FILTER (WHERE classify IN ('减持', '卖出', '回避')) AS underperform_count,
                -- 目标价一致预期
                AVG(target_price) FILTER (WHERE target_price > 0) AS target_price_avg,
                STDDEV(target_price) FILTER (WHERE target_price > 0) AS target_price_std,
                MIN(target_price) FILTER (WHERE target_price > 0) AS target_price_min,
                MAX(target_price) FILTER (WHERE target_price > 0) AS target_price_max,
                -- EPS 一致预期
                AVG(eps) FILTER (WHERE eps IS NOT NULL) AS eps_consensus,
                -- 最新研报日期
                MAX(report_date) AS latest_report_date
            FROM report_90d
            GROUP BY ts_code
        )
        SELECT
            ts_code,
            analyst_org_count,
            analyst_count,
            report_count_90d,
            buy_count,
            outperform_count,
            neutral_count,
            underperform_count,
            -- 评级得分（买入=2, 增持=1, 中性=0, 减持=-1）
            CASE WHEN (buy_count + outperform_count + neutral_count + underperform_count) > 0
                THEN (buy_count * 2.0 + outperform_count * 1.0 - underperform_count * 1.0)
                     / (buy_count + outperform_count + neutral_count + underperform_count)
                ELSE NULL
            END AS rating_score,
            -- 买入+增持占比
            CASE WHEN report_count_90d > 0
                THEN (buy_count + outperform_count) * 100.0 / report_count_90d
                ELSE NULL
            END AS positive_rating_pct,
            target_price_avg,
            target_price_std,
            target_price_min,
            target_price_max,
            -- 目标价离散度
            CASE WHEN target_price_avg > 0
                THEN target_price_std / target_price_avg
                ELSE NULL
            END AS target_price_dispersion,
            eps_consensus,
            latest_report_date,
            -- 血缘字段
            'rawdata.stock_report_rc' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM coverage_stats
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_analyst_coverage_ts_code "
            "ON features.mv_stock_analyst_coverage (ts_code)",
        ]
