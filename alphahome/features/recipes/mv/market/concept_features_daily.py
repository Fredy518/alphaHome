"""
概念板块特征（日频）

开盘啦概念板块强度、轮动信号，A 股题材驱动的核心特征。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register


@feature_register
class ConceptFeaturesDailyMV(BaseFeatureView):
    """开盘啦概念板块特征"""

    name = "concept_features_daily"
    description = "概念板块涨停数量、上涨数量、板块强度与轮动信号"
    source_tables = ["rawdata.stock_kplconcept", "rawdata.stock_kplmember"]
    refresh_strategy = "full"

    create_sql = """
        CREATE MATERIALIZED VIEW features.mv_concept_features_daily AS
        WITH concept_stats AS (
            -- 概念板块日度统计
            SELECT
                trade_date,
                ts_code AS concept_code,
                name AS concept_name,
                z_t_num AS limit_up_count,    -- 涨停数量
                up_num AS up_count            -- 上涨数量
            FROM rawdata.stock_kplconcept
            WHERE trade_date IS NOT NULL
        ),
        market_agg AS (
            -- 市场层面聚合
            SELECT
                trade_date,
                COUNT(DISTINCT concept_code) AS total_concepts,
                SUM(limit_up_count) AS market_concept_limit_up,
                AVG(limit_up_count) AS avg_concept_limit_up,
                MAX(limit_up_count) AS max_concept_limit_up,
                -- 强势概念数（涨停数 >= 3）
                COUNT(*) FILTER (WHERE limit_up_count >= 3) AS strong_concept_count,
                -- Top 5 概念涨停占比
                SUM(limit_up_count) FILTER (
                    WHERE limit_up_count >= (
                        SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY limit_up_count)
                        FROM concept_stats cs2 WHERE cs2.trade_date = concept_stats.trade_date
                    )
                ) * 100.0 / NULLIF(SUM(limit_up_count), 0) AS top_concept_concentration
            FROM concept_stats
            GROUP BY trade_date
        ),
        with_rolling AS (
            SELECT
                m.*,
                -- 5 日均值
                AVG(strong_concept_count) OVER (
                    ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS strong_concept_count_ma5,
                -- 概念活跃度变化
                strong_concept_count - LAG(strong_concept_count) OVER (ORDER BY trade_date) AS strong_concept_chg
            FROM market_agg m
        )
        SELECT
            trade_date,
            total_concepts,
            market_concept_limit_up,
            avg_concept_limit_up,
            max_concept_limit_up,
            strong_concept_count,
            strong_concept_count_ma5,
            strong_concept_chg,
            top_concept_concentration,
            -- 题材活跃度信号
            CASE
                WHEN strong_concept_count > strong_concept_count_ma5 * 1.5 THEN 1
                WHEN strong_concept_count < strong_concept_count_ma5 * 0.5 THEN -1
                ELSE 0
            END AS concept_activity_signal,
            -- 血缘字段
            'rawdata.stock_kplconcept,rawdata.stock_kplmember' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM with_rolling
    """

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_concept_features_daily_trade_date "
            "ON features.mv_concept_features_daily (trade_date)",
        ]
