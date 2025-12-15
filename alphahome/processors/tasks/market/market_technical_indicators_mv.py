"""
市场技术指标物化视图任务

负责创建和管理 market_technical_indicators_mv 物化视图。
该物化视图从 rawdata.market_technical 表读取数据，
进行数据对齐、聚合计算、数据标准化，最后输出到 materialized_views schema。

**Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
**Validates: Requirements 8.1, 8.2, 8.3**
"""

from typing import Any, Dict, List, Optional
import pandas as pd

from alphahome.processors.materialized_views import MaterializedViewTask, MaterializedViewSQL


class MarketTechnicalIndicatorsMV(MaterializedViewTask):
    """市场技术指标物化视图任务。"""

    name = "market_technical_indicators_mv"
    description = "市场技术指标物化视图"

    is_materialized_view: bool = True
    materialized_view_name: str = "market_technical_indicators_mv"
    materialized_view_schema: str = "materialized_views"

    refresh_strategy: str = "full"

    source_tables: List[str] = ["rawdata.market_technical"]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["trade_date", "close", "vol", "turnover_rate"],
            "threshold": 0.05,
        },
        "outlier_check": {
            "columns": ["close", "vol", "turnover_rate"],
            "method": "iqr",
            "threshold": 3.0,
        },
        "row_count_change": {
            "threshold": 0.3,
        },
    }

    def __init__(self, db_connection=None, **kwargs):
        super().__init__(db_connection=db_connection, **kwargs)
        self.logger.info(
            f"初始化 {self.name}: "
            f"物化视图={self.materialized_view_schema}.{self.materialized_view_name}, "
            f"数据源={self.source_tables}"
        )

    async def define_materialized_view_sql(self) -> str:
        sql = """
        CREATE MATERIALIZED VIEW materialized_views.market_technical_indicators_mv AS
        WITH normalized AS (
            SELECT
                -- 1. 数据对齐（格式标准化）
                CASE
                    WHEN trade_date::text ~ '^\\d{8}$' THEN to_date(trade_date::text, 'YYYYMMDD')
                    ELSE trade_date::date
                END AS trade_date_dt,

                CASE
                    WHEN ts_code ~ '\\.' THEN ts_code
                    WHEN ts_code LIKE '6%' THEN ts_code || '.SH'
                    WHEN ts_code LIKE '0%' THEN ts_code || '.SZ'
                    WHEN ts_code LIKE '3%' THEN ts_code || '.SZ'
                    ELSE ts_code
                END AS ts_code_std,

                -- 2. 数据标准化（数值列转换为 DECIMAL）
                CAST(close AS DECIMAL(10,2)) as close,
                CAST(vol AS DECIMAL(15,0)) as vol,
                CAST(turnover_rate AS DECIMAL(10,4)) as turnover_rate,
                CAST(amount AS DECIMAL(15,2)) as amount
            FROM rawdata.market_technical
            WHERE
                trade_date IS NOT NULL
                AND ts_code IS NOT NULL
                AND close IS NOT NULL
                AND close > 0
                AND vol IS NOT NULL
                AND vol >= 0
                AND turnover_rate IS NOT NULL
                AND turnover_rate >= 0
        ),
        stats AS (
            SELECT
                trade_date_dt as trade_date,

                -- 3. 聚合计算（横截面统计）
                COUNT(DISTINCT ts_code_std) as total_count,

                -- 价格统计
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close) as close_median,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY close) as close_q25,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY close) as close_q75,
                AVG(close) as close_mean,
                STDDEV(close) as close_std,
                MIN(close) as close_min,
                MAX(close) as close_max,

                -- 成交量统计
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol) as vol_median,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY vol) as vol_q25,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vol) as vol_q75,
                AVG(vol) as vol_mean,
                STDDEV(vol) as vol_std,
                SUM(vol) as vol_total,

                -- 换手率统计
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_rate) as turnover_rate_median,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY turnover_rate) as turnover_rate_q25,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY turnover_rate) as turnover_rate_q75,
                AVG(turnover_rate) as turnover_rate_mean,
                STDDEV(turnover_rate) as turnover_rate_std,

                -- 成交额统计
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as amount_median,
                AVG(amount) as amount_mean,
                SUM(amount) as amount_total
            FROM normalized
            GROUP BY trade_date_dt
        ),
        ratios AS (
            SELECT
                n.trade_date_dt as trade_date,
                COUNT(DISTINCT n.ts_code_std) as total_count,
                COUNT(DISTINCT CASE WHEN n.close > 100 THEN n.ts_code_std END)::float
                    / NULLIF(COUNT(DISTINCT n.ts_code_std), 0) as high_price_ratio,
                COUNT(DISTINCT CASE WHEN n.vol > s.vol_median * 1.5 THEN n.ts_code_std END)::float
                    / NULLIF(COUNT(DISTINCT n.ts_code_std), 0) as high_vol_ratio,
                COUNT(DISTINCT CASE WHEN n.turnover_rate > 5 THEN n.ts_code_std END)::float
                    / NULLIF(COUNT(DISTINCT n.ts_code_std), 0) as high_turnover_ratio
            FROM normalized n
            JOIN stats s
                ON s.trade_date = n.trade_date_dt
            GROUP BY n.trade_date_dt, s.vol_median
        ),
        aggregated AS (
            SELECT
                s.*,
                r.high_price_ratio,
                r.high_vol_ratio,
                r.high_turnover_ratio
            FROM stats s
            JOIN ratios r
                USING (trade_date)
        )
        SELECT
            trade_date,

            -- 4. 数据标准化（转换为 DECIMAL）
            CAST(total_count AS INTEGER) as total_count,
            CAST(close_median AS DECIMAL(10,2)) as close_median,
            CAST(close_q25 AS DECIMAL(10,2)) as close_q25,
            CAST(close_q75 AS DECIMAL(10,2)) as close_q75,
            CAST(close_mean AS DECIMAL(10,2)) as close_mean,
            CAST(close_std AS DECIMAL(10,2)) as close_std,
            CAST(close_min AS DECIMAL(10,2)) as close_min,
            CAST(close_max AS DECIMAL(10,2)) as close_max,

            CAST(vol_median AS DECIMAL(15,0)) as vol_median,
            CAST(vol_q25 AS DECIMAL(15,0)) as vol_q25,
            CAST(vol_q75 AS DECIMAL(15,0)) as vol_q75,
            CAST(vol_mean AS DECIMAL(15,2)) as vol_mean,
            CAST(vol_std AS DECIMAL(15,2)) as vol_std,
            CAST(vol_total AS DECIMAL(20,0)) as vol_total,

            CAST(turnover_rate_median AS DECIMAL(10,4)) as turnover_rate_median,
            CAST(turnover_rate_q25 AS DECIMAL(10,4)) as turnover_rate_q25,
            CAST(turnover_rate_q75 AS DECIMAL(10,4)) as turnover_rate_q75,
            CAST(turnover_rate_mean AS DECIMAL(10,4)) as turnover_rate_mean,
            CAST(turnover_rate_std AS DECIMAL(10,4)) as turnover_rate_std,

            CAST(amount_median AS DECIMAL(15,2)) as amount_median,
            CAST(amount_mean AS DECIMAL(15,2)) as amount_mean,
            CAST(amount_total AS DECIMAL(20,2)) as amount_total,

            CAST(high_price_ratio AS DECIMAL(5,4)) as high_price_ratio,
            CAST(high_vol_ratio AS DECIMAL(5,4)) as high_vol_ratio,
            CAST(high_turnover_ratio AS DECIMAL(5,4)) as high_turnover_ratio,

            -- 5. 血缘元数据
            'rawdata.market_technical' as _source_table,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM aggregated
        WHERE trade_date IS NOT NULL
        ORDER BY trade_date DESC;
        """

        self.logger.info(
            f"定义物化视图 SQL: {self.materialized_view_schema}.{self.materialized_view_name}"
        )

        return sql.strip()

    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        return None

    async def process_data(self, data: pd.DataFrame, stop_event=None, **kwargs) -> Optional[pd.DataFrame]:
        return None

    async def save_result(self, data: pd.DataFrame, **kwargs):
        pass
