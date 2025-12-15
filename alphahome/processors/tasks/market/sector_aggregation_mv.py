"""
行业板块聚合物化视图任务

负责创建和管理 sector_aggregation_mv 物化视图。
该物化视图从 rawdata.industry_classification 和 rawdata.stock_daily 表读取数据，
进行行业分组、横截面统计、数据标准化，最后输出到 materialized_views schema。

**Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
**Validates: Requirements 8.1, 8.2, 8.3**
"""

from typing import Any, Dict, List, Optional
import pandas as pd

from alphahome.processors.materialized_views import MaterializedViewTask, MaterializedViewSQL


class SectorAggregationMV(MaterializedViewTask):
    """行业板块聚合物化视图任务。"""

    name = "sector_aggregation_mv"
    description = "行业板块聚合物化视图"

    is_materialized_view: bool = True
    materialized_view_name: str = "sector_aggregation_mv"
    materialized_view_schema: str = "materialized_views"

    refresh_strategy: str = "full"

    source_tables: List[str] = [
        "rawdata.industry_classification",
        "rawdata.stock_daily",
    ]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["trade_date", "industry_code", "stock_count", "close_median"],
            "threshold": 0.05,
        },
        "outlier_check": {
            "columns": ["close_median", "close_mean", "vol_total", "turnover_rate_mean"],
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
        CREATE MATERIALIZED VIEW materialized_views.sector_aggregation_mv AS
        WITH industry_norm AS (
            SELECT DISTINCT
                CASE
                    WHEN ic.ts_code ~ '\\.' THEN ic.ts_code
                    WHEN ic.ts_code LIKE '6%' THEN ic.ts_code || '.SH'
                    WHEN ic.ts_code LIKE '0%' THEN ic.ts_code || '.SZ'
                    WHEN ic.ts_code LIKE '3%' THEN ic.ts_code || '.SZ'
                    ELSE ic.ts_code
                END AS ts_code_std,
                ic.industry_code,
                ic.industry_name
            FROM rawdata.industry_classification ic
            WHERE
                ic.ts_code IS NOT NULL
                AND ic.industry_code IS NOT NULL
        ),
        normalized AS (
            SELECT
                -- 1. 数据对齐（格式标准化）
                CASE
                    WHEN sd.trade_date::text ~ '^\\d{8}$'
                    THEN to_date(sd.trade_date::text, 'YYYYMMDD')
                    ELSE sd.trade_date::date
                END AS trade_date_dt,

                CASE
                    WHEN sd.ts_code ~ '\\.' THEN sd.ts_code
                    WHEN sd.ts_code LIKE '6%' THEN sd.ts_code || '.SH'
                    WHEN sd.ts_code LIKE '0%' THEN sd.ts_code || '.SZ'
                    WHEN sd.ts_code LIKE '3%' THEN sd.ts_code || '.SZ'
                    ELSE sd.ts_code
                END AS ts_code_std,

                ind.industry_code,
                ind.industry_name,

                -- 2. 数据标准化（数值列转换为 DECIMAL）
                CAST(sd.close AS DECIMAL(10,2)) as close,
                CAST(sd.vol AS DECIMAL(15,0)) as vol,
                CAST(sd.turnover_rate AS DECIMAL(10,4)) as turnover_rate,
                CAST(sd.amount AS DECIMAL(15,2)) as amount
            FROM rawdata.stock_daily sd
            LEFT JOIN industry_norm ind
                ON (
                    CASE
                        WHEN sd.ts_code ~ '\\.' THEN sd.ts_code
                        WHEN sd.ts_code LIKE '6%' THEN sd.ts_code || '.SH'
                        WHEN sd.ts_code LIKE '0%' THEN sd.ts_code || '.SZ'
                        WHEN sd.ts_code LIKE '3%' THEN sd.ts_code || '.SZ'
                        ELSE sd.ts_code
                    END
                ) = ind.ts_code_std
            WHERE
                sd.trade_date IS NOT NULL
                AND sd.ts_code IS NOT NULL
                AND sd.close IS NOT NULL
                AND sd.close > 0
                AND sd.vol IS NOT NULL
                AND sd.vol >= 0
                AND sd.turnover_rate IS NOT NULL
                AND sd.turnover_rate >= 0
                AND ind.industry_code IS NOT NULL
        ),
        stats AS (
            SELECT
                trade_date_dt as trade_date,
                industry_code,
                industry_name,

                -- 3. 横截面统计
                COUNT(DISTINCT ts_code_std) as stock_count,

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
            GROUP BY trade_date_dt, industry_code, industry_name
        ),
        ratios AS (
            SELECT
                n.trade_date_dt as trade_date,
                n.industry_code,
                n.industry_name,
                COUNT(DISTINCT n.ts_code_std) as stock_count,
                COUNT(DISTINCT CASE WHEN n.close > 100 THEN n.ts_code_std END)::float
                    / NULLIF(COUNT(DISTINCT n.ts_code_std), 0) as high_price_ratio,
                COUNT(DISTINCT CASE WHEN n.vol > s.vol_median * 1.5 THEN n.ts_code_std END)::float
                    / NULLIF(COUNT(DISTINCT n.ts_code_std), 0) as high_vol_ratio,
                COUNT(DISTINCT CASE WHEN n.turnover_rate > 5 THEN n.ts_code_std END)::float
                    / NULLIF(COUNT(DISTINCT n.ts_code_std), 0) as high_turnover_ratio
            FROM normalized n
            JOIN stats s
                ON s.trade_date = n.trade_date_dt
               AND s.industry_code = n.industry_code
               AND s.industry_name = n.industry_name
            GROUP BY n.trade_date_dt, n.industry_code, n.industry_name, s.vol_median
        ),
        aggregated AS (
            SELECT
                s.*,
                r.high_price_ratio,
                r.high_vol_ratio,
                r.high_turnover_ratio
            FROM stats s
            JOIN ratios r
                USING (trade_date, industry_code, industry_name)
        )
        SELECT
            trade_date,
            industry_code,
            industry_name,

            -- 4. 数据标准化（转换为 DECIMAL）
            CAST(stock_count AS INTEGER) as stock_count,

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
            'rawdata.industry_classification, rawdata.stock_daily' as _source_tables,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM aggregated
        WHERE trade_date IS NOT NULL
            AND industry_code IS NOT NULL
            AND stock_count > 0
        ORDER BY trade_date DESC, industry_code;
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
