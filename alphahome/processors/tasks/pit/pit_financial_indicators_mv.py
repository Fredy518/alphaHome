"""
PIT 财务指标物化视图任务

负责创建和管理 pit_financial_indicators_mv 物化视图。
该物化视图从 rawdata.pit_financial_indicators 表读取数据，
进行数据对齐、时间序列展开、数据标准化，最后输出到 materialized_views schema。

**Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
**Validates: Requirements 8.1, 8.2, 8.3**

**Feature: materialized-views-system, Property 8: Data alignment consistency**
**Validates: Requirements 5.1, 8.1**
"""

from typing import Any, Dict, List, Optional
import pandas as pd

from alphahome.processors.materialized_views import MaterializedViewTask, MaterializedViewSQL


class PITFinancialIndicatorsMV(MaterializedViewTask):
    """PIT 财务指标物化视图任务。"""

    name = "pit_financial_indicators_mv"
    description = "PIT 财务指标物化视图"

    is_materialized_view: bool = True
    materialized_view_name: str = "pit_financial_indicators_mv"
    materialized_view_schema: str = "materialized_views"

    refresh_strategy: str = "full"

    source_tables: List[str] = ["rawdata.pit_financial_indicators"]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["ts_code", "ann_date", "pe_ttm", "pb"],
            "threshold": 0.01,
        },
        "outlier_check": {
            "columns": ["pe_ttm", "pb"],
            "method": "iqr",
            "threshold": 3.0,
        },
        "row_count_change": {
            "threshold": 0.5,
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
        CREATE MATERIALIZED VIEW materialized_views.pit_financial_indicators_mv AS
        WITH normalized AS (
            SELECT
                -- 1. 数据对齐（格式标准化）
                CASE
                    WHEN ts_code ~ '\\\\.' THEN ts_code
                    WHEN ts_code LIKE '6%' THEN ts_code || '.SH'
                    WHEN ts_code LIKE '0%' THEN ts_code || '.SZ'
                    WHEN ts_code LIKE '3%' THEN ts_code || '.SZ'
                    ELSE ts_code
                END AS ts_code_std,

                -- 2. 日期标准化（兼容 YYYYMMDD 整数 / 字符串 / date）
                CASE
                    WHEN ann_date::text ~ '^\\\\d{8}$' THEN to_date(ann_date::text, 'YYYYMMDD')
                    ELSE ann_date::date
                END AS ann_date_dt,
                CASE
                    WHEN end_date::text ~ '^\\\\d{8}$' THEN to_date(end_date::text, 'YYYYMMDD')
                    ELSE end_date::date
                END AS end_date_dt,

                pe_ttm,
                pb,
                ps,
                dv_ttm,
                total_mv
            FROM rawdata.pit_financial_indicators
            WHERE
                ts_code IS NOT NULL
                AND ann_date IS NOT NULL
                AND end_date IS NOT NULL
                AND pe_ttm IS NOT NULL
                AND pb IS NOT NULL
                AND pe_ttm BETWEEN -1000000 AND 1000000
                AND pb BETWEEN -1000000 AND 1000000
        )
        SELECT
            ts_code_std as ts_code,

            -- 3. 时间序列展开（PIT 特有）
            ann_date_dt as query_start_date,
            COALESCE(
                LEAD(ann_date_dt) OVER (PARTITION BY ts_code_std ORDER BY ann_date_dt) - INTERVAL '1 day',
                '2099-12-31'::date
            ) as query_end_date,
            end_date_dt as end_date,

            -- 4. 数据标准化（数值列转换为 DECIMAL）
            CAST(pe_ttm AS DECIMAL(10,2)) as pe_ttm,
            CAST(pb AS DECIMAL(10,2)) as pb,
            CAST(ps AS DECIMAL(10,2)) as ps,
            CAST(dv_ttm AS DECIMAL(10,2)) as dv_ttm,
            CAST(total_mv AS DECIMAL(15,2)) as total_mv,

            -- 5. 血缘元数据
            'rawdata.pit_financial_indicators' as _source_table,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM normalized
        WHERE
            ann_date_dt IS NOT NULL
            AND end_date_dt IS NOT NULL
        ORDER BY ts_code_std, ann_date_dt DESC;
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
