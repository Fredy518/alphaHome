"""
PIT 行业分类物化视图任务

负责创建和管理 pit_industry_classification_mv 物化视图。
该物化视图从 rawdata.pit_industry_classification 表读取数据，
进行数据对齐、时间序列展开、数据标准化，最后输出到 materialized_views schema。

**Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
**Validates: Requirements 8.1, 8.2, 8.3**

**Feature: materialized-views-system, Property 8: Data alignment consistency**
**Validates: Requirements 5.1, 8.1**
"""

from typing import Any, Dict, List, Optional
import pandas as pd

from alphahome.processors.materialized_views import MaterializedViewTask, MaterializedViewSQL


class PITIndustryClassificationMV(MaterializedViewTask):
    """PIT 行业分类物化视图任务。"""

    name = "pit_industry_classification_mv"
    description = "PIT 行业分类物化视图"

    is_materialized_view: bool = True
    materialized_view_name: str = "pit_industry_classification_mv"
    materialized_view_schema: str = "materialized_views"

    refresh_strategy: str = "full"

    source_tables: List[str] = ["rawdata.pit_industry_classification"]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["ts_code", "obs_date", "industry_level1", "industry_level2"],
            "threshold": 0.01,
        },
        "outlier_check": {
            "columns": [],
            "method": "none",
            "threshold": 0.0,
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
        CREATE MATERIALIZED VIEW materialized_views.pit_industry_classification_mv AS
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
                    WHEN obs_date::text ~ '^\\\\d{8}$' THEN to_date(obs_date::text, 'YYYYMMDD')
                    ELSE obs_date::date
                END AS obs_date_dt,

                TRIM(data_source) AS data_source_std,
                TRIM(industry_level1) as industry_level1,
                TRIM(industry_level2) as industry_level2,
                TRIM(industry_level3) as industry_level3,
                TRIM(industry_code1) as industry_code1,
                TRIM(industry_code2) as industry_code2,
                TRIM(industry_code3) as industry_code3,

                -- 特殊处理标识
                requires_special_gpa_handling,
                TRIM(gpa_calculation_method) as gpa_calculation_method,
                TRIM(special_handling_reason) as special_handling_reason,

                -- 数据质量标识
                TRIM(data_quality) as data_quality
            FROM rawdata.pit_industry_classification
            WHERE
                ts_code IS NOT NULL
                AND obs_date IS NOT NULL
                AND industry_level1 IS NOT NULL
                AND industry_level2 IS NOT NULL
                AND data_source IS NOT NULL
        )
        SELECT
            ts_code_std as ts_code,

            -- 3. 时间序列展开（PIT 特有）
            obs_date_dt as query_start_date,
            COALESCE(
                LEAD(obs_date_dt) OVER (PARTITION BY ts_code_std, data_source_std ORDER BY obs_date_dt) - INTERVAL '1 day',
                '2099-12-31'::date
            ) as query_end_date,
            obs_date_dt as obs_date,

            -- 4. 行业分类数据（标准化）
            data_source_std as data_source,
            industry_level1,
            industry_level2,
            industry_level3,
            industry_code1,
            industry_code2,
            industry_code3,

            -- 5. 特殊处理标识
            requires_special_gpa_handling,
            gpa_calculation_method,
            special_handling_reason,

            -- 6. 数据质量标识
            data_quality,

            -- 7. 血缘元数据
            'rawdata.pit_industry_classification' as _source_table,
            NOW() as _processed_at,
            CURRENT_DATE as _data_version
        FROM normalized
        WHERE
            obs_date_dt IS NOT NULL
            AND data_source_std IS NOT NULL
            AND data_source_std <> ''
            AND industry_level1 IS NOT NULL AND industry_level1 <> ''
            AND industry_level2 IS NOT NULL AND industry_level2 <> ''
        ORDER BY ts_code_std, data_source_std, obs_date_dt DESC;
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
