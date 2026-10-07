""" 
股票行业分类（月度快照）物化视图定义

从 rawdata.index_swmember（申万）和 rawdata.index_cimember（中信）读取行业分类变更记录，
用纯 SQL 展开为“月度月末快照”，输出到 features.mv_stock_industry_monthly_snapshot。

功能对标：
    完全对标 pit.pit_industry_classification 逻辑
    - 月度快照格式（每月月末每只股票每个数据源一条记录）
    - 双数据源：sw（申万）+ ci（中信）
    - 唯一键：(ts_code, obs_date, data_source)

数据流:
    rawdata.index_swmember (申万) ─┬─→ features.mv_stock_industry_monthly_snapshot
    rawdata.index_cimember (中信) ─┘

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: stock_industry_monthly_snapshot.py
- 类名: StockIndustryMonthlySnapshotMV
- recipe.name: stock_industry_monthly_snapshot
- 输出表名: features.mv_stock_industry_monthly_snapshot
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class StockIndustryMonthlySnapshotMV(BaseFeatureView):
    """股票行业分类（月度月末快照，sw+ci 双数据源）。"""

    name = "stock_industry_monthly_snapshot"
    description = "股票行业分类（月度月末快照，sw+ci 双数据源，对标 pit_industry_classification）"

    refresh_strategy = "full"
    source_tables: List[str] = ["rawdata.index_swmember", "rawdata.index_cimember"]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["ts_code", "obs_date", "industry_level1"],
            "threshold": 0.01,
        },
        "row_count_change": {
            "threshold": 0.3,
        },
    }

    def get_create_sql(self) -> str:
        sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_industry_monthly_snapshot AS
        WITH
        -- 仅生成已结束的月末；月中不得生成未来观察日。
        month_series AS (
            SELECT
                (DATE_TRUNC('month', generate_series) + INTERVAL '1 month' - INTERVAL '1 day')::date AS obs_date
            FROM generate_series(
                '2000-01-01'::date,
                CASE WHEN CURRENT_DATE = (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')::date
                     THEN CURRENT_DATE
                     ELSE DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day' END,
                '1 month'::interval
            ) AS generate_series
        ),

        -- 申万行业变更记录（data_source = 'sw'）
        sw_industry AS (
            SELECT
                ts_code,
                l1_name AS industry_level1,
                l2_name AS industry_level2,
                l3_name AS industry_level3,
                l1_code AS industry_code1,
                l2_code AS industry_code2,
                l3_code AS industry_code3,
                in_date,
                COALESCE(out_date, '2099-12-31'::date) AS out_date,
                'sw' AS data_source
            FROM rawdata.index_swmember
            WHERE ts_code IS NOT NULL
              AND in_date IS NOT NULL
              AND l1_name IS NOT NULL
        ),

        -- 中信行业变更记录（data_source = 'ci'）
        ci_industry AS (
            SELECT
                ts_code,
                l1_name AS industry_level1,
                l2_name AS industry_level2,
                l3_name AS industry_level3,
                l1_code AS industry_code1,
                l2_code AS industry_code2,
                l3_code AS industry_code3,
                in_date,
                COALESCE(out_date, '2099-12-31'::date) AS out_date,
                'ci' AS data_source
            FROM rawdata.index_cimember
            WHERE ts_code IS NOT NULL
              AND in_date IS NOT NULL
              AND l1_name IS NOT NULL
        ),

        -- 合并两个数据源
        all_industry AS (
            SELECT * FROM sw_industry
            UNION ALL
            SELECT * FROM ci_industry
        ),

        -- 月度快照展开：每个月末检查每只股票的有效行业分类
        monthly_snapshot_raw AS (
            SELECT i.ts_code,m.obs_date,i.data_source,
                   i.industry_level1,i.industry_level2,i.industry_level3,
                   i.industry_code1,i.industry_code2,i.industry_code3,i.in_date,
                   DENSE_RANK() OVER(
                       PARTITION BY i.ts_code,m.obs_date,i.data_source
                       ORDER BY i.in_date DESC
                   ) AS start_rank
            FROM month_series m JOIN all_industry i
              ON i.in_date<=m.obs_date AND i.out_date>m.obs_date
        ),
        monthly_latest AS (
            SELECT ts_code,obs_date,data_source,MAX(in_date) AS in_date,
                   MIN(industry_level1) AS industry_level1,
                   MIN(industry_level2) AS industry_level2,
                   MIN(industry_level3) AS industry_level3,
                   MIN(industry_code1) AS industry_code1,
                   MIN(industry_code2) AS industry_code2,
                   MIN(industry_code3) AS industry_code3,
                   COUNT(DISTINCT ROW(industry_level1,industry_level2,industry_level3,
                                      industry_code1,industry_code2,industry_code3))>1 AS ambiguous
            FROM monthly_snapshot_raw WHERE start_rank=1
            GROUP BY ts_code,obs_date,data_source
        ),
        monthly_snapshot AS (
            SELECT ts_code,obs_date,data_source,in_date,ambiguous,
                   CASE WHEN NOT ambiguous THEN industry_level1 END AS industry_level1,
                   CASE WHEN NOT ambiguous THEN industry_level2 END AS industry_level2,
                   CASE WHEN NOT ambiguous THEN industry_level3 END AS industry_level3,
                   CASE WHEN NOT ambiguous THEN industry_code1 END AS industry_code1,
                   CASE WHEN NOT ambiguous THEN industry_code2 END AS industry_code2,
                   CASE WHEN NOT ambiguous THEN industry_code3 END AS industry_code3
            FROM monthly_latest
        )

        SELECT
            ts_code::VARCHAR(30) AS ts_code,
            obs_date,
            data_source,

            industry_level1::VARCHAR(50) AS industry_level1,
            industry_level2::VARCHAR(50) AS industry_level2,
            industry_level3::VARCHAR(100) AS industry_level3,
            industry_code1::VARCHAR(20) AS industry_code1,
            industry_code2::VARCHAR(20) AS industry_code2,
            industry_code3::VARCHAR(20) AS industry_code3,

            CASE
                WHEN ambiguous THEN TRUE
                WHEN industry_level1 IN ('银行', '非银金融', '金融', '银行业')
                     OR industry_level2 LIKE '%银行%'
                     OR industry_level2 LIKE '%证券%'
                     OR industry_level2 LIKE '%保险%'
                     OR industry_level2 LIKE '%信托%'
                THEN TRUE
                ELSE FALSE
            END AS requires_special_gpa_handling,

            CASE
                WHEN ambiguous THEN 'null'
                WHEN industry_level1 IN ('银行', '非银金融', '金融', '银行业')
                     OR industry_level2 LIKE '%银行%'
                     OR industry_level2 LIKE '%证券%'
                     OR industry_level2 LIKE '%保险%'
                     OR industry_level2 LIKE '%信托%'
                THEN 'null'
                ELSE 'standard'
            END AS gpa_calculation_method,

            CASE
                WHEN ambiguous THEN 'same_start_classification_conflict_public_vintage_unverified'
                WHEN industry_level1 IN ('银行', '银行业') OR industry_level2 LIKE '%银行%'
                THEN '银行业营业成本为0导致GPA=100%，需要特殊处理'
                WHEN industry_level2 LIKE '%证券%'
                THEN '证券业成本结构特殊，GPA指标不适用'
                WHEN industry_level2 LIKE '%保险%'
                THEN '保险业成本结构特殊，GPA指标不适用'
                WHEN industry_level1 IN ('非银金融', '金融') OR industry_level2 LIKE '%信托%'
                THEN '金融业成本结构特殊，GPA指标可能不适用'
                ELSE NULL
            END AS special_handling_reason,

            CASE WHEN ambiguous THEN 'ambiguous_same_start_classification' ELSE 'normal' END AS data_quality,

            in_date AS original_in_date,
            'rawdata.index_swmember,rawdata.index_cimember' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version

        FROM monthly_snapshot
        ORDER BY ts_code, obs_date, data_source;
        """
        target, query = sql.strip().split(' AS', 1)
        for source in ('index_swmember', 'index_cimember'):
            original = 'FROM rawdata.' + source + '\n            WHERE ts_code IS NOT NULL'
            bounded = 'FROM rawdata.' + source + '\n            WHERE ts_code = repair_stock.ts_code AND ts_code IS NOT NULL'
            if query.count(original) != 1:
                raise ValueError('industry_source_projection_contract_changed')
            query = query.replace(original, bounded, 1)
        return target + ' AS SELECT result.* FROM (' + (
            'SELECT ts_code FROM rawdata.index_swmember WHERE ts_code IS NOT NULL '
            'UNION SELECT ts_code FROM rawdata.index_cimember WHERE ts_code IS NOT NULL'
        ) + ') repair_stock CROSS JOIN LATERAL (' + query.rstrip(';') + ') result'


    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_industry_monthly_snapshot_ts_code_obs_date_source "
            "ON features.mv_stock_industry_monthly_snapshot (ts_code, obs_date, data_source)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_industry_monthly_snapshot_obs_date "
            "ON features.mv_stock_industry_monthly_snapshot (obs_date)",
        ]
