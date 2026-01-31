"""
股票资产负债表物化视图定义

整合两个数据源实现完整的 PIT 资产负债表：
- rawdata.fina_balancesheet (正式报告 report)
- rawdata.fina_express (业绩快报 express，含 total_assets 和权益数据)

说明：
- PIT 是时间语义/安全原则，应贯穿所有特征；这里的"PIT 展开"是实现细节。
- 该特征归类到 stock 域，提供可 PIT 消费的资产负债表季度快照。
- 完全对标 pgs_factors.pit_balance_quarterly 的数据逻辑。

数据流:
    rawdata.fina_balancesheet + fina_express → features.mv_stock_balance_quarterly

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: stock_balance_quarterly.py
- 类名: StockBalanceQuarterlyMV
- recipe.name: stock_balance_quarterly
- 输出表名: features.mv_stock_balance_quarterly

验收标准（见 D-1/D-2/D-3）：
- D-1: query_start_date=ann_date, query_end_date由LEAD推导, report_period=end_date
- D-2: 可与 pgs_factors.pit_balance_quarterly 做抽样对比（行数覆盖率 80%-120%）
- D-3: 幂等刷新, 血缘字段完备
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class StockBalanceQuarterlyMV(BaseFeatureView):
    """股票资产负债表物化视图（带 PIT 时间窗口，整合 report+express）。"""

    name = "stock_balance_quarterly"
    description = "股票资产负债表物化视图（带 PIT 时间窗口，整合 fina_balancesheet/express）"

    refresh_strategy = "full"
    source_tables: List[str] = [
        "rawdata.fina_balancesheet",
        "rawdata.fina_express",
    ]

    quality_checks: Dict[str, Any] = {
        "null_check": {
            "columns": ["ts_code", "ann_date", "end_date"],
            "threshold": 0.01,
        },
        "row_count_change": {
            "threshold": 0.3,
        },
    }

    def get_create_sql(self) -> str:
        sql = """
        CREATE MATERIALIZED VIEW features.mv_stock_balance_quarterly AS
        WITH 
        -- 1. 正式报告数据 (report)
        report_data AS (
            SELECT
                ts_code,
                COALESCE(f_ann_date, ann_date) AS pit_ann_date,
                end_date,
                total_assets,
                total_liab,
                total_hldr_eqy_exc_min_int AS tot_equity,
                total_cur_assets,
                total_cur_liab,
                inventories,
                'report' AS data_source
            FROM rawdata.fina_balancesheet
            WHERE
                ts_code IS NOT NULL
                AND COALESCE(f_ann_date, ann_date) IS NOT NULL
                AND end_date IS NOT NULL
        ),
        -- 2. 业绩快报数据 (express)
        -- fina_express 只有 total_assets 和 total_hldr_eqy_exc_min_int
        express_data AS (
            SELECT
                ts_code,
                ann_date AS pit_ann_date,
                end_date,
                total_assets,
                NULL::numeric AS total_liab,
                total_hldr_eqy_exc_min_int AS tot_equity,
                NULL::numeric AS total_cur_assets,
                NULL::numeric AS total_cur_liab,
                NULL::numeric AS inventories,
                'express' AS data_source
            FROM rawdata.fina_express
            WHERE
                ts_code IS NOT NULL
                AND ann_date IS NOT NULL
                AND end_date IS NOT NULL
        ),
        -- 3. 合并所有数据源
        all_data AS (
            SELECT * FROM report_data
            UNION ALL
            SELECT * FROM express_data
        ),
        -- 4. 获取每个股票的所有不同公告日期（跨所有 data_source）
        distinct_pit_ann_dates AS (
            SELECT DISTINCT ts_code, pit_ann_date
            FROM all_data
        ),
        -- 5. 计算下一个不同的公告日期
        next_dates AS (
            SELECT
                ts_code,
                pit_ann_date,
                LEAD(pit_ann_date) OVER (
                    PARTITION BY ts_code
                    ORDER BY pit_ann_date
                ) AS next_ann_date
            FROM distinct_pit_ann_dates
        )
        SELECT
            a.ts_code,

            -- PIT 时间范围（D-1 验收要求）
            a.pit_ann_date AS query_start_date,
            COALESCE(n.next_ann_date - INTERVAL '1 day', '2099-12-31'::date)::date AS query_end_date,
            a.end_date AS report_period,
            a.pit_ann_date AS ann_date,

            -- 核心资产负债指标（对标 PIT 表字段）
            a.total_assets AS tot_assets,
            a.total_liab AS tot_liab,
            a.tot_equity,
            a.total_cur_assets,
            a.total_cur_liab,
            a.inventories,

            -- 数据来源标识（对标 PIT 表）
            a.data_source,

            -- 血缘元数据（D-3 验收要求）
            CASE a.data_source
                WHEN 'report' THEN 'rawdata.fina_balancesheet'
                WHEN 'express' THEN 'rawdata.fina_express'
            END AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM all_data a
        LEFT JOIN next_dates n ON a.ts_code = n.ts_code AND a.pit_ann_date = n.pit_ann_date
        ORDER BY a.ts_code, a.pit_ann_date DESC, a.data_source;
        """
        return sql.strip()

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_balance_quarterly_ts_code_query_window "
            "ON features.mv_stock_balance_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_balance_quarterly_report_period "
            "ON features.mv_stock_balance_quarterly (report_period)",
        ]
