"""
股票利润表物化视图定义

整合三个数据源实现完整的 PIT 利润表：
- rawdata.fina_income (正式报告 report)
- rawdata.fina_express (业绩快报 express)
- rawdata.fina_forecast (业绩预告 forecast)

说明：
- PIT 是时间语义/安全原则，应贯穿所有特征；这里的"PIT 展开"是实现细节。
- 该特征归类到 stock 域，提供可 PIT 消费的利润表季度快照。
- 完全对标 pgs_factors.pit_income_quarterly 的数据逻辑。

数据流:
    rawdata.fina_income + fina_express + fina_forecast → features.mv_stock_income_quarterly

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: stock_income_quarterly.py
- 类名: StockIncomeQuarterlyMV
- recipe.name: stock_income_quarterly
- 输出表名: features.mv_stock_income_quarterly

验收标准（见 D-1/D-2/D-3）：
- D-1: query_start_date=ann_date, query_end_date由LEAD推导, report_period=end_date
- D-2: 可与 pgs_factors.pit_income_quarterly 做抽样对比（行数覆盖率 80%-120%）
- D-3: 幂等刷新, 血缘字段完备
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class StockIncomeQuarterlyMV(BaseFeatureView):
    """股票利润表物化视图（带 PIT 时间窗口，整合 report+express+forecast）。"""

    name = "stock_income_quarterly"
    description = "股票利润表物化视图（带 PIT 时间窗口，整合 fina_income/express/forecast）"

    refresh_strategy = "full"
    source_tables: List[str] = [
        "rawdata.fina_income",
        "rawdata.fina_express",
        "rawdata.fina_forecast",
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
        CREATE MATERIALIZED VIEW features.mv_stock_income_quarterly AS
        WITH 
        -- 1. 正式报告数据 (report)
        report_data AS (
            SELECT
                ts_code,
                COALESCE(f_ann_date, ann_date) AS pit_ann_date,
                end_date,
                revenue,
                oper_cost,
                operate_profit,
                total_profit,
                n_income,
                n_income_attr_p,
                'report' AS data_source
            FROM rawdata.fina_income
            WHERE
                ts_code IS NOT NULL
                AND COALESCE(f_ann_date, ann_date) IS NOT NULL
                AND end_date IS NOT NULL
        ),
        -- 2. 业绩快报数据 (express)
        express_data AS (
            SELECT
                ts_code,
                ann_date AS pit_ann_date,
                end_date,
                revenue,
                NULL::numeric AS oper_cost,
                operate_profit,
                total_profit,
                n_income,
                NULL::numeric AS n_income_attr_p,
                'express' AS data_source
            FROM rawdata.fina_express
            WHERE
                ts_code IS NOT NULL
                AND ann_date IS NOT NULL
                AND end_date IS NOT NULL
        ),
        -- 3. 业绩预告数据 (forecast)
        -- 注意：forecast 只有预测区间，取中值作为预估
        forecast_data AS (
            SELECT
                ts_code,
                ann_date AS pit_ann_date,
                end_date,
                NULL::numeric AS revenue,
                NULL::numeric AS oper_cost,
                NULL::numeric AS operate_profit,
                NULL::numeric AS total_profit,
                (COALESCE(net_profit_min, 0) + COALESCE(net_profit_max, 0)) / 2.0 AS n_income,
                NULL::numeric AS n_income_attr_p,
                'forecast' AS data_source
            FROM rawdata.fina_forecast
            WHERE
                ts_code IS NOT NULL
                AND ann_date IS NOT NULL
                AND end_date IS NOT NULL
        ),
        -- 4. 合并所有数据源
        all_data AS (
            SELECT * FROM report_data
            UNION ALL
            SELECT * FROM express_data
            UNION ALL
            SELECT * FROM forecast_data
        ),
        -- 5. 获取每个股票的所有不同公告日期（跨所有 data_source）
        distinct_pit_ann_dates AS (
            SELECT DISTINCT ts_code, pit_ann_date
            FROM all_data
        ),
        -- 6. 计算下一个不同的公告日期
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

            -- 核心利润指标
            a.revenue,
            a.oper_cost,
            a.operate_profit,
            a.total_profit,
            a.n_income,
            a.n_income_attr_p,

            -- 数据来源标识（对标 PIT 表）
            a.data_source,

            -- 血缘元数据（D-3 验收要求）
            CASE a.data_source
                WHEN 'report' THEN 'rawdata.fina_income'
                WHEN 'express' THEN 'rawdata.fina_express'
                WHEN 'forecast' THEN 'rawdata.fina_forecast'
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
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_income_quarterly_ts_code_query_window "
            "ON features.mv_stock_income_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_income_quarterly_report_period "
            "ON features.mv_stock_income_quarterly (report_period)",
        ]
