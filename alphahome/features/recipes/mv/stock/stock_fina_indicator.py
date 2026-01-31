"""
股票财务指标物化视图定义

直接从 rawdata.fina_indicator 读取 Tushare 财务指标数据，
用纯 SQL 实现 PIT 展开逻辑，输出到 features.mv_stock_fina_indicator。

说明：
- PIT 是时间语义/安全原则，应贯穿所有特征；这里的“PIT 展开”是实现细节。
- 因此将该特征从 pit 域更名并归类到 stock 域。

数据流:
    rawdata.fina_indicator → features.mv_stock_fina_indicator

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: stock_fina_indicator.py
- 类名: StockFinaIndicatorMV
- recipe.name: stock_fina_indicator
- 输出表名: features.mv_stock_fina_indicator
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView


@feature_register
class StockFinaIndicatorMV(BaseFeatureView):
    """股票财务指标物化视图（带 PIT 时间窗口）。"""

    name = "stock_fina_indicator"
    description = "股票财务指标物化视图（带 PIT 时间窗口，源 rawdata.fina_indicator）"

    refresh_strategy = "full"
    source_tables: List[str] = ["rawdata.fina_indicator"]

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
        CREATE MATERIALIZED VIEW features.mv_stock_fina_indicator AS
        WITH base_data AS (
            SELECT
                ts_code,
                ann_date,
                end_date,

                -- 盈利能力指标
                roe,                    -- 净资产收益率
                roe_dt,                 -- 净资产收益率(扣非)
                roa,                    -- 总资产报酬率
                npta,                   -- 总资产净利润
                grossprofit_margin,     -- 毛利率
                netprofit_margin,       -- 净利率
                profit_to_gr,           -- 利润/营收

                -- 营运能力指标
                assets_turn,            -- 总资产周转率
                ar_turn,                -- 应收账款周转率
                inv_turn,               -- 存货周转率
                ca_turn,                -- 流动资产周转率
                fa_turn,                -- 固定资产周转率

                -- 单季度指标
                q_roe,                  -- 单季度 ROE
                q_sales_yoy,            -- 单季度营收同比
                q_profit_yoy,           -- 单季度利润同比

                -- 其他重要指标
                eps,                    -- 每股收益
                dt_eps,                 -- 稀释每股收益
                ebit,                   -- 息税前利润
                ebitda,                 -- 息税折旧摊销前利润
                current_ratio,          -- 流动比率
                quick_ratio             -- 速动比率

            FROM rawdata.fina_indicator
            WHERE
                ts_code IS NOT NULL
                AND ann_date IS NOT NULL
                AND end_date IS NOT NULL
        ),
        -- 获取每个股票的所有不同公告日期（用于计算 PIT 窗口）
        distinct_ann_dates AS (
            SELECT DISTINCT ts_code, ann_date
            FROM base_data
        ),
        -- 计算下一个不同的公告日期
        next_dates AS (
            SELECT
                ts_code,
                ann_date,
                LEAD(ann_date) OVER (
                    PARTITION BY ts_code
                    ORDER BY ann_date
                ) AS next_ann_date
            FROM distinct_ann_dates
        )
        SELECT
            b.ts_code,

            -- PIT 时间范围
            b.ann_date AS query_start_date,
            COALESCE(n.next_ann_date - INTERVAL '1 day', '2099-12-31'::date)::date AS query_end_date,
            b.end_date AS report_period,
            b.ann_date,

            -- 盈利能力指标
            b.roe,
            b.roe_dt,
            b.roa,
            b.npta,
            b.grossprofit_margin,
            b.netprofit_margin,
            b.profit_to_gr,

            -- 营运能力指标
            b.assets_turn,
            b.ar_turn,
            b.inv_turn,
            b.ca_turn,
            b.fa_turn,

            -- 单季度指标
            b.q_roe,
            b.q_sales_yoy,
            b.q_profit_yoy,

            -- 其他重要指标
            b.eps,
            b.dt_eps,
            b.ebit,
            b.ebitda,
            b.current_ratio,
            b.quick_ratio,

            -- 血缘元数据
            'rawdata.fina_indicator' AS _source_table,
            NOW() AS _processed_at,
            CURRENT_DATE AS _data_version
        FROM base_data b
        LEFT JOIN next_dates n ON b.ts_code = n.ts_code AND b.ann_date = n.ann_date
        ORDER BY b.ts_code, b.ann_date DESC;
        """
        return sql.strip()

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_fina_indicator_ts_code_query_window "
            "ON features.mv_stock_fina_indicator (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_fina_indicator_report_period "
            "ON features.mv_stock_fina_indicator (report_period)",
        ]
