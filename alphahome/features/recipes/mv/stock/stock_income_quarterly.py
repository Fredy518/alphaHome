"""
股票利润表物化视图定义

整合三个数据源实现完整的 PIT 利润表：
- rawdata.fina_income (正式报告 report)
- rawdata.fina_express (业绩快报 express)
- rawdata.fina_forecast (业绩预告 forecast)

说明：
- PIT 是时间语义/安全原则，应贯穿所有特征；这里的"PIT 展开"是实现细节。
- 该特征归类到 stock 域，提供可 PIT 消费的利润表季度快照。
- 完全对标 pit.pit_income_quarterly 的数据逻辑。

数据流:
    rawdata.fina_income + fina_express + fina_forecast → features.mv_stock_income_quarterly

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: stock_income_quarterly.py
- 类名: StockIncomeQuarterlyMV
- recipe.name: stock_income_quarterly
- 输出表名: features.mv_stock_income_quarterly

验收标准（见 D-1/D-2/D-3）：
- D-1: query_start_date=ann_date, query_end_date由LEAD推导, report_period=end_date
- D-2: 可与 pit.pit_income_quarterly 做抽样对比（行数覆盖率 80%-120%）
- D-3: 幂等刷新, 血缘字段完备
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView
from .financial_pit_sql import financial_statement_sql


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
            "columns": ["ts_code", "ann_date", "report_period"],
            "threshold": 0.01,
        },
        "row_count_change": {
            "threshold": 0.3,
        },
    }

    def get_create_sql(self) -> str:
        fields = ('revenue', 'oper_cost', 'operate_profit', 'total_profit', 'n_income', 'n_income_attr_p')
        sources = [
            ('report', 'rawdata.fina_income', fields),
            ('express', 'rawdata.fina_express', ('revenue', 'NULL::numeric', 'operate_profit',
                                              'total_profit', 'n_income', 'NULL::numeric')),
            ('forecast', 'rawdata.fina_forecast', ('NULL::numeric', 'NULL::numeric', 'NULL::numeric',
                                                 'NULL::numeric',
                                                 '(COALESCE(net_profit_min, 0) + COALESCE(net_profit_max, 0)) / 2.0',
                                                 'NULL::numeric')),
        ]
        return financial_statement_sql(self.full_name, sources, fields)


    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_income_quarterly_ts_code_query_window "
            "ON features.mv_stock_income_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_income_quarterly_report_period "
            "ON features.mv_stock_income_quarterly (report_period)",
        ]
