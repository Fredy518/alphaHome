"""
股票资产负债表物化视图定义

整合两个数据源实现完整的 PIT 资产负债表：
- rawdata.fina_balancesheet (正式报告 report)
- rawdata.fina_express (业绩快报 express，含 total_assets 和权益数据)

说明：
- PIT 是时间语义/安全原则，应贯穿所有特征；这里的"PIT 展开"是实现细节。
- 该特征归类到 stock 域，提供可 PIT 消费的资产负债表季度快照。
- 完全对标 pit.pit_balance_quarterly 的数据逻辑。

数据流:
    rawdata.fina_balancesheet + fina_express → features.mv_stock_balance_quarterly

命名规范（见 docs/architecture/features_module_design.md Section 3.2.1）:
- 文件名: stock_balance_quarterly.py
- 类名: StockBalanceQuarterlyMV
- recipe.name: stock_balance_quarterly
- 输出表名: features.mv_stock_balance_quarterly

验收标准（见 D-1/D-2/D-3）：
- D-1: query_start_date=ann_date, query_end_date由LEAD推导, report_period=end_date
- D-2: 可与 pit.pit_balance_quarterly 做抽样对比（行数覆盖率 80%-120%）
- D-3: 幂等刷新, 血缘字段完备
"""

from typing import Any, Dict, List

from alphahome.features.registry import feature_register
from alphahome.features.storage.base_view import BaseFeatureView
from .financial_pit_sql import financial_statement_sql


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
            "columns": ["ts_code", "ann_date", "report_period"],
            "threshold": 0.01,
        },
        "row_count_change": {
            "threshold": 0.3,
        },
    }

    def get_create_sql(self) -> str:
        fields = ('tot_assets', 'tot_liab', 'tot_equity', 'total_cur_assets', 'total_cur_liab', 'inventories')
        sources = [
            ('report', 'rawdata.fina_balancesheet', ('total_assets', 'total_liab', 'total_hldr_eqy_exc_min_int',
                                                   'total_cur_assets', 'total_cur_liab', 'inventories')),
            ('express', 'rawdata.fina_express', ('total_assets', 'NULL::numeric', 'total_hldr_eqy_exc_min_int',
                                               'NULL::numeric', 'NULL::numeric', 'NULL::numeric')),
        ]
        return financial_statement_sql(self.full_name, sources, fields)


    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_balance_quarterly_ts_code_query_window "
            "ON features.mv_stock_balance_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_balance_quarterly_report_period "
            "ON features.mv_stock_balance_quarterly (report_period)",
        ]
