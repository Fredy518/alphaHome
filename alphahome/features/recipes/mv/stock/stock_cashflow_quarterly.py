"""
现金流量表 PIT 展开（季度）

与 stock_income_quarterly / stock_balance_quarterly 形成财报三表完整 PIT 体系。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from .financial_pit_sql import financial_statement_sql
from alphahome.features.registry import feature_register


@feature_register
class StockCashflowQuarterlyMV(BaseFeatureView):
    """现金流量表（季度，PIT 时间窗口）"""

    name = "stock_cashflow_quarterly"
    description = "现金流量表 PIT 展开，含经营/投资/筹资活动现金流"
    source_tables = ["rawdata.fina_cashflow"]
    refresh_strategy = "full"

    def get_create_sql(self) -> str:
        fields = ('net_profit', 'c_fr_sale_sg', 'n_cashflow_act', 'c_pay_acq_const_fiolta',
                  'c_paid_invest', 'n_cashflow_inv_act', 'c_recp_borrow', 'n_cash_flows_fnc_act',
                  'n_incr_cash_cash_equ', 'free_cashflow')
        return financial_statement_sql(self.full_name, [('report', 'rawdata.fina_cashflow', fields)], fields)


    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_cashflow_quarterly_ts_code "
            "ON features.mv_stock_cashflow_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_stock_cashflow_quarterly_period "
            "ON features.mv_stock_cashflow_quarterly (report_period)",
        ]
