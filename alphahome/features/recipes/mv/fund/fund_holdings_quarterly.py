"""
公募基金重仓股（季度，PIT）

公募持仓是机构行为研究基础，用于抱团股识别、行业配置分析。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register
from alphahome.features.recipes.mv.bounded_pit_sql import bounded_stock_source_sql, cumulative_position_delta_sql
from alphahome.features.recipes.mv.pit_asof_rank_sql import fund_holdings_sql


@feature_register
class FundHoldingsQuarterlyMV(BaseFeatureView):
    """公募基金重仓股"""

    name = "fund_holdings_quarterly"
    description = "公募基金重仓股持仓市值、比例、基金数量"
    source_tables = ["rawdata.fund_portfolio"]
    refresh_strategy = "full"

    create_sql = fund_holdings_sql(min_history_periods=4)

    def __init__(self, db_manager=None, schema: str = "features", *, min_history_periods: int = 4):
        self.min_history_periods = min_history_periods
        self.create_sql = fund_holdings_sql(min_history_periods=min_history_periods)
        super().__init__(db_manager, schema)

    def get_create_sql(self) -> str:
        return bounded_stock_source_sql(cumulative_position_delta_sql(self.create_sql))

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_fund_holdings_quarterly_ts_code "
            "ON features.mv_fund_holdings_quarterly (ts_code, query_start_date, query_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_fund_holdings_quarterly_end_date "
            "ON features.mv_fund_holdings_quarterly (end_date)",
        ]
