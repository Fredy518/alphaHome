"""
AH 溢价特征（日频）

AH 溢价指数与个股 AH 溢价分位，用于跨市场估值比较与套利。
"""

from alphahome.features.storage.base_view import BaseFeatureView
from alphahome.features.registry import feature_register
from alphahome.features.recipes.mv.pit_asof_rank_sql import ah_premium_sql


@feature_register
class AHPremiumDailyMV(BaseFeatureView):
    """AH 溢价特征"""

    name = "ah_premium_daily"
    description = "AH 溢价指数、个股 AH 溢价及历史分位"
    source_tables = ["rawdata.stock_ahcomparison"]
    refresh_strategy = "full"

    create_sql = ah_premium_sql(history_interval='1 year', min_observations=60)

    def __init__(self, db_manager=None, schema: str = "features", *, history_interval: str = '1 year', min_observations: int = 60):
        self.history_interval = history_interval
        self.min_observations = min_observations
        self.create_sql = ah_premium_sql(history_interval=history_interval, min_observations=min_observations)
        super().__init__(db_manager, schema)

    def get_create_sql(self) -> str:
        return self.create_sql

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_mv_ah_premium_daily_trade_date "
            "ON features.mv_ah_premium_daily (trade_date)",
            "CREATE INDEX IF NOT EXISTS idx_mv_ah_premium_daily_ts_code "
            "ON features.mv_ah_premium_daily (ts_code, trade_date)",
        ]
