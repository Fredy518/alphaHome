"""
recipes.mv.stock - 股票相关物化视图配方

业务域: stock
"""

from .stock_balance_quarterly import StockBalanceQuarterlyMV
from .stock_daily_enriched import StockDailyEnrichedMV
from .stock_fina_indicator import StockFinaIndicatorMV
from .stock_income_quarterly import StockIncomeQuarterlyMV
from .stock_industry_monthly_snapshot import StockIndustryMonthlySnapshotMV
# 新增三星级特征
from .stock_cashflow_quarterly import StockCashflowQuarterlyMV
from .stock_shareholder_concentration import StockShareholderConcentrationMV
from .stock_toplist_event_daily import StockToplistEventDailyMV, StockToplistSignalMV
from .stock_analyst_coverage import StockAnalystCoverageMV
from .stock_sharefloat_schedule import StockSharefloatScheduleMV

__all__ = [
    "StockBalanceQuarterlyMV",
    "StockDailyEnrichedMV",
    "StockFinaIndicatorMV",
    "StockIncomeQuarterlyMV",
    "StockIndustryMonthlySnapshotMV",
    # 新增三星级特征
    "StockCashflowQuarterlyMV",
    "StockShareholderConcentrationMV",
    "StockToplistEventDailyMV",
    "StockToplistSignalMV",
    "StockAnalystCoverageMV",
    "StockSharefloatScheduleMV",
]
