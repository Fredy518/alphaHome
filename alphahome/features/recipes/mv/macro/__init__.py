"""
recipes.mv.macro - 宏观特征物化视图
"""

from .macro_liquidity_monthly import MacroLiquidityMonthlyMV
from .macro_rate_daily import MacroRateDailyMV

__all__ = [
    "MacroLiquidityMonthlyMV",
    "MacroRateDailyMV",
]
