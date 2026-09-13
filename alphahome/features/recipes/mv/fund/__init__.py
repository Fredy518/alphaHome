"""
recipes.mv.fund - 基金特征物化视图
"""

from .fund_holdings_quarterly import FundHoldingsQuarterlyMV
from .etf_product_facts_current import ETFProductFactsCurrentMV

__all__ = [
    "FundHoldingsQuarterlyMV",
    "ETFProductFactsCurrentMV",
]
