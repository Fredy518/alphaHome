"""
recipes.mv - 物化视图特征配方

使用 SQL 物化视图实现的特征配方。

目录结构:
    recipes/mv/
    ├── stock/       # 股票相关 MV（个股级别）
    ├── market/      # 市场横截面 MV（全市场聚合）
    ├── index/       # 指数级别 MV（核心指数特征）
    ├── industry/    # 行业级别 MV（行业聚合）
    ├── derivatives/ # 衍生品 MV（期货/期权）
    ├── macro/       # 宏观因子 MV
    └── fund/        # 基金相关 MV

注意:
- 本模块使用 @feature_register 装饰器自动注册到 FeatureRegistry
- discover() 会动态扫描所有子模块，无需手动维护导入列表
- 以下显式导入仅用于兼容期的外部 import（A6 兼容策略）
"""

# =============================================================================
# 按子域组织的导入
# =============================================================================

# --- stock 子域 ---
from .stock.stock_industry_monthly_snapshot import StockIndustryMonthlySnapshotMV
from .stock.stock_fina_indicator import StockFinaIndicatorMV
from .stock.stock_income_quarterly import StockIncomeQuarterlyMV
from .stock.stock_balance_quarterly import StockBalanceQuarterlyMV
from .stock.stock_daily_enriched import StockDailyEnrichedMV
from .stock.stock_cashflow_quarterly import StockCashflowQuarterlyMV
from .stock.stock_shareholder_concentration import StockShareholderConcentrationMV
from .stock.stock_toplist_event_daily import StockToplistEventDailyMV, StockToplistSignalMV
from .stock.stock_analyst_coverage import StockAnalystCoverageMV
from .stock.stock_sharefloat_schedule import StockSharefloatScheduleMV

# --- market 子域 ---
from .market.market_stats_daily import MarketStatsDailyMV
from .market.market_sentiment_daily import MarketSentimentDailyMV
from .market.market_technical_daily import MarketTechnicalDailyMV
from .market.market_size_daily import MarketSizeDailyMV
from .market.margin_turnover_daily import MarginTurnoverDailyMV
from .market.etf_flow_daily import ETFFlowDailyMV
from .market.risk_appetite_daily import RiskAppetiteDailyMV
from .market.money_flow_daily import MoneyFlowDailyMV
from .market.limit_industry_daily import LimitIndustryDailyMV
from .market.concept_features_daily import ConceptFeaturesDailyMV
from .market.ah_premium_daily import AHPremiumDailyMV
from .market.repurchase_weekly import RepurchaseWeeklyMV
from .market.holdertrade_weekly import HolderTradeWeeklyMV

# --- index 子域 ---
from .index.index_features_daily import IndexFeaturesDailyMV
from .index.index_technical_daily import IndexTechnicalDailyMV
from .index.index_fundamental_daily import IndexFundamentalDailyMV
from .index.index_rsrs_daily import IndexRSRSDailyMV
from .index.style_features_daily import StyleFeaturesDailyMV

# --- industry 子域 ---
from .industry.industry_features_daily import IndustryFeaturesDailyMV
from .industry.industry_toplist_signal_daily import IndustryToplistSignalDailyMV
from .industry.dc_index_features_daily import DCIndexFeaturesDailyMV

# --- derivatives 子域 ---
from .derivatives.futures_features_daily import FuturesFeaturesDailyMV
from .derivatives.option_sentiment_daily import OptionSentimentDailyMV

# --- macro 子域 ---
from .macro.macro_rate_daily import MacroRateDailyMV
from .macro.macro_liquidity_monthly import MacroLiquidityMonthlyMV

# --- fund 子域 ---
from .fund.fund_holdings_quarterly import FundHoldingsQuarterlyMV

# =============================================================================
# 兼容别名（后续版本将逐步移除）
# =============================================================================
MarketStatsMV = MarketStatsDailyMV
StockSwIndustryMV = StockIndustryMonthlySnapshotMV
FuturesFeaturesDaily = FuturesFeaturesDailyMV
RiskAppetiteDaily = RiskAppetiteDailyMV
MarketSizeDaily = MarketSizeDailyMV

__all__ = [
    # === stock 子域 ===
    "StockIndustryMonthlySnapshotMV",
    "StockFinaIndicatorMV",
    "StockIncomeQuarterlyMV",
    "StockBalanceQuarterlyMV",
    "StockDailyEnrichedMV",
    "StockCashflowQuarterlyMV",
    "StockShareholderConcentrationMV",
    "StockToplistEventDailyMV",
    "StockToplistSignalMV",
    "StockAnalystCoverageMV",
    "StockSharefloatScheduleMV",
    # === market 子域 ===
    "MarketStatsDailyMV",
    "MarketSentimentDailyMV",
    "MarketTechnicalDailyMV",
    "MarketSizeDailyMV",
    "MarginTurnoverDailyMV",
    "ETFFlowDailyMV",
    "RiskAppetiteDailyMV",
    "MoneyFlowDailyMV",
    "LimitIndustryDailyMV",
    "ConceptFeaturesDailyMV",
    "AHPremiumDailyMV",
    "RepurchaseWeeklyMV",
    "HolderTradeWeeklyMV",
    # === index 子域 ===
    "IndexFeaturesDailyMV",
    "IndexTechnicalDailyMV",
    "IndexFundamentalDailyMV",
    "IndexRSRSDailyMV",
    "StyleFeaturesDailyMV",
    # === industry 子域 ===
    "IndustryFeaturesDailyMV",
    "IndustryToplistSignalDailyMV",
    "DCIndexFeaturesDailyMV",
    # === derivatives 子域 ===
    "FuturesFeaturesDailyMV",
    "OptionSentimentDailyMV",
    # === macro 子域 ===
    "MacroRateDailyMV",
    "MacroLiquidityMonthlyMV",
    # === fund 子域 ===
    "FundHoldingsQuarterlyMV",
    # === 兼容别名（后续版本将移除）===
    "MarketStatsMV",
    "StockSwIndustryMV",
    "FuturesFeaturesDaily",
    "RiskAppetiteDaily",
    "MarketSizeDaily",
]

