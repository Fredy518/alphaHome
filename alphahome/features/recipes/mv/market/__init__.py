"""
recipes.mv.market - 市场横截面相关物化视图配方

业务域: market
模块说明:
- 全市场个股截面聚合统计（粒度：日期）
- 与 index 的区别：market 是全市场个股聚合，index 是针对指数本身
- 与 industry 的区别：market 是全市场聚合，industry 是按行业分组聚合

模块命名规范:
- market_ 前缀: 全市场截面聚合统计
- margin_ 前缀: 两融相关特征
- etf_ 前缀: ETF 资金流相关特征
- risk_ 前缀: 风险偏好代理指标
- money_ 前缀: 资金流向特征

已迁移到其他子域的特征:
- index_* → recipes/mv/index/（指数级别特征）
- style_* → recipes/mv/index/（风格指数）
- industry_* → recipes/mv/industry/（行业级别特征）
- dc_index_* → recipes/mv/industry/（打板指数）
- futures_* → recipes/mv/derivatives/（股指期货）
- option_* → recipes/mv/derivatives/（期权情绪）
"""

from .market_stats_daily import MarketStatsDailyMV
from .market_sentiment_daily import MarketSentimentDailyMV
from .market_technical_daily import MarketTechnicalDailyMV
from .market_size_daily import MarketSizeDailyMV
from .margin_turnover_daily import MarginTurnoverDailyMV
from .etf_flow_daily import ETFFlowDailyMV
from .risk_appetite_daily import RiskAppetiteDailyMV
from .money_flow_daily import MoneyFlowDailyMV
from .limit_industry_daily import LimitIndustryDailyMV
from .concept_features_daily import ConceptFeaturesDailyMV
from .ah_premium_daily import AHPremiumDailyMV
from .repurchase_weekly import RepurchaseWeeklyMV
from .holdertrade_weekly import HolderTradeWeeklyMV

# 兼容别名（后续版本将移除）
MarketStatsMV = MarketStatsDailyMV
MarketSizeDaily = MarketSizeDailyMV
RiskAppetiteDaily = RiskAppetiteDailyMV

# 兼容导入：从新位置 re-export（后续版本将移除）
from ..index import (
    IndexFeaturesDailyMV,
    IndexTechnicalDailyMV,
    IndexFundamentalDailyMV,
    IndexRSRSDailyMV,
    StyleFeaturesDailyMV,
)
from ..industry import IndustryFeaturesDailyMV, DCIndexFeaturesDailyMV
from ..derivatives import FuturesFeaturesDailyMV, OptionSentimentDailyMV

# 兼容别名
FuturesFeaturesDaily = FuturesFeaturesDailyMV

__all__ = [
    # 本模块核心特征
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
    # 兼容别名（后续版本将移除）
    "MarketStatsMV",
    "MarketSizeDaily",
    "RiskAppetiteDaily",
    # 兼容导入（后续版本将移除，请改用 from ..index import ...）
    "IndexFeaturesDailyMV",
    "IndexTechnicalDailyMV",
    "IndexFundamentalDailyMV",
    "IndexRSRSDailyMV",
    "StyleFeaturesDailyMV",
    "IndustryFeaturesDailyMV",
    "DCIndexFeaturesDailyMV",
    "FuturesFeaturesDailyMV",
    "FuturesFeaturesDaily",
    "OptionSentimentDailyMV",
]
