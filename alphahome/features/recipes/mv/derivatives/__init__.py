"""
recipes.mv.derivatives - 衍生品特征物化视图配方

业务域: derivatives
模块说明:
- 股指期货、ETF期权等衍生品相关特征
- 用于观测市场对冲情绪、机构行为

包含特征:
- futures_features_daily: 股指期货基差、会员持仓、前20席位
- option_sentiment_daily: ETF期权 Put/Call Ratio
"""

from .futures_features_daily import FuturesFeaturesDailyMV
from .option_sentiment_daily import OptionSentimentDailyMV

# 兼容旧符号名
FuturesFeaturesDaily = FuturesFeaturesDailyMV

__all__ = [
    "FuturesFeaturesDailyMV",
    "FuturesFeaturesDaily",  # 兼容别名
    "OptionSentimentDailyMV",
]
