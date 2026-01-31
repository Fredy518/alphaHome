"""
recipes.mv.industry - 行业级别特征物化视图配方

业务域: industry
模块说明:
- 行业轮动、行业分散度等行业级别聚合特征
- 与 market 的区别：industry 是按行业分组聚合，market 是全市场聚合

包含特征:
- industry_features_daily: 申万二级行业宽度/分散度/收益分布
- industry_toplist_signal_daily: 申万一级/二级行业龙虎榜聚合信号
- dc_index_features_daily: 打板指数、连板指数、高度板特征
"""

from .industry_features_daily import IndustryFeaturesDailyMV
from .industry_toplist_signal_daily import IndustryToplistSignalDailyMV
from .dc_index_features_daily import DCIndexFeaturesDailyMV

__all__ = [
    "IndustryFeaturesDailyMV",
    "IndustryToplistSignalDailyMV",
    "DCIndexFeaturesDailyMV",
]
