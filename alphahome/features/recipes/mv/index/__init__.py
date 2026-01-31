"""
recipes.mv.index - 指数级别特征物化视图配方

业务域: index
模块说明:
- 核心宽基指数（HS300/ZZ500/ZZ1000/SZ50/CYB等）的综合特征
- 与 market 的区别：index 是针对指数本身的特征，market 是全市场个股截面聚合

包含特征:
- index_features_daily: 估值分位 + 波动率 + ERP
- index_technical_daily: 布林带突破 + MA偏离度
- index_fundamental_daily: PIT权重加权 PE/PB/股息率
- index_rsrs_daily: RSRS择时指标组件
- style_features_daily: 风格指数收益与相对强弱
"""

from .index_features_daily import IndexFeaturesDailyMV
from .index_technical_daily import IndexTechnicalDailyMV
from .index_fundamental_daily import IndexFundamentalDailyMV
from .index_rsrs_daily import IndexRSRSDailyMV
from .style_features_daily import StyleFeaturesDailyMV

__all__ = [
    "IndexFeaturesDailyMV",
    "IndexTechnicalDailyMV",
    "IndexFundamentalDailyMV",
    "IndexRSRSDailyMV",
    "StyleFeaturesDailyMV",
]
