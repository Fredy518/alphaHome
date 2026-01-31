"""
alphahome.features - 离线特征工程模块

提供离线特征的存储、计算和管理能力。

子模块:
- storage: 特征存储层（物化视图管理）
- recipes: 特征计算配方
  - recipes/mv/stock: 个股级别特征
  - recipes/mv/market: 全市场截面聚合特征
  - recipes/mv/index: 指数级别特征
  - recipes/mv/industry: 行业级别特征
  - recipes/mv/derivatives: 衍生品（期货/期权）特征
  - recipes/mv/macro: 宏观因子
  - recipes/mv/fund: 基金相关特征
- cards: 特征入库卡片（YAML 元数据）
- registry: 特征注册表与装饰器

核心 API:
- FeatureRegistry: 特征注册表，管理所有 Recipe 的发现/获取
- feature_register: 装饰器，用于自动注册 Recipe 类

使用示例:
    from alphahome.features import FeatureRegistry, feature_register
    
    # 发现所有 Recipe
    recipes = FeatureRegistry.discover()
    
    # 获取指定 Recipe
    recipe_cls = FeatureRegistry.get("market_stats_daily")
"""

__version__ = "0.2.0"


# 直接导出核心 API
from .registry import FeatureRegistry, feature_register

__all__ = [
    "FeatureRegistry",
    "feature_register",
]
