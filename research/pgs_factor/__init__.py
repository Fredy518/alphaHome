"""
P/G/S因子计算模块 - 全新架构
============================

轻量级、高性能的A股P/G/S因子计算系统

🏗️ 三层数据流架构：
```
AlphaHome原始数据 → PIT数据库 → 因子存储
        ↑              ↑           ↑
   SourceLoader   PITManager  Production*Calculator
        ↑              ↑           ↑
            DataPipeline (统一协调)
```

🎯 设计原则：
- 简单胜过复杂
- 清晰的数据流和职责分离
- 高性能和可扩展性
- 统一的架构模式

📦 核心组件：
- DataPipeline: 统一数据流协调器
- PITManager: PIT数据转换枢纽
- SourceLoader: 原始数据加载器
"""

# 核心组件导出
# from .core import PITManager, DataPipeline  # TODO: 暂时注释，等待core模块实现
# from .data import SourceLoader  # TODO: 暂时注释，等待data模块实现

__all__ = [
    'DataPipeline',    # 统一入口
    'PITManager',      # PIT数据管理
    'SourceLoader'     # 数据加载
]

__version__ = '2.0.0-clean'
