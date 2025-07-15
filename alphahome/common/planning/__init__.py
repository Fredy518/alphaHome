# alphahome/common/planning/__init__.py

"""
批处理规划模块

提供声明式、可组合的批处理规划功能，支持：
1. 基础批处理功能 (BatchPlanner)
2. 智能时间批处理 (SmartTimePartition)
3. 多维度分批处理 (StatusPartition, MarketPartition, CompositePartition)
4. 扩展映射策略 (ExtendedMap)
5. 性能监控和统计 (ExtendedBatchPlanner)
"""

# 原有核心功能 - 保持完全兼容
from .batch_planner import (
    BatchPlanner,
    Source,
    Partition,
    Map
)

# 扩展功能 - 新增智能批处理能力
from .extended_batch_planner import (
    # 扩展的核心类
    ExtendedBatchPlanner,

    # 智能分区策略
    SmartTimePartition,
    StatusPartition,
    MarketPartition,
    CompositePartition,

    # 扩展映射策略
    ExtendedMap,

    # 专用数据源
    TimeRangeSource,
    StockListSource,

    # 便利函数
    create_smart_time_planner,
    create_stock_status_planner
)

# 版本信息
__version__ = "2.0.0"

# 兼容性说明
__compatibility__ = {
    "batch_planner": "1.0.0 - 完全兼容",
    "extended_batch_planner": "2.0.0 - 新增功能"
}

# 使用建议
__usage_guide__ = """
使用建议：

1. 现有任务继续使用原有API：
   from alphahome.common.planning import BatchPlanner, Source, Partition, Map

2. 新任务或需要智能优化的任务使用扩展功能：
   from alphahome.common.planning import ExtendedBatchPlanner, SmartTimePartition

3. 便利函数快速创建常用配置：
   from alphahome.common.planning import create_smart_time_planner
"""