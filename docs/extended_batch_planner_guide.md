# 扩展 BatchPlanner 使用指南

## 概述

扩展的 BatchPlanner 基于现有 BatchPlanner 架构，集成了智能时间批处理策略和多维度分批能力，提供更广泛的批处理场景支持，同时保持完全的向后兼容性。

## 核心功能

### 1. 智能时间批处理策略

集成已验证的四级智能拆分策略：
- **≤31天**: 单批次策略（边界条件优化）
- **32天-3个月**: 月度拆分（精细粒度）
- **4个月-2年**: 季度拆分（平衡效率和精度）
- **2-10年**: 半年度拆分（提高长期更新效率）
- **>10年**: 年度拆分（超长期数据优化）

### 2. 多维度分批能力

- **按股票状态分批**: 支持按 `list_status`（L-上市、D-退市、P-暂停）分区
- **按市场分批**: 支持按 `market`（主板、中小板、创业板、科创板、北交所）分区
- **按交易所分批**: 支持按 `exchange`（SSE-上交所、SZSE-深交所、BSE-北交所）分区
- **组合维度分批**: 支持多维度组合分批，如"按交易所+上市状态"同时分区

### 3. 性能监控和统计

- 批次生成时间统计
- 智能时间优化效果分析
- 批次数量对比和减少比例计算

## 新增分区策略类

### SmartTimePartition - 智能时间分区

```python
from alphahome.common.planning.extended_batch_planner import SmartTimePartition

# 创建智能时间分区策略
partition_strategy = SmartTimePartition.create(date_format="%Y%m%d")

# 使用示例
date_range = ["20200101", "20241231"]  # 5年数据
partitions = partition_strategy(date_range)
# 结果：10个半年度批次，相比1827个日批次减少99.5%
```

### StatusPartition - 按状态分区

```python
from alphahome.common.planning.extended_batch_planner import StatusPartition

# 创建按状态分区策略
partition_strategy = StatusPartition.create(field="list_status")

# 使用示例
stocks = [
    {"ts_code": "000001.SZ", "list_status": "L"},
    {"ts_code": "000002.SZ", "list_status": "D"},
    # ...
]
partitions = partition_strategy(stocks)
# 结果：按L、D、P状态分为3个批次
```

### MarketPartition - 按市场分区

```python
from alphahome.common.planning.extended_batch_planner import MarketPartition

# 创建按市场分区策略
partition_strategy = MarketPartition.create(field="market")

# 使用示例
stocks = [
    {"ts_code": "000001.SZ", "market": "主板"},
    {"ts_code": "300750.SZ", "market": "创业板"},
    # ...
]
partitions = partition_strategy(stocks)
# 结果：按市场类型分为多个批次
```

### CompositePartition - 组合分区

```python
from alphahome.common.planning.extended_batch_planner import CompositePartition, MarketPartition, StatusPartition

# 创建组合分区策略
composite_partition = CompositePartition.create([
    MarketPartition.create("exchange"),  # 先按交易所分区
    StatusPartition.create("list_status")  # 再按状态分区
])

# 使用示例
stocks = [
    {"ts_code": "000001.SZ", "exchange": "SZSE", "list_status": "L"},
    {"ts_code": "600519.SH", "exchange": "SSE", "list_status": "L"},
    # ...
]
partitions = composite_partition(stocks)
# 结果：按交易所+状态组合分区
```

## 扩展映射策略

### ExtendedMap.to_smart_time_range - 智能时间范围映射

```python
from alphahome.common.planning.extended_batch_planner import ExtendedMap

# 创建智能时间范围映射
map_strategy = ExtendedMap.to_smart_time_range("start_date", "end_date")

# 使用示例
batch = ["20240101", "20240331"]
result = map_strategy(batch)
# 结果：{"start_date": "20240101", "end_date": "20240331"}
```

### ExtendedMap.to_grouped_dict - 分组字典映射

```python
# 创建分组字典映射
map_strategy = ExtendedMap.to_grouped_dict("list_status", "stocks")

# 使用示例
batch = [
    {"ts_code": "000001.SZ", "list_status": "L"},
    {"ts_code": "000002.SZ", "list_status": "L"}
]
result = map_strategy(batch)
# 结果：{"list_status": "L", "stocks": [...]}
```

## 便利函数

### create_smart_time_planner - 智能时间批处理规划器

```python
from alphahome.common.planning.extended_batch_planner import create_smart_time_planner

# 创建智能时间批处理规划器
planner = create_smart_time_planner(
    start_date="20200101",
    end_date="20241231",
    start_field="start_date",
    end_field="end_date",
    enable_stats=True
)

# 生成批次
batches = await planner.generate()
stats = planner.get_stats()

print(f"生成批次数: {len(batches)}")
print(f"优化效果: 减少 {stats['smart_time_optimization']['reduction_rate']:.1f}% 批次")
```

### create_stock_status_planner - 股票状态分批规划器

```python
from alphahome.common.planning.extended_batch_planner import create_stock_status_planner

# 创建按股票状态分批的规划器
planner = create_stock_status_planner(
    db_manager=db_manager,  # 可选
    api_instance=api_instance,  # 可选
    status_field="list_status",
    enable_stats=True
)

# 生成批次
batches = await planner.generate()
```

## 实际使用示例

### 示例1：智能时间批处理任务

```python
from alphahome.common.planning.extended_batch_planner import (
    ExtendedBatchPlanner, SmartTimePartition, ExtendedMap, TimeRangeSource
)

class SmartTimeTask:
    async def get_batch_list(self, start_date: str, end_date: str, **kwargs):
        # 使用扩展的 BatchPlanner 进行智能时间分批
        planner = ExtendedBatchPlanner(
            source=TimeRangeSource.create(start_date, end_date),
            partition_strategy=SmartTimePartition.create(),
            map_strategy=ExtendedMap.to_smart_time_range(),
            enable_stats=True
        )
        
        batches = await planner.generate()
        stats = planner.get_stats()
        
        self.logger.info(f"智能批次生成完成 - 生成 {len(batches)} 个批次")
        if "smart_time_optimization" in stats:
            opt = stats["smart_time_optimization"]
            self.logger.info(f"相比传统方案减少 {opt['reduction_rate']:.1f}% 批次数量")
        
        return batches
```

### 示例2：多维度股票分批任务

```python
from alphahome.common.planning.extended_batch_planner import (
    ExtendedBatchPlanner, CompositePartition, MarketPartition, 
    StatusPartition, ExtendedMap, StockListSource
)

class MultiDimensionStockTask:
    async def get_batch_list(self, **kwargs):
        # 组合分区：先按交易所，再按状态
        composite_partition = CompositePartition.create([
            MarketPartition.create("exchange"),
            StatusPartition.create("list_status")
        ])
        
        planner = ExtendedBatchPlanner(
            source=StockListSource.create(self.db_manager, self.api_instance),
            partition_strategy=composite_partition,
            map_strategy=ExtendedMap.with_custom_func(
                lambda batch: {
                    "exchange": batch[0]["exchange"] if batch else None,
                    "list_status": batch[0]["list_status"] if batch else None,
                    "stocks": [stock["ts_code"] for stock in batch],
                    "count": len(batch)
                }
            ),
            enable_stats=True
        )
        
        batches = await planner.generate()
        stats = planner.get_stats()
        
        self.logger.info(f"多维度分批完成 - 生成 {len(batches)} 个批次")
        self.logger.info(f"生成耗时: {stats.get('generation_time', 0):.3f}s")
        
        return batches
```

## 性能优化效果

### 智能时间批处理优化

| 时间跨度 | 传统批次数 | 智能批次数 | 减少比例 | 策略类型 |
|----------|------------|------------|----------|----------|
| 1个月 | 31 | 1 | 96.8% | 单批次 |
| 1年 | 366 | 4 | 98.9% | 季度 |
| 5年 | 1,827 | 10 | 99.5% | 半年度 |
| 15年 | 5,479 | 15 | 99.7% | 年度 |

### 多维度分批优化

- **精细化控制**: 支持按多个维度同时分批，提高数据处理精度
- **并行处理**: 不同维度的批次可以并行执行，提高整体效率
- **错误隔离**: 单个维度的错误不会影响其他维度的处理
- **资源优化**: 减少单批次数据量，降低内存和网络消耗

## 向后兼容性

扩展的 BatchPlanner 完全兼容原有 API：

```python
# 原有方式仍然有效
from alphahome.common.planning.batch_planner import BatchPlanner, Source, Partition, Map

planner = BatchPlanner(
    source=Source.from_list(["000001.SZ", "600519.SH"]),
    partition_strategy=Partition.by_size(1),
    map_strategy=Map.to_dict("ts_code")
)

# 扩展方式提供额外功能
from alphahome.common.planning.extended_batch_planner import ExtendedBatchPlanner

extended_planner = ExtendedBatchPlanner(
    source=Source.from_list(["000001.SZ", "600519.SH"]),
    partition_strategy=Partition.by_size(1),
    map_strategy=Map.to_dict("ts_code"),
    enable_stats=True  # 额外的统计功能
)
```

## 最佳实践

1. **选择合适的分区策略**: 根据数据特性选择最适合的分区方式
2. **启用性能统计**: 使用 `enable_stats=True` 监控批次生成性能
3. **组合使用**: 对于复杂场景，可以组合多种分区策略
4. **测试验证**: 在生产环境使用前，充分测试批次生成的正确性
5. **监控优化效果**: 定期检查批次优化带来的性能提升

## 故障排除

### 常见问题

1. **批次数量为0**: 检查输入数据格式和分区策略配置
2. **映射错误**: 确保映射策略与分区结果格式匹配
3. **性能问题**: 对于大数据量，考虑使用组合分区减少单批次大小

### 调试技巧

```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 检查统计信息
stats = planner.get_stats()
print(f"批次生成统计: {stats}")

# 验证分区结果
partitions = partition_strategy(test_data)
print(f"分区结果: {partitions}")
```
