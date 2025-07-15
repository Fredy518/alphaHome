# 新任务开发指南 - 批处理最佳实践

## 概述

本指南为新任务开发提供批处理功能的最佳实践，帮助开发者选择合适的批处理方案并正确实现。

## 🎯 批处理方案选择

### 方案对比

| 特性 | SmartBatchMixin | ExtendedBatchPlanner | 推荐场景 |
|------|-----------------|---------------------|----------|
| **适用场景** | 纯时间序列数据 | 复杂多维度分批 | - |
| **实现复杂度** | 简单 | 中等 | 快速开发 vs 功能丰富 |
| **功能丰富度** | 基础 | 丰富 | 基础需求 vs 复杂需求 |
| **扩展性** | 有限 | 强 | 固定需求 vs 可能扩展 |
| **性能监控** | 基础 | 详细 | 简单监控 vs 深度分析 |

### 选择决策树

```
新任务批处理需求
├── 纯时间序列数据？
│   ├── 是 → 需要复杂统计？
│   │   ├── 否 → SmartBatchMixin ✅
│   │   └── 是 → ExtendedBatchPlanner ✅
│   └── 否 → 需要多维度分批？
│       ├── 是 → ExtendedBatchPlanner ✅
│       └── 否 → 原始 BatchPlanner
```

## 📋 实现模板

### 模板1：时间序列任务 (推荐 ExtendedBatchPlanner)

```python
from alphahome.common.planning import create_smart_time_planner
from alphahome.fetchers.sources.tushare.tushare_task import TushareTask

class NewTimeSeriesTask(TushareTask):
    """新时间序列数据任务 - 推荐实现"""
    
    # 核心属性
    domain = "your_domain"
    name = "new_time_series_task"
    description = "新时间序列任务描述"
    table_name = "your_table"
    primary_keys = ["key1", "key2", "date"]
    date_column = "trade_date"
    default_start_date = "20200101"
    
    # API配置
    api_name = "your_api"
    fields = ["field1", "field2", "trade_date"]
    
    async def get_batch_list(self, **kwargs):
        """使用 ExtendedBatchPlanner 实现智能时间分批"""
        start_date = kwargs.get("start_date", self.default_start_date)
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))
        
        # 创建智能时间批处理规划器
        planner = create_smart_time_planner(
            start_date=start_date,
            end_date=end_date,
            enable_stats=True
        )
        
        time_batches = await planner.generate()
        
        # 转换为任务特定格式
        batches = []
        for time_batch in time_batches:
            batch = {
                "start_date": time_batch["start_date"],
                "end_date": time_batch["end_date"]
            }
            # 添加任务特有参数
            if kwargs.get("ts_code"):
                batch["ts_code"] = kwargs["ts_code"]
            batches.append(batch)
        
        # 记录优化效果
        stats = planner.get_stats()
        if "smart_time_optimization" in stats:
            opt = stats["smart_time_optimization"]
            self.logger.info(f"智能批次优化：减少 {opt['reduction_rate']:.1f}% 批次数量")
        
        return batches
```

### 模板2：多维度分批任务

```python
from alphahome.common.planning import (
    ExtendedBatchPlanner, CompositePartition, 
    StatusPartition, MarketPartition, ExtendedMap
)

class NewMultiDimensionTask(TushareTask):
    """新多维度分批任务"""
    
    async def get_batch_list(self, **kwargs):
        """多维度分批实现"""
        # 获取股票列表
        stocks = await self.get_stock_list()
        
        # 创建组合分区策略
        composite_partition = CompositePartition.create([
            MarketPartition.create("exchange"),  # 按交易所分区
            StatusPartition.create("list_status")  # 按状态分区
        ])
        
        planner = ExtendedBatchPlanner(
            source=Source.from_list(stocks),
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
        
        self.logger.info(f"多维度分批完成：生成 {len(batches)} 个批次")
        self.logger.info(f"生成耗时：{stats.get('generation_time', 0):.3f}s")
        
        return batches
```

### 模板3：兼容现有任务 (SmartBatchMixin)

```python
from alphahome.fetchers.base.smart_batch_mixin import SmartBatchMixin

class ExistingStyleTask(TushareTask, SmartBatchMixin):
    """兼容现有任务风格的实现"""
    
    async def get_batch_list(self, **kwargs):
        """使用 SmartBatchMixin 的传统方式"""
        start_date = kwargs.get("start_date", self.default_start_date)
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))
        
        # 使用 SmartBatchMixin 的智能批次拆分
        time_batches = self.generate_smart_time_batches(start_date, end_date)
        
        # 转换为任务特定格式
        batches = []
        for time_batch in time_batches:
            batch = dict(time_batch)
            # 添加任务特有参数
            if kwargs.get("ts_code"):
                batch["ts_code"] = kwargs["ts_code"]
            batches.append(batch)
        
        # 记录优化效果
        stats = self.get_batch_optimization_stats(start_date, end_date)
        self.logger.info(
            f"智能批次生成完成 - 采用{stats.get('strategy', '未知')}策略，"
            f"生成 {len(batches)} 个批次，减少 {stats.get('reduction_rate', 0):.1f}% 批次数量"
        )
        
        return batches
```

## 🚀 开发最佳实践

### 1. 任务设计原则

#### 时间序列任务
- **优先使用 ExtendedBatchPlanner**: 获得更好的性能监控和扩展性
- **合理设置默认起始日期**: 根据数据可用性设置 `default_start_date`
- **支持增量更新**: 实现智能的增量更新逻辑
- **添加性能统计**: 启用 `enable_stats=True` 监控优化效果

#### 多维度分批任务
- **明确分批维度**: 清楚定义需要按哪些维度分批
- **选择合适的分区策略**: 使用预定义的分区策略或自定义
- **优化批次大小**: 平衡API调用效率和系统资源消耗
- **实现错误隔离**: 确保单个批次失败不影响其他批次

### 2. 性能优化建议

#### 批次数量优化
```python
# 好的做法：使用智能时间分区
planner = create_smart_time_planner(
    start_date="20200101",
    end_date="20241231",
    enable_stats=True
)

# 避免：固定大小分批
# 这会产生过多的小批次
batches = [{"start_date": date, "end_date": date} for date in date_list]
```

#### 内存使用优化
```python
# 好的做法：流式处理大数据集
async def process_large_dataset(self):
    async for batch in self.get_batch_iterator():
        await self.process_batch(batch)

# 避免：一次性加载所有数据
# all_data = await self.load_all_data()  # 可能导致内存溢出
```

### 3. 错误处理模式

#### 批次级错误处理
```python
async def get_batch_list(self, **kwargs):
    try:
        # 批次生成逻辑
        planner = create_smart_time_planner(...)
        return await planner.generate()
    except Exception as e:
        self.logger.error(f"批次生成失败: {e}")
        # 回退到安全的单批次策略
        return [{
            "start_date": kwargs.get("start_date"),
            "end_date": kwargs.get("end_date")
        }]
```

#### 参数验证
```python
async def get_batch_list(self, **kwargs):
    # 参数验证
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    
    if not start_date or not end_date:
        self.logger.error("缺少必要的日期参数")
        return []
    
    # 日期格式验证
    try:
        datetime.strptime(start_date, "%Y%m%d")
        datetime.strptime(end_date, "%Y%m%d")
    except ValueError as e:
        self.logger.error(f"日期格式错误: {e}")
        return []
```

### 4. 测试建议

#### 单元测试模板
```python
import pytest
from your_task import YourTask

class TestYourTask:
    @pytest.fixture
    def task(self):
        return YourTask()
    
    @pytest.mark.asyncio
    async def test_get_batch_list_basic(self, task):
        """测试基础批次生成功能"""
        batches = await task.get_batch_list(
            start_date="20241201",
            end_date="20241231"
        )
        
        assert len(batches) > 0
        assert all("start_date" in batch for batch in batches)
        assert all("end_date" in batch for batch in batches)
    
    @pytest.mark.asyncio
    async def test_get_batch_list_edge_cases(self, task):
        """测试边界情况"""
        # 单日数据
        batches = await task.get_batch_list(
            start_date="20241215",
            end_date="20241215"
        )
        assert len(batches) == 1
        
        # 长期数据
        batches = await task.get_batch_list(
            start_date="20200101",
            end_date="20241231"
        )
        assert len(batches) > 1
```

## 📊 性能监控

### 监控指标
- **批次数量**: 监控批次数量的合理性
- **生成时间**: 批次生成的耗时
- **优化效果**: 相比传统方案的改进程度
- **错误率**: 批次生成的失败率

### 监控实现
```python
async def get_batch_list(self, **kwargs):
    import time
    start_time = time.time()
    
    planner = create_smart_time_planner(
        start_date=start_date,
        end_date=end_date,
        enable_stats=True
    )
    
    batches = await planner.generate()
    generation_time = time.time() - start_time
    
    # 记录监控指标
    stats = planner.get_stats()
    self.logger.info(f"批次生成监控 - "
                    f"数量: {len(batches)}, "
                    f"耗时: {generation_time:.3f}s, "
                    f"优化率: {stats.get('smart_time_optimization', {}).get('reduction_rate', 0):.1f}%")
    
    return batches
```

## 🔧 故障排除

### 常见问题

1. **批次数量为0**
   - 检查输入参数格式
   - 验证日期范围有效性
   - 查看错误日志

2. **性能问题**
   - 检查批次大小是否合理
   - 考虑使用更粗粒度的分批策略
   - 监控内存使用情况

3. **异步调用问题**
   - 确保在异步上下文中调用
   - 正确处理异步异常
   - 避免阻塞事件循环

### 调试技巧
```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 检查批次生成统计
stats = planner.get_stats()
print(f"批次生成统计: {stats}")

# 验证分区结果
partitions = partition_strategy(test_data)
print(f"分区结果: {partitions}")
```

## 📚 参考资源

- [ExtendedBatchPlanner 使用指南](extended_batch_planner_guide.md)
- [BatchPlanner 迁移指南](batch_planner_migration_guide.md)
- [SmartBatchMixin 迁移报告](smart_batch_mixin_migration_report.md)

## 🎯 总结

选择合适的批处理方案对任务性能至关重要：

- **新时间序列任务**: 推荐使用 ExtendedBatchPlanner
- **多维度分批需求**: 必须使用 ExtendedBatchPlanner
- **简单兼容需求**: 可以继续使用 SmartBatchMixin

遵循本指南的最佳实践，可以确保新任务具有良好的性能、可维护性和扩展性。
