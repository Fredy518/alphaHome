# BatchPlanner 迁移指南

## 概述

本指南说明如何从原有的 BatchPlanner 迁移到扩展的 BatchPlanner，以及如何处理原有文件和新功能的共存。

## 文件结构和兼容性策略

### 当前文件结构
```
alphahome/common/planning/
├── batch_planner.py              # 原文件 - 保持不变
├── extended_batch_planner.py     # 新扩展功能
└── __init__.py                   # 统一导入接口
```

### 兼容性策略

#### ✅ 完全向后兼容
- **原 batch_planner.py 保持不变**：所有现有功能继续正常工作
- **现有任务无需修改**：导入语句和API调用保持一致
- **渐进式升级**：可以选择性地使用新功能

#### ✅ 统一导入接口
```python
# 原有方式仍然有效
from alphahome.common.planning import BatchPlanner, Source, Partition, Map

# 新功能通过同一接口导入
from alphahome.common.planning import ExtendedBatchPlanner, SmartTimePartition
```

## 迁移策略

### 阶段1：保持现状（推荐）
**适用场景**：现有任务运行稳定，暂不需要优化

```python
# 现有代码无需任何修改
from alphahome.common.planning.batch_planner import BatchPlanner, Source, Partition, Map

class ExistingTask:
    async def get_batch_list(self, **kwargs):
        planner = BatchPlanner(
            source=Source.from_list(["L", "D", "P"]),
            partition_strategy=Partition.by_size(1),
            map_strategy=Map.to_dict("list_status")
        )
        return await planner.generate()
```

### 阶段2：选择性升级（建议）
**适用场景**：需要性能优化或新功能的任务

#### 2.1 时间序列任务升级到智能批次拆分
```python
# 原有实现
from alphahome.common.planning.batch_planner import BatchPlanner
from alphahome.fetchers.sources.tushare.batch_utils import generate_trade_day_batches

class OriginalTimeSeriesTask:
    async def get_batch_list(self, start_date, end_date, **kwargs):
        return await generate_trade_day_batches(
            start_date=start_date,
            end_date=end_date,
            batch_size=30,
            logger=self.logger
        )

# 升级后实现
from alphahome.common.planning import create_smart_time_planner

class UpgradedTimeSeriesTask:
    async def get_batch_list(self, start_date, end_date, **kwargs):
        planner = create_smart_time_planner(
            start_date=start_date,
            end_date=end_date,
            enable_stats=True
        )
        batches = await planner.generate()
        
        # 记录优化效果
        stats = planner.get_stats()
        if "smart_time_optimization" in stats:
            opt = stats["smart_time_optimization"]
            self.logger.info(f"智能批次优化：减少 {opt['reduction_rate']:.1f}% 批次数量")
        
        return batches
```

#### 2.2 多维度分批任务升级
```python
# 原有实现
from alphahome.common.planning.batch_planner import BatchPlanner, Source, Partition, Map

class OriginalStockTask:
    async def get_batch_list(self, **kwargs):
        planner = BatchPlanner(
            source=Source.from_list(["L", "D", "P"]),
            partition_strategy=Partition.by_size(1),
            map_strategy=Map.to_dict("list_status")
        )
        return await planner.generate()

# 升级后实现
from alphahome.common.planning import ExtendedBatchPlanner, StatusPartition, ExtendedMap

class UpgradedStockTask:
    async def get_batch_list(self, **kwargs):
        # 获取实际股票数据而不是状态列表
        stocks = await self.get_stock_list()
        
        planner = ExtendedBatchPlanner(
            source=Source.from_list(stocks),
            partition_strategy=StatusPartition.create("list_status"),
            map_strategy=ExtendedMap.to_grouped_dict("list_status", "stocks"),
            enable_stats=True
        )
        
        batches = await planner.generate()
        stats = planner.get_stats()
        
        self.logger.info(f"多维度分批完成：生成 {len(batches)} 个批次")
        self.logger.info(f"生成耗时：{stats.get('generation_time', 0):.3f}s")
        
        return batches
```

### 阶段3：全面升级（可选）
**适用场景**：追求最佳性能和最新功能

#### 3.1 组合维度分批
```python
from alphahome.common.planning import (
    ExtendedBatchPlanner, CompositePartition, 
    MarketPartition, StatusPartition, ExtendedMap
)

class AdvancedStockTask:
    async def get_batch_list(self, **kwargs):
        stocks = await self.get_stock_list()
        
        # 组合分区：先按交易所，再按状态
        composite_partition = CompositePartition.create([
            MarketPartition.create("exchange"),
            StatusPartition.create("list_status")
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
        
        return await planner.generate()
```

## 具体任务迁移示例

### tushare_stock_daily 任务迁移

#### 迁移前
```python
from alphahome.fetchers.sources.tushare.batch_utils import generate_trade_day_batches

class TushareStockDailyTask:
    async def get_batch_list(self, **kwargs):
        start_date = kwargs.get("start_date", self.default_start_date)
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))
        
        return await generate_trade_day_batches(
            start_date=start_date,
            end_date=end_date,
            batch_size=30,
            logger=self.logger
        )
```

#### 迁移后
```python
from alphahome.common.planning import create_smart_time_planner

class TushareStockDailyTask:
    async def get_batch_list(self, **kwargs):
        start_date = kwargs.get("start_date", self.default_start_date)
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))
        ts_code = kwargs.get("ts_code")
        exchange = kwargs.get("exchange", "SSE")
        
        # 使用智能时间批处理
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
            if ts_code:
                batch["ts_code"] = ts_code
            if exchange:
                batch["exchange"] = exchange
            batches.append(batch)
        
        # 记录优化效果
        stats = planner.get_stats()
        if "smart_time_optimization" in stats:
            opt = stats["smart_time_optimization"]
            self.logger.info(f"智能批次优化：减少 {opt['reduction_rate']:.1f}% 批次数量")
        
        return batches
```

### tushare_stock_basic 任务迁移

#### 迁移前
```python
from alphahome.common.planning.batch_planner import BatchPlanner, Source, Partition, Map

class TushareStockBasicTask:
    async def get_batch_list(self, **kwargs):
        planner = BatchPlanner(
            source=Source.from_list(["L", "D", "P"]),
            partition_strategy=Partition.by_size(1),
            map_strategy=Map.to_dict("list_status")
        )
        return await planner.generate()
```

#### 迁移后
```python
from alphahome.common.planning import create_stock_status_planner

class TushareStockBasicTask:
    async def get_batch_list(self, **kwargs):
        planner = create_stock_status_planner(
            db_manager=self.db_manager,
            api_instance=self.api_instance,
            enable_stats=True
        )
        
        batches = await planner.generate()
        stats = planner.get_stats()
        
        self.logger.info(f"状态分批完成：生成 {len(batches)} 个批次")
        self.logger.info(f"生成耗时：{stats.get('generation_time', 0):.3f}s")
        
        return batches
```

## 迁移检查清单

### 迁移前检查
- [ ] 确认任务类型（时间序列 vs 非时间序列）
- [ ] 评估当前性能瓶颈
- [ ] 确定是否需要新功能
- [ ] 备份现有实现

### 迁移过程
- [ ] 选择合适的迁移策略
- [ ] 实现新的 get_batch_list 方法
- [ ] 保留原有参数处理逻辑
- [ ] 添加性能统计和日志

### 迁移后验证
- [ ] 功能正确性测试
- [ ] 性能提升验证
- [ ] 边界条件测试
- [ ] 生产环境验证

## 风险控制

### 低风险迁移策略
1. **保留原有实现**：重命名为 `get_batch_list_legacy`
2. **并行运行**：同时运行新旧实现进行对比
3. **渐进式切换**：先在测试环境验证，再逐步推广

### 回滚方案
```python
class SafeMigrationTask:
    def __init__(self):
        self.use_legacy = False  # 控制开关
    
    async def get_batch_list(self, **kwargs):
        if self.use_legacy:
            return await self.get_batch_list_legacy(**kwargs)
        else:
            return await self.get_batch_list_extended(**kwargs)
    
    async def get_batch_list_legacy(self, **kwargs):
        # 原有实现
        pass
    
    async def get_batch_list_extended(self, **kwargs):
        # 新实现
        pass
```

## 总结

### 推荐的处理方式

1. **保持原 batch_planner.py 不变**
2. **通过统一接口提供新功能**
3. **采用渐进式迁移策略**
4. **建立完善的测试和回滚机制**

### 迁移优先级建议

1. **高优先级**：时间序列数据任务（如 tushare_stock_daily）
2. **中优先级**：需要多维度分批的任务（如 tushare_stock_basic）
3. **低优先级**：运行稳定的简单任务

### 长期规划

- **短期**：新功能与原功能并存
- **中期**：逐步迁移适合的任务
- **长期**：根据使用情况考虑是否统一到扩展版本

这种处理方式确保了系统的稳定性，同时为团队提供了灵活的升级路径。
