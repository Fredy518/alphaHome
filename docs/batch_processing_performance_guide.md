# 批处理性能优化指南

## 概述

本指南提供批处理系统的性能监控、分析和优化建议，帮助开发者和运维人员最大化批处理效率。

## 📊 性能监控体系

### 核心性能指标

#### 1. 批次效率指标
```python
# 批次数量优化率
reduction_rate = (original_batches - optimized_batches) / original_batches * 100

# 批次大小分布
batch_sizes = [len(batch.get("items", [])) for batch in batches]
avg_batch_size = sum(batch_sizes) / len(batch_sizes)

# 时间跨度效率
time_span_days = (end_date - start_date).days
batches_per_day = len(batches) / time_span_days
```

#### 2. 系统资源指标
```python
import psutil
import time

def monitor_batch_generation():
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss
    
    # 批次生成过程
    batches = await generate_batches()
    
    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss
    
    return {
        "generation_time": end_time - start_time,
        "memory_usage": end_memory - start_memory,
        "batch_count": len(batches),
        "efficiency": len(batches) / (end_time - start_time)
    }
```

#### 3. API调用效率指标
```python
class APICallMonitor:
    def __init__(self):
        self.call_count = 0
        self.total_time = 0
        self.error_count = 0
    
    async def monitored_api_call(self, api_func, *args, **kwargs):
        start_time = time.time()
        try:
            result = await api_func(*args, **kwargs)
            self.call_count += 1
            return result
        except Exception as e:
            self.error_count += 1
            raise
        finally:
            self.total_time += time.time() - start_time
    
    def get_stats(self):
        return {
            "total_calls": self.call_count,
            "total_time": self.total_time,
            "avg_call_time": self.total_time / max(self.call_count, 1),
            "error_rate": self.error_count / max(self.call_count, 1),
            "calls_per_second": self.call_count / max(self.total_time, 0.001)
        }
```

### 监控实现示例

#### 集成监控的任务实现
```python
from alphahome.common.planning import create_smart_time_planner
import time
import psutil

class MonitoredTask(TushareTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.performance_stats = {}
    
    async def get_batch_list(self, **kwargs):
        """带性能监控的批次生成"""
        monitor_start = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        try:
            # 创建智能时间批处理规划器
            planner = create_smart_time_planner(
                start_date=kwargs.get("start_date"),
                end_date=kwargs.get("end_date"),
                enable_stats=True
            )
            
            batches = await planner.generate()
            stats = planner.get_stats()
            
            # 计算性能指标
            generation_time = time.time() - monitor_start
            memory_used = psutil.Process().memory_info().rss - start_memory
            
            # 记录性能统计
            self.performance_stats = {
                "batch_count": len(batches),
                "generation_time": generation_time,
                "memory_usage_mb": memory_used / 1024 / 1024,
                "batches_per_second": len(batches) / generation_time,
                "optimization_stats": stats.get("smart_time_optimization", {}),
                "timestamp": time.time()
            }
            
            # 性能日志
            self.log_performance_stats()
            
            return batches
            
        except Exception as e:
            self.logger.error(f"批次生成失败: {e}")
            self.performance_stats["error"] = str(e)
            raise
    
    def log_performance_stats(self):
        """记录性能统计日志"""
        stats = self.performance_stats
        opt_stats = stats.get("optimization_stats", {})
        
        self.logger.info(
            f"批次生成性能统计 - "
            f"批次数: {stats['batch_count']}, "
            f"耗时: {stats['generation_time']:.3f}s, "
            f"内存: {stats['memory_usage_mb']:.1f}MB, "
            f"效率: {stats['batches_per_second']:.1f} batches/s, "
            f"优化率: {opt_stats.get('reduction_rate', 0):.1f}%"
        )
```

## 🚀 性能优化策略

### 1. 智能批次大小优化

#### 动态批次大小调整
```python
class AdaptiveBatchPlanner:
    def __init__(self):
        self.performance_history = []
    
    def calculate_optimal_batch_size(self, data_size, historical_performance):
        """根据历史性能计算最优批次大小"""
        if not historical_performance:
            return min(1000, data_size // 10)  # 默认策略
        
        # 分析历史性能数据
        best_performance = max(historical_performance, 
                             key=lambda x: x['throughput'])
        
        optimal_size = best_performance['batch_size']
        
        # 根据数据规模调整
        scale_factor = data_size / best_performance['data_size']
        adjusted_size = int(optimal_size * scale_factor ** 0.5)
        
        return max(100, min(5000, adjusted_size))
    
    async def generate_adaptive_batches(self, data, target_batch_count=None):
        """生成自适应批次"""
        if target_batch_count:
            batch_size = len(data) // target_batch_count
        else:
            batch_size = self.calculate_optimal_batch_size(
                len(data), self.performance_history
            )
        
        batches = []
        for i in range(0, len(data), batch_size):
            batches.append(data[i:i + batch_size])
        
        return batches
```

#### 时间序列数据优化
```python
def optimize_time_series_batching(start_date, end_date, data_frequency="daily"):
    """优化时间序列数据的批次策略"""
    time_span = (end_date - start_date).days
    
    # 根据数据频率和时间跨度选择策略
    if data_frequency == "daily":
        if time_span <= 31:
            return "single"  # 单批次
        elif time_span <= 365:
            return "monthly"  # 月度批次
        elif time_span <= 1825:  # 5年
            return "quarterly"  # 季度批次
        else:
            return "yearly"  # 年度批次
    
    elif data_frequency == "monthly":
        if time_span <= 365:
            return "single"
        elif time_span <= 1825:
            return "yearly"
        else:
            return "multi_year"
    
    return "adaptive"  # 自适应策略
```

### 2. 内存优化

#### 流式批次处理
```python
class StreamingBatchProcessor:
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
    
    async def process_stream(self, data_generator):
        """流式处理大数据集"""
        batch = []
        
        async for item in data_generator:
            batch.append(item)
            
            if len(batch) >= self.batch_size:
                yield batch
                batch = []  # 释放内存
        
        # 处理最后一个不完整的批次
        if batch:
            yield batch
    
    async def process_large_dataset(self, data_source):
        """处理大数据集而不占用过多内存"""
        total_processed = 0
        
        async for batch in self.process_stream(data_source):
            await self.process_batch(batch)
            total_processed += len(batch)
            
            # 内存清理
            if total_processed % 10000 == 0:
                import gc
                gc.collect()
```

#### 内存使用监控
```python
import tracemalloc

class MemoryMonitor:
    def __init__(self):
        self.snapshots = []
    
    def start_monitoring(self):
        """开始内存监控"""
        tracemalloc.start()
        self.snapshots.append(tracemalloc.take_snapshot())
    
    def take_snapshot(self, label=""):
        """获取内存快照"""
        snapshot = tracemalloc.take_snapshot()
        self.snapshots.append((label, snapshot))
        return snapshot
    
    def analyze_memory_growth(self):
        """分析内存增长"""
        if len(self.snapshots) < 2:
            return None
        
        current = self.snapshots[-1][1] if isinstance(self.snapshots[-1], tuple) else self.snapshots[-1]
        previous = self.snapshots[-2][1] if isinstance(self.snapshots[-2], tuple) else self.snapshots[-2]
        
        top_stats = current.compare_to(previous, 'lineno')
        
        return {
            "top_differences": top_stats[:10],
            "total_memory_mb": sum(stat.size for stat in current.statistics('filename')) / 1024 / 1024
        }
```

### 3. 并发优化

#### 并发批次处理
```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

class ConcurrentBatchProcessor:
    def __init__(self, max_concurrent=5):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_batch_concurrent(self, batch):
        """并发处理单个批次"""
        async with self.semaphore:
            # 模拟批次处理
            await asyncio.sleep(0.1)  # 替换为实际处理逻辑
            return f"Processed {len(batch)} items"
    
    async def process_all_batches(self, batches):
        """并发处理所有批次"""
        tasks = [
            self.process_batch_concurrent(batch) 
            for batch in batches
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理异常
        successful = [r for r in results if not isinstance(r, Exception)]
        failed = [r for r in results if isinstance(r, Exception)]
        
        return {
            "successful": len(successful),
            "failed": len(failed),
            "results": successful,
            "errors": failed
        }
```

#### 自适应并发控制
```python
class AdaptiveConcurrencyController:
    def __init__(self, initial_concurrency=3):
        self.current_concurrency = initial_concurrency
        self.performance_history = []
        self.adjustment_threshold = 5  # 调整阈值
    
    async def adaptive_process(self, batches):
        """自适应并发处理"""
        start_time = time.time()
        
        # 使用当前并发度处理
        semaphore = asyncio.Semaphore(self.current_concurrency)
        
        async def process_with_semaphore(batch):
            async with semaphore:
                return await self.process_batch(batch)
        
        tasks = [process_with_semaphore(batch) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 记录性能
        processing_time = time.time() - start_time
        throughput = len(batches) / processing_time
        
        self.performance_history.append({
            "concurrency": self.current_concurrency,
            "throughput": throughput,
            "processing_time": processing_time,
            "batch_count": len(batches)
        })
        
        # 调整并发度
        self.adjust_concurrency()
        
        return results
    
    def adjust_concurrency(self):
        """根据性能历史调整并发度"""
        if len(self.performance_history) < self.adjustment_threshold:
            return
        
        recent_performance = self.performance_history[-self.adjustment_threshold:]
        avg_throughput = sum(p["throughput"] for p in recent_performance) / len(recent_performance)
        
        # 如果性能下降，减少并发度
        if len(self.performance_history) > self.adjustment_threshold:
            previous_avg = sum(
                p["throughput"] for p in 
                self.performance_history[-self.adjustment_threshold*2:-self.adjustment_threshold]
            ) / self.adjustment_threshold
            
            if avg_throughput < previous_avg * 0.9:  # 性能下降10%
                self.current_concurrency = max(1, self.current_concurrency - 1)
            elif avg_throughput > previous_avg * 1.1:  # 性能提升10%
                self.current_concurrency = min(10, self.current_concurrency + 1)
```

## 📈 性能分析工具

### 批次性能分析器
```python
class BatchPerformanceAnalyzer:
    def __init__(self):
        self.metrics = []
    
    def analyze_batch_distribution(self, batches):
        """分析批次分布"""
        batch_sizes = [len(batch.get("items", [])) for batch in batches]
        
        return {
            "total_batches": len(batches),
            "avg_batch_size": sum(batch_sizes) / len(batch_sizes),
            "min_batch_size": min(batch_sizes),
            "max_batch_size": max(batch_sizes),
            "size_variance": self.calculate_variance(batch_sizes),
            "distribution": self.get_size_distribution(batch_sizes)
        }
    
    def calculate_variance(self, values):
        """计算方差"""
        mean = sum(values) / len(values)
        return sum((x - mean) ** 2 for x in values) / len(values)
    
    def get_size_distribution(self, sizes):
        """获取大小分布"""
        from collections import Counter
        return dict(Counter(sizes))
    
    def generate_performance_report(self, task_name, metrics):
        """生成性能报告"""
        report = {
            "task_name": task_name,
            "timestamp": time.time(),
            "summary": {
                "total_batches": sum(m.get("batch_count", 0) for m in metrics),
                "avg_generation_time": sum(m.get("generation_time", 0) for m in metrics) / len(metrics),
                "total_memory_usage": sum(m.get("memory_usage_mb", 0) for m in metrics),
                "avg_optimization_rate": sum(
                    m.get("optimization_stats", {}).get("reduction_rate", 0) 
                    for m in metrics
                ) / len(metrics)
            },
            "recommendations": self.generate_recommendations(metrics)
        }
        
        return report
    
    def generate_recommendations(self, metrics):
        """生成优化建议"""
        recommendations = []
        
        avg_time = sum(m.get("generation_time", 0) for m in metrics) / len(metrics)
        if avg_time > 1.0:
            recommendations.append("批次生成时间较长，考虑优化分区策略")
        
        avg_memory = sum(m.get("memory_usage_mb", 0) for m in metrics) / len(metrics)
        if avg_memory > 100:
            recommendations.append("内存使用较高，考虑使用流式处理")
        
        avg_optimization = sum(
            m.get("optimization_stats", {}).get("reduction_rate", 0) 
            for m in metrics
        ) / len(metrics)
        if avg_optimization < 50:
            recommendations.append("批次优化效果不佳，检查时间跨度和分区策略")
        
        return recommendations
```

## 🎯 性能优化检查清单

### 开发阶段
- [ ] 选择合适的批处理策略
- [ ] 实现性能监控
- [ ] 添加内存使用监控
- [ ] 设置合理的并发限制
- [ ] 实现错误处理和重试机制

### 测试阶段
- [ ] 进行不同数据规模的性能测试
- [ ] 验证内存使用是否合理
- [ ] 测试并发处理的稳定性
- [ ] 验证批次优化效果
- [ ] 测试边界条件和异常情况

### 生产阶段
- [ ] 监控批次生成性能
- [ ] 定期分析性能趋势
- [ ] 根据实际使用情况调整参数
- [ ] 建立性能告警机制
- [ ] 定期进行性能优化

## 📊 性能基准

### 推荐性能指标
| 指标 | 优秀 | 良好 | 需要优化 |
|------|------|------|----------|
| 批次生成时间 | <0.1s | <1s | >1s |
| 内存使用 | <50MB | <100MB | >100MB |
| 批次优化率 | >80% | >50% | <50% |
| API调用效率 | >10 calls/s | >5 calls/s | <5 calls/s |
| 错误率 | <1% | <5% | >5% |

### 性能目标设定
```python
PERFORMANCE_TARGETS = {
    "batch_generation_time": 0.5,  # 秒
    "memory_usage_mb": 75,          # MB
    "optimization_rate": 70,        # 百分比
    "throughput": 100,              # batches/minute
    "error_rate": 0.02              # 2%
}

def check_performance_targets(metrics):
    """检查是否达到性能目标"""
    results = {}
    
    for metric, target in PERFORMANCE_TARGETS.items():
        actual = metrics.get(metric, 0)
        
        if metric == "error_rate":
            results[metric] = actual <= target
        else:
            results[metric] = actual >= target if metric in ["optimization_rate", "throughput"] else actual <= target
    
    return results
```

通过遵循本指南的性能优化策略和监控建议，可以显著提升批处理系统的效率和稳定性。
