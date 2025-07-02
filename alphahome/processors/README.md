# AlphaHome 数据处理模块 (Processors)

## 概述

AlphaHome 数据处理模块采用分层架构设计，提供了灵活、可扩展的数据处理框架。该模块专门用于量化投资研究中的数据处理任务，包括数据清洗、特征工程、技术指标计算等。

## 架构设计

### 五层架构

```
┌─────────────────────────────────────────────────────────────┐
│                        引擎层 (Engine)                        │
│                   ProcessorEngine                           │
│              任务调度、执行监控、资源管理                      │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                        任务层 (Tasks)                         │
│                   ProcessorTaskBase                         │
│              具体的数据处理任务实现                           │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                      流水线层 (Pipelines)                     │
│                   ProcessingPipeline                        │
│            组合多个操作完成复杂业务逻辑                        │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                       操作层 (Operations)                     │
│                Operation, OperationPipeline                 │
│                  原子级数据处理操作                           │
└─────────────────────────────────────────────────────────────┘
                                │
┌─────────────────────────────────────────────────────────────┐
│                       基础层 (Base)                          │
│          BaseProcessor, DataProcessor, BlockProcessor        │
│                处理器基类和通用功能                           │
└─────────────────────────────────────────────────────────────┘
```

### 各层职责

#### 1. 基础层 (Base)
- **BaseProcessor**: 所有处理器的抽象基类
- **DataProcessor**: 数据处理器的具体基类
- **BlockProcessor**: 支持分块处理的处理器
- **BlockProcessorMixin**: 分块处理功能混入类

#### 2. 操作层 (Operations)
- **Operation**: 原子级操作的抽象基类
- **OperationPipeline**: 操作流水线，串联多个操作

#### 3. 流水线层 (Pipelines)
- **ProcessingPipeline**: 高级处理流水线，组合操作完成业务逻辑

#### 4. 任务层 (Tasks)
- **ProcessorTaskBase**: 处理任务基类
- 各种具体的数据处理任务实现

#### 5. 引擎层 (Engine)
- **ProcessorEngine**: 处理引擎，负责任务调度和执行

## 快速开始

### 基本使用

```python
import asyncio
from alphahome.processors import ProcessorEngine

# 创建处理引擎
engine = ProcessorEngine(max_workers=4)

# 执行单个任务
async def run_task():
    result = await engine.execute_task("stock_adjusted_price_v2")
    print(f"任务状态: {result['status']}")
    print(f"处理行数: {result['rows']}")

asyncio.run(run_task())
```

### 创建自定义操作

```python
from alphahome.processors import Operation
import pandas as pd

class MyOperation(Operation):
    def __init__(self, config=None):
        super().__init__(name="MyOperation", config=config)
    
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        # 实现具体的数据处理逻辑
        result = data.copy()
        # ... 处理逻辑 ...
        return result
```

### 创建自定义流水线

```python
from alphahome.processors import ProcessingPipeline

class MyPipeline(ProcessingPipeline):
    def build_pipeline(self):
        self.add_stage("数据清洗", MyCleaningOperation())
        self.add_stage("特征工程", MyFeatureOperation())
        self.add_stage("数据验证", MyValidationOperation())
```

### 创建自定义任务

```python
from alphahome.processors import ProcessorTaskBase, task_register

@task_register()
class MyTask(ProcessorTaskBase):
    name = "my_task"
    table_name = "my_result_table"
    description = "我的数据处理任务"
    
    def create_pipeline(self):
        return MyPipeline(config=self.get_pipeline_config())
    
    async def fetch_data(self, **kwargs):
        # 实现数据获取逻辑
        return pd.DataFrame()
    
    async def save_result(self, data, **kwargs):
        # 实现结果保存逻辑
        pass
```

## 核心特性

### 1. 分层架构
- 清晰的职责分离
- 高度可扩展性
- 易于维护和测试

### 2. 异步处理
- 支持异步操作
- 高效的并发处理
- 非阻塞式执行

### 3. 分块处理
- 支持大数据集处理
- 内存友好
- 可配置的分块策略

### 4. 统计监控
- 详细的执行统计
- 性能监控
- 错误追踪

### 5. 灵活配置
- 丰富的配置选项
- 运行时参数调整
- 环境适应性

## 示例任务

### 股票后复权价格计算

```python
# 使用新架构的股票后复权价格计算任务
result = await engine.execute_task("stock_adjusted_price_v2")
```

该任务演示了如何：
- 组合多个操作（复权因子计算、数据验证）
- 使用流水线处理复杂业务逻辑
- 集成到统一任务系统

## 最佳实践

### 1. 操作设计
- 保持操作的原子性
- 实现幂等性
- 提供详细的日志记录

### 2. 流水线设计
- 合理划分处理阶段
- 使用条件执行控制流程
- 实现错误恢复机制

### 3. 任务设计
- 明确定义输入输出
- 实现数据验证
- 提供配置灵活性

### 4. 性能优化
- 合理使用分块处理
- 优化数据库访问
- 监控内存使用

## 迁移指南

### 从旧架构迁移

1. **保持兼容性**: 旧的 `ProcessorTask` 仍然可用
2. **逐步迁移**: 可以逐个任务迁移到新架构
3. **测试验证**: 确保迁移后功能正确

### 迁移步骤

1. 继承 `ProcessorTaskBase` 而不是 `ProcessorTask`
2. 实现 `create_pipeline()` 方法
3. 将处理逻辑拆分为操作和流水线
4. 更新任务注册和配置

## 扩展开发

### 添加新操作
1. 继承 `Operation` 基类
2. 实现 `apply()` 方法
3. 添加必要的配置和验证

### 添加新流水线
1. 继承 `ProcessingPipeline` 基类
2. 实现 `build_pipeline()` 方法
3. 组合现有操作或创建新操作

### 添加新任务
1. 继承 `ProcessorTaskBase` 基类
2. 使用 `@task_register()` 装饰器
3. 实现必要的方法和配置

## 故障排除

### 常见问题

1. **任务未注册**: 确保使用了 `@task_register()` 装饰器
2. **导入错误**: 检查模块导入路径
3. **异步问题**: 确保正确使用 `async/await`
4. **内存不足**: 考虑使用分块处理

### 调试技巧

1. 启用详细日志记录
2. 使用引擎统计信息
3. 检查任务执行结果
4. 监控系统资源使用

## 版本历史

- **v0.2.0**: 引入分层架构，重构核心组件
- **v0.1.0**: 初始版本，基础处理功能

## 贡献指南

欢迎贡献代码和建议！请遵循以下原则：

1. 保持架构的一致性
2. 编写完整的测试用例
3. 提供详细的文档说明
4. 遵循代码规范和最佳实践
