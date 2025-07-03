# AlphaHome 数据处理模块 (Processors)

## 概述

AlphaHome 数据处理模块采用全新的**三层架构**设计，提供了简洁、灵活且高度可扩展的数据处理框架。该模块专门用于量化投资研究中的数据处理任务，包括数据清洗、特征工程、技术指标计算等。

## 🏗️ 架构设计

### 三层架构概览

新的架构遵循"关注点分离"原则，将调度、业务逻辑和原子操作清晰地分离开来：

```
┌─────────────────────────────────────────────────────────────┐
│                        引擎层 (Engine)                        │
│                   ProcessorEngine                           │
│              任务调度、并发控制、执行监控                      │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                        任务层 (Tasks)                         │
│   ProcessorTaskBase, BlockProcessingTaskMixin               │
│      业务流程单元，负责数据IO、操作编排、状态管理              │
└─────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                       操作层 (Operations)                     │
│                Operation, OperationPipeline                 │
│              可复用的、无状态的原子级数据转换                  │
└─────────────────────────────────────────────────────────────┘
```

### 架构优势

✅ **职责清晰**：每层都有明确的职责边界，易于理解和维护
✅ **高度复用**：Operation可在不同Task中自由组合使用
✅ **易于扩展**：新功能可以通过添加Operation或Task轻松实现
✅ **并发友好**：Engine层提供完整的并发控制和资源管理
✅ **测试友好**：每层都可以独立测试，提高代码质量

### 各层详细职责

#### 1. 引擎层 (Engine)
- **主要职责**: 任务调度、执行监控、资源管理、并发控制
- **核心组件**: `ProcessorEngine`
- **关键特性**:
  - 支持单任务和批量任务执行
  - 提供并发控制和超时管理
  - 统计执行信息和性能监控
  - 依赖关系检查和管理

#### 2. 任务层 (Tasks)
- **主要职责**: 封装完整的业务处理流程
- **核心组件**: `ProcessorTaskBase`, `BlockProcessingTaskMixin`
- **标准流程**:
  1. **获取数据** (`fetch_data`): 从数据源获取原始数据
  2. **处理数据** (`process_data`): 编排Operation执行数据转换
  3. **保存结果** (`save_result`): 将处理结果保存到目标位置
- **关键特性**:
  - 支持分块处理大数据集
  - 内置数据验证和错误处理
  - 自动任务注册和发现机制

#### 3. 操作层 (Operations)
- **主要职责**: 执行原子级数据转换操作
- **核心组件**: `Operation`, `OperationPipeline`
- **设计原则**:
  - **原子性**: 每个Operation只做一件事
  - **无状态**: 不保存执行间的状态信息
  - **可组合**: 可通过Pipeline灵活组合
- **关键特性**:
  - 支持异步执行
  - 内置错误处理和日志记录
  - 支持条件执行和流程控制

## 🚀 快速开始

### 完整示例：从Operation到Engine的端到端流程

以下是一个完整的示例，展示如何从创建Operation开始，到最终通过Engine执行任务的完整流程：

#### 步骤1: 创建自定义操作 (Operation)

```python
from alphahome.processors import Operation
import pandas as pd

class DataCleaningOperation(Operation):
    """数据清洗操作：移除空值和异常值"""

    def __init__(self, name: str = "DataCleaning"):
        super().__init__(name)

    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """执行数据清洗"""
        result = data.copy()

        # 移除空值
        result = result.dropna()

        # 移除异常值（示例：移除超出3个标准差的值）
        if 'value' in result.columns:
            mean = result['value'].mean()
            std = result['value'].std()
            result = result[abs(result['value'] - mean) <= 3 * std]

        self.logger.info(f"数据清洗完成：{len(data)} -> {len(result)} 行")
        return result

class FeatureEngineeringOperation(Operation):
    """特征工程操作：创建新特征"""

    def __init__(self, name: str = "FeatureEngineering"):
        super().__init__(name)

    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """执行特征工程"""
        result = data.copy()

        # 创建新特征
        if 'value' in result.columns:
            result['value_squared'] = result['value'] ** 2
            result['value_log'] = result['value'].apply(lambda x: pd.np.log(x) if x > 0 else 0)

        self.logger.info(f"特征工程完成，新增 {len(result.columns) - len(data.columns)} 个特征")
        return result
```

#### 步骤2: 创建自定义任务 (Task)

```python
from alphahome.processors import ProcessorTaskBase, OperationPipeline, task_register
import pandas as pd

@task_register()
class ExampleProcessorTask(ProcessorTaskBase):
    """示例数据处理任务"""

    name = "example_processor_task"
    table_name = "example_result_table"
    description = "完整的数据处理示例任务"

    # 可选：定义源表（用于依赖检查）
    source_tables = ["raw_data_table"]

    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        """获取数据 - 必须实现的方法"""
        # 实际项目中，这里会从数据库或API获取数据
        # 这里使用模拟数据作为示例
        data = pd.DataFrame({
            'id': range(1, 101),
            'value': pd.np.random.normal(100, 15, 100),
            'category': pd.np.random.choice(['A', 'B', 'C'], 100)
        })

        self.logger.info(f"获取到 {len(data)} 行原始数据")
        return data

    async def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """处理数据 - 必须实现的方法"""
        # 创建操作流水线
        pipeline = OperationPipeline("ExampleProcessingPipeline")

        # 添加操作到流水线
        pipeline.add_operation(DataCleaningOperation())
        pipeline.add_operation(FeatureEngineeringOperation())

        # 执行流水线
        processed_data = await pipeline.apply(data, **kwargs)

        self.logger.info(f"数据处理完成，最终数据形状: {processed_data.shape}")
        return processed_data

    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存结果 - 必须实现的方法"""
        # 实际项目中，这里会保存到数据库
        # 这里只是打印结果作为示例
        self.logger.info(f"保存 {len(data)} 行数据到表 {self.table_name}")
        print(f"模拟保存数据到 {self.table_name}:")
        print(data.head())
```

#### 步骤3: 使用引擎执行任务 (Engine)

```python
import asyncio
from alphahome.processors import ProcessorEngine
from alphahome.common.db_manager import DBManager
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.common.config_manager import get_database_url

async def run_example():
    """完整的执行示例"""
    db_manager = None

    try:
        # 1. 初始化数据库连接
        db_url = get_database_url()
        if not db_url:
            raise ValueError("需要配置有效的数据库URL")

        db_manager = DBManager(db_url)
        await db_manager.connect()

        # 2. 初始化任务工厂
        await UnifiedTaskFactory.initialize(db_url=db_url)

        # 3. 创建处理引擎
        engine = ProcessorEngine(db_manager=db_manager, max_workers=2)

        # 4. 执行任务
        print("开始执行示例任务...")
        result = await engine.execute_task("example_processor_task")

        # 5. 检查结果
        print(f"任务执行状态: {result['status']}")
        print(f"处理行数: {result.get('rows', 0)}")
        print(f"执行时间: {result.get('engine_metadata', {}).get('execution_time', 0):.2f}秒")

        # 6. 关闭引擎
        engine.shutdown()

    finally:
        # 7. 清理资源
        if db_manager:
            await db_manager.close()

# 运行示例
if __name__ == "__main__":
    asyncio.run(run_example())
```

## 📚 详细开发指南

### Operation 开发指南

#### Operation 基本结构

所有Operation都必须继承`Operation`基类并实现`apply`方法：

```python
from alphahome.processors import Operation
import pandas as pd
from typing import Optional, Dict, Any

class MyOperation(Operation):
    def __init__(self, name: str = "MyOperation", config: Optional[Dict[str, Any]] = None):
        super().__init__(name, config)
        # 在这里初始化操作特定的配置
        self.threshold = self.config.get('threshold', 0.5)

    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        执行具体的数据处理逻辑

        Args:
            data: 输入的DataFrame
            **kwargs: 额外的参数

        Returns:
            pd.DataFrame: 处理后的DataFrame
        """
        # 实现具体的处理逻辑
        result = data.copy()

        # 使用self.logger记录日志
        self.logger.info(f"开始执行 {self.name} 操作")

        # 具体的处理逻辑
        # ...

        self.logger.info(f"{self.name} 操作完成，处理了 {len(result)} 行数据")
        return result
```

#### Operation 设计原则

1. **原子性**: 每个Operation只做一件事，功能单一且明确
2. **无状态**: 不在实例变量中保存执行状态，确保可重复使用
3. **异步友好**: 使用async/await支持异步执行
4. **错误处理**: 适当处理异常，使用日志记录关键信息
5. **配置驱动**: 通过config参数支持灵活配置

#### OperationPipeline 使用

```python
from alphahome.processors import OperationPipeline

# 创建流水线
pipeline = OperationPipeline("MyPipeline", config={
    "stop_on_error": True  # 遇到错误时停止执行
})

# 添加操作
pipeline.add_operation(DataCleaningOperation())
pipeline.add_operation(FeatureEngineeringOperation())

# 支持条件执行
pipeline.add_operation(
    AdvancedProcessingOperation(),
    condition=lambda data: len(data) > 1000  # 只有数据量大于1000时才执行
)

# 执行流水线
result = await pipeline.apply(input_data)
```

### Task 开发指南

#### Task 基本模板

```python
from alphahome.processors import ProcessorTaskBase, task_register
import pandas as pd
from typing import Optional

@task_register()  # 必须使用装饰器注册任务
class MyProcessorTask(ProcessorTaskBase):
    """任务描述"""

    # 必需的类属性
    name = "my_processor_task"  # 任务唯一标识
    table_name = "my_result_table"  # 结果表名
    description = "任务详细描述"  # 任务描述

    # 可选的类属性
    source_tables = ["source_table1", "source_table2"]  # 依赖的源表
    dependencies = ["prerequisite_task"]  # 依赖的其他任务

    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        """
        获取数据 - 必须实现

        Returns:
            pd.DataFrame: 原始数据
        """
        # 从数据库、API或文件获取数据
        # 示例：
        query = "SELECT * FROM source_table WHERE date >= %s"
        data = await self.db_manager.fetch_dataframe(query, [kwargs.get('start_date')])

        self.logger.info(f"获取到 {len(data)} 行原始数据")
        return data

    async def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理数据 - 必须实现

        Args:
            data: 从fetch_data获取的原始数据

        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 创建处理流水线
        pipeline = OperationPipeline("MyTaskPipeline")
        pipeline.add_operation(MyOperation1())
        pipeline.add_operation(MyOperation2())

        # 执行处理
        result = await pipeline.apply(data, **kwargs)

        return result

    async def save_result(self, data: pd.DataFrame, **kwargs):
        """
        保存结果 - 必须实现

        Args:
            data: 处理后的数据
        """
        # 保存到数据库
        await self.db_manager.save_dataframe(
            data,
            self.table_name,
            if_exists='replace'  # 或 'append'
        )

        self.logger.info(f"成功保存 {len(data)} 行数据到 {self.table_name}")
```

#### 异步方法实现要求

1. **fetch_data**:
   - 必须是异步方法 (`async def`)
   - 返回`pd.DataFrame`
   - 处理数据获取异常
   - 记录获取的数据量

2. **process_data**:
   - 必须是异步方法 (`async def`)
   - 接收DataFrame参数，返回DataFrame
   - 在此方法中编排Operation的执行
   - 不要直接操作数据，而是通过Operation组合

3. **save_result**:
   - 必须是异步方法 (`async def`)
   - 接收处理后的DataFrame
   - 负责将结果保存到目标位置
   - 处理保存异常

#### 分块处理支持

对于大数据集，可以使用`BlockProcessingTaskMixin`：

```python
from alphahome.processors import ProcessorTaskBase, BlockProcessingTaskMixin, task_register

@task_register()
class LargeDataProcessorTask(ProcessorTaskBase, BlockProcessingTaskMixin):
    name = "large_data_processor"
    table_name = "large_result_table"
    description = "大数据集处理任务"

    # 分块处理配置
    block_size = 10000  # 每块处理的行数

    async def get_data_blocks(self, **kwargs):
        """定义如何分块获取数据"""
        # 返回数据块的迭代器
        total_count = await self.get_total_count()

        for offset in range(0, total_count, self.block_size):
            query = f"SELECT * FROM large_table LIMIT {self.block_size} OFFSET {offset}"
            block_data = await self.db_manager.fetch_dataframe(query)
            yield block_data

    async def process_block(self, block_data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """处理单个数据块"""
        # 对单个块执行处理逻辑
        pipeline = OperationPipeline("BlockPipeline")
        pipeline.add_operation(MyOperation())

        return await pipeline.apply(block_data, **kwargs)
```

### ProcessorEngine 使用指南

#### 引擎初始化

```python
from alphahome.processors import ProcessorEngine
from alphahome.common.db_manager import DBManager
from alphahome.common.task_system import UnifiedTaskFactory

async def initialize_engine():
    # 1. 初始化数据库管理器
    db_manager = DBManager("postgresql://user:pass@localhost:5432/db")
    await db_manager.connect()

    # 2. 初始化任务工厂
    await UnifiedTaskFactory.initialize(db_url="postgresql://user:pass@localhost:5432/db")

    # 3. 创建处理引擎
    engine = ProcessorEngine(
        db_manager=db_manager,
        max_workers=4,  # 最大并发数
        timeout=3600,   # 任务超时时间（秒）
        config={        # 引擎配置
            "task1": {"param1": "value1"},  # 任务特定配置
            "task2": {"param2": "value2"}
        }
    )

    return engine, db_manager
```

#### 任务执行方式

```python
# 执行单个任务
result = await engine.execute_task("my_task", param1="value1")

# 执行多个任务（并行）
results = await engine.execute_tasks([
    "task1", "task2", "task3"
], parallel=True)

# 执行多个任务（顺序）
results = await engine.execute_tasks([
    "task1", "task2", "task3"
], parallel=False)

# 批量执行（带参数）
results = await engine.execute_batch({
    "task1": {"start_date": "2023-01-01"},
    "task2": {"end_date": "2023-12-31"}
})
```

#### 错误处理和监控

```python
try:
    result = await engine.execute_task("my_task")

    if result["status"] == "success":
        print(f"任务成功完成，处理了 {result.get('rows', 0)} 行数据")
        print(f"执行时间: {result['engine_metadata']['execution_time']:.2f}秒")
    else:
        print(f"任务失败: {result.get('error', 'Unknown error')}")

except Exception as e:
    print(f"引擎执行异常: {e}")

# 获取引擎统计信息
stats = engine.get_stats()
print(f"成功率: {stats['success_rate']:.2%}")
print(f"平均执行时间: {stats['average_execution_time']:.2f}秒")
```

## 🎯 最佳实践

### Operation 设计最佳实践

1. **单一职责**: 每个Operation只做一件事
   ```python
   # ✅ 好的设计
   class RemoveNullValuesOperation(Operation):
       async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
           return data.dropna()

   # ❌ 避免的设计
   class DataProcessingOperation(Operation):
       async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
           data = data.dropna()  # 移除空值
           data['new_feature'] = data['value'] * 2  # 特征工程
           data = data[data['value'] > 0]  # 过滤数据
           return data
   ```

2. **配置驱动**: 使用配置参数提高灵活性
   ```python
   class FilterOperation(Operation):
       def __init__(self, name: str = "Filter", config: Optional[Dict] = None):
           super().__init__(name, config)
           self.min_value = self.config.get('min_value', 0)
           self.max_value = self.config.get('max_value', float('inf'))

       async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
           return data[(data['value'] >= self.min_value) &
                      (data['value'] <= self.max_value)]
   ```

3. **错误处理**: 优雅处理异常情况
   ```python
   class SafeCalculationOperation(Operation):
       async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
           try:
               result = data.copy()
               result['ratio'] = result['numerator'] / result['denominator']
               return result
           except ZeroDivisionError:
               self.logger.warning("发现除零错误，使用默认值")
               result = data.copy()
               result['ratio'] = 0
               return result
           except Exception as e:
               self.logger.error(f"计算过程中发生错误: {e}")
               raise
   ```

### Task 设计最佳实践

1. **清晰的数据流**: 明确定义输入、处理、输出
2. **适当的日志记录**: 记录关键步骤和数据量变化
3. **参数验证**: 验证输入参数的有效性
4. **资源管理**: 适当处理数据库连接等资源

### Engine 使用最佳实践

1. **资源管理**: 始终正确关闭引擎和数据库连接
   ```python
   async def safe_execution():
       db_manager = None
       engine = None

       try:
           db_manager = DBManager(db_url)
           await db_manager.connect()

           engine = ProcessorEngine(db_manager=db_manager)
           result = await engine.execute_task("my_task")

           return result
       finally:
           if engine:
               engine.shutdown()
           if db_manager:
               await db_manager.close()
   ```

2. **并发控制**: 根据系统资源合理设置并发数
3. **超时设置**: 为长时间运行的任务设置合理的超时时间

## ❓ 常见问题解答

### Q: 如何处理大数据集？
A: 使用`BlockProcessingTaskMixin`进行分块处理：
```python
@task_register()
class LargeDataTask(ProcessorTaskBase, BlockProcessingTaskMixin):
    block_size = 10000  # 每次处理10000行

    async def get_data_blocks(self, **kwargs):
        # 实现分块逻辑
        pass

    async def process_block(self, block_data, **kwargs):
        # 处理单个块
        pass
```

### Q: 如何在Operation之间传递参数？
A: 通过kwargs参数传递：
```python
# 在Task中
async def process_data(self, data: pd.DataFrame, **kwargs):
    pipeline = OperationPipeline("MyPipeline")
    pipeline.add_operation(Operation1())
    pipeline.add_operation(Operation2())

    # 传递额外参数
    result = await pipeline.apply(data, custom_param="value")
    return result

# 在Operation中接收
class Operation2(Operation):
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        custom_param = kwargs.get('custom_param', 'default')
        # 使用参数进行处理
        return data
```

### Q: 如何调试任务执行？
A: 使用日志和异常处理：
```python
# 设置日志级别
import logging
logging.getLogger('alphahome.processors').setLevel(logging.DEBUG)

# 在代码中添加调试信息
self.logger.debug(f"数据形状: {data.shape}")
self.logger.debug(f"数据列: {data.columns.tolist()}")
```

### Q: 如何实现条件执行？
A: 使用OperationPipeline的条件功能：
```python
pipeline.add_operation(
    ExpensiveOperation(),
    condition=lambda data: len(data) > 1000  # 只有数据量大时才执行
)
```

## 🔧 故障排除

### 常见错误及解决方案

1. **TypeError: ProcessorEngine() missing required argument: 'db_manager'**
   - 原因: 未提供数据库管理器
   - 解决: 先初始化DBManager再传递给ProcessorEngine

2. **AttributeError: 'coroutine' object has no attribute 'empty'**
   - 原因: 异步方法未正确await
   - 解决: 确保所有异步方法都使用await调用

3. **任务未注册错误**
   - 原因: 忘记使用@task_register()装饰器
   - 解决: 为所有Task类添加装饰器

## 📈 性能优化建议

1. **合理设置并发数**: 根据CPU核心数和I/O特性调整max_workers
2. **使用分块处理**: 对大数据集使用BlockProcessingTaskMixin
3. **优化数据库查询**: 在fetch_data中使用高效的SQL查询
4. **内存管理**: 及时释放不需要的DataFrame引用

## 📋 版本历史

- **v0.3.0** (当前版本):
  - ✅ 重构为三层架构 (Engine -> Task -> Operation)
  - ✅ 移除废弃的base和pipelines层
  - ✅ 完善异步支持和错误处理
  - ✅ 添加完整的测试覆盖
  - ✅ 更新文档和示例

- **v0.2.0**: 引入分层架构，重构核心组件
- **v0.1.0**: 初始版本，基础处理功能

## 🤝 贡献指南

欢迎贡献代码和建议！请遵循以下原则：

### 代码贡献
1. **保持架构一致性**: 遵循三层架构设计原则
2. **编写测试用例**: 为新功能编写完整的单元测试
3. **更新文档**: 为新功能提供详细的文档说明
4. **代码规范**: 遵循PEP 8和项目代码规范

### 提交流程
1. Fork项目仓库
2. 创建功能分支
3. 编写代码和测试
4. 更新文档
5. 提交Pull Request

### 问题报告
- 使用GitHub Issues报告bug
- 提供详细的错误信息和复现步骤
- 包含系统环境和版本信息

---

## 📞 支持与反馈

如有问题或建议，请通过以下方式联系：
- 📧 Email: support@alphahome.com
- 💬 GitHub Issues: [项目Issues页面]
- 📚 文档: [在线文档地址]

感谢使用AlphaHome数据处理模块！🚀
