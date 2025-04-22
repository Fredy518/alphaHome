# Task 类知识参考

## 基类概述

### Task 基类

Task 是数据处理系统的核心单元，负责数据的获取、处理、验证和存储。它提供了一个统一的接口和生命周期管理机制，使得不同数据源的任务可以以一致的方式被执行和管理。

主要特点：
- 标准化的任务生命周期（初始化、前处理、数据获取、数据处理、数据验证、数据保存、后处理）
- 统一的错误处理机制
- 可扩展的设计，支持各种数据源
- 依赖任务管理

#### 核心属性
- `name`: 任务名称，用于标识任务
- `description`: 任务描述
- `table_name`: 数据库表名
- `primary_keys`: 主键列表
- `date_column`: 日期列名
- `indexes`: 自定义索引配置
- `schema`: 表结构定义

#### 核心方法
- `fetch_data()`: 获取数据（抽象方法，子类必须实现）
- `process_data()`: 处理数据
- `validate_data()`: 验证数据
- `save_data()`: 保存数据
- `execute()`: 执行整个任务流程
- `pre_execute()`: 任务执行前的准备工作
- `post_execute()`: 任务执行后的清理工作
- `get_latest_date()`: 获取表中的最新日期

### TushareTask 扩展

TushareTask 是 Task 的一个特定实现，专门用于处理来自 Tushare API 的数据。

特有特点：
- 分批处理：将大查询拆分为多个小批次，提高并行性和稳定性
- 重试机制：针对网络错误和临时失败进行自动重试
- 参数映射：将业务参数映射到 Tushare API 参数
- 列名映射：支持将 API 返回的列名映射为数据库列名

#### TushareTask 特有属性
- `api_name`: Tushare API 名称
- `fields`: 需要获取的字段列表
- `transformations`: 数据类型转换规则
- `column_mapping`: 列名映射规则
- `validations`: 数据验证规则

#### TushareTask 特有方法
- `get_batch_list()`: 生成批处理参数列表（抽象方法，子类必须实现）
- `prepare_params()`: 准备 API 调用参数
- `fetch_batch()`: 获取单批次数据

## 数据验证机制

### 验证规则格式

验证规则是一个函数列表，每个函数接收一个 DataFrame 并返回一个布尔值或布尔值序列：

```python
validations = [
    lambda df: df["column_name"] >= 0,  # 返回布尔值序列
    lambda df: some_condition(df)  # 可能返回单个布尔值
]
```

### 验证结果处理策略

验证有两种策略：
1. **整批拒绝**：任何验证规则返回 False，整个批次都被拒绝
   ```python
   lambda df: all(df["column"] >= 0)
   ```

2. **行级过滤**：根据验证规则返回的布尔值序列，只过滤不符合条件的行
   ```python
   lambda df: df["column"] >= 0
   ```

在 TushareTask 中，验证规则返回的布尔值序列会被用来过滤不符合条件的行，因此推荐使用行级过滤策略。

### 验证规则示例

```python
# 财务数据验证
validations = [
    # 净利润在合理范围内
    lambda df: df["n_income"].fillna(0).abs() < df["total_revenue"].fillna(0).abs() * 10,
    # 日期格式正确
    lambda df: pd.to_datetime(df["end_date"], errors="coerce").notna()
]

# 股票日线数据验证
validations = [
    # 价格字段合理
    lambda df: df["close"] >= 0,
    lambda df: df["high"] >= df["low"],
    # 交易量为正
    lambda df: df["volume"] >= 0
]
```

## 任务注册与管理

### 任务注册装饰器

使用 `@task_register()` 装饰器可以自动将任务类注册到任务注册表中，便于统一管理和调用。

```python
from ...task_decorator import task_register

@task_register()
class MyCustomTask(Task):
    # 任务定义
    ...
```

### 任务管理器

任务管理器负责任务的实例化、配置和执行，主要功能包括：

- 任务依赖管理
- 任务并行执行
- 任务调度
- 执行结果收集和报告

## 数据库交互

### 表结构管理

每个任务都需要定义自己的表结构：

```python
schema = {
    "column1": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
    "column2": {"type": "DATE", "constraints": "NOT NULL"},
    "column3": {"type": "NUMERIC(10,4)"}
}
```

### 自动创建表

如果表不存在，系统会根据 `schema` 定义自动创建表和索引。

### 数据保存

数据保存支持两种模式：
- `insert`：插入新数据
- `upsert`：插入或更新数据（根据主键）

## 批处理策略

### 交易日批处理

适用于需要按交易日批量处理的数据，例如股票日线数据：

```python
async def get_batch_list(self, **kwargs):
    batch_list = await generate_trade_day_batches(
        start_date=start_date,
        end_date=end_date,
        batch_size=self.batch_trade_days_single_code if ts_code else self.batch_trade_days_all_codes,
        ts_code=ts_code,
        exchange=exchange,
        logger=self.logger
    )
    return batch_list
```

### 自然日批处理

适用于按照自然日期批量处理的数据，例如财务报表数据：

```python
async def get_batch_list(self, **kwargs):
    batch_list = await generate_natural_days_batches(
        start_date=start_date,
        end_date=end_date,
        batch_size=365,  # 使用365天作为批次大小
        ts_code=ts_code,
        logger=self.logger
    )
    return batch_list
```

## 最佳实践

### 任务设计原则

1. **单一职责**：每个任务只负责一种数据的处理
2. **可重用**：设计通用的基类和工具函数
3. **健壮性**：合理的错误处理和重试机制
4. **可测试**：任务的各个环节可以单独测试
5. **高效性**：批处理和并行执行

### 验证规则设计

1. **返回布尔值序列**：让验证函数返回布尔值序列而不是单个布尔值，以支持行级过滤
2. **合理使用 fillna**：在比较前处理可能的 NaN 值
3. **使用 notna 验证日期**：使用 `pd.to_datetime(...).notna()` 验证日期格式

### 调试技巧

1. 使用日志记录任务执行过程
2. 单独测试各个环节（获取、处理、验证、保存）
3. 使用小批量数据进行测试
4. 检查验证规则是否按预期工作 