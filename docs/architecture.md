# 金融数据任务系统架构设计

## 核心设计理念

从第一性原理出发，我们将金融数据管理系统简化为一个纯粹的数据任务系统。这种设计极其简洁，但功能完备，具有以下特点：

1. **高内聚低耦合**：每个任务自包含所有逻辑和元数据
2. **易于扩展**：添加新任务只需创建新的任务类
3. **依赖透明**：系统自动处理任务间的依赖关系
4. **存储抽象**：支持多种存储后端，易于切换
5. **异步设计**：全面采用异步编程模型，提高性能

## 系统架构

### 1. 基础任务类（Task）

所有数据任务的基类，定义了任务的通用接口和生命周期。

#### 核心属性
- `name`：任务名称，唯一标识符
- `table_name`：数据表名
- `primary_keys`：主键字段列表
- `date_column`：日期字段名
- `dependencies`：依赖的其他任务列表
- `schema`：表结构定义，包含字段名、类型和描述

#### 核心方法
- `execute(start_date, end_date, **kwargs)`：执行任务的完整生命周期
- `fetch_data(start_date, end_date, **kwargs)`：获取原始数据（子类必须实现）
- `process_data(data)`：处理原始数据
- `validate_data(data)`：验证数据有效性
- `save_data(data)`：将处理后的数据保存到数据库
- `_save_to_database(data)`：将数据保存到数据库的内部方法
- `_create_table_if_not_exists()`：如果表不存在则创建

#### 快捷方法
- `full_update(**kwargs)`：执行全量数据更新
- `incremental_update(days_lookback, **kwargs)`：执行增量数据更新

### 2. 数据源特定任务类

#### TushareTask

专门用于从Tushare获取数据的任务基类。

##### 特有属性
- `api_name`：Tushare API名称
- `fields`：需要的字段列表

##### 特有方法
- `fetch_data`：从Tushare获取数据的具体实现

### 3. 具体业务任务类

继承自数据源特定任务类，实现具体的业务逻辑。

#### 示例：StockDailyTask

```python
class StockDailyTask(TushareTask):
    """股票日线数据任务"""
    
    # 核心属性
    name = "stock_daily"
    table_name = "stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # Tushare特有属性
    api_name = "daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    
    # 数据处理规则
    transformations = {
        "trade_date": lambda x: pd.to_datetime(x),
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "vol": float,
        "amount": float
    }
```

### 4. 数据库管理器（DBManager）

负责数据库连接和SQL执行。

#### 核心属性
- `connection_pool`：数据库连接池
- `database_config`：数据库配置信息

#### 核心方法
- `connect()`：连接数据库
- `execute(query, params)`：执行SQL查询
- `executemany(query, params_list)`：批量执行SQL
- `fetch(query, params)`：执行查询并返回结果
- `create_table(table_name, schema)`：创建表
- `table_exists(table_name)`：检查表是否存在

### 5. 任务管理器（TaskManager）

管理任务的注册、依赖关系和执行。

#### 核心属性
- `tasks`：注册的任务字典
- `db`：数据库管理器实例

#### 核心方法
- `register_task(task)`：注册新任务
- `get_task(name)`：获取任务实例
- `execute_task(name, start_date, end_date, **kwargs)`：执行指定任务
- `execute_tasks(names, start_date, end_date, **kwargs)`：执行多个任务
- `full_update(task_names, **kwargs)`：执行全量更新
- `incremental_update(task_names, days_lookback, **kwargs)`：执行增量更新

## 任务定义方式

采用声明式的方式定义任务，使用类属性来描述任务的各个方面：

1. **身份标识**：name, description
2. **数据源配置**：api_name, fields
3. **数据结构定义**：table_name, primary_keys, date_column, schema
4. **数据处理规则**：transformations, validations
5. **执行控制**：dependencies, retry_policy, timeout

## 系统使用示例

```python
async def main():
    # 创建数据库连接
    db = await DBManager.create("postgresql://user:password@localhost/finance")
    
    # 创建任务管理器
    task_manager = TaskManager(db)
    
    # 注册任务
    task_manager.register_task(StockDailyTask)
    task_manager.register_task(StockDailyBasicTask)
    
    # 执行全量更新
    await task_manager.full_update(["stock_daily", "stock_daily_basic"], 
                                  ts_code="000001.SZ,000002.SZ")
    
    # 执行增量更新（默认回溯7天）
    await task_manager.incremental_update(["stock_daily", "stock_daily_basic"], 
                                        days_lookback=7, 
                                        ts_code="000001.SZ,000002.SZ")
```

## 扩展性设计

### 添加新数据源

1. 创建新的数据源特定任务类，继承自Task
2. 实现特定的fetch_data方法
3. 创建使用该数据源的具体业务任务类

### 添加新任务

1. 创建新的任务类，继承自适当的数据源特定任务类
2. 定义必要的属性（name, table_name等）
3. 根据需要覆盖默认方法（process_data, validate_data等）

## 优势

1. **极简设计**：核心只有一个Task基类和数据源特定的子类
2. **声明式定义**：通过类属性清晰定义任务特性
3. **灵活性**：支持自定义数据处理和验证逻辑
4. **可扩展性**：轻松添加新数据源和新任务
5. **易用性**：提供快捷方法简化常见操作
6. **异步支持**：全面采用异步编程模型，提高性能

## 数据流程

### 数据导入流程
1. 用户通过TaskManager调用full_update方法
2. TaskManager根据任务名称获取相应的Task实例
3. TaskManager处理任务依赖关系，确保按正确顺序执行
4. 对每个Task，调用其execute方法
5. Task.execute调用fetch_data获取原始数据
6. Task.execute调用process_data处理数据
7. Task.execute调用validate_data验证数据
8. Task.execute调用save_data保存数据
9. TaskManager记录任务执行结果

### 增量更新流程
1. 用户通过TaskManager调用incremental_update方法
2. TaskManager根据任务名称获取相应的Task实例
3. TaskManager处理任务依赖关系，确保按正确顺序执行
4. 对每个Task，TaskManager计算需要更新的日期范围（最新日期+回溯天数到当前日期）
5. TaskManager调用Task的execute方法，传入计算的日期范围
6. 后续步骤与全量更新相同

## 错误处理与日志

1. 每个任务执行过程中的错误都会被捕获并记录
2. 系统提供了重试机制，可以配置重试次数和间隔
3. 详细的日志记录了任务执行的每个阶段和关键信息
4. 数据验证失败会生成特定的错误报告

## 未来扩展

1. **分布式执行**：支持在多个节点上并行执行任务
2. **任务调度**：集成定时调度功能
3. **监控仪表板**：提供Web界面监控任务执行状态
4. **数据质量报告**：自动生成数据质量评估报告
5. **版本控制**：支持数据和表结构的版本管理

这种设计完全符合第一性原理，聚焦于系统的本质功能，没有任何冗余或复杂性。
