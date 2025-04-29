# 用户指南

本指南将帮助您了解自动金融数据库管理系统的基本使用方法和功能。

## 目录

- [用户指南](#用户指南)
  - [目录](#目录)
  - [系统概述](#系统概述)
  - [开始使用](#开始使用)
    - [安装配置](#安装配置)
    - [初始设置](#初始设置)
  - [数据获取](#数据获取)
    - [支持的数据源](#支持的数据源)
    - [数据获取命令](#数据获取命令)
    - [数据更新策略](#数据更新策略)
  - [数据库连接管理](#数据库连接管理)
    - [DBManager概述](#dbmanager概述)
    - [连接管理](#连接管理)
    - [基本查询操作](#基本查询操作)
    - [批量操作](#批量操作)
    - [数据导入与更新](#数据导入与更新)
  - [数据任务基类](#数据任务基类)
    - [Task基类概述](#task基类概述)
    - [任务生命周期](#任务生命周期)
    - [实现自定义任务](#实现自定义任务)
    - [数据处理与验证](#数据处理与验证)
    - [智能增量更新](#智能增量更新)
  - [任务工厂](#任务工厂)
    - [TaskFactory概述](#taskfactory概述)
    - [任务注册与获取](#任务注册与获取)
    - [数据库连接管理](#数据库连接管理-1)
    - [任务配置管理](#任务配置管理)
  - [数据质量检查](#数据质量检查)
    - [运行质量检查](#运行质量检查)
    - [理解质量报告](#理解质量报告)
    - [图表生成](#图表生成)
    - [常见数据质量问题](#常见数据质量问题)
  - [常见使用场景](#常见使用场景)
    - [日常数据更新](#日常数据更新)
    - [新增数据类型](#新增数据类型)
    - [历史数据补充](#历史数据补充)
  - [批处理与自动化](#批处理与自动化)
  - [常见问题解答](#常见问题解答)
  - [故障排除](#故障排除)
    - [日志检查](#日志检查)
    - [常见错误代码](#常见错误代码)
    - [联系支持](#联系支持)

## 系统概述

自动金融数据库管理系统是一套用于自动化获取、处理、存储和验证金融数据的工具集。本系统支持多种金融数据类型，包括股票、基金、指数和宏观经济数据等。系统设计为模块化架构，便于扩展和定制。

主要功能包括：

- 从多个数据源自动获取最新数据
- 自动数据清洗和标准化处理
- 增量数据更新以减少重复获取
- 数据质量验证和异常检测
- 生成数据质量报告和可视化图表
- 批量处理和定时任务支持

## 开始使用

### 安装配置

1. 确保已安装必要的依赖：
   - Python 3.8+
   - PostgreSQL 12+

2. 安装系统：
   ```bash
   git clone https://github.com/yourusername/autoDatabase.git
   cd autoDatabase
   pip install -r requirements.txt
   ```

3. 配置数据库连接：
   
   创建`.env`文件并设置以下环境变量：
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=auto_finance
   DB_USER=your_username
   DB_PASSWORD=your_password
   ```

4. 配置数据源：
   
   在`.env`文件中添加数据源的API密钥和配置：
   ```
   TUSHARE_TOKEN=your_tushare_token
   # 其他数据源配置...
   ```

### 初始设置

1. 初始化数据库：
   ```bash
   python scripts/init_database.py
   ```

2. 配置任务设置：
   
   根据需要修改`config/tasks.json`文件，设置需要获取的数据类型和参数。

## 数据获取

### 支持的数据源

系统当前支持以下数据源：

1. **Tushare**：中国金融数据服务商，提供股票、基金、指数等数据
   - 数据类型：日线数据、财务数据、指数数据、基金数据等

2. **其他数据源**：可通过开发对应的适配器来支持更多数据源

### 数据获取命令

获取股票数据：
```bash
python scripts/tasks/stock/fetch_stock_daily.py --start_date 2023-01-01 --end_date 2023-12-31
```

获取基金数据：
```bash
python scripts/tasks/fund/fetch_fund_daily.py --start_date 2023-01-01 --end_date 2023-12-31
```

获取指数数据：
```bash
python scripts/tasks/index/fetch_index_daily.py --start_date 2023-01-01 --end_date 2023-12-31
```

获取财务数据：
```bash
python scripts/tasks/finance/fetch_financial_report.py --period 20231231
```

### 数据更新策略

系统支持多种数据更新策略：

1. **完全更新**：重新获取指定时间范围内的所有数据
   ```bash
   python scripts/tasks/stock/fetch_stock_daily.py --start_date 2023-01-01 --end_date 2023-12-31 --mode full
   ```

2. **增量更新**：仅获取数据库中不存在的数据
   ```bash
   python scripts/tasks/stock/fetch_stock_daily.py --start_date 2023-01-01 --end_date 2023-12-31 --mode incremental
   ```

3. **修复更新**：检查并修复数据缺失或异常的部分
   ```bash
   python scripts/tasks/stock/fetch_stock_daily.py --start_date 2023-01-01 --end_date 2023-12-31 --mode repair
   ```

## 数据库连接管理

### DBManager概述

`DBManager`是系统的核心组件，负责管理与PostgreSQL数据库的所有交互操作。它提供了一个统一的接口，用于执行数据库查询、事务管理和批量数据操作，同时处理连接池的创建和维护。系统的所有组件都通过`DBManager`来访问数据库，确保了数据库操作的一致性和安全性。

主要功能包括：

1. **连接池管理**：创建和维护数据库连接池，优化连接资源的使用
2. **查询执行**：提供执行SQL查询的标准化方法
3. **数据导入/导出**：支持从DataFrame导入数据和将查询结果导出为DataFrame
4. **事务管理**：支持数据库事务操作，确保数据一致性
5. **表结构操作**：提供创建表、检查表存在性等功能
6. **高级操作**：实现UPSERT（插入或更新）等高级数据库操作

### 连接管理

`DBManager`使用asyncpg库创建和管理连接池，支持异步数据库操作。这是一个典型的连接管理流程：

```python
from data_module import DBManager

async def example_db_operations():
    # 创建数据库管理器
    db_manager = DBManager("postgresql://username:password@localhost:5432/mydatabase")
    
    try:
        # 连接到数据库
        await db_manager.connect()
        
        # 执行数据库操作
        # ...
        
    finally:
        # 关闭数据库连接
        await db_manager.close()
```

在实际应用中，通常不需要直接创建`DBManager`实例，而是通过`TaskFactory`来获取它：

```python
from data_module import TaskFactory

async def example_task_factory():
    # 初始化TaskFactory（会自动创建DBManager）
    await TaskFactory.initialize()
    
    try:
        # 获取数据库管理器
        db_manager = TaskFactory.get_db_manager()
        
        # 使用数据库管理器执行操作
        # ...
        
    finally:
        # 关闭TaskFactory（会关闭DBManager）
        await TaskFactory.shutdown()
```

### 基本查询操作

`DBManager`提供了多种执行SQL查询的方法，适用于不同的场景：

#### 执行无返回结果的查询

```python
# 执行更新操作
await db_manager.execute("UPDATE stocks SET price = $1 WHERE symbol = $2", 100.5, "AAPL")
```

#### 获取所有查询结果

```python
# 获取所有符合条件的记录
rows = await db_manager.fetch("SELECT * FROM stocks WHERE price > $1", 100)
for row in rows:
    print(f"Symbol: {row['symbol']}, Price: {row['price']}")
```

#### 获取单行结果

```python
# 获取单行结果
row = await db_manager.fetch_one("SELECT * FROM stocks WHERE symbol = $1", "AAPL")
if row:
    print(f"Price: {row['price']}")
```

#### 获取单个值

```python
# 获取单个值
max_price = await db_manager.fetch_val("SELECT MAX(price) FROM stocks")
print(f"Maximum price: {max_price}")
```

### 批量操作

对于需要处理大量数据的情况，`DBManager`提供了高效的批量操作方法：

#### 批量执行SQL语句

```python
# 准备批量插入的数据
data = [
    ("AAPL", 150.5, "2023-01-01"),
    ("MSFT", 300.2, "2023-01-01"),
    ("GOOG", 2500.0, "2023-01-01")
]

# 批量插入数据
count = await db_manager.executemany(
    "INSERT INTO stocks (symbol, price, date) VALUES ($1, $2, $3)",
    data
)
print(f"插入了 {count} 条记录")
```

#### 从DataFrame导入数据

```python
import pandas as pd

# 创建示例DataFrame
df = pd.DataFrame({
    'symbol': ['AAPL', 'MSFT', 'GOOG'],
    'price': [150.5, 300.2, 2500.0],
    'date': ['2023-01-01', '2023-01-01', '2023-01-01']
})

# 将DataFrame数据复制到数据库表
rows_copied = await db_manager.copy_from_dataframe(df, "stocks")
print(f"复制了 {rows_copied} 行数据")
```

### 表管理

`DBManager`提供了表管理相关的方法：

#### 检查表是否存在

```python
# 检查表是否存在
exists = await db_manager.table_exists("stocks")
if exists:
    print("表已存在")
else:
    print("表不存在")
```

#### 获取表结构

```python
# 获取表结构
schema = await db_manager.get_table_schema("stocks")
for column in schema:
    print(f"列名: {column['column_name']}, 类型: {column['data_type']}")
```

#### 获取表中最新日期

```python
# 获取表中最新日期
latest_date = await db_manager.get_latest_date("stocks", "date")
print(f"最新日期: {latest_date}")
```

### 数据导入与更新

`DBManager`提供了强大的数据导入和更新功能，特别是通过`upsert`方法支持"插入或更新"操作：

#### UPSERT操作

UPSERT（"Update or Insert"的缩写）是一种数据库操作，当插入的记录与已有记录在指定的列上发生冲突时，可以选择更新部分列或忽略此次插入。

```python
import pandas as pd

# 创建示例DataFrame
df = pd.DataFrame({
    'symbol': ['AAPL', 'MSFT', 'GOOG'],
    'price': [155.5, 305.2, 2550.0],  # 价格已更新
    'date': ['2023-01-01', '2023-01-01', '2023-01-01']
})

# 执行UPSERT操作（冲突时更新）
affected_rows = await db_manager.upsert(
    table_name="stocks",
    data=df,
    conflict_columns=["symbol", "date"],  # 冲突检测的列
    update_columns=["price"]  # 冲突时要更新的列
)
print(f"更新了 {affected_rows} 行数据")
```

这个操作会根据`symbol`和`date`列检查是否存在冲突。如果存在，则更新`price`列；如果不存在，则插入新记录。

#### 自动添加时间戳

`upsert`方法支持自动添加时间戳列，记录数据的更新时间：

```python
# 执行UPSERT操作，并自动添加更新时间
affected_rows = await db_manager.upsert(
    table_name="stocks",
    data=df,
    conflict_columns=["symbol", "date"],
    update_columns=["price"],
    timestamp_column="update_time"  # 指定时间戳列
)
```

这会在插入或更新数据时，自动将`update_time`列设置为当前时间。

## 数据任务基类

### Task基类概述

`Task`是系统中所有数据任务的抽象基类，提供了统一的任务执行框架和生命周期管理。它定义了数据处理任务的标准流程，包括数据获取、处理、验证和保存等阶段。所有具体的数据任务都应继承自此基类，并根据需要实现或重写相应的方法。

`Task`基类的主要特点：

1. **统一的任务接口**：提供标准化的任务执行流程
2. **自动表管理**：支持表的自动创建和索引管理
3. **内置数据验证**：提供数据验证框架
4. **智能增量更新**：内置增量更新策略
5. **错误处理**：统一的错误处理机制
6. **自动时间戳**：自动添加更新时间戳
7. **任务类型标识**: 每个任务类都包含一个 `task_type` 类属性 (字符串)，用于区分不同类型的任务。默认值为 `'fetch'` (表示数据获取任务)。衍生数据计算任务的基类 (`BaseDerivativeTask`) 将此属性覆盖为 `'derivative'`。这有助于脚本根据需要筛选和执行特定类型的任务。

每个Task子类必须定义以下属性：
- `name`：任务的唯一标识符
- `table_name`：与任务关联的数据库表名

可选属性包括：
- `primary_keys`：表的主键列表
- `date_column`：日期列名（用于增量更新）
- `description`：任务描述
- `schema`：表结构定义

### 任务生命周期

`Task`的执行遵循一个标准的生命周期，包含以下阶段：

1. **预执行** (`pre_execute`)：任务执行前的准备工作
2. **数据获取** (`fetch_data`)：从数据源获取原始数据
3. **数据处理** (`process_data`)：处理和转换原始数据
4. **数据验证** (`validate_data`)：验证处理后的数据
5. **数据保存** (`save_data`)：将数据保存到数据库
6. **后执行** (`post_execute`)：任务完成后的清理工作

整个执行流程通过`execute`方法统一管理：

```python
async def task_execution_example():
    # 获取任务实例
    task = await TaskFactory.get_task("my_task")
    
    # 执行任务（会自动按顺序执行所有生命周期阶段）
    result = await task.execute(start_date="20230101", end_date="20230131")
    
    print(f"任务执行结果：{result}")
    # 输出示例：{'status': 'success', 'table': 'my_table', 'rows': 100}
```

如果任何阶段发生错误，任务会进入错误处理流程，返回错误信息。

### 实现自定义任务

创建自定义任务需要继承`Task`基类，并至少实现`fetch_data`方法。以下是一个完整的自定义任务示例：

```python
from data_module import Task, task_register
import pandas as pd

@task_register()  # 自动注册任务
class StockPriceTask(Task):
    """股票价格数据任务"""
    
    # 必需属性
    name = "stock_price"
    table_name = "stock_prices"
    
    # 可选属性
    primary_keys = ["symbol", "date"]
    date_column = "date"
    description = "获取和存储股票价格数据"
    
    # 表结构定义
    schema = {
        "symbol": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": "FLOAT",
        "high": "FLOAT",
        "low": "FLOAT",
        "close": "FLOAT",
        "volume": "BIGINT"
    }
    
    # 数据转换定义
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int
    }
    
    # 数据验证函数列表
    validations = [
        lambda df: len(df) > 0,  # 确保数据不为空
        lambda df: df["close"].min() > 0  # 确保收盘价大于0
    ]
    
    # 必须实现的方法：获取数据
    async def fetch_data(self, **kwargs):
        """从数据源获取股票价格数据"""
        self.logger.info(f"获取股票价格数据，参数: {kwargs}")
        
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        symbols = kwargs.get("symbols", ["AAPL", "MSFT", "GOOG"])
        
        # 在实际应用中，这里应该是从API或其他数据源获取数据
        # 这里仅用模拟数据作为示例
        data = []
        for symbol in symbols:
            # 创建模拟数据
            data.append({
                "symbol": symbol,
                "date": "2023-01-01",
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.5,
                "volume": 1000000
            })
        
        return pd.DataFrame(data)
    
    # 可选：重写数据处理方法（如果默认的处理不满足需求）
    def process_data(self, data):
        """处理获取的数据"""
        # 首先应用基类的处理
        data = super().process_data(data)
        
        # 添加额外的处理逻辑
        if "adj_close" not in data.columns:
            data["adj_close"] = data["close"]  # 添加新列
        
        return data
    
    # 可选：重写数据验证方法
    def validate_data(self, data):
        """验证数据有效性"""
        # 首先应用基类的验证
        basic_validation = super().validate_data(data)
        
        # 添加额外的验证逻辑
        if not basic_validation:
            return False
            
        # 例如，确保所有必需列都存在
        required_columns = ["symbol", "date", "close"]
        for col in required_columns:
            if col not in data.columns:
                self.logger.warning(f"缺少必需列: {col}")
                return False
                
        return True

### 数据处理与验证

`Task`基类提供了强大的数据处理和验证框架：

#### 数据处理

数据处理通过`process_data`方法实现，它会自动应用`transformations`中定义的转换函数：

```python
# 在Task子类中定义数据转换
transformations = {
    "price": float,
    "volume": int,
    "percent_change": lambda x: float(x.strip('%')) / 100
}
```

这些转换会按顺序应用到对应的列。转换过程中会自动处理缺失值(NaN)，确保数据处理的健壮性。

#### 数据验证

数据验证通过`validate_data`方法实现，它会应用`validations`中定义的验证函数：

```python
# 在Task子类中定义数据验证
validations = [
    # 确保数据不为空
    lambda df: len(df) > 0,
    
    # 确保价格为正数
    lambda df: (df["price"] > 0).all(),
    
    # 自定义复杂验证
    lambda df: check_data_consistency(df)
]

# 外部定义的复杂验证函数
def check_data_consistency(df):
    # 确保最高价不低于收盘价
    if (df["high"] < df["close"]).any():
        return False
    # 确保最低价不高于收盘价
    if (df["low"] > df["close"]).any():
        return False
    return True
```

验证失败不会中断任务执行，但会被记录并影响最终的任务状态。

### 智能增量更新

`Task`基类提供了`smart_incremental_update`方法，支持基于日期的智能增量更新：

```python
async def example_incremental_update():
    # 获取任务实例
    task = await TaskFactory.get_task("stock_price")
    
    # 方法1：基于自然日的增量更新（回溯30天）
    result1 = await task.smart_incremental_update(
        lookback_days=30,      # 回溯30个自然日
        use_trade_days=False,  # 使用自然日
        symbols=["AAPL", "MSFT", "GOOG"]  # 其他参数
    )
    
    # 方法2：基于交易日的增量更新（回溯20个交易日）
    result2 = await task.smart_incremental_update(
        lookback_days=20,     # 回溯20个交易日
        use_trade_days=True,  # 使用交易日
        symbols=["AAPL", "MSFT", "GOOG"]  # 其他参数
    )
    
    # 方法3：自动增量更新（从上次更新日期开始）
    result3 = await task.smart_incremental_update(
        symbols=["AAPL", "MSFT", "GOOG"]  # 其他参数
    )
```

`smart_incremental_update`方法会自动：

1. 检查数据库中最新数据的日期
2. 根据指定的策略确定更新的起止日期
3. 执行任务获取并保存新数据
4. 处理交易日历（当`use_trade_days=True`时）

这使得数据更新过程更加智能和高效，避免了不必要的数据获取和处理。

## 任务工厂

### TaskFactory概述

`TaskFactory` 是系统任务管理的核心枢纽。它负责以下关键功能：

1.  **任务注册**：自动发现并注册所有通过 `@task_register()` 装饰器标记的 `Task` 子类。
2.  **任务实例化**：根据任务名称创建具体的任务实例。
3.  **数据库连接共享**：管理全局的 `DBManager` 实例，确保所有任务共享同一个数据库连接池。
4.  **配置管理**：加载和提供任务所需的配置信息（未来可能实现）。

通过 `TaskFactory`，系统可以统一管理所有的任务，简化了任务的调用和执行流程。开发者只需要定义好任务类并使用装饰器注册，`TaskFactory` 就能自动将其纳入管理。

### 任务注册与获取

任务的注册通过 `@task_register()` 装饰器自动完成。开发者在定义 `Task` 子类时，只需要添加这个装饰器即可：

```python
from data_module.base_task import Task
from data_module.task_decorator import task_register

@task_register()
class MyTask(Task):
    # ... 任务实现 ...
```

一旦任务被注册，可以通过 `TaskFactory` 来获取它们：

1.  **获取所有已注册任务的名称列表**：
    ```python
    from data_module import TaskFactory

    async def get_all_tasks():
        await TaskFactory.initialize() # 确保已初始化
        task_names = await TaskFactory.get_all_task_names()
        print(f"所有已注册的任务: {task_names}")
        await TaskFactory.shutdown()
    ```
    这对于需要批量执行所有可用任务的场景（如 `scripts/batch/update_all_tasks.py`）非常有用。

2.  **获取特定任务的实例**：
    ```python
    async def run_specific_task():
        await TaskFactory.initialize()
        task_instance = await TaskFactory.get_task("stock_daily_adj") # 获取任务实例
        if task_instance:
            # 执行任务
            await task_instance.execute(start_date="2024-01-01", end_date="2024-01-31")
        await TaskFactory.shutdown()
    ```

### 数据库连接管理

`TaskFactory` 负责集中管理数据库连接，确保系统中只有一个活跃的数据库连接实例，避免连接泄漏和资源浪费。

```python
# 初始化数据库连接
await TaskFactory.initialize(db_url="postgresql://user:pass@localhost:5432/db")

# 获取数据库管理器实例
db_manager = TaskFactory.get_db_manager()

# 执行数据库操作
result = await db_manager.fetch("SELECT * FROM my_table LIMIT 10")

# 关闭数据库连接
await TaskFactory.shutdown()
```

数据库连接参数默认从配置文件或环境变量中读取，也可以在初始化时显式指定：

- 从环境变量：`DATABASE_URL`
- 从配置文件：`config.json` 中的 `database.url`
- 显式指定：`TaskFactory.initialize(db_url=...)`

### 任务配置管理

`TaskFactory` 负责从配置文件加载任务特定的配置参数，并在创建任务实例时应用这些配置。

配置文件结构 (`data_module/config.json`)：

```json
{
  "database": {
    "url": "postgresql://user:pass@localhost:5432/db"
  },
  "api": {
    "tushare_token": "your_token"
  },
  "tasks": {
    "tushare_stock_daily": {
      "concurrent_limit": 5,
      "page_size": 1000,
      "retry_times": 3
    },
    "tushare_fund_nav": {
      "concurrent_limit": 3,
      "page_size": 500
    }
  }
}
```

获取任务配置的方法：

```python
from data_module.task_factory import get_task_config

# 获取特定任务的完整配置
fund_nav_config = get_task_config("tushare_fund_nav")

# 获取特定配置项，并指定默认值
concurrent_limit = get_task_config("tushare_fund_nav", "concurrent_limit", 2)
```

### 按类型获取任务名称

`TaskFactory` 提供了一个方便的方法来获取特定类型的已注册任务的名称列表。

```python
from data_module.task_factory import TaskFactory

async def example_get_tasks_by_type():
    await TaskFactory.initialize()
    
    try:
        # 获取所有数据获取任务的名称
        fetch_task_names = TaskFactory.get_task_names_by_type('fetch')
        print(f"数据获取任务: {fetch_task_names}")
        
        # 获取所有衍生数据计算任务的名称
        derivative_task_names = TaskFactory.get_task_names_by_type('derivative')
        print(f"衍生数据任务: {derivative_task_names}")
        
        # 可以根据名称列表获取并执行任务
        for task_name in fetch_task_names:
            task = await TaskFactory.get_task(task_name)
            # ... 执行 task.execute() 或 task.smart_incremental_update() ...
            
    finally:
        await TaskFactory.shutdown()
```

这个功能对于区分数据获取和数据计算任务、按顺序执行任务或为不同类型的任务应用不同的执行逻辑（例如传递不同的参数）非常有用。

## 数据质量检查

### 运行质量检查

现在系统提供了全面的数据质量检查功能，可以通过`scripts/check_db_quality.py`脚本运行。该脚本使用`DataQualityChecker`类来分析数据库中表的质量，包括：

- 检查数据完整性和空值率
- 分析日期覆盖情况
- 识别数据质量问题
- 生成可视化报告和图表

脚本用法示例：

```bash
# 检查所有表
python scripts/check_db_quality.py

# 检查特定表
python scripts/check_db_quality.py -t tushare_stock_daily tushare_fund_daily

# 检查特定日期范围
python scripts/check_db_quality.py -s 20220101 -e 20221231

# 指定输出目录
python scripts/check_db_quality.py -o ./logs/custom_quality_check

# 显示详细日志
python scripts/check_db_quality.py -v
```

参数说明：
- `-t, --tables`: 指定要检查的表名列表（用空格分隔多个表名）
- `-s, --start-date`: 指定检查的开始日期（YYYYMMDD格式）
- `-e, --end-date`: 指定检查的结束日期（YYYYMMDD格式）
- `-o, --output-dir`: 指定输出目录路径
- `-v, --verbose`: 启用详细日志输出
- `-h, --help`: 显示帮助信息

### 理解质量报告

执行检查后，脚本会在指定目录（默认为`logs/db_quality_<timestamp>`）生成以下文件：

1. `detailed_results.json` - 详细的检查结果，包含每个表的完整分析数据
2. `summary.json` - 摘要信息，包括问题表的列表和问题分类
3. `report.html` - 可视化HTML报告，包含图表和统计数据

HTML报告包括：
- 总体摘要（检查表总数、成功率、发现问题）
- 问题表列表及问题类型（如高空值率、低日期覆盖率）
- 每个表的详细检查结果
- 空值分析和统计
- 日期覆盖率分析
- 月度数据完整性分析
- 可视化图表

### 图表生成

对于带有日期列的表，系统会自动生成两种图表：

1. **日期覆盖图表** - 显示每个日期的记录数量，用于识别数据间隔和缺失
   - 文件命名：`<表名>_date_coverage.png`
   - 展示：条形图，x轴为日期，y轴为记录数

2. **月度覆盖率图表** - 显示每月的数据覆盖百分比，用于识别长期趋势和缺失
   - 文件命名：`<表名>_monthly_coverage.png`
   - 展示：条形图，x轴为月份，y轴为覆盖率百分比

这些图表保存在输出目录的`charts`子目录中，并在HTML报告中引用。通过这些图表，用户可以快速识别：
- 哪些日期完全缺失数据
- 哪些月份的数据覆盖率异常低
- 数据覆盖的长期趋势
- 数据更新的季节性模式

### 常见数据质量问题

系统可以识别的常见问题包括：

- **高空值率** - 当某列的空值比例超过10%（标记为警告）或20%（标记为严重）
- **低日期覆盖率** - 当日期覆盖率低于90%
- **月度数据不均衡** - 当某些月份的覆盖率明显低于其他月份
- **处理错误** - 检查过程中发生的任何错误

## 常见使用场景

### 日常数据更新

每日数据更新的推荐流程：

1. 执行增量更新获取最新数据
   ```bash
   python scripts/batch/daily_update.py
   ```

2. 运行质量检查确保数据正确
   ```bash
   python scripts/check_db_quality.py -s $(date -d "7 days ago" +%Y%m%d) -e $(date +%Y%m%d)
   ```

3. 检查质量报告并处理异常

### 新增数据类型

当需要新增一种数据类型时：

1. 确认数据源是否支持该数据类型
2. 检查系统中是否已有对应的任务模块
3. 如果没有，参考[开发者指南](developer_guide.md)创建新的任务类型
4. 首次获取全部历史数据
5. 配置定期更新计划

### 历史数据补充

当需要补充历史数据时：

1. 确定需要补充的时间范围和数据类型
2. 使用修复模式获取数据
   ```bash
   python scripts/tasks/stock/fetch_stock_daily.py --start_date 2020-01-01 --end_date 2020-12-31 --mode repair
   ```
3. 运行质量检查验证补充效果
4. 如有必要，手动处理特殊异常情况

## 批处理与自动化

系统提供多种批处理脚本用于自动化数据管理：

1. **每日数据更新**
   ```bash
   python scripts/batch/daily_update.py
   ```

2. **周度数据质量检查**
   ```bash
   python scripts/batch/weekly_quality_check.py
   ```

3. **月度全面更新**
   ```bash
   python scripts/batch/monthly_full_update.py
   ```

配置定时任务（使用crontab）：
```
# 每天早上6点执行数据更新
0 6 * * * cd /path/to/autoDatabase && python scripts/batch/daily_update.py

# 每周一早上8点执行数据质量检查
0 8 * * 1 cd /path/to/autoDatabase && python scripts/batch/weekly_quality_check.py

# 每月1日凌晨2点执行全面更新
0 2 1 * * cd /path/to/autoDatabase && python scripts/batch/monthly_full_update.py
```

## 常见问题解答

1. **问题**：数据更新失败，出现API调用错误
   
   **解决方案**：
   - 检查API密钥是否正确
   - 确认API请求限制是否已达上限
   - 检查网络连接状态
   - 查看`.env`文件中的配置是否正确

2. **问题**：数据质量报告显示大量缺失数据
   
   **解决方案**：
   - 确认目标日期是否为交易日
   - 检查数据源是否提供该时间段的数据
   - 尝试使用修复模式重新获取数据
   - 检查数据库连接和表结构是否正确

3. **问题**：系统运行速度变慢
   
   **解决方案**：
   - 优化数据库索引
   - 增加数据库连接池大小
   - 减少单次请求的数据量
   - 检查服务器资源使用情况

4. **问题**：质量检查工具报错
   
   **解决方案**：
   - 检查日志了解详细错误信息
   - 确保数据库连接正常
   - 验证表结构是否符合预期
   - 使用`-v`参数运行以查看详细日志

5. **问题**：TaskFactory初始化失败
   
   **解决方案**：
   - 确认数据库连接字符串是否正确
   - 检查数据库服务是否正常运行
   - 查看日志文件中的详细错误信息
   - 确保没有多次调用initialize()而忘记shutdown()

6. **问题**：获取任务实例失败
   
   **解决方案**：
   - 确认任务名称是否正确
   - 检查任务类是否正确注册
   - 确保先调用TaskFactory.initialize()
   - 查看任务导入路径是否正确

## 故障排除

### 日志检查

系统日志位于`logs/`目录下，按日期和功能分类：

```
logs/
├── api_calls/           # API调用日志
├── db_operations/       # 数据库操作日志
├── db_quality/          # 数据质量检查日志
└── error/               # 错误日志
```

检查错误日志：
```bash
tail -n 100 logs/error/latest.log
```

### 常见错误代码

- **E001**：数据源连接失败
- **E002**：数据库操作失败
- **E003**：数据验证错误
- **E004**：参数配置错误
- **E005**：系统资源不足
- **E006**：TaskFactory初始化错误
- **E007**：任务注册或获取错误

### 联系支持

如果遇到无法解决的问题，请联系技术支持团队：

- 邮件：support@example.com
- 问题报告：https://github.com/yourusername/autoDatabase/issues