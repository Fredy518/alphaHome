# 金融数据任务系统用户指南

## 目录

1. [系统概述](#系统概述)
2. [基本任务类型](#基本任务类型)
3. [创建标准数据任务](#创建标准数据任务)
4. [创建特殊数据任务](#创建特殊数据任务)
   - [非标准参数API的任务创建](#1-非标准参数api的任务创建)
   - [重写execute方法处理特殊执行流程](#2-重写execute方法处理特殊执行流程)
   - [适配增量更新机制](#3-适配增量更新机制)
   - [实际使用案例：财务报表数据任务](#4-实际使用案例财务报表数据任务)
   - [使用特殊任务的示例](#5-使用特殊任务的示例)
5. [数据处理和验证](#数据处理和验证)
   - [数据处理](#1-数据处理)
   - [数据验证](#2-数据验证)
   - [验证策略](#3-验证策略)
6. [任务执行和调度](#任务执行和调度)
   - [任务执行方式](#1-任务执行方式)
   - [任务依赖管理](#2-任务依赖管理)
   - [批量任务执行](#3-批量任务执行)
   - [错误处理和重试](#4-错误处理和重试)
   - [定时调度](#5-定时调度)
7. [扩展和定制](#扩展和定制)
   - [添加新数据源](#1-添加新数据源)
   - [自定义数据处理组件](#2-自定义数据处理组件)
   - [自定义数据验证器](#3-自定义数据验证器)
8. [最佳实践](#最佳实践)
   - [任务设计原则](#1-任务设计原则)
   - [数据处理最佳实践](#2-数据处理最佳实践)
   - [性能优化建议](#3-性能优化建议)
   - [错误处理和日志记录](#4-错误处理和日志记录)
   - [系统维护和监控](#5-系统维护和监控)
   - [API 速率限制管理](#6-api-速率限制管理)

## 系统概述

金融数据任务系统是一个灵活、模块化的框架，用于获取、处理和存储金融数据。系统基于任务模型，每个数据获取和处理过程都被封装为一个独立的任务。

系统的核心特点包括：

- **声明式定义**：任务通过类属性定义，清晰直观
- **高内聚低耦合**：每个任务包含完整的数据获取、处理和存储逻辑
- **异步执行**：利用Python的异步特性提高效率
- **扩展性**：易于添加新的数据源和任务类型

## 基本任务类型

系统中的任务分为以下几种类型：

### 基础任务类（Task）

所有数据任务的基类，定义了任务的通用接口和生命周期。Task类是整个系统的核心，它提供了数据获取、处理、验证和存储的标准流程。

#### 核心属性

| 属性名 | 类型 | 描述 |
|--------|------|------|
| name | str | 任务名称，作为唯一标识符 |
| description | str | 任务描述 |
| table_name | str | 数据表名 |
| primary_keys | List[str] | 主键字段列表 |
| date_column | str | 日期字段名，用于增量更新 |
| schema | Dict | 表结构定义，格式: {"col_name": {"type": "SQL_TYPE", "constraints": "..."}} |
| column_mapping | Dict | 源数据到数据库列名的映射，例如: {'source_field': 'db_column'} |
| transformations | Dict | 列名到转换函数的映射，例如: {"col_name": float} |
| validations | List[Callable] | 验证函数列表，每个函数接收DataFrame并返回布尔值 |
| dependencies | List[str] | 依赖的其他任务列表 |

#### 核心方法

| 方法名 | 描述 | 是否需要子类实现 |
|--------|------|----------------|
| execute | 执行任务的完整生命周期 | 否，基类已实现 |
| fetch_data | 获取原始数据 | 是，子类必须实现 |
| process_data | 处理原始数据 | 否，可选重写 |
| validate_data | 验证数据有效性 | 否，可选重写 |
| save_data | 将处理后的数据保存到数据库 | 否，基类已实现 |
| full_update | 执行全量数据更新 | 否，基类已实现 |
| incremental_update | 执行增量数据更新 | 否，基类已实现 |
| fetch_batch | 获取单个批次的数据 | 否，基类已实现 |

#### 任务执行流程

1. **初始化**：创建任务实例，连接数据库
2. **执行**：调用`execute`方法，该方法会依次执行以下步骤：
   - 调用`fetch_data`获取原始数据
   - 调用`process_data`处理数据
   - 调用`validate_data`验证数据
   - 调用`save_data`保存数据
3. **更新**：可以使用`full_update`或`incremental_update`方法执行更新

#### 使用示例

```python
# 创建自定义任务类
class MyCustomTask(Task):
    name = "my_custom_task"
    table_name = "my_data_table"
    primary_keys = ["id", "date"]
    date_column = "date"
    
    async def fetch_data(self, start_date, end_date, **kwargs):
        # 实现数据获取逻辑
        # 返回pandas DataFrame
        return pd.DataFrame(...)
        
# 使用任务类
async def main():
    # 创建任务实例
    task = MyCustomTask(db_manager)
    
    # 执行任务
    result = await task.execute(
        start_date="20220101", 
        end_date="20220131",
        other_param="value"  # 其他参数
    )
    
    print(f"任务执行结果: {result}")
```

### 数据源特定任务类

系统设计为支持多种数据源，每种数据源都有对应的特定任务基类。这些特定任务基类继承自`Task`基类，并实现了与特定数据源交互所需的功能。

#### TushareTask 类

TushareTask是专门用于从Tushare金融数据API获取数据的任务基类。它提供了批量处理、参数准备和数据获取的标准实现。

##### 特有属性

| 属性名 | 类型 | 描述 |
|--------|------|------|
| api_name | str | Tushare API名称，对应要调用的接口 |
| fields | List[str] | 需要从API获取的字段列表 |
| default_concurrent_limit | int | 默认并发请求限制，默认为5 |
| default_page_size | int | 默认每页数据量，默认为2000 |
| default_page_interval | float | 默认页面请求间隔（秒），默认为0.5 |

##### 核心方法

| 方法名 | 描述 | 是否需要子类实现 |
|--------|------|----------------|
| get_batch_list | 生成批处理参数列表 | 是，子类必须实现 |
| prepare_params | 准备API调用参数 | 是，子类必须实现 |
| fetch_batch | 获取单个批次的数据 | 否，基类已实现 |
| fetch_data | 从Tushare获取数据（不推荐直接使用） | 否，基类已实现 |
| process_data | 处理从Tushare获取的数据 | 否，可选重写 |

##### get_batch_list 方法

这个方法负责将用户的查询参数转换为一系列批处理参数，每个批处理参数将用于一次API调用。主要目的是将大型查询分解为多个小型查询，以避免超出API限制或提高并行性。

实现此方法时，应考虑以下几点：

1. 如何根据日期范围、股票代码列表等参数拆分成多个批次
2. 每个批次的大小应适中，既不会导致单次请求数据量过大，也不会产生过多的请求次数
3. 对于小数据量查询，可以直接返回单个批次，避免不必要的拆分

```python
def get_batch_list(self, **kwargs) -> List[Dict]:
    """示例实现：按日期和股票代码分批"""
    # 提取参数
    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')
    ts_code = kwargs.get('ts_code')
    
    # 处理股票代码
    ts_codes = []
    if ts_code:
        if isinstance(ts_code, str):
            ts_codes = ts_code.split(',')
        elif isinstance(ts_code, list):
            ts_codes = ts_code
    
    # 小数据量查询直接返回单个批次
    if len(ts_codes) <= 3:
        return [{'start_date': start_date, 'end_date': end_date, 'ts_code': ','.join(ts_codes)}]
    
    # 大数据量查询按股票代码分批
    batch_list = []
    for i in range(0, len(ts_codes), 10):
        batch_codes = ts_codes[i:i+10]
        batch_list.append({
            'start_date': start_date,
            'end_date': end_date,
            'ts_code': ','.join(batch_codes)
        })
    
    return batch_list
```

##### prepare_params 方法

这个方法负责将批处理参数转换为Tushare API调用所需的确切参数格式。主要目的是处理参数格式转换、默认值设置、参数验证等工作。

实现此方法时，应考虑以下几点：

1. Tushare API的具体参数要求（参数名称、格式等）
2. 必要时进行参数格式转换（如日期格式转换为'YYYYMMDD'）
3. 添加API调用所需的任何额外参数（如字段列表）

```python
def prepare_params(self, batch_params: Dict) -> Dict:
    """示例实现：准备API调用参数"""
    # 复制参数字典，避免修改原始字典
    api_params = batch_params.copy()
    
    # 处理日期格式
    for date_field in ['start_date', 'end_date']:
        if date_field in api_params and isinstance(api_params[date_field], datetime):
            api_params[date_field] = api_params[date_field].strftime('%Y%m%d')
    
    # 添加字段列表
    if 'fields' not in api_params and self.fields:
        api_params['fields'] = ','.join(self.fields)
    
    return api_params
```

##### 使用示例

```python
# 创建Tushare任务子类
class StockDailyTask(TushareTask):
    """股票日线数据任务"""
    
    # 任务标识
    name = "stock_daily"
    table_name = "stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # Tushare特有属性
    api_name = "daily"  # Tushare的日线数据接口
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    
    # 实现必要的方法
    def get_batch_list(self, **kwargs):
        # 实现批处理参数生成逻辑
        pass
    
    def prepare_params(self, batch_params):
        # 实现参数准备逻辑
        pass

# 使用任务类
async def main():
    # 创建任务实例
    task = StockDailyTask(db_manager, api_token="your_tushare_token")
    
    # 执行任务
    result = await task.execute(
        start_date="20220101", 
        end_date="20220131",
        ts_code="000001.SZ,000002.SZ",
        concurrent_limit=5  # 并发处理5个批次
    )
    
    print(f"获取到 {result['rows']} 行数据")
```

### 具体业务任务类

具体业务任务类继承自数据源特定任务类（如TushareTask），并实现特定业务需求的数据获取和处理逻辑。这些类是系统中实际使用的任务实现。

#### 设计原则

在设计具体业务任务类时，应遵循以下原则：

1. **声明式定义**：使用类属性来定义任务的各个方面
2. **单一职责**：每个任务类应专注于一种特定类型的数据
3. **完整定义**：包含所有必要的元数据，如表名、主键、日期列等
4. **数据转换**：定义清晰的数据转换规则

#### 示例：股票日线数据任务

```python
class StockDailyTask(TushareTask):
    """股票日线数据任务"""
    
    # 任务标识
    name = "stock_daily"
    description = "股票日线行情数据"
    # 数据库表定义
    table_name = "stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # Tushare特有属性
    api_name = "daily"  # Tushare API名称
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    
    # 列名映射（API字段名 -> 数据库字段名）
    column_mapping = {
        "vol": "volume"  # 将API中的'vol'字段映射为数据库中的'volume'字段
    }
    
    # 数据转换规则
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,  # 注意这里使用的是映射后的字段名
        "amount": float
    }
    
    # 数据验证规则
    validations = [
        lambda df: all(df["close"] >= 0),
        lambda df: all(df["high"] >= df["low"]),
        lambda df: all(df["volume"] >= 0),
        lambda df: all(df["amount"] >= 0)
    ]
    
    def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表"""
        # 提取参数
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        ts_code = kwargs.get('ts_code')
        
        # 处理股票代码列表
        ts_codes = []
        if ts_code:
            if isinstance(ts_code, str):
                ts_codes = ts_code.split(',')
            elif isinstance(ts_code, list):
                ts_codes = ts_code
        
        # 分批处理股票代码
        batch_size = 10  # 每批处理的股票数量
        batch_list = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            batch_list.append({
                'start_date': start_date,
                'end_date': end_date,
                'ts_code': ','.join(batch_codes)
            })
        
        return batch_list
    
    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数"""
        # 复制参数字典
        api_params = batch_params.copy()
        
        # 处理日期格式
        for date_field in ['start_date', 'end_date']:
            if date_field in api_params and api_params[date_field]:
                if isinstance(api_params[date_field], datetime):
                    api_params[date_field] = api_params[date_field].strftime('%Y%m%d')
                elif isinstance(api_params[date_field], str):
                    try:
                        date_obj = pd.to_datetime(api_params[date_field])
                        api_params[date_field] = date_obj.strftime('%Y%m%d')
                    except:
                        pass
        
        # 添加字段列表
        if self.fields and 'fields' not in api_params:
            api_params['fields'] = ','.join(self.fields)
            
        return api_params
```

#### 更多任务类示例

##### 股票基本信息任务

```python
class StockBasicTask(TushareTask):
    """股票基本信息任务"""
    
    name = "stock_basic"
    table_name = "stock_basic"
    primary_keys = ["ts_code"]
    date_column = "list_date"  # 上市日期
    
    api_name = "stock_basic"
    fields = ["ts_code", "symbol", "name", "area", "industry", "list_date"]
    
    def get_batch_list(self, **kwargs) -> List[Dict]:
        # 股票基本信息通常不需要分批，直接返回单个批次
        return [kwargs]
    
    def prepare_params(self, batch_params: Dict) -> Dict:
        api_params = batch_params.copy()
        
        # 设置默认交易所参数
        if 'exchange' not in api_params:
            api_params['exchange'] = ''  # 默认获取所有交易所
            
        # 设置默认上市状态
        if 'list_status' not in api_params:
            api_params['list_status'] = 'L'  # 默认只获取上市股票
            
        # 添加字段列表
        if self.fields and 'fields' not in api_params:
            api_params['fields'] = ','.join(self.fields)
            
        return api_params
```

##### 财务指标任务

```python
class FinancialIndicatorTask(TushareTask):
    """财务指标任务"""
    
    name = "financial_indicator"
    table_name = "financial_indicator"
    primary_keys = ["ts_code", "end_date"]
    date_column = "end_date"
    
    api_name = "fina_indicator"
    fields = ["ts_code", "end_date", "eps", "bps", "roe", "current_ratio", "quick_ratio"]
    
    # 数据转换
    transformations = {
        "eps": float,
        "bps": float,
        "roe": float,
        "current_ratio": float,
        "quick_ratio": float
    }
    
    def process_data(self, data):
        """处理财务指标数据"""
        # 调用父类的处理方法
        data = super().process_data(data)
        
        if data.empty:
            return data
        
        # 计算额外指标
        if "roe" in data.columns and "eps" in data.columns:
            data["quality_factor"] = data["roe"] * data["eps"]
        
        return data
```

#### 使用示例

以下是一个完整的使用示例，展示如何创建和使用多个任务类来获取和处理数据：

```python
import asyncio
from database import create_db_connection

async def main():
    # 创建数据库连接
    db = await create_db_connection("postgresql://user:password@localhost:5432/finance_db")
    
    # 创建任务实例
    stock_basic_task = StockBasicTask(db, api_token="your_tushare_token")
    stock_daily_task = StockDailyTask(db, api_token="your_tushare_token")
    
    # 1. 获取股票基本信息
    basic_result = await stock_basic_task.execute()
    print(f"获取到 {basic_result['rows']} 支股票的基本信息")
    
    # 2. 获取前5支股票的日线数据
    stocks = await db.fetch("SELECT ts_code FROM stock_basic LIMIT 5")
    ts_codes = [stock['ts_code'] for stock in stocks]
    
    # 3. 获取这些股票的最近30天数据
    from datetime import datetime, timedelta
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    
    daily_result = await stock_daily_task.execute(
        start_date=start_date,
        end_date=end_date,
        ts_code=','.join(ts_codes),
        concurrent_limit=5  # 并发处理5个批次
    )
    
    print(f"获取到 {daily_result['rows']} 行日线数据")
    
    # 关闭数据库连接
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## 创建标准数据任务

本节介绍如何创建和使用标准数据任务，以获取和处理常规金融数据。

### 创建标准数据任务的步骤

#### 1. 定义任务类

创建一个新的任务类，继承自适当的数据源特定任务类：

```python
from data_module.task import TushareTask
import pandas as pd

class StockDailyBasicTask(TushareTask):
    """股票每日指标任务"""
    
    # 任务标识
    name = "stock_daily_basic"
    description = "获取股票每日基本面指标"
    
    # 数据库表定义
    table_name = "stock_daily_basic"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # Tushare特有属性
    api_name = "daily_basic"
    fields = ["ts_code", "trade_date", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_mv", "circ_mv"]
    
    # 数据转换规则
    transformations = {
        "trade_date": lambda x: pd.to_datetime(x),
        "pe": float,
        "pe_ttm": float,
        "pb": float,
        "ps": float,
        "ps_ttm": float,
        "dv_ratio": float,
        "dv_ttm": float,
        "total_mv": float,
        "circ_mv": float
    }
```

#### 2. 实现数据处理逻辑

根据需要重写`process_data`方法，实现自定义的数据处理逻辑：

```python
def process_data(self, data):
    """处理股票每日指标数据"""
    # 调用父类的处理方法
    data = super().process_data(data)
    
    if data.empty:
        return data
    
    # 应用数据转换规则
    for column, transform_func in self.transformations.items():
        if column in data.columns:
            data[column] = data[column].apply(transform_func)
    
    # 计算额外指标
    if "pe" in data.columns and "pb" in data.columns:
        data["peg"] = data["pe"] / data["pb"]
    
    return data
```

#### 3. 实现数据验证逻辑

重写`validate_data`方法，实现自定义的数据验证逻辑：

```python
def validate_data(self, data):
    """验证股票每日指标数据"""
    if data.empty:
        self.logger.warning("数据为空")
        return True
    
    # 验证必要字段
    required_columns = ["ts_code", "trade_date", "pe", "pb"]
    for col in required_columns:
        if col not in data.columns:
            raise ValueError(f"缺少必要字段: {col}")
    
    # 验证数值范围
    if (data["pe"] < 0).any():
        self.logger.warning("存在负PE值")
    
    if (data["pb"] < 0).any():
        self.logger.warning("存在负PB值")
    
    return True
```

#### 4. 设置任务依赖关系

如果任务依赖于其他任务，可以通过`dependencies`属性定义依赖关系：

```python
class StockDailyBasicTask(TushareTask):
    # 其他属性...
    
    # 依赖于StockDailyTask
    dependencies = ["stock_daily"]
```

### 使用标准数据任务

#### 1. 创建任务实例

```python
# 创建数据库管理器
db_manager = DBManager(connection_string)

# 创建API客户端
api_client = TushareAPI(api_token="your_tushare_token")

# 创建任务实例
stock_daily_task = StockDailyTask(db_manager, api_client)
stock_daily_basic_task = StockDailyBasicTask(db_manager, api_client)
```

#### 2. 执行任务

```python
# 执行股票日线数据任务
await stock_daily_task.execute(
    start_date="20220101",
    end_date="20220131",
    ts_code="000001.SZ",
    concurrent_limit=5  # 并发处理5个批次
)

# 执行股票每日指标任务
await stock_daily_basic_task.execute(
    start_date="20220101",
    end_date="20220131",
    ts_code="000001.SZ"
)
```

#### 3. 使用快捷方法

系统提供了一些快捷方法，简化常见操作：

```python
# 执行全量更新
await stock_daily_task.full_update(ts_code="000001.SZ")

# 执行智能增量更新
await stock_daily_basic_task.incremental_update(ts_code="000001.SZ")

# 指定安全天数的增量更新
await stock_daily_task.incremental_update(
    safety_days=1,  # 回溯1天确保数据连续性
    ts_code="000001.SZ",
    show_progress=True  # 显示进度条
)
```

##### 3.1 智能增量更新机制

系统实现了一种智能增量更新机制，可以自动检测数据库中最新的数据日期，并只获取该日期之后的新数据：

```python
# 智能增量更新的工作原理
# 1. 查询数据库中最新的数据日期
# 2. 从最新日期开始，减去安全天数（默认为1天）作为开始日期
# 3. 获取从开始日期到当前日期的数据
# 4. 如果数据库中没有数据，则默认获取近30天的数据

# 使用智能增量更新获取股票数据
await stock_daily_task.incremental_update(
    safety_days=1,  # 安全天数，默认为1
    ts_code="000001.SZ,000002.SZ",  # 股票代码
    show_progress=True,  # 显示进度条
    progress_desc='增量更新股票日线数据'  # 进度条描述
)
```

##### 3.2 增量更新参数说明

| 参数名 | 类型 | 默认值 | 描述 |
|--------|------|------|----|
| safety_days | int | 1 | 安全天数，为了确保数据连续性，会额外回溯的天数 |
| days_lookback | int | None | 向后兼容参数，如果指定则相当于设置 safety_days |
| show_progress | bool | False | 是否显示进度条 |
| progress_desc | str | None | 进度条描述文本 |

##### 3.3 增量更新的优势

智能增量更新相比传统的增量更新方式有以下优势：

1. **精确获取数据**：只获取数据库中最新日期之后的数据，避免重复获取和处理
2. **自动判断**：智能判断是否需要更新，如果数据已是最新则跳过
3. **安全回溯**：通过 `safety_days` 参数确保数据连续性
4. **高效资源利用**：减少不必要的API调用和数据库操作
5. **自动处理空数据库**：当数据库中没有数据时，会自动获取近30天的数据


### 实际案例：指数成分股任务

下面是一个完整的实际案例，展示如何创建和使用指数成分股任务：

```python
class IndexComponentTask(TushareTask):
    """指数成分股任务"""
    
    # 任务标识
    name = "index_component"
    description = "获取指数成分股数据"
    
    # 数据库表定义
    table_name = "index_component"
    primary_keys = ["index_code", "con_code", "in_date"]
    date_column = "in_date"
    
    # Tushare特有属性
    api_name = "index_weight"
    fields = ["index_code", "con_code", "trade_date", "weight", "in_date", "out_date"]
    
    # 数据转换规则
    transformations = {
        "trade_date": lambda x: pd.to_datetime(x),
        "in_date": lambda x: pd.to_datetime(x) if x else None,
        "out_date": lambda x: pd.to_datetime(x) if x else None,
        "weight": float
    }
    
    def process_data(self, data):
        """处理指数成分股数据"""
        # 调用父类的处理方法
        data = super().process_data(data)
        
        if data.empty:
            return data
        
        # 应用数据转换规则
        for column, transform_func in self.transformations.items():
            if column in data.columns:
                data[column] = data[column].apply(transform_func)
        
        return data
    
    def validate_data(self, data):
        """验证指数成分股数据"""
        if data.empty:
            self.logger.warning("数据为空")
            return True
        
        # 验证必要字段
        required_columns = ["index_code", "con_code", "weight"]
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"缺少必要字段: {col}")
        
        # 验证权重和为100%
        for index_code, group in data.groupby("index_code"):
            weight_sum = group["weight"].sum()
            if abs(weight_sum - 100) > 1:  # 允许1%的误差
                self.logger.warning(f"指数 {index_code} 的权重和为 {weight_sum}%，不等于100%")
        
        return True
```

使用指数成分股任务：

```python
# 创建任务实例
index_task = IndexComponentTask(db_manager, api_client)

# 获取上证50指数成分股
await index_task.execute(
    index_code="000016.SH",
    start_date="20220101",
    end_date="20220131"
)

# 获取沪深300指数成分股
await index_task.execute(
    index_code="000300.SH",
    start_date="20220101",
    end_date="20220131"
)
```

1. **基础任务类 (Task)**：所有任务的基类，定义了任务的通用接口和生命周期。

2. **数据源特定任务类**：针对特定数据源的任务基类，如TushareTask。

3. **具体业务任务类**：实现特定业务逻辑的任务类，如StockDailyTask。

每个任务类都定义了以下生命周期方法：

- `execute()`：执行完整的任务生命周期
- `fetch_data()`：从数据源获取原始数据
- `process_data()`：处理原始数据
- `validate_data()`：验证数据有效性
- `save_data()`：将数据保存到数据库

## 创建特殊数据任务

某些API可能不使用标准的日期范围参数（start_date和end_date），而是使用其他参数模式。本节将详细介绍如何创建和使用这类特殊数据任务。

## 数据处理和验证

数据处理和验证是任务生命周期中的关键环节，确保数据质量和一致性。

### 1. 数据处理

数据处理通过`process_data`方法实现，可以包括以下操作：

- **类型转换**：将字符串转换为日期、数字等正确类型
- **缺失值处理**：填充或删除缺失值
- **异常值处理**：检测和处理异常值
- **数据计算**：计算新的指标或字段
- **数据格式化**：标准化数据格式

有两种方式定义数据处理规则：

#### 1.1 使用transformations字典

这是一种声明式的方式，适用于简单的类型转换：

```python
class MyTask(Task):
    # 其他属性...
    
    # 数据转换规则
    transformations = {
        "date": lambda x: pd.to_datetime(x),
        "price": float,
        "volume": int,
        "name": str.strip  # 移除空白字符
    }
```

#### 1.2 重写process_data方法

对于复杂的数据处理逻辑，可以重写`process_data`方法：

```python
class MyTask(Task):
    # 其他属性...
    
    async def process_data(self, data):
        # 首先调用父类的process_data方法执行基本转换
        data = await super().process_data(data)
        
        if data.empty:
            return data
        
        # 计算新字段
        if "close" in data.columns and "open" in data.columns:
            data["daily_change"] = data["close"] - data["open"]
            data["daily_change_pct"] = data["daily_change"] / data["open"] * 100
        
        # 处理缺失值
        numeric_columns = data.select_dtypes(include=["number"]).columns
        data[numeric_columns] = data[numeric_columns].fillna(0)
        
        # 处理日期格式
        if self.date_column in data.columns:
            data[self.date_column] = pd.to_datetime(data[self.date_column])
        
        return data
```

### 2. 数据验证

数据验证通过`validate_data`方法实现，用于确保数据符合预期的质量标准。

同样有两种定义验证规则的方式：

#### 2.1 使用validations列表

这是一种声明式的方式，适用于简单的验证规则：

```python
class MyTask(Task):
    # 其他属性...
    
    # 数据验证规则
    validations = [
        lambda df: not df.empty,  # 数据不为空
        lambda df: df["price"].notna().all(),  # price列不包含空值
        lambda df: (df["price"] > 0).all(),  # price列必须大于0
        lambda df: df["volume"].between(0, 1e10).all()  # volume列在合理范围内
    ]
```

#### 2.2 重写validate_data方法

对于复杂的验证逻辑，可以重写`validate_data`方法：

```python
class MyTask(Task):
    # 其他属性...
    
    async def validate_data(self, data):
        # 首先调用父类的validate_data方法执行基本验证
        await super().validate_data(data)
        
        if data.empty:
            self.logger.warning("数据为空，跳过验证")
            return True
        
        # 自定义验证规则
        # 1. 检查必要字段
        required_columns = ["date", "price", "volume"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"缺少必要字段: {missing_columns}")
        
        # 2. 检查数据类型
        if not pd.api.types.is_datetime64_any_dtype(data["date"]):
            raise TypeError("date列必须是日期类型")
        
        # 3. 检查数值范围
        if (data["price"] <= 0).any():
            raise ValueError("price列必须大于0")
        
        # 4. 检查数据一致性
        if data["date"].duplicated().any():
            raise ValueError("存在重复的日期")
        
        self.logger.info("数据验证通过")
        return True
```

### 3. 验证策略

根据业务需求，可以采用不同的验证策略：

- **严格验证**：任何验证失败都会终止任务并抛出异常
- **警告验证**：验证失败会记录警告日志，但任务继续执行
- **清洗验证**：验证失败的数据被过滤或修正，任务继续执行

例如，实现清洗验证策略：

```python
class CleaningValidationTask(Task):
    # 其他属性...
    
    async def validate_data(self, data):
        if data.empty:
            return data
        
        # 记录原始数据行数
        original_rows = len(data)
        
        # 清洗数据
        # 1. 移除空值行
        data = data.dropna(subset=["price", "volume"])
        
        # 2. 移除异常值
        data = data[(data["price"] > 0) & (data["volume"] >= 0)]
        
        # 3. 移除重复行
        data = data.drop_duplicates(subset=self.primary_keys)
        
        # 记录清洗后的行数
        cleaned_rows = len(data)
        removed_rows = original_rows - cleaned_rows
        
        if removed_rows > 0:
            self.logger.warning(f"数据清洗: 移除了{removed_rows}行数据 ({removed_rows/original_rows:.2%})")
        
        return data
```

## 任务执行和调度

本节介绍如何执行和调度数据任务，包括全量更新、增量更新和任务依赖管理。

### 1. 任务执行方式

系统提供了多种任务执行方式，可根据需求选择：

#### 1.1 直接执行

使用`execute`方法直接执行任务，需要指定日期范围和其他参数：

```python
# 创建任务实例
task = StockDailyTask(db_manager, api_client)

# 执行任务，获取指定日期范围的数据
await task.execute(
    start_date="20220101",
    end_date="20220131",
    ts_code="000001.SZ"
)
```

#### 1.2 全量更新

使用`full_update`方法执行全量数据更新，系统会自动获取所有历史数据：

```python
# 全量更新，获取所有历史数据
await task.full_update(ts_code="000001.SZ")
```

#### 1.3 增量更新

使用`incremental_update`方法执行增量数据更新，系统会自动获取最新数据：

```python
# 增量更新，获取最近7天的数据
await task.incremental_update(
    days_lookback=7,  # 回溯天数
    ts_code="000001.SZ"
)
```

### 2. 任务依赖管理

系统支持任务依赖管理，确保任务按正确的顺序执行：

#### 2.1 定义任务依赖

在任务类中通过`dependencies`属性定义依赖关系：

```python
class StockDailyBasicTask(TushareTask):
    # 其他属性...
    
    # 依赖于StockDailyTask
    dependencies = ["stock_daily"]
```

#### 2.2 使用TaskManager执行带依赖的任务

`TaskManager`会自动处理任务依赖，确保依赖任务先执行：

```python
# 创建任务管理器
task_manager = TaskManager(db_manager, api_client)

# 注册任务
task_manager.register_task(StockDailyTask)
task_manager.register_task(StockDailyBasicTask)

# 执行任务（会自动先执行StockDailyTask）
await task_manager.execute_task(
    "stock_daily_basic",
    start_date="20220101",
    end_date="20220131",
    ts_code="000001.SZ"
)
```

### 3. 批量任务执行

对于需要处理多个股票或多个时间段的场景，可以使用批量执行方式：

#### 3.1 多股票批量执行

```python
# 定义股票列表
stock_list = ["000001.SZ", "000002.SZ", "000003.SZ"]

# 批量执行
for ts_code in stock_list:
    await task.execute(
        start_date="20220101",
        end_date="20220131",
        ts_code=ts_code
    )
```

#### 3.2 分段执行大时间范围任务

对于跨度很大的时间范围，可以分段执行以避免单次请求数据量过大：

```python
# 定义时间段列表
date_ranges = [
    ("20200101", "20200331"),
    ("20200401", "20200630"),
    ("20200701", "20200930"),
    ("20201001", "20201231")
]

# 分段执行
for start_date, end_date in date_ranges:
    await task.execute(
        start_date=start_date,
        end_date=end_date,
        ts_code="000001.SZ"
    )
```

### 4. 错误处理和重试

系统提供了错误处理和重试机制，确保任务执行的稳定性：

#### 4.1 使用retry装饰器

```python
from functools import wraps
import asyncio

def async_retry(max_retries=3, retry_delay=1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        raise
                    print(f"Error: {e}, retrying {retries}/{max_retries}...")
                    await asyncio.sleep(retry_delay)
        return wrapper
    return decorator

class MyTask(Task):
    # 其他属性...
    
    @async_retry(max_retries=3, retry_delay=2)
    async def fetch_data(self, start_date, end_date, **kwargs):
        # 获取数据的代码...
```

#### 4.2 任务执行异常处理

```python
try:
    await task.execute(
        start_date="20220101",
        end_date="20220131",
        ts_code="000001.SZ"
    )
except Exception as e:
    print(f"任务执行失败: {e}")
    # 记录错误日志
    # 发送告警通知
    # 执行回滚操作
```

### 5. 定时调度

对于需要定期执行的任务，可以使用定时调度功能：

#### 5.1 使用asyncio定时执行

```python
import asyncio

async def scheduled_task():
    # 创建任务实例
    task = StockDailyTask(db_manager, api_client)
    
    # 执行增量更新
    await task.incremental_update(days_lookback=1)

async def scheduler():
    while True:
        # 每天凌晨1点执行
        now = datetime.datetime.now()
        target_time = now.replace(hour=1, minute=0, second=0, microsecond=0)
        if now > target_time:
            target_time = target_time + datetime.timedelta(days=1)
        
        # 计算等待时间
        wait_seconds = (target_time - now).total_seconds()
        print(f"等待{wait_seconds}秒后执行任务")
        
        # 等待到执行时间
        await asyncio.sleep(wait_seconds)
        
        # 执行任务
        try:
            await scheduled_task()
            print("任务执行成功")
        except Exception as e:
            print(f"任务执行失败: {e}")
        
        # 等待一分钟，避免重复执行
        await asyncio.sleep(60)

# 启动调度器
asyncio.run(scheduler())
```

#### 5.2 使用第三方调度库

也可以使用更强大的第三方调度库，如APScheduler：

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 创建调度器
scheduler = AsyncIOScheduler()

# 添加任务，每天凌晨1点执行
scheduler.add_job(
    scheduled_task,
    CronTrigger(hour=1, minute=0),
    id="daily_update"
)

# 启动调度器
scheduler.start()

# 保持程序运行
try:
    asyncio.get_event_loop().run_forever()
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
```

### 创建特殊数据任务

本节介绍如何创建和使用特殊数据任务，以适应不同的数据源和业务需求。

#### 1. 非标准参数API的任务创建

对于不使用标准日期范围参数的API，需要重写基类的`fetch_data`方法来适配特殊的参数模式：

```python
from data_module.task import Task
import pandas as pd

class NonStandardParamsTask(Task):
    # 任务标识
    name = "non_standard_params_task"
    description = "使用非标准参数的特殊数据任务"
    
    # 数据库表定义
    table_name = "special_data"
    primary_keys = ["code", "report_date"]
    date_column = "report_date"  # 仍然需要指定日期列用于增量更新
    
    # 表结构定义
    schema = [
        {"name": "code", "type": "varchar(20)", "description": "代码"},
        {"name": "report_date", "type": "date", "description": "报告日期"},
        {"name": "value1", "type": "float", "description": "数值1"},
        {"name": "value2", "type": "float", "description": "数值2"}
    ]
    
    async def fetch_data(self, start_date=None, end_date=None, **kwargs):
        """重写fetch_data方法，适配特殊参数模式"""
        self.logger.info(f"获取特殊数据，使用自定义参数")
        
        # 构建API所需的特殊参数
        api_params = {
            "period": kwargs.get("period", "annual"),  # 例如：年度报告
            "report_type": kwargs.get("report_type", "1"),  # 例如：报告类型
            "code": kwargs.get("code", "")  # 例如：股票代码
        }
        
        # 如果提供了日期范围，可以转换为API需要的格式
        if start_date and end_date:
            # 例如API需要的是季度而不是具体日期
            api_params["year"] = start_date[:4]
            api_params["quarter"] = str((int(start_date[4:6]) - 1) // 3 + 1)
        
        # 调用API获取数据
        try:
            data = await self.api.get_data(**api_params)
            return pd.DataFrame(data)
        except Exception as e:
            self.logger.error(f"获取数据失败: {str(e)}")
            raise
```

### 2. 重写execute方法处理特殊执行流程

有时可能需要完全自定义任务的执行流程：

```python
class CustomExecutionTask(Task):
    # 其他属性...
    
    async def execute(self, **kwargs):
        """重写execute方法，实现自定义的执行流程"""
        self.logger.info(f"开始执行自定义任务流程")
        
        try:
            # 1. 预处理
            await self.pre_execute()
            
            # 2. 获取配置参数
            config = await self.get_task_config()
            
            # 3. 分批获取数据
            all_data = []
            for batch_params in self.generate_batch_params(config):
                batch_data = await self.fetch_data(**batch_params)
                if not batch_data.empty:
                    processed_data = self.process_data(batch_data)
                    all_data.append(processed_data)
            
            # 4. 合并数据
            if all_data:
                final_data = pd.concat(all_data, ignore_index=True)
                
                # 5. 验证数据
                self.validate_data(final_data)
                
                # 6. 保存数据
                result = await self.save_data(final_data)
            else:
                result = {"status": "no_data", "rows": 0}
            
            # 7. 后处理
            await self.post_execute(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"任务执行失败: {str(e)}")
            return self.handle_error(e)
```

### 3. 适配智能增量更新机制

对于特殊的数据任务，可能需要调整智能增量更新的逻辑：

```python
class CustomIncrementalTask(Task):
    # 其他属性...
    
    async def incremental_update(self, safety_days=1, **kwargs):
        """重写增量更新方法，适应特殊的更新逻辑"""
        self.logger.info(f"开始执行自定义增量更新")
        
        # 获取最新记录
        latest_record = await self._get_latest_record()
        
        if latest_record:
            # 根据最新记录构建更新参数
            update_params = self._build_update_params(latest_record, safety_days)
            
            # 执行更新
            result = await self.execute(**update_params, **kwargs)
            
            self.logger.info(f"增量更新完成: {result}")
            return result
        else:
            # 如果没有历史记录，执行全量更新
            self.logger.info("没有历史记录，执行全量更新")
            return await self.full_update(**kwargs)
    
    async def _get_latest_record(self):
        """获取最新记录的自定义实现"""
        query = f"""
        SELECT *
        FROM {self.table_name}
        ORDER BY {self.date_column} DESC
        LIMIT 1
        """
        result = await self.db.fetch_one(query)
        return result
    
    def _build_update_params(self, latest_record, safety_days):
        """根据最新记录构建更新参数"""
        # 获取最新日期并考虑安全天数
        latest_date = latest_record[self.date_column]
        from datetime import datetime, timedelta
        
        # 如果是字符串格式，转换为日期对象
        if isinstance(latest_date, str):
            latest_date = datetime.strptime(latest_date, '%Y%m%d')
            
        # 计算开始日期（减去安全天数）
        start_date = (latest_date - timedelta(days=safety_days)).strftime('%Y%m%d')
        
        # 实现特定的参数构建逻辑
        return {
            "start_date": start_date,
            "end_date": datetime.now().strftime('%Y%m%d'),
            "other_param": latest_record.get("other_field")
        }
```

### 4. 实际使用案例：财务报表数据任务

这是一个完整的实际案例，展示如何创建和使用特殊数据任务：

```python
class FinancialReportTask(Task):
    """财务报表数据任务"""
    
    name = "financial_report"
    description = "获取上市公司财务报表数据"
    table_name = "financial_reports"
    primary_keys = ["code", "report_date", "report_type"]
    date_column = "report_date"
    
    # 表结构定义
    schema = {
        "code": {"type": "VARCHAR(20)", "comment": "股票代码"},
        "report_date": {"type": "DATE", "comment": "报告日期"},
        "report_type": {"type": "VARCHAR(10)", "comment": "报告类型"},
        "revenue": {"type": "DECIMAL(20,2)", "comment": "营业收入"},
        "net_profit": {"type": "DECIMAL(20,2)", "comment": "净利润"},
        "total_assets": {"type": "DECIMAL(20,2)", "comment": "总资产"},
        "update_time": {"type": "TIMESTAMP", "comment": "更新时间"}
    }
    
    # 数据验证规则
    validations = [
        lambda df: not df.empty,
        lambda df: df["revenue"].notna().all(),
        lambda df: (df["revenue"] >= 0).all(),
        lambda df: df["net_profit"].notna().all(),
        lambda df: df["total_assets"].notna().all(),
        lambda df: (df["total_assets"] > 0).all()
    ]
    
    async def fetch_data(self, start_date=None, end_date=None, **kwargs):
        """获取财务报表数据"""
        # 构建API参数
        api_params = {
            "report_type": kwargs.get("report_type", "Q4"),
            "fields": "code,report_date,revenue,net_profit,total_assets"
        }
        
        if start_date:
            api_params["year"] = start_date[:4]
        
        # 获取数据
        try:
            data = await self.api.get_financial_report(**api_params)
            df = pd.DataFrame(data)
            
            # 添加更新时间
            df["update_time"] = datetime.now()
            
            return df
            
        except Exception as e:
            self.logger.error(f"获取财务数据失败: {str(e)}")
            raise
    
    def process_data(self, data):
        """处理财务数据"""
        # 基础处理
        data = super().process_data(data)
        
        if data.empty:
            return data
        
        # 数值类型转换
        numeric_columns = ["revenue", "net_profit", "total_assets"]
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors="coerce")
        
        # 日期处理
        if self.date_column in data.columns:
            data[self.date_column] = pd.to_datetime(data[self.date_column])
        
        return data
```

### 5. 使用特殊任务的示例

```python
# 创建任务实例
financial_task = FinancialReportTask(db_manager, api_client)

# 执行年度报表更新
await financial_task.execute(
    start_date="20220101",
    end_date="20221231",
    report_type="annual"
)

# 执行季度报表批量更新
quarters = ["Q1", "Q2", "Q3", "Q4"]
for quarter in quarters:
    await financial_task.execute(
        start_date="20220101",
        end_date="20221231",
        report_type=quarter
    )

## 扩展和定制

本节介绍如何扩展和定制系统，以适应不同的数据源和业务需求。

### 扩展和定制方法

#### 1. 添加新数据源

系统设计为支持多数据源，可以通过以下步骤添加新的数据源：

#### 1.1 创建数据源基类

首先创建一个新的数据源基类，继承自`Task`基类：

```python
class WindTask(Task):
    """Wind数据源任务基类"""
    
    def __init__(self, db_manager, api_client=None):
        super().__init__(db_manager)
        self.api_client = api_client or WindClient()  # 创建默认的Wind API客户端
    
    async def fetch_data(self, start_date, end_date, **kwargs):
        """从 Wind API 获取数据"""
        if not hasattr(self, 'api_name'):
            raise NotImplementedError("必须在子类中定义api_name")
        
        # 调用Wind API
        result = await self.api_client.query(
            self.api_name,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
        
        # 将结果转换为Pandas DataFrame
        if result and 'data' in result:
            return pd.DataFrame(result['data'])
        
        return pd.DataFrame()
```

#### 1.2 创建数据源客户端

然后创建一个与新数据源交互的客户端类：

```python
class WindClient:
    """Wind API客户端"""
    
    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ.get('WIND_API_KEY')
        # 初始化Wind API连接
        self._initialize()
    
    def _initialize(self):
        # 这里实现Wind API的初始化逻辑
        # 例如：加载SDK、登录等
        pass
    
    async def query(self, api_name, **params):
        """执行Wind API查询"""
        # 这里实现实际的API调用逻辑
        # 返回结果格式应该与系统兼容
        pass
```

#### 1.3 创建具体任务类

最后，创建使用新数据源的具体任务类：

```python
class WindStockDailyTask(WindTask):
    """从 Wind 获取股票日线数据的任务"""
    
    # 基本属性
    name = "wind_stock_daily"
    table_name = "wind_stock_daily"
    primary_keys = ["code", "trade_date"]
    date_column = "trade_date"
    
    # Wind特有属性
    api_name = "wsd"  # Wind的日线数据 API
    
    # 数据转换规则
    transformations = {
        "trade_date": lambda x: pd.to_datetime(x),
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        "amount": float
    }
    
    # 可选：重写process_data方法处理Wind特有的数据格式
    async def process_data(self, data):
        # 先调用父类的处理方法
        data = await super().process_data(data)
        
        # Wind特有的数据处理逻辑
        # ...
        
        return data
```

### 2. 自定义数据处理组件

除了标准的数据处理流程外，还可以创建自定义的数据处理组件：

#### 2.1 创建数据转换器

```python
class DataTransformer:
    """数据转换器基类"""
    
    def __init__(self, config=None):
        self.config = config or {}
    
    async def transform(self, data):
        """实现数据转换逻辑"""
        raise NotImplementedError("必须在子类中实现transform方法")


class StockAdjustmentTransformer(DataTransformer):
    """股票数据复权转换器"""
    
    async def transform(self, data):
        """对股票数据进行复权转换"""
        if data.empty:
            return data
        
        # 实现复权逻辑
        # ...
        
        return data
```

#### 2.2 在任务中使用自定义转换器

```python
class AdjustedStockDailyTask(TushareTask):
    """带复权处理的股票日线数据任务"""
    
    # 基本属性
    name = "adjusted_stock_daily"
    table_name = "adjusted_stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # Tushare特有属性
    api_name = "daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    
    def __init__(self, db_manager, api_client=None):
        super().__init__(db_manager, api_client)
        # 创建复权转换器
        self.adjuster = StockAdjustmentTransformer()
    
    async def process_data(self, data):
        # 先调用父类的处理方法
        data = await super().process_data(data)
        
        if data.empty:
            return data
        
        # 使用复权转换器
        data = await self.adjuster.transform(data)
        
        return data
```

### 3. 自定义数据验证器

类似地，可以创建自定义的数据验证器：

```python
class DataValidator:
    """数据验证器基类"""
    
    def __init__(self, config=None):
        self.config = config or {}
        self.errors = []
        self.warnings = []
    
    async def validate(self, data):
        """实现数据验证逻辑"""
        raise NotImplementedError("必须在子类中实现validate方法")
    
    def is_valid(self):
        """检查验证是否通过"""
        return len(self.errors) == 0
    
    def get_errors(self):
        """获取验证错误"""
        return self.errors
    
    def get_warnings(self):
        """获取验证警告"""
        return self.warnings


class StockDataValidator(DataValidator):
    """股票数据验证器"""
    
    async def validate(self, data):
        """验证股票数据"""
        if data.empty:
            self.warnings.append("数据为空")
            return True
        
        # 验证必要字段
        required_columns = ["trade_date", "open", "close"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            self.errors.append(f"缺少必要字段: {missing_columns}")
        
        # 验证数据类型
        if "trade_date" in data.columns and not pd.api.types.is_datetime64_any_dtype(data["trade_date"]):
            self.errors.append("trade_date列必须是日期类型")
        
        # 验证数值范围
        if "close" in data.columns:
            if (data["close"] <= 0).any():
                self.errors.append("close列必须大于0")
            
            # 警告检查
            if (data["close"] > 10000).any():
                self.warnings.append("有可能异常高的close值")
        
        return self.is_valid()
```

#### 3.1 在任务中使用自定义验证器

```python
class ValidatedStockDailyTask(TushareTask):
    """带自定义验证的股票日线数据任务"""
    
    # 基本属性
    name = "validated_stock_daily"
    table_name = "validated_stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # Tushare特有属性
    api_name = "daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    
    def __init__(self, db_manager, api_client=None):
        super().__init__(db_manager, api_client)
        # 创建自定义验证器
        self.validator = StockDataValidator()
    
    async def validate_data(self, data):
        # 使用自定义验证器
        is_valid = await self.validator.validate(data)
        
        # 处理验证错误
        if not is_valid:
            errors = self.validator.get_errors()
            error_message = "\n".join(errors)
            raise ValueError(f"数据验证失败: {error_message}")
        
        # 处理验证警告
        warnings = self.validator.get_warnings()
        if warnings:
            warning_message = "\n".join(warnings)
            self.logger.warning(f"数据验证警告: {warning_message}")
        
        return True
```

## 最佳实践

本节提供一些使用系统的最佳实践，帮助您更有效地管理金融数据任务。

### 任务设计和执行最佳实践

#### 1. 任务设计原则

#### 1.1 单一职责原则

每个任务类应该只负责一种数据的获取和处理，避免在一个任务中混合多种不同的数据：

```python
# 好的做法：分开定义不同的任务
class StockDailyTask(TushareTask):
    # 只负责股票日线数据
    pass

class StockDailyBasicTask(TushareTask):
    # 只负责股票每日指标数据
    pass

# 不好的做法：在一个任务中混合多种数据
class StockAllDataTask(TushareTask):
    # 同时负责日线、指标、财务等多种数据
    pass
```

#### 1.2 合理设置依赖关系

任务之间的依赖关系应该清晰明确，避免循环依赖：

```python
# 好的做法：清晰的依赖链
class StockListTask(TushareTask):
    # 股票列表，无依赖
    dependencies = []

class StockDailyTask(TushareTask):
    # 依赖股票列表
    dependencies = ["stock_list"]

class StockIndicatorTask(TushareTask):
    # 依赖日线数据
    dependencies = ["stock_daily"]

# 不好的做法：循环依赖
class TaskA(Task):
    dependencies = ["task_b"]

class TaskB(Task):
    dependencies = ["task_a"]  # 循环依赖！
```

#### 2. 数据处理最佳实践

##### 2.1 数据类型转换

始终明确定义数据类型转换规则，确保数据类型的一致性：

```python
# 好的做法：明确定义转换规则
transformations = {
    "trade_date": lambda x: pd.to_datetime(x),
    "open": float,
    "high": float,
    "low": float,
    "close": float,
    "vol": float,
    "amount": float
}

# 不好的做法：依赖默认转换或隐式转换
# 没有定义transformations，依赖系统默认行为
```

#### 2.2 数据验证策略

根据数据重要性选择合适的验证策略：

- 关键数据：使用严格验证，任何错误都会终止任务
- 非关键数据：使用警告验证，记录问题但继续执行
- 大批量数据：使用清洗验证，过滤问题数据后继续执行

```python
# 关键财务数据：严格验证
class FinancialReportTask(TushareTask):
    async def validate_data(self, data):
        # 严格验证，任何错误都会抛出异常
        if data.empty:
            raise ValueError("数据为空")
        # 其他验证...

# 市场行情数据：清洗验证
class MarketDataTask(TushareTask):
    async def validate_data(self, data):
        # 记录原始数据行数
        original_rows = len(data)
        
        # 清洗数据
        data = data.dropna(subset=["close"])
        data = data[(data["close"] > 0) & (data["volume"] >= 0)]
        
        # 记录清洗结果
        cleaned_rows = len(data)
        removed_rows = original_rows - cleaned_rows
        
        if removed_rows > 0:
            self.logger.warning(f"数据清洗: 移除了{removed_rows}行数据")
        
        return data
```

#### 3. 性能优化建议

##### 3.1 批量处理

对于大量数据，使用批量处理方式提高效率：

```python
# 批量获取多个股票的数据
async def batch_update_stocks(task, stock_list, start_date, end_date, batch_size=50):
    """批量更新多个股票的数据"""
    for i in range(0, len(stock_list), batch_size):
        batch = stock_list[i:i+batch_size]
        ts_codes = ",".join(batch)
        
        try:
            await task.execute(
                start_date=start_date,
                end_date=end_date,
                ts_code=ts_codes
            )
            print(f"已处理 {i+len(batch)}/{len(stock_list)} 只股票")
        except Exception as e:
            print(f"处理批次 {i//batch_size + 1} 时出错: {e}")
```

#### 3.2 并行执行

对于相互独立的任务，可以并行执行提高效率：

```python
import asyncio

async def parallel_execute_tasks(tasks, start_date, end_date, **kwargs):
    """并行执行多个任务"""
    coroutines = []
    for task in tasks:
        coro = task.execute(start_date=start_date, end_date=end_date, **kwargs)
        coroutines.append(coro)
    
    # 并行执行所有任务
    results = await asyncio.gather(*coroutines, return_exceptions=True)
    
    # 处理结果
    for task, result in zip(tasks, results):
        if isinstance(result, Exception):
            print(f"任务 {task.name} 执行失败: {result}")
        else:
            print(f"任务 {task.name} 执行成功")
    
    return results
```

#### 4. 错误处理和日志记录

##### 4.1 分级日志记录

使用分级日志记录不同重要性的信息：

```python
import logging

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_tasks.log"),
        logging.StreamHandler()
    ]
)

class LoggingTask(Task):
    def __init__(self, db_manager):
        super().__init__(db_manager)
        self.logger = logging.getLogger(self.name)
    
    async def execute(self, start_date, end_date, **kwargs):
        self.logger.info(f"开始执行任务 {self.name}, 日期范围: {start_date} 到 {end_date}")
        
        try:
            result = await super().execute(start_date, end_date, **kwargs)
            self.logger.info(f"任务 {self.name} 执行成功")
            return result
        except Exception as e:
            self.logger.error(f"任务 {self.name} 执行失败: {e}", exc_info=True)
            raise
```

#### 4.2 优雅的错误恢复

实现错误恢复机制，确保系统稳定性：

```python
async def robust_execute(task, max_retries=3, retry_delay=5, **kwargs):
    """带重试机制的任务执行"""
    retries = 0
    last_error = None
    
    while retries < max_retries:
        try:
            return await task.execute(**kwargs)
        except Exception as e:
            retries += 1
            last_error = e
            print(f"任务执行失败 ({retries}/{max_retries}): {e}")
            
            if retries < max_retries:
                print(f"等待 {retry_delay} 秒后重试...")
                await asyncio.sleep(retry_delay)
                # 指数退避策略
                retry_delay *= 2
    
    print(f"达到最大重试次数，任务执行失败: {last_error}")
    raise last_error
```

#### 5. 系统维护和监控

##### 5.1 定期数据质量检查

实现定期数据质量检查任务：

```python
async def check_data_quality(db_manager, table_name, date_column, days_lookback=7):
    """检查数据质量"""
    # 计算检查日期范围
    end_date = datetime.datetime.now().strftime("%Y%m%d")
    start_date = (datetime.datetime.now() - datetime.timedelta(days=days_lookback)).strftime("%Y%m%d")
    
    # 查询数据覆盖情况
    query = f"""
    SELECT date({date_column}) as date, count(*) as count
    FROM {table_name}
    WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY date({date_column})
    ORDER BY date({date_column})
    """
    
    results = await db_manager.execute_query(query)
    
    # 检查日期覆盖
    dates = [row['date'] for row in results]
    all_dates = [(datetime.datetime.now() - datetime.timedelta(days=i)).date() for i in range(days_lookback)]
    
    missing_dates = [d for d in all_dates if d not in dates]
    if missing_dates:
        print(f"警告: {table_name} 缺少以下日期的数据: {missing_dates}")
    
    # 检查数据量异常
    counts = [row['count'] for row in results]
    if counts:
        avg_count = sum(counts) / len(counts)
        for date, count in zip(dates, counts):
            if count < avg_count * 0.5:
                print(f"警告: {table_name} 在 {date} 的数据量异常偏低: {count} (平均: {avg_count:.0f})")
```

##### 5.2 系统监控指标

实现关键系统监控指标：

```python
async def collect_system_metrics(db_manager):
    """收集系统监控指标"""
    metrics = {}
    
    # 数据库大小
    query = """
    SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
    """
    result = await db_manager.execute_query(query)
    metrics['database_size'] = result[0]['db_size']
    
    # 表数量
    query = """
    SELECT count(*) as table_count FROM information_schema.tables
    WHERE table_schema = 'public'
    """
    result = await db_manager.execute_query(query)
    metrics['table_count'] = result[0]['table_count']
    
    # 最大表
    query = """
    SELECT tablename, pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as size
    FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
    LIMIT 5
    """
    result = await db_manager.execute_query(query)
    metrics['largest_tables'] = result
    
    # 最新数据日期
    tables_with_date = ['stock_daily', 'stock_daily_basic']  # 示例表名
    latest_dates = {}
    
    for table in tables_with_date:
        try:
            query = f"""
            SELECT MAX(trade_date) as latest_date FROM {table}
            """
            result = await db_manager.execute_query(query)
            latest_dates[table] = result[0]['latest_date']
        except Exception as e:
            latest_dates[table] = f"错误: {e}"
    
    metrics['latest_dates'] = latest_dates
    
    return metrics
```

#### 6. API 速率限制与批量处理管理

##### 6.1 Tushare API 速率限制

Tushare API 对不同的数据接口有不同的调用频率限制。系统提供了按 API 接口分别设置速率限制的功能，以便更精确地控制 API 调用频率，避免超出限制。

```python
# 设置不同 API 的速率限制
from data_module.sources.tushare import TushareAPI

# 设置特定 API 的速率限制（每分钟请求数）
TushareAPI.set_api_rate_limit("daily", 500)       # 股票日线数据
TushareAPI.set_api_rate_limit("weekly", 200)      # 股票周线数据
TushareAPI.set_api_rate_limit("monthly", 200)     # 股票月线数据
TushareAPI.set_api_rate_limit("stock_basic", 100) # 股票基本信息

# 设置默认速率限制（用于未特别指定的 API）
TushareAPI.set_default_rate_limit(50)
```

##### 6.2 速率限制与并发控制原理

系统使用 `asyncio.Semaphore` 实现 API 速率限制和并发控制：

1. **API 级别的限制**：每个 API 接口有独立的速率限制，符合 Tushare 的实际限制模型
2. **动态调整**：可以根据账户权限或业务需求动态调整每个 API 的限制
3. **默认限制**：对于未特别指定的 API，使用默认限制值
4. **并发控制**：使用信号量控制同时进行的 API 请求数量，避免超出限制

##### 6.3 分批处理机制

系统实现了高效的分批处理机制，特别适合处理大量数据：

```python
# 执行任务时指定分批处理参数
await stock_daily_task.execute(
    start_date="20200101", 
    end_date="20210101",
    concurrent_limit=5,  # 并发数量限制
    batch_size=90,       # 每批处理的交易日数量（约一个季度）
    show_progress=True   # 显示进度条
)
```

##### 6.3.1 TushareTask 分批处理流程

TushareTask 类实现了优化的分批处理模式，其工作流程如下：

1. **批次生成**：`get_batch_list` 方法生成多个批次参数
2. **批次处理**：对每个批次执行以下步骤：
   - 通过 `fetch_batch` 获取单批次数据
   - 处理单批次数据
   - 验证单批次数据
   - 保存单批次数据到数据库
3. **并发控制**：使用 `concurrent_limit` 参数控制同时处理的批次数量

##### 6.3.2 分批处理的主要优势

1. **内存效率**：每次只处理一批数据，避免内存溢出
2. **错误隔离**：每个批次独立处理，一个批次的错误不会影响其他批次
3. **实时反馈**：每批数据处理完成后立即保存，用户可以更快看到结果
4. **并行处理**：支持并发处理多个批次，提高整体效率
5. **独立事务**：每个批次使用独立的数据库事务，提高可靠性

##### 6.4 最佳实践

1. **根据文档设置**：根据 Tushare 官方文档中各个接口的实际限制设置对应的速率限制
2. **考虑账户等级**：不同等级的 Tushare 账户有不同的 API 调用限制，请根据您的账户等级设置合适的限制
3. **系统初始化时设置**：在系统初始化时设置 API 速率限制，确保所有任务使用正确的限制
4. **监控 API 调用**：定期监控 API 调用情况，及时调整速率限制
5. **调整批量大小**：根据数据量和系统资源调整批量大小，通常每批90天（约一个季度）是合理的默认值
6. **设置合理并发数**：根据API限制和系统性能设置合理的并发数量，通常5-10是一个好的起点
7. **大数据集使用分批处理**：对于大数据集，始终使用分批处理模式，避免内存溢出
8. **小数据集可以简化**：对于小数据集，可以使用较少的批次或更大的批次大小

##### 6.5 示例：在应用启动时设置速率限制并执行批量任务

```python
async def initialize_system():
    # 设置数据库连接
    db_connection = await create_db_connection("postgresql://user:password@localhost/finance_db")
    
    # 设置 Tushare API 速率限制
    TushareAPI.set_api_rate_limit("daily", 500)
    TushareAPI.set_api_rate_limit("weekly", 200)
    TushareAPI.set_api_rate_limit("monthly", 200)
    TushareAPI.set_api_rate_limit("stock_basic", 100)
    TushareAPI.set_default_rate_limit(50)
    
    # 创建任务实例
    stock_daily_task = StockDailyTask(db_connection, api_token="your_tushare_token")
    
    # 执行全量更新（从1991年至今，使用批量处理）
    await stock_daily_task.full_update(
        concurrent_limit=5,  # 并发数量限制
        batch_size=90,       # 每批处理90个交易日（约一个季度）
        show_progress=True   # 显示进度条
    )
``` 
