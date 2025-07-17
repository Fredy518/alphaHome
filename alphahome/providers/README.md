# AlphaHome 数据提供层 - 极简方案

## 🎯 设计理念

基于**80/20原则**的极简数据提供层：
- **80%的研究需求**通过5个核心方法满足
- **20%的特殊需求**通过灵活接口处理
- **单一入口 + 内部模块化**：对外暴露简洁API，内部采用模块化架构

## 🚀 快速开始

### 基础使用（80%需求）

```python
from research.tools.context import ResearchContext

with ResearchContext() as context:
    data_tool = context.data_tool
    
    # 1. 获取股票行情数据
    stock_data = data_tool.get_stock_data(
        symbols=['000001.SZ', '000002.SZ'], 
        start_date='2024-01-01', 
        end_date='2024-03-31'
    )
    
    # 2. 获取指数权重数据
    weights = data_tool.get_index_weights(
        index_code='000300.SH',
        start_date='2024-01-01',
        end_date='2024-03-31',
        monthly=True  # 只获取月末数据
    )
    
    # 3. 获取股票基本信息
    stock_info = data_tool.get_stock_info(
        symbols=['000001.SZ', '000002.SZ'],
        list_status='L'
    )
    
    # 4. 获取交易日历
    trade_dates = data_tool.get_trade_dates(
        start_date='2024-01-01',
        end_date='2024-03-31'
    )
    
    # 5. 获取行业分类数据
    industry_data = data_tool.get_industry_data(
        symbols=['000001.SZ', '000002.SZ'],
        level='sw_l1'
    )
```

### 高级使用（20%特殊需求）

```python
# 自定义复杂查询
complex_data = data_tool.custom_query('''
    SELECT s.ts_code, s.close, w.weight, i.industry_name
    FROM stock_daily s
    JOIN index_weight w ON s.ts_code = w.con_code
    JOIN stock_industry i ON s.ts_code = i.ts_code
    WHERE w.index_code = %(index_code)s
    AND s.trade_date = %(trade_date)s
''', {
    'index_code': '000300.SH',
    'trade_date': '2024-01-31'
})

# 获取原始数据库管理器进行底层操作
db = data_tool.get_raw_db_manager()
await db.copy_from_dataframe(custom_df, 'custom_table')
```

## 📋 API 参考

### 核心方法（5个）

#### 1. get_stock_data()
获取股票行情数据

```python
get_stock_data(
    symbols: Union[str, List[str]],  # 股票代码
    start_date: str,                 # 开始日期 'YYYY-MM-DD'
    end_date: str,                   # 结束日期 'YYYY-MM-DD'
    adjust: bool = True              # 是否使用复权价格
) -> pd.DataFrame
```

**返回字段**：
- `ts_code`: 股票代码
- `trade_date`: 交易日期
- `open`, `high`, `low`, `close`: 开高低收价格
- `vol`, `amount`: 成交量、成交额
- `pct_chg`: 涨跌幅

#### 2. get_index_weights()
获取指数权重数据

```python
get_index_weights(
    index_code: str,        # 指数代码，如 '000300.SH'
    start_date: str,        # 开始日期
    end_date: str,          # 结束日期
    monthly: bool = False   # 是否只获取月末数据
) -> pd.DataFrame
```

**返回字段**：
- `index_code`: 指数代码
- `con_code`: 成分股代码
- `trade_date`: 交易日期
- `weight`: 权重

#### 3. get_stock_info()
获取股票基本信息

```python
get_stock_info(
    symbols: Optional[Union[str, List[str]]] = None,  # 股票代码，None=所有
    list_status: str = 'L'                            # 上市状态
) -> pd.DataFrame
```

**返回字段**：
- `ts_code`: 股票代码
- `name`: 股票名称
- `industry`: 所属行业
- `area`: 地域
- `list_date`: 上市日期

#### 4. get_trade_dates()
获取交易日历

```python
get_trade_dates(
    start_date: str,        # 开始日期
    end_date: str,          # 结束日期
    exchange: str = 'SSE'   # 交易所代码
) -> pd.DataFrame
```

**返回字段**：
- `cal_date`: 日期
- `is_open`: 是否开市（1=开市，0=休市）

#### 5. get_industry_data()
获取行业分类数据

```python
get_industry_data(
    symbols: Optional[Union[str, List[str]]] = None,  # 股票代码
    level: str = 'sw_l1'                              # 行业级别
) -> pd.DataFrame
```

**返回字段**：
- `ts_code`: 股票代码
- `industry_code`: 行业代码
- `industry_name`: 行业名称

### 灵活接口（2个）

#### custom_query()
自定义SQL查询

```python
custom_query(
    sql: str,                           # SQL查询语句
    params: Optional[Dict[str, Any]] = None  # 查询参数
) -> pd.DataFrame
```

#### get_raw_db_manager()
获取原始数据库管理器

```python
get_raw_db_manager() -> DBManager
```

## 🏗️ 架构设计

### 目录结构
```
alphahome/providers/
├── __init__.py          # AlphaDataTool 主类
├── _stock_queries.py    # 股票数据查询模块
├── _index_queries.py    # 指数数据查询模块
├── _helpers.py          # 辅助工具模块
├── examples/            # 使用示例
│   └── usage_example.py
└── README.md           # 本文档
```

### 内部模块化

- **StockQueries**: 专门处理股票相关查询
- **IndexQueries**: 专门处理指数相关查询  
- **DataHelpers**: 提供通用辅助功能

### 智能特性

1. **自动表名检测**：智能检测可用的数据表名
2. **数据类型转换**：自动处理日期、数值类型转换
3. **错误处理**：优雅处理数据缺失和查询异常
4. **缓存机制**：缓存常用的静态数据
5. **模式适配**：同时支持异步和同步数据库操作

## 🔧 扩展策略

### 扩展触发条件
- 使用频率 ≥ 30%
- 代码重复 ≥ 5次
- 用户请求 ≥ 3个
- 覆盖 ≥ 2个研究场景

### 扩展边界控制
- **核心方法**：最多5个（50%+使用率）
- **扩展方法**：最多3个（30%+使用率）
- **高级方法**：最多2个（10%+使用率）

### 候选扩展功能
1. `get_futures_data()` - 期货数据（使用率40%）
2. `get_financial_data()` - 财务数据（使用率35%）
3. `get_macro_data()` - 宏观数据（使用率25%）

## 📊 使用示例

完整的使用示例请参考：`examples/usage_example.py`

运行示例：
```bash
cd alphahome/providers/examples
python usage_example.py
```

## 🔗 集成说明

### 与ResearchContext集成

AlphaDataTool已经集成到ResearchContext中，通过`data_tool`属性访问：

```python
from research.tools.context import ResearchContext

with ResearchContext() as context:
    # 直接使用数据访问工具
    data_tool = context.data_tool
    
    # 原有功能仍然可用
    db_manager = context.db_manager
    planner = context.planner
```

### 向后兼容

- 保留对原始`db_manager`的完全访问权限
- 现有的`query_dataframe`等方法继续可用
- 不破坏任何现有代码

## 🎯 预期效果

- **学习成本**：几乎为零，5个方法即可上手
- **覆盖率**：80%的研究场景通过标准方法满足
- **灵活性**：20%的特殊需求通过灵活接口处理
- **维护性**：内部模块化，代码清晰易维护
- **扩展性**：有序扩展，不会失控

这个方案完美平衡了简洁性、功能性和可维护性，是一个真正实用且优雅的解决方案。
