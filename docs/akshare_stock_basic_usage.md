# AkshareStockBasicTask 使用指南

## 概述

`AkshareStockBasicTask` 是 alphahome 中用于从 akshare 数据源获取股票基本信息的任务类。该任务对标 hikyuu 的实现方式，使用 akshare 获取A股的基本信息，包括股票代码、名称、市场、交易所等。

## 功能特性

### 数据来源
- **数据源**: akshare 数据接口
- **接口**: `stock_info_sh_name_code`, `stock_info_sz_name_code`, `stock_info_bj_name_code`
- **更新方式**: 全量更新，对标 hikyuu 的做法

### 支持的市场
任务可以获取以下市场的股票基本信息：

| 市场代码 | 市场名称 | 数据接口 |
|---------|---------|---------|
| SH | 上海证券交易所 | `stock_info_sh_name_code` |
| SZ | 深圳证券交易所 | `stock_info_sz_name_code` |
| BJ | 北京证券交易所 | `stock_info_bj_name_code` |

## 数据表结构

### 表名
`akshare_stock_basic`

### 字段定义

| 字段名 | 类型 | 约束 | 说明 |
|-------|------|------|------|
| `ts_code` | VARCHAR(15) | NOT NULL | 股票代码（如：000001.SH） |
| `symbol` | VARCHAR(15) | - | 股票代码（不含市场标识） |
| `name` | VARCHAR(100) | - | 股票名称 |
| `market` | VARCHAR(50) | - | 市场名称 |
| `list_date` | DATE | - | 上市日期 |
| `industry` | VARCHAR(100) | - | 行业分类 |
| `update_time` | TIMESTAMP | - | 数据更新时间 |

### 索引
- `idx_akshare_basic_ts_code`: ts_code 字段索引
- `idx_akshare_basic_market`: market 字段索引
- `idx_akshare_basic_update_time`: update_time 字段索引

## 使用方法

### 基本使用

```python
from alphahome.fetchers.tasks.stock import AkshareStockBasicTask

# 创建任务实例
task = AkshareStockBasicTask(db_connection)

# 执行任务（全量更新）
await task.execute()
```

### 高级配置

```python
# 指定市场获取
# 只获取上海市场股票信息
params = {"market": "SH"}
await task.execute(params=params)

# 获取深圳市场股票信息
params = {"market": "SZ"}
await task.execute(params=params)

# 获取全市场（默认）
params = {}  # 或不设置market参数
await task.execute(params=params)
```

## 数据查询示例

### 查询特定股票的基本信息

```sql
SELECT * FROM akshare_stock_basic
WHERE ts_code = '000001.SH';
```

### 查询特定市场的股票

```sql
-- 查询上海市场所有股票
SELECT ts_code, name, symbol FROM akshare_stock_basic
WHERE market = 'SH'
ORDER BY symbol;

-- 查询深圳市场所有股票
SELECT ts_code, name, symbol FROM akshare_stock_basic
WHERE market = 'SZ'
ORDER BY symbol;
```

### 按市场统计股票数量

```sql
SELECT market, COUNT(*) as stock_count
FROM akshare_stock_basic
GROUP BY market
ORDER BY stock_count DESC;
```

### 搜索特定股票

```sql
-- 通过股票名称搜索
SELECT ts_code, name, market FROM akshare_stock_basic
WHERE name LIKE '%银行%'
ORDER BY ts_code;
```

## 与其他数据源对比

| 特性 | AkshareStockBasicTask | TushareStockBasicTask | Hikyu做法 |
|------|----------------------|----------------------|-----------|
| **数据源** | akshare接口 | Tushare API | akshare接口 |
| **信息类型** | 基础股票信息 | 结构化基础字段 | 基础股票信息 |
| **数据量** | 中等（代码+名称等） | 丰富（包含行业等） | 中等 |
| **更新频率** | 实时更新 | 定期更新 | 实时更新 |
| **复杂度** | 低 | 低 | 低 |
| **对标程度** | ✅ 完全对标 | - | ✅ 完全对标 |

## 注意事项

### 1. 数据量考虑
- 全市场股票数量约为5000只
- 数据结构简单，主要包含代码、名称、市场等基础信息
- 适合全量更新和定期刷新

### 2. 性能优化
- 串行处理各个市场的数据获取
- 每个市场的数据获取相对较快
- 适合定期批量更新

### 3. 数据更新策略
- 每次执行都是全量更新，会覆盖旧数据
- 适合定期批量更新，不适合实时查询
- 可以配合定时任务定期执行

### 4. 错误处理
- 单个市场获取失败不会影响其他市场
- 会记录详细的错误日志便于排查
- 网络异常时会重试

## 扩展开发

### 添加新的市场支持

```python
# 在 _fetch_raw_data 方法中添加新的市场
if not market_filter or market_filter.upper() == 'NEW_MARKET':
    # 获取新市场数据
    pass
```

### 自定义数据处理

```python
def _transform_data(self, raw_data: List[Dict], params: Dict) -> pd.DataFrame:
    # 自定义数据转换逻辑
    df = super()._transform_data(raw_data, params)

    # 添加自定义处理
    # ...

    return df
```

## 故障排除

### 网络连接问题
- 检查网络连接状态
- 确认akshare服务可用性
- 查看日志中的网络错误信息

### 数据获取失败
- 检查akshare版本是否最新
- 确认API接口是否变更
- 查看akshare的返回状态

### 性能问题
- 检查网络带宽
- 确认系统资源充足
- 分批次执行或错峰运行

## 相关文档

- [FetcherTask基类文档](./fetcher_task.md)
- [数据获取最佳实践](./data_fetching_best_practices.md)
- [akshare官方文档](https://akshare.readthedocs.io/)
