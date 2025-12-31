# DolphinDB 集成指南

## 概述

AlphaHome 集成 DolphinDB 作为高速查询层，用于处理大规模时间序列数据（如分钟线行情）的快速查询与计算。

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                      AlphaHome                               │
├─────────────────────────────────────────────────────────────┤
│  PostgreSQL (主存储)          DolphinDB (高速查询)           │
│  ├─ 基础数据                  ├─ 5分钟K线                   │
│  ├─ 因子数据                  ├─ 分钟级指标                 │
│  └─ 元数据                    └─ 实时计算                   │
└─────────────────────────────────────────────────────────────┘
```

## 配置

### 连接配置

在 `~/.alphahome/config.json` 中添加：

```json
{
  "dolphindb": {
    "host": "localhost",
    "port": 8848,
    "username": "admin",
    "password": "123456"
  }
}
```

### 环境变量

也可以通过环境变量配置：

```bash
export DOLPHINDB_HOST=localhost
export DOLPHINDB_PORT=8848
export DOLPHINDB_USERNAME=admin
export DOLPHINDB_PASSWORD=123456
```

## CLI 命令

### 初始化 5 分钟 K 线表

```bash
# 创建分区表
ah ddb init-kline5m --db-path dfs://kline_5min --table kline_5min
```

### 导入数据

```bash
# 从 Hikyuu HDF5 文件导入
ah ddb import-hikyuu-5min --codes "000001.SZ,600000.SH"

# 导入所有股票
ah ddb import-hikyuu-5min --all

# 指定数据目录
ah ddb import-hikyuu-5min --data-dir /path/to/hikyuu/data --all
```

### 查询数据

```bash
# 查询数据概览
ah ddb query "select count(*) from kline_5min"

# 查询特定股票
ah ddb query "select * from kline_5min where code='000001.SZ' limit 10"
```

## Python API

### 基础连接

```python
from alphahome.integrations.dolphindb import DolphinDBClient

# 创建客户端
client = DolphinDBClient()

# 执行查询
result = client.run("select top 10 * from kline_5min")
print(result)
```

### 数据导入

```python
from alphahome.integrations.dolphindb import Kline5MinImporter

importer = Kline5MinImporter(client)

# 导入单只股票
importer.import_stock("000001.SZ", data_df)

# 批量导入
importer.import_batch(["000001.SZ", "600000.SH"])
```

### 数据查询

```python
import pandas as pd

# 查询指定时间范围
query = """
select 
    code, datetime, open, high, low, close, volume
from kline_5min
where code = '000001.SZ'
    and datetime between 2024.01.01 : 2024.12.31
"""
df = client.run(query)
```

## 分区设计

### 5 分钟 K 线表

```
数据库: dfs://kline_5min
分区方式: VALUE(date) + HASH(code, 50)
```

分区策略说明：
- 按日期 VALUE 分区：便于按时间范围查询
- 按股票代码 HASH 分区：均衡数据分布

### 建表语句

```dolphindb
// 创建数据库
if(existsDatabase("dfs://kline_5min")){
    dropDatabase("dfs://kline_5min")
}

db1 = database(, VALUE, 2020.01.01..2030.12.31)
db2 = database(, HASH, [SYMBOL, 50])
db = database("dfs://kline_5min", COMPO, [db1, db2])

// 创建表
schema = table(
    1:0, 
    `code`datetime`open`high`low`close`volume`amount,
    [SYMBOL, DATETIME, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE]
)

db.createPartitionedTable(
    schema, 
    `kline_5min, 
    `datetime`code
)
```

## 性能优化

### 查询优化

1. **使用分区裁剪**：查询时指定日期范围
2. **避免全表扫描**：始终在 WHERE 子句中包含分区列
3. **使用 SQL 聚合**：在 DolphinDB 端完成聚合运算

```python
# ❌ 不推荐：拉取全量数据后在 Python 聚合
df = client.run("select * from kline_5min where code='000001.SZ'")
daily_agg = df.groupby(df['datetime'].dt.date).agg(...)

# ✅ 推荐：在 DolphinDB 端聚合
query = """
select 
    date(datetime) as date,
    first(open) as open,
    max(high) as high,
    min(low) as low,
    last(close) as close,
    sum(volume) as volume
from kline_5min
where code = '000001.SZ'
group by date(datetime)
"""
daily_df = client.run(query)
```

### 批量写入

```python
# 使用 tableInsert 批量写入
client.run("""
    t = loadTable("dfs://kline_5min", "kline_5min")
    t.tableInsert(data)
""", data=df)
```

## 故障排除

### 连接问题

```python
# 检查连接
try:
    client = DolphinDBClient()
    client.run("1+1")
    print("连接成功")
except Exception as e:
    print(f"连接失败: {e}")
```

### 内存问题

对于大查询，使用流式读取：

```python
# 分批读取
for chunk in client.run_chunked(query, chunk_size=100000):
    process(chunk)
```

## 相关文档

- [Hikyuu 集成指南](../hikyuu_integration_guide.md)
- [CLI 使用指南](../CLI_USAGE_GUIDE.md)
