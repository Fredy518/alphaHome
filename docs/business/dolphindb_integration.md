# DolphinDB 集成指南

## 概述

AlphaHome 使用 DolphinDB 作为 5 分钟 K 线的高速查询层。当前主要链路是从 Hikyuu HDF5 文件读取 `sh_5min.h5`、`sz_5min.h5`、`bj_5min.h5`，转换后写入 DolphinDB DFS 表。

统一 CLI 已下线，当前使用：

```bash
python -m alphahome.integrations.dolphindb.cli ...
```

或 PowerShell 一键脚本：

```powershell
.\scripts\import_all_hikyuu_to_ddb.ps1
```

## 配置

`~/.alphahome/config.json`：

```json
{
  "backtesting": {
    "hikyuu_data_dir": "E:/stock"
  },
  "dolphindb": {
    "host": "localhost",
    "port": 8848,
    "username": "admin",
    "password": "123456"
  }
}
```

也可以在导入命令中使用 `--hikyuu-data-dir` 覆盖 Hikyuu 路径。

## 初始化表

```bash
python -m alphahome.integrations.dolphindb.cli init-kline5m
```

常用参数：

```bash
python -m alphahome.integrations.dolphindb.cli init-kline5m \
  --db-path dfs://kline_5min \
  --table kline_5min \
  --start-month 200501 \
  --end-month 203012 \
  --hash-buckets 10
```

当前 DFS 表字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `ts_code` | SYMBOL | `000001.SZ` 格式 |
| `trade_time` | TIMESTAMP | 交易时间 |
| `month` | INT | `yyyymm`，用于 VALUE 分区 |
| `open` / `high` / `low` / `close` | DOUBLE | 价格 |
| `vol` | LONG | 成交量 |
| `amount` | DOUBLE | 成交额 |

分区方式：`VALUE(month)` + `HASH(ts_code, hash_buckets)`。

## 导入 Hikyuu 5min

```bash
# 单批代码
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min \
  --codes "000001.SZ,600000.SH"

# 从文件读取代码
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min \
  --codes-file scripts/tickers/all.txt

# 指定日期范围
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min \
  --codes-file scripts/tickers/all.txt \
  --start 2024-01-01 \
  --end 2024-12-31

# 增量导入
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min \
  --codes-file scripts/tickers/all.txt \
  --incremental

# 只读取和转换，不写入
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min \
  --codes "000001.SZ" \
  --dry-run
```

增量模式会查询 DolphinDB 中每只股票的 `max(trade_time)`，只写入更晚的数据。

## 一键脚本

```powershell
# 首次导入，必要时初始化表
.\scripts\import_all_hikyuu_to_ddb.ps1 -InitTable

# 日常增量
.\scripts\import_all_hikyuu_to_ddb.ps1 -Incremental

# 删除并重建 dfs://kline_5min，谨慎使用
.\scripts\import_all_hikyuu_to_ddb.ps1 -ResetDb -InitTable

# 导入后备份 DolphinDB DFS 目录
.\scripts\import_all_hikyuu_to_ddb.ps1 -Incremental -Backup -DfsRoot "D:/dolphindb/server/data/dfs"
```

## Python API

```python
from alphahome.integrations.dolphindb import DolphinDBManager

ddb = DolphinDBManager()
ddb.connect()
try:
    df = ddb.query("""
        select top 10 *
        from loadTable("dfs://kline_5min", "kline_5min")
        where ts_code = "000001.SZ"
    """)
    print(df)
finally:
    ddb.close()
```

导入器：

```python
from alphahome.integrations.dolphindb import (
    DolphinDBManager,
    Hikyuu5MinImporterConfig,
    HikyuuKline5MinImporter,
)

ddb = DolphinDBManager()
ddb.connect()
try:
    importer = HikyuuKline5MinImporter(
        ddb,
        Hikyuu5MinImporterConfig(hikyuu_data_dir="E:/stock"),
    )
    result = importer.import_many(["000001.SZ"], incremental=True)
    print(result)
finally:
    ddb.close()
```

## 查询建议

优先在 DolphinDB 端完成过滤和聚合：

```python
daily = ddb.query("""
select
    date(trade_time) as trade_date,
    first(open) as open,
    max(high) as high,
    min(low) as low,
    last(close) as close,
    sum(vol) as vol,
    sum(amount) as amount
from loadTable("dfs://kline_5min", "kline_5min")
where ts_code = "000001.SZ" and month between 202401:202412
group by date(trade_time)
order by trade_date
""")
```

查询时尽量带上 `month` 和 `ts_code` 条件，利用分区裁剪。

## 故障排查

- `Missing Hikyuu data dir`: 配置 `backtesting.hikyuu_data_dir`、环境变量 `HIKYUU_DATA_DIR` 或传 `--hikyuu-data-dir`。
- `Hikyuu 5min H5 file not found`: 确认目录下存在 `sh_5min.h5`、`sz_5min.h5`、`bj_5min.h5`。
- `Dataset not found`: 代码必须带后缀，如 `000001.SZ`；同时确认 Hikyuu 文件中有 `/data/SZ000001`。
- 连接 DolphinDB 失败：确认服务端口、用户名密码和 Python 包 `dolphindb`。

## 相关文档

- [Hikyuu 集成指南](../hikyuu_integration_guide.md)
- [CLI 下线说明](../CLI_USAGE_GUIDE.md)
