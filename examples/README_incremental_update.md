# 股票日线数据增量更新工具

这个工具用于增量更新股票日线数据，支持多种更新策略和参数配置，相比全量更新更加灵活和高效。

## 功能特点

- 支持多种增量更新策略：
  - 从数据库最新日期开始更新（自动模式）
  - 更新最近N个交易日的数据
  - 指定日期范围进行更新
  - 全量更新（从股市开始日期更新到现在）
- 智能识别交易日，避免在非交易日进行无效更新
- 详细的日志记录，便于跟踪更新进度
- 命令行参数支持，便于集成到自动化流程中

## 使用方法

### 基本用法

```bash
# 默认更新最近3个交易日的数据
python examples/stock_daily_incremental_update.py

# 自动模式：从数据库最新日期的下一个交易日开始更新
python examples/stock_daily_incremental_update.py --auto

# 更新最近10个交易日的数据
python examples/stock_daily_incremental_update.py --days 10

# 指定日期范围进行更新
python examples/stock_daily_incremental_update.py --start-date 20230101 --end-date 20230131

# 全量更新（从股市开始日期更新到现在）
python examples/stock_daily_incremental_update.py --full-update
```

### 参数说明

- `--days N`: 更新最近N个交易日的数据
- `--start-date YYYYMMDD`: 指定更新的起始日期
- `--end-date YYYYMMDD`: 指定更新的结束日期（默认为当前日期）
- `--auto`: 自动模式，从数据库最新日期的下一个交易日开始更新
- `--full-update`: 全量更新，从股市开始日期更新到现在

## 使用场景

### 场景1：日常增量更新

每天运行一次，自动获取最新数据：

```bash
# 使用自动模式，根据数据库情况智能更新
python examples/stock_daily_incremental_update.py --auto
```

### 场景2：补充特定日期的数据

如果发现某段时间的数据有问题，可以指定日期范围进行更新：

```bash
# 更新2023年1月的数据
python examples/stock_daily_incremental_update.py --start-date 20230101 --end-date 20230131
```

### 场景3：新建数据库后的初始化

对于新建的数据库，可以先进行最近一段时间的更新，再根据需要逐步扩展历史数据：

```bash
# 先更新最近30个交易日
python examples/stock_daily_incremental_update.py --days 30

# 如果需要全量数据，再执行全量更新
python examples/stock_daily_incremental_update.py --full-update
```

## 运行环境要求

1. Python 3.7+
2. 必要的环境变量:
   - `TUSHARE_TOKEN`: Tushare API令牌
   - `DATABASE_URL`: 数据库连接URL（可选，也可以在配置文件中设置）

## 集成到定时任务

在Linux/Unix系统中，可以使用crontab添加定时任务：

```bash
# 每天晚上8点执行增量更新
0 20 * * * cd /path/to/project && python examples/stock_daily_incremental_update.py --auto >> /path/to/logs/update.log 2>&1
```

## 注意事项

1. 增量更新适合日常维护，但如果长时间未更新，建议使用全量更新或指定较大的时间范围
2. 执行前请确保数据库配置正确，且有足够的权限
3. 如果遇到API限流问题，可以在配置文件中调整并发限制
4. 更新大量数据时，建议在非交易时间执行，避免影响交易系统 