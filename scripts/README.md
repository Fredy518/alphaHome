# 数据更新脚本

本目录包含了所有数据更新相关的脚本。

## 目录结构

```
scripts/
├── base/                    # 基础类文件
│   └── task_updater_base.py  # 更新任务的基类
├── tasks/                   # 任务更新脚本
│   ├── finance/            # 财务数据相关
│   │   ├── update_balancesheet.py  # 资产负债表更新
│   │   ├── update_cashflow.py      # 现金流量表更新
│   │   ├── update_express.py       # 业绩快报更新
│   │   ├── update_forecast.py      # 业绩预告更新
│   │   ├── update_income.py        # 利润表更新
│   │   └── update_indicator.py     # 财务指标更新
│   ├── stock/              # 股票数据相关
│   │   ├── update_adjfactor.py     # 复权因子更新
│   │   ├── update_daily.py         # 日线行情更新
│   │   └── update_dailybasic.py    # 每日指标更新
│   └── index/              # 指数数据相关（预留）
├── check_db_quality.py     # 数据库表质量全面检查工具
├── tools/                  # 工具脚本
│   └── check_stock_daily_quality.py  # 股票日线数据质量检查
├── batch/                  # 批量更新脚本
│   └── update_all_tasks.py          # 全量更新脚本
└── README.md               # 本文档

## 使用说明

### 1. 单任务更新

每个更新脚本都支持以下参数：

- `--quarters`: 指定要更新的季度数
- `--years`: 指定要更新的年数
- `--report-period`: 指定要更新的报告期（如 20230331）
- `--start-date`: 指定更新的起始日期
- `--end-date`: 指定更新的结束日期
- `--full-update`: 执行全量更新

示例：
```bash
# 更新最近4个季度的现金流量表数据
python scripts/tasks/finance/update_cashflow.py --quarters 4

# 更新指定报告期的利润表数据
python scripts/tasks/finance/update_income.py --report-period 20230331

# 更新指定日期范围的股票日线数据
python scripts/tasks/stock/update_daily.py --start-date 20230101 --end-date 20230331
```

### 2. 批量更新

使用 `update_all_tasks.py` 可以批量更新多个任务：

```bash
# 更新所有任务的最新数据
python scripts/batch/update_all_tasks.py

# 更新指定任务列表的数据
python scripts/batch/update_all_tasks.py --tasks "tushare_fina_cashflow,tushare_fina_income"
```

### 3. 数据质量检查

#### 3.1 单表数据质量检查

使用 `check_stock_daily_quality.py` 可以检查股票日线数据的质量：

```bash
# 检查指定日期范围的数据质量
python scripts/tools/check_stock_daily_quality.py --start-date 20230101 --end-date 20230331
```

#### 3.2 全面数据库质量检查

使用 `check_db_quality.py` 可以对整个数据库或指定表进行全面质量检查：

```bash
# 运行全面的数据库表质量检查
python scripts/check_db_quality.py

# 检查特定表的数据质量
python scripts/check_db_quality.py -t tushare_stock_daily tushare_fund_daily

# 检查特定日期范围的数据质量
python scripts/check_db_quality.py -s 20230101 -e 20231231

# 指定输出目录（默认为logs/db_quality_时间戳）
python scripts/check_db_quality.py -o logs/custom_quality_check

# 启用详细日志输出
python scripts/check_db_quality.py -v
```

支持的参数：
- `-t, --tables`: 指定要检查的表名（多个表用空格分隔）
- `-s, --start-date`: 指定检查的起始日期（格式：YYYYMMDD）
- `-e, --end-date`: 指定检查的结束日期（格式：YYYYMMDD）
- `-o, --output-dir`: 指定输出目录路径
- `-v, --verbose`: 启用详细日志输出
- `-h, --help`: 显示帮助信息

检查完成后，脚本会生成包含以下内容的报告：
- HTML格式的综合质量报告
- 各表数据覆盖率的可视化图表
- 详细的JSON格式质量数据
- 按月数据覆盖分析

## 开发说明

1. 所有更新脚本都继承自 `TaskUpdaterBase` 类
2. 新增更新脚本时，请遵循现有的目录结构和命名规范
3. 请确保添加适当的日志记录和错误处理
4. 建议在更新脚本中添加数据质量检查逻辑 