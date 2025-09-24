# PIT数据统一管理器

## 概述

`main.py` 是PIT数据模块的统一入口程序，负责协调所有PIT数据表的历史回填、增量更新等功能。

## 功能特点

1. **统一管理**：管理4个PIT数据表（行业分类、资产负债表、利润表、财务指标）
2. **顺序执行**：按正确依赖顺序执行数据更新
3. **多种模式**：支持历史全量回填和增量更新
4. **数据验证**：提供数据完整性验证和状态检查
5. **命令行友好**：丰富的命令行参数配置

## 更新顺序

按以下顺序执行数据更新，确保依赖关系正确：

1. **行业分类** (`pit_industry_classification`) - 基础分类数据
2. **资产负债表** (`pit_balance_quarterly`) - 财务基础数据
3. **利润表** (`pit_income_quarterly`) - 利润相关数据
4. **财务指标** (`pit_financial_indicators`) - 基于前三者的计算指标

## 使用方法

### 基本语法

```bash
cd research/pit_data
python main.py --mode <模式> [参数...]
```

### 主要模式

#### 1. 历史全量回填所有表

```bash
# 使用默认参数（回填最近3年数据）
python main.py --mode full-backfill-all

# 指定日期范围
python main.py --mode full-backfill-all --start-date 2020-01-01 --end-date 2024-12-31

# 自定义批次大小
python main.py --mode full-backfill-all --batch-size 2000
```

#### 2. 增量更新所有表

```bash
# 使用默认参数（检查最近7天的变更）
python main.py --mode incremental-all

# 指定检查天数
python main.py --mode incremental-all --days 30

# 自定义批次大小
python main.py --mode incremental-all --batch-size 500
```

#### 3. 单表操作

```bash
# 历史全量回填单个表
python main.py --mode full-backfill --table pit_balance_quarterly --start-date 2023-01-01

# 增量更新单个表
python main.py --mode incremental --table pit_financial_indicators --days 14
```

#### 4. 状态检查

```bash
# 检查所有表状态
python main.py --mode status

# 显示可用表列表
python main.py --mode list-tables
```

#### 5. 数据验证

```bash
# 验证所有表数据完整性
python main.py --mode validate
```

## 命令行参数

### 主要参数

- `--mode`: 执行模式（必需）
  - `full-backfill-all`: 历史全量回填所有表
  - `incremental-all`: 增量更新所有表
  - `full-backfill`: 历史全量回填单个表
  - `incremental`: 增量更新单个表
  - `status`: 检查表状态
  - `validate`: 验证数据完整性
  - `list-tables`: 列出可用表

- `--table`: 目标表名（单表操作时必需）
  - `pit_industry_classification`
  - `pit_balance_quarterly`
  - `pit_income_quarterly`
  - `pit_financial_indicators`

### 可选参数

- `--start-date`: 开始日期 (YYYY-MM-DD)
- `--end-date`: 结束日期 (YYYY-MM-DD)
- `--days`: 增量更新检查天数
- `--batch-size`: 批次大小
- `--skip-validation`: 跳过依赖验证

## 使用示例

### 日常维护

```bash
# 每日增量更新
python main.py --mode incremental-all --days 1

# 每周增量更新
python main.py --mode incremental-all --days 7
```

### 定期维护

```bash
# 每月历史回填
python main.py --mode full-backfill-all --start-date 2024-01-01 --end-date 2024-12-31

# 季度性数据验证
python main.py --mode validate
```

### 问题排查

```bash
# 检查表状态
python main.py --mode status

# 验证特定表
python main.py --mode validate
```

### 特定场景

```bash
# 只更新财务指标（依赖其他表已更新）
python main.py --mode incremental --table pit_financial_indicators --days 30

# 重新回填特定时间范围的数据
python main.py --mode full-backfill --table pit_income_quarterly --start-date 2023-01-01 --end-date 2023-12-31
```

## 输出说明

### 执行结果

程序会输出详细的执行信息，包括：

- 处理的表数量
- 每张表处理的数据量
- 执行时间统计
- 成功/失败状态
- 详细的错误信息（如有）

### 日志输出

所有操作都会生成详细的日志，包括：

- 数据库连接信息
- 数据处理进度
- 性能统计
- 错误和警告信息

## 依赖关系

程序会自动处理表之间的依赖关系：

- 财务指标表依赖利润表和资产负债表
- 其他表相对独立
- 可以选择跳过依赖验证（`--skip-validation`）

## 注意事项

1. **数据库连接**：确保数据库配置正确
2. **权限要求**：需要对PIT表有读写权限
3. **数据量考虑**：历史回填可能需要较长时间
4. **依赖顺序**：建议按默认顺序执行以确保数据一致性
5. **监控日志**：定期检查日志文件了解执行情况

## 故障排除

### 常见问题

1. **连接失败**：检查数据库配置和网络连接
2. **权限不足**：确认用户有相应表权限
3. **数据不存在**：检查tushare数据源是否可用
4. **依赖问题**：确保前序表已正确更新

### 日志位置

日志文件保存在 `logs/` 目录下，命名格式：`pit_{table_name}_{date}.log`

## 技术支持

如遇到问题，请：

1. 查看详细日志输出
2. 检查数据库连接状态
3. 确认tushare数据源可用性
4. 联系技术支持团队
