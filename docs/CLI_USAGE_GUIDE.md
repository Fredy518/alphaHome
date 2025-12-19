# AlphaHome 统一 CLI 管理工具

## 概述

`ah` 命令是 AlphaHome 的统一命令行界面入口，整合了所有生产脚本、数据库工具和系统功能到一个统一的命令体系中。

## 安装

统一 CLI 作为 alphahome 包的一部分自动安装：

```bash
pip install -e .
```

安装后，以下命令都会可用：
- `ah` - 主要统一入口（推荐使用）
- `alphahome-cli` - `ah` 的别名
- `alphahome` - GUI 启动（保持现有行为）
- `alphahome-ddb` - DolphinDB 工具（保持现有行为）
- `refresh-materialized-view` - 物化视图管理（保持现有行为）

## 快速开始

### 查看帮助

```bash
# 查看主命令帮助
ah --help

# 查看特定命令组的帮助
ah prod --help
ah ddb --help
ah mv --help
ah gui --help

# 查看特定子命令的帮助
ah prod run --help
ah ddb init-kline5m --help
```

### 列出生产脚本

```bash
ah prod list
```

输出可用的生产脚本别名和描述。

## 命令详解

### 1. 生产脚本管理 (`ah prod`)

#### 列出可用脚本
```bash
ah prod list
```

#### 运行脚本

基本语法：
```bash
ah prod run <alias> [-- script_args...]
```

**可用脚本别名**：
- `data-collection` - 通用数据采集智能增量更新
- `pit-update` - PIT 数据统一更新
- `g-factor` - G因子年度并行计算启动器
- `g-factor-quarterly` - G因子季度并行计算启动器
- `p-factor` - P因子年度并行计算启动器
- `p-factor-quarterly` - P因子季度并行计算启动器

**示例**：

```bash
# 运行数据采集，设置 5 个并发工作进程
ah prod run data-collection -- --workers 5

# 运行数据采集，指定日志级别为 DEBUG
ah prod run data-collection -- --workers 5 --log_level DEBUG

# 运行 G因子计算，设置时间范围
ah prod run g-factor -- --start_year 2020 --end_year 2024 --workers 10

# 运行 PIT 数据更新
ah prod run pit-update -- --target all --mode incremental
```

**参数说明**：
- `--` 之后的参数会被原样传递给脚本
- 每个脚本支持的参数不同，使用脚本的 `--help` 查看详情
- 通过 `ah prod run <alias> -- --help` 可以看到脚本的完整参数列表

### 2. DolphinDB 工具 (`ah ddb`)

#### 初始化 5分钟 K 线表

```bash
ah ddb init-kline5m [--db-path PATH] [--table TABLE] [--start-month M] [--end-month M]
```

**示例**：
```bash
# 使用默认配置初始化
ah ddb init-kline5m

# 指定自定义路径和分区范围
ah ddb init-kline5m --db-path dfs://my_kline --start-month 202001 --end-month 202412
```

#### 导入 Hikyuu 5分钟数据

```bash
ah ddb import-hikyuu-5min [--codes CODES] [--codes-file FILE] [--incremental] [--init]
```

**示例**：
```bash
# 导入特定股票的 5分钟数据
ah ddb import-hikyuu-5min --codes "000001.SZ,600000.SH" --incremental

# 从文件读取股票代码
ah ddb import-hikyuu-5min --codes-file scripts/tickers/sh_all.txt

# 初始化表并导入数据
ah ddb import-hikyuu-5min --codes "000001.SZ" --init --incremental
```

#### 删除数据库

```bash
ah ddb drop-db [--db-path PATH] --yes
```

**注意**：
- 必须提供 `--yes` 参数以确认删除操作
- 此操作不可撤销

### 3. 物化视图管理 (`ah mv`)

#### 刷新单个视图

```bash
ah mv refresh <view_name> [--strategy STRATEGY]
```

**示例**：
```bash
# 使用默认策略（full）刷新视图
ah mv refresh pit_financial_indicators_mv

# 使用并发策略刷新
ah mv refresh pit_financial_indicators_mv --strategy concurrent
```

#### 刷新所有视图

```bash
ah mv refresh-all [--strategy STRATEGY]
```

**示例**：
```bash
# 全量刷新所有视图
ah mv refresh-all

# 并发刷新
ah mv refresh-all --strategy concurrent
```

#### 查看视图状态

```bash
ah mv status <view_name>
```

#### 查看所有视图状态

```bash
ah mv status-all
```

### 4. 启动图形界面 (`ah gui`)

```bash
ah gui
```

启动 AlphaHome 图形用户界面。

## 全局参数

以下参数对所有命令都适用，放在命令名之前：

```bash
ah [GLOBAL_OPTIONS] <command> <subcommand> [COMMAND_OPTIONS]
```

### 日志级别

```bash
ah --log-level DEBUG prod list
ah --log-level INFO prod run data-collection -- --workers 3
```

可选值：`DEBUG`, `INFO`, `WARNING`, `ERROR`（默认：`INFO`）

### 输出格式

```bash
ah --format json prod list
```

可选值：`text`, `json`（默认：`text`）

### 配置文件

```bash
ah --config ~/.alphahome/config.json prod list
```

指定自定义配置文件路径。

## 退出码

CLI 使用标准的退出码约定：

| 退出码 | 含义 | 例子 |
|-------|------|------|
| 0 | 成功 | 命令正常完成 |
| 1 | 业务失败 | 脚本执行失败，数据处理错误 |
| 2 | 参数错误 | 缺少必填参数，无效的参数值 |
| 3 | 资源不可用 | 数据库连接失败，文件不存在 |
| 4 | 内部错误 | 未处理的异常 |
| 130 | 用户中断 | 用户按 Ctrl-C 中断 |

## 实际使用场景

### 场景 1：日常数据更新

```bash
# 每天运行一次数据采集
0 2 * * * cd /path/to/alphahome && ah --log-level INFO prod run data-collection -- --workers 3

# 每周刷新一次物化视图
0 3 * * 0 cd /path/to/alphahome && ah mv refresh-all --strategy concurrent
```

### 场景 2：月度因子计算

```bash
# 计算 G因子（按年份）
ah prod run g-factor -- --start_year 2024 --end_year 2024 --workers 8

# 计算 P因子（按季度）
ah prod run p-factor-quarterly -- --start_year 2024 --end_year 2024
```

### 场景 3：测试与调试

```bash
# 使用 DEBUG 日志运行一个命令
ah --log-level DEBUG prod run data-collection -- --dry-run

# 查看某个命令的完整帮助
ah prod run g-factor -- --help
```

## 故障排除

### 问题 1：命令未找到

```
Command 'ah' not found
```

**解决**：确保安装了最新版本的 alphahome：
```bash
pip install -e . --upgrade
```

### 问题 2：导入错误

```
ImportError: No module named 'alphahome.cli'
```

**解决**：重新安装包：
```bash
pip install -e . --no-deps
```

### 问题 3：参数错误

```
error: invalid choice: 'invalid-script' (choose from 'data-collection', 'g-factor', ...)
```

**解决**：使用 `ah prod list` 查看可用的脚本别名。

### 问题 4：数据库连接失败

```
ResourceError: Unable to connect to database
```

**解决**：检查配置文件中的数据库连接信息：
```bash
cat ~/.alphahome/config.json
```

## 高级用法

### 链式执行

```bash
# 先刷新物化视图，再运行数据采集
ah mv refresh-all && ah prod run data-collection -- --workers 3
```

### 条件执行

```bash
# 仅在数据采集成功时刷新视图
ah prod run data-collection -- --workers 3 && ah mv refresh-all --strategy concurrent
```

### 在脚本中使用

```bash
#!/bin/bash

# 每日更新脚本
echo "Starting daily update..."

ah --log-level INFO prod run data-collection -- --workers 5 || exit 1
echo "Data collection completed"

ah --log-level INFO mv refresh-all || exit 1
echo "Materialized views refreshed"

echo "Daily update finished successfully"
```

## 后续优化计划

1. **脚本改造**：逐步改造生产脚本为包内可导入模块，避免 subprocess 开销
2. **命令补全**：提供 bash/zsh 命令补全脚本
3. **扩展命令**：添加新的命令组（factor 计算、数据分析等）
4. **输出格式**：支持更多的输出格式（CSV、JSON、表格等）
5. **配置管理**：支持通过 CLI 管理配置

## 获取帮助

如有问题或建议，请参考项目文档或提交 Issue。

---

**版本**: 1.0
**最后更新**: 2025-12-19
