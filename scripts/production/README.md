# 生产环境脚本

本目录包含用于生产环境运行的脚本，按功能模块组织，与分析和调试脚本分离。

## 📁 目录结构

```
scripts/production/
├── README.md                                           # 本文件
├── config/                                             # 配置文件目录
│   └── tushare_update_config.yaml                      # Tushare更新配置文件
├── data_updaters/                                      # 数据更新器模块
│   ├── tushare/                                        # Tushare数据更新
│   │   ├── tushare_smart_update_production.py         # Tushare智能增量更新主脚本
│   │   └── start_tushare_smart_update.bat              # Tushare更新批处理启动器
│   └── pit/                                            # PIT数据更新
│       ├── pit_data_update_production.py               # PIT数据统一更新主脚本
│       ├── start_pit_data_update.bat                   # PIT数据更新批处理启动器
│       ├── pit_balance_quarterly_manager.py           # 资产负债表管理器
│       ├── pit_income_quarterly_manager.py            # 利润表管理器
│       ├── pit_financial_indicators_manager.py        # 财务指标管理器
│       ├── pit_industry_classification_manager.py     # 行业分类管理器
│       ├── base/                                       # 基础组件
│       │   ├── pit_config.py                           # PIT配置管理
│       │   └── pit_table_manager.py                    # PIT表管理器
│       └── calculators/                                # 计算器
│           └── financial_indicators_calculator.py     # 财务指标计算器
├── factor_calculators/                                 # 因子计算器模块
│   └── g_factor/                                       # G因子计算
│       ├── g_factor_parallel_by_year.py               # G因子年度并行计算脚本
│       ├── g_factor_parallel_by_quarter.py            # G因子季度并行计算脚本
│       ├── start_parallel_g_factor_calculation.py     # G因子年度并行计算启动器
│       ├── start_parallel_g_factor_calculation.bat    # G因子年度计算批处理启动器
│       ├── start_parallel_g_factor_calculation_quarterly.py   # G因子季度并行计算启动器
│       └── start_parallel_g_factor_calculation_quarterly.bat  # G因子季度计算批处理启动器
└── shared/                                             # 共享工具和文档
    ├── TUSHARE_UPDATE_EXAMPLE.md                       # Tushare更新使用示例文档
    └── verify_stats.py                                 # 统计验证工具
```

## 🚀 使用方法

### Tushare 数据智能增量更新

#### Python启动器（推荐）

```bash
# 基本用法 - 使用默认参数
python scripts/production/data_updaters/tushare/tushare_smart_update_production.py

# 自定义并发数和重试策略
python scripts/production/data_updaters/tushare/tushare_smart_update_production.py --workers 5 --max_retries 5 --retry_delay 10

# 调试模式
python scripts/production/data_updaters/tushare/tushare_smart_update_production.py --log_level DEBUG
```

参数说明:
- `--workers`: 最大并发进程数 (默认: 3，建议: 2-5，考虑Tushare API限制)
- `--max_retries`: 单个任务最大重试次数 (默认: 3)
- `--retry_delay`: 重试间隔秒数 (默认: 5)
- `--log_level`: 日志级别: DEBUG, INFO, WARNING, ERROR (默认: INFO)

**并发数设置建议**:
- **保守设置**: `--workers 2` (适合网络不稳定或API限制严格的情况)
- **平衡设置**: `--workers 3` (默认，适合大多数情况)
- **激进设置**: `--workers 5` (适合网络稳定且API限制宽松的情况)
- **最大限制**: 不超过 Tushare API 并发限制的 1/2 (通常≤10)

#### 批处理启动器

```bash
# Windows系统 - 使用默认参数
scripts\production\data_updaters\tushare\start_tushare_smart_update.bat

# 自定义参数 (进程数, 重试次数, 间隔, 日志级别)
scripts\production\data_updaters\tushare\start_tushare_smart_update.bat 5 3 5 INFO
```

### PIT数据智能增量更新

**配置说明**: PIT数据更新使用项目的统一配置文件系统 (`config.json`)，无需额外的配置文件。请确保 `config.json` 中的数据库连接配置正确。

#### Python启动器（推荐）

```bash
# 基本用法 - 更新所有PIT数据（增量模式）
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental

# 更新特定数据类型
python scripts/production/data_updaters/pit/pit_data_update_production.py --target balance income --mode incremental

# 全量更新财务指标
python scripts/production/data_updaters/pit/pit_data_update_production.py --target financial_indicators --mode full

# 并行执行所有任务
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --parallel --workers 4

# 调试模式
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --log-level DEBUG
```

参数说明:
- `--target`: 要更新的数据类型
  - `balance`: 资产负债表数据
  - `income`: 利润表数据
  - `financial_indicators`: 财务指标数据
  - `industry_classification`: 行业分类数据
  - `all`: 所有数据类型
- `--mode`: 更新模式
  - `incremental`: 增量更新（默认）
  - `full`: 全量更新
- `--parallel`: 是否并行执行
- `--workers`: 最大并发进程数（默认: 2）

#### 批处理启动器

```bash
# Windows系统 - 更新所有数据
scripts\production\data_updaters\pit\start_pit_data_update.bat all incremental false

# 更新特定数据类型
scripts\production\data_updaters\pit\start_pit_data_update.bat "balance income" incremental false

# 并行全量更新
scripts\production\data_updaters\pit\start_pit_data_update.bat all full true
```

### 年度并行计算

#### Python启动器（推荐）

```bash
# 基本用法
python scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation.py --start_year 2020 --end_year 2024 --workers 5

# 参数说明
--start_year    开始年份 (默认: 2020)
--end_year      结束年份 (默认: 2024)
--workers       工作进程数 (默认: 10，会自动调整为不超过年份数)
--delay         进程启动间隔秒数 (默认: 2)
```

#### 批处理启动器

```bash
# Windows系统
scripts\production\factor_calculators\g_factor\start_parallel_g_factor_calculation.bat
```

### 季度并行计算（新增）

#### Python启动器（推荐）

```bash
# 基本用法
python scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16

# 参数说明
--start_year    开始年份 (默认: 2020)
--end_year      结束年份 (默认: 2024)
--workers       工作进程数 (默认: 16，会自动调整为不超过季度数)
--delay         进程启动间隔秒数 (默认: 2)
```

#### 批处理启动器

```bash
# Windows系统
scripts\production\factor_calculators\g_factor\start_parallel_g_factor_calculation_quarterly.bat 2020 2024 16
```

## 📊 功能说明

### Tushare 数据更新系统

#### 🚀 核心特性

- **智能任务发现**: 自动扫描并识别所有 Tushare 相关的数据获取任务
- **并行执行引擎**: 支持多任务并发执行，大幅提升更新效率
- **智能增量更新**: 只更新新增或变更的数据，避免重复处理
- **容错与重试**: 内置重试机制，确保网络波动时的稳定性
- **详细监控日志**: 实时显示执行进度和状态，提供详细的错误诊断信息
- **生产级可靠性**: 支持长时间运行，自动处理异常情况

#### 📋 支持的任务类型

目前支持以下类别的 Tushare 数据源任务（共 41 个任务）：

- **股票数据**: 基础信息、日线行情、财务数据等
- **基金数据**: 基金基本信息、净值数据、持仓数据、ETF数据等
- **指数数据**: 指数基本信息、日线行情、成分股权重、行业指数等
- **期货数据**: 合约信息、日线行情、持仓排名等
- **宏观数据**: CPI、HIBOR 等经济指标
- **财务数据**: 资产负债表、利润表、现金流量表等

#### ⚡ 性能优化

- **并发控制**: 可配置的最大并发进程数，避免系统过载
- **资源管理**: 智能的数据库连接池管理
- **内存优化**: 分批处理大量数据，避免内存溢出
- **网络优化**: 请求限流和超时控制，确保 API 稳定性

#### ⚠️ Tushare API 并发限制

**重要提醒**: Tushare 数据源有内置的并发控制机制

- **默认并发限制**: 20个并发请求
- **API特定限制**:
  - `daily` (日线数据): 80并发
  - `stock_basic` (股票基本信息): 20并发
  - `index_weight` (指数权重): 50并发
- **速率限制**: 每分钟最大请求数限制
- **建议设置**: 脚本并发数 ≤ Tushare API 限制的 1/2

**并发问题排查**:
1. 如果并行数=1成功，并行数>1失败 → 可能是 Tushare API 并发限制
2. 如果所有并行数都失败 → 检查网络连接或 API token
3. 如果部分任务失败 → 检查特定 API 的并发限制

### PIT数据更新系统

#### 🚀 核心特性

- **统一管理**: 集中管理所有PIT（Point-in-Time）数据更新任务
- **智能调度**: 支持增量更新和全量更新模式
- **并行执行**: 可配置并行处理，提升更新效率
- **模块化设计**: 每个数据类型都有独立的更新管理器
- **配置驱动**: 通过YAML配置文件灵活控制更新行为
- **生产级监控**: 详细的执行日志和性能监控

#### 📋 支持的数据类型

目前支持以下PIT数据类型的更新和管理：

- **资产负债表数据** (`pit_balance_quarterly`): 企业资产负债表的历史时点数据
- **利润表数据** (`pit_income_quarterly`): 企业利润表的时点性财务数据
- **财务指标数据** (`pit_financial_indicators`): 基于财务数据计算的各项指标
- **行业分类数据** (`pit_industry_classification`): 企业的行业分类信息

#### ⚡ 性能优化

- **智能批处理**: 根据数据类型自动调整批处理大小
- **连接池管理**: 使用项目的统一数据库连接池管理
- **内存优化**: 分批处理大量数据，控制内存使用
- **并发控制**: 可配置的最大并发数，平衡性能和稳定性

#### 📊 更新策略

- **增量更新**: 只更新最近N天的数据，适合日常维护
- **全量更新**: 重新计算所有历史数据，适合数据修复或首次导入
- **单任务更新**: 支持针对特定股票或时间范围的精确更新
- **并行处理**: 多任务并发执行，提高整体更新效率
- **统一配置**: 使用项目的config.json配置文件，无需额外配置

### G因子并行计算系统

#### 年度并行计算
- **并行策略**: "土法"并行 - 多终端年度并行计算
- **数据一致性**: 100%保证，使用原始计算逻辑
- **性能提升**: 理论加速比 = 工作进程数
- **监控方式**: 每个终端窗口显示一个工作进程的进度

#### 季度并行计算（新增）
- **并行策略**: "土法"并行 - 多终端季度并行计算
- **粒度更细**: 按季度分割，适合大规模计算
- **负载均衡**: 智能季度分配算法，平衡各进程计算量
- **灵活配置**: 支持自定义工作进程数和季度范围

### 脚本说明

#### 年度并行脚本
1. **factor_calculators/g_factor/start_parallel_g_factor_calculation.py**
   - 智能启动器，自动调整工作进程数
   - 支持跨平台（Windows/Linux/Mac）
   - 提供详细的性能预期和监控说明

2. **factor_calculators/g_factor/start_parallel_g_factor_calculation.bat**
   - Windows批处理版本
   - 简化配置，避免中文字符问题
   - 适合快速启动

3. **factor_calculators/g_factor/g_factor_parallel_by_year.py**
   - 核心计算脚本
   - 基于原始G因子计算逻辑
   - 支持年度范围计算

#### 季度并行脚本（新增）
4. **factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py**
   - 季度并行启动器，支持按季度分配计算任务
   - 智能季度分配算法，按时间顺序轮询分配
   - 自动调整工作进程数，避免资源浪费

5. **factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.bat**
   - Windows批处理版本
   - 支持命令行参数传递
   - 环境检查和错误处理

6. **factor_calculators/g_factor/g_factor_parallel_by_quarter.py**
   - 季度计算脚本，支持单个或多个季度计算
   - 基于原始G因子计算逻辑
   - 支持季度日期范围自动计算

## ⚠️ 注意事项

1. **数据库连接**: 监控数据库连接数，避免连接池耗尽
2. **磁盘空间**: 定期检查磁盘空间，确保有足够存储空间
3. **进程管理**: 可以随时关闭单个终端窗口来停止对应进程
4. **路径依赖**: 所有脚本需要在项目根目录下运行
5. **季度并行**: 季度并行比年度并行粒度更细，适合大规模计算
6. **资源分配**: 建议根据系统资源合理设置工作进程数

## ⚙️ 配置说明

### 配置文件

Tushare 更新脚本支持通过 `config/tushare_update_config.yaml` 进行详细配置：

```yaml
# 基本配置
production:
  max_workers: 3          # 最大并发进程数
  max_retries: 3          # 重试次数
  retry_delay: 5          # 重试间隔
  log_level: INFO         # 日志级别

# 任务筛选
task_filter:
  include_sources: ["tushare"]  # 只执行 tushare 数据源
  exclude_tasks: []             # 排除特定任务

# 性能监控
monitoring:
  enable_performance_log: true
  alert_thresholds:
    success_rate: 0.8           # 成功率告警阈值
```

### 环境变量

支持以下环境变量进行配置：

- `TUSHARE_MAX_WORKERS`: 覆盖默认并发数
- `TUSHARE_LOG_LEVEL`: 设置日志级别
- `TUSHARE_DRY_RUN`: 启用试运行模式

## 📊 监控和日志

### 日志文件

- 默认日志位置: `logs/tushare_production_update.log`
- 包含详细的执行过程和错误信息
- 支持按日期分割的日志轮转

### 执行报告

每次执行完成后会生成详细的执行报告，包括：

- 总执行时间和平均任务耗时
- 各任务的执行状态和耗时
- 成功率统计和失败原因分析
- 性能指标和系统资源使用情况

### 告警机制

内置告警机制，当以下情况发生时会记录警告：

- 任务成功率低于阈值（默认80%）
- 单任务执行时间超过限制
- 网络连接异常或API错误
- 数据库连接问题

## 🔧 维护说明

### 生产环境脚本

- 生产环境脚本与分析和调试脚本分离
- 路径引用已更新为相对路径
- 支持智能工作进程数调整
- 避免中文字符导致的Windows路径问题
- 新增 Tushare 数据更新支持

### 定期维护任务

建议设置以下定期维护任务：

1. **每日数据更新**: 使用脚本执行智能增量更新
2. **日志清理**: 定期清理旧的日志文件
3. **磁盘空间监控**: 确保有足够存储空间
4. **数据库维护**: 定期执行数据库索引优化

### 故障排除

常见问题及解决方案：

1. **网络连接问题**: 检查网络连接和 Tushare API 状态
2. **数据库连接失败**: 确认数据库服务运行正常
3. **内存不足**: 降低并发进程数或增加系统内存
4. **执行超时**: 调整超时设置或检查网络稳定性

## 📈 性能对比

### 数据更新脚本对比

| 脚本类型 | 并发性 | 适用场景 | 维护复杂度 | 可靠性 |
|---------|--------|----------|------------|--------|
| Tushare生产脚本 | 高并发 | 大规模数据更新 | 中等 | 高 |
| 年度并行计算 | 中并发 | 年度数据计算 | 低 | 高 |
| 季度并行计算 | 高并发 | 季度数据计算 | 中等 | 高 |

### 建议使用场景

- **小规模更新（<10个任务）**: 使用默认配置的 Tushare 脚本
- **大规模更新（≥10个任务）**: 增加并发数，启用详细日志
- **网络不稳定环境**: 增加重试次数和重试间隔
- **资源受限环境**: 降低并发数，启用性能监控

**建议**：
- 根据系统资源和网络状况调整并发数
- 定期查看执行日志，及时发现和解决问题
- 在非高峰期执行大规模数据更新
- 设置监控告警，及时响应执行异常
