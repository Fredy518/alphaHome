# Requirements Document

## Introduction

本文档定义了 alphahome/processors 模块的数据分层规范。当前 processors 任务混合了数据处理与特征计算逻辑，缺少清晰的分层边界。本次重构旨在：

1. 建立清晰的"处理层（Clean）vs 特征层（Feature）"分层规范
2. 创建独立的 `clean` schema 存放已清洗、标准化的数据，作为研究和特征层的统一输入
3. 对现有任务进行分类梳理，明确拆分方向
4. 定义特征入库白名单与判定标准

由于当前模块尚未正式投入使用，允许破坏性变更。

## Scope

**范围内：**
- 处理层：数据拉取、校验、对齐、单位/复权/币种标准化、血缘记录、幂等入库（输出到 `clean` schema）
- 特征层：在处理层数据上计算滚动/分位/zscore/去极值/归一化/插值等，不与处理层耦合
- 任务层：编排 fetch → clean → feature（可选）→ save，不内嵌特征实现
- 现有任务分类与拆分指引

**范围外：**
- 策略级 alpha 评估与回测
- 具体重构代码实现（本文档仅定义规范）
- 线上服务部署与监控

## Success Criteria

| 成功标准 | 输出物 | 验收方式 |
|----------|--------|----------|
| clean schema 建立 | `clean` schema 中建立核心表（index_valuation_base, index_volatility_base, industry_base, market_technical_base），均包含时间列/主键/血缘/单位元数据 | 表结构 DDL 评审通过 |
| 现有任务分类 | 任务分类表（CSV/Markdown），覆盖所有现有 processor 任务 | 分类表评审通过 |
| 特征入库白名单 | 白名单文档，包含判定流程、更新/回填策略、版本管理方案 | 白名单评审通过 |
| 分层边界契约 | 接口契约文档（处理层输出 schema、特征层函数签名） | 契约文档评审通过 |

## Key Standards Summary

| 标准项 | 规范值 |
|--------|--------|
| 时间列 | `trade_date`（YYYYMMDD 或 datetime） |
| 主键 | `trade_date + ts_code`（股票级）或按业务调整 |
| 标的编码 | `ts_code`（如 000001.SZ） |
| 货币单位 | CNY（元） |
| 数量单位 | 股/手（明确标注） |
| 复权口径 | 表元数据中明确标注（前复权/后复权/不复权） |
| 血缘字段 | `_source_table`, `_processed_at`, `_data_version` |
| 幂等策略 | UPSERT（默认全量覆盖，可配置） |
| min_periods 默认 | 等于 window（减少早期窗口偏差） |
| 滚动/分位 NaN 处理 | 不足窗口保留 NaN，不填 0 |

## Glossary

- **处理层（Clean Layer）**: 负责数据拉取、校验、对齐、标准化的层级，输出到 `clean` schema
- **特征层（Feature Layer）**: 在处理层数据基础上计算衍生特征的层级，保持纯函数特性
- **任务层（Task Layer）**: 编排数据流的层级，调用处理层和特征层组件
- **clean schema**: 数据库中存放已清洗数据的独立 schema
- **血缘（Lineage）**: 数据来源和处理过程的元数据记录
- **幂等入库**: 重复执行相同操作产生相同结果的数据写入方式
- **复权口径**: 股票价格复权的计算方式（前复权/后复权/不复权）
- **UPSERT**: INSERT ON CONFLICT UPDATE，主键存在时更新，不存在时插入

## Requirements

### Requirement 1: 处理层数据校验

**User Story:** As a data engineer, I want the clean layer to validate incoming data, so that downstream consumers receive consistent and reliable data.

**Rationale:** 脏数据会污染下游特征计算和策略研究，必须在入口处拦截。

#### Acceptance Criteria

1. WHEN the Clean Layer receives source data THEN the Clean Layer SHALL validate column types against the expected schema
2. WHEN the Clean Layer detects missing required columns THEN the Clean Layer SHALL raise an exception with the list of missing columns
3. WHEN the Clean Layer detects duplicate records by primary key THEN the Clean Layer SHALL keep the latest record and log the deduplication count
4. WHEN the Clean Layer detects null values in required fields THEN the Clean Layer SHALL reject the batch and report the null field names
5. WHEN the Clean Layer detects values outside valid ranges THEN the Clean Layer SHALL flag the records with a `_validation_flag` column
6. THE Clean Layer SHALL NOT silently drop or rename columns during processing

#### Required Columns by Table Type

| 表类型 | 必需列 |
|--------|--------|
| 股票日频 | `trade_date`, `ts_code`, `open`, `high`, `low`, `close`, `vol` |
| 指数日频 | `trade_date`, `ts_code`, `close` |
| 行业分类 | `trade_date`, `ts_code`, `industry_code` |

*注：其它表类型的必需列按各任务 schema 定义，详见任务分类表。*

### Requirement 2: 处理层数据对齐

**User Story:** As a quantitative analyst, I want all clean data aligned to a standard format, so that I can join and compare data from different sources without manual transformation.

**Rationale:** 不同数据源使用不同的日期格式和标的编码，统一后才能高效 JOIN。

#### Acceptance Criteria

1. THE Clean Layer SHALL align all date columns to `trade_date` column with format YYYYMMDD (integer) or datetime
2. THE Clean Layer SHALL align all security identifier columns to `ts_code` format (e.g., 000001.SZ)
3. THE Clean Layer SHALL use `trade_date + ts_code` as the default composite primary key for stock-level tables
4. WHEN source data uses different date formats THEN the Clean Layer SHALL convert to the standard format using explicit parsing rules
5. WHEN source data uses different security identifiers THEN the Clean Layer SHALL map to `ts_code` using the security master table
6. THE Clean Layer SHALL enforce primary key uniqueness at write time

#### Identifier Mapping Rules

| 源格式 | 目标格式 | 映射规则 |
|--------|----------|----------|
| `000001` | `000001.SZ` | 6位代码 + 交易所后缀 |
| `sh600000` | `600000.SH` | 去前缀 + 交易所后缀 |
| `symbol` | `ts_code` | 查询 security_master 表 |

*注：`ts_code` 为 clean 层主键列标准。如需对外暴露 `symbol`，通过 security_master 映射，不在 clean 层存储 symbol 列。*

### Requirement 3: 处理层数据标准化

**User Story:** As a data consumer, I want standardized units and adjustment factors, so that I can use data directly without manual conversions.

**Rationale:** 不同数据源的单位不一致（万元/亿元/元），统一后避免计算错误。

#### Acceptance Criteria

1. THE Clean Layer SHALL convert all monetary values to CNY (Chinese Yuan) with unit Yuan (元)
2. THE Clean Layer SHALL convert volume units to shares (股) for stock data
3. THE Clean Layer SHALL document the price adjustment method in table metadata column `_adj_method`
4. THE Clean Layer SHALL preserve original unadjusted price columns with suffix `_unadj` when adjustment is applied
5. WHEN source data uses different units THEN the Clean Layer SHALL apply the appropriate conversion factor and log the conversion
6. THE Clean Layer SHALL handle corporate actions (分红、送转、拆并) according to the documented adjustment method

#### Unit Conversion Rules

| 源单位 | 目标单位 | 转换因子 |
|--------|----------|----------|
| 万元 | 元 | × 10,000 |
| 亿元 | 元 | × 100,000,000 |
| 手 | 股 | × 100 |

### Requirement 4: 处理层血缘记录

**User Story:** As a data governance officer, I want data lineage tracked, so that I can trace data issues back to their source.

**Rationale:** 数据问题排查需要追溯来源，血缘记录是数据治理的基础。

#### Acceptance Criteria

1. THE Clean Layer SHALL add `_source_table` column recording the source table name (multiple sources separated by comma)
2. THE Clean Layer SHALL add `_processed_at` column recording the processing timestamp (UTC)
3. THE Clean Layer SHALL add `_data_version` column recording the data version (format: YYYYMMDD_HHMMSS or batch ID)
4. THE Clean Layer SHALL add `_ingest_job_id` column recording the task execution instance ID for traceability
5. WHEN data is updated THEN the Clean Layer SHALL overwrite lineage fields with the latest processing metadata
6. THE Clean Layer SHALL store lineage as inline columns (not in a separate metadata table)

### Requirement 5: 处理层幂等入库

**User Story:** As a system operator, I want idempotent data loading, so that I can safely retry failed jobs without creating duplicates.

**Rationale:** 任务失败重试是常态，幂等性保证数据一致性。

#### Acceptance Criteria

1. WHEN the Clean Layer writes data with existing primary key THEN the Clean Layer SHALL perform UPSERT (default: full row replacement, configurable merge strategy)
2. WHEN the Clean Layer writes a batch THEN the Clean Layer SHALL use database transactions to ensure atomicity
3. WHEN a write operation fails mid-batch THEN the Clean Layer SHALL rollback the entire batch
4. THE Clean Layer SHALL support incremental updates based on date range parameters
5. THE Clean Layer SHALL use configurable batch size (default: 10,000 rows) with configurable retry attempts (default: 3)

#### Conflict Resolution Strategy

| 场景 | 默认策略 | 可配置选项 |
|------|----------|------------|
| 主键冲突 | 全量覆盖（UPSERT） | 合并策略（仅更新非空列） |
| 部分列更新 | 全行写入 | 可配置合并策略 |
| 批次失败 | 整批回滚，重试整批 | 可配置重试次数和间隔 |

### Requirement 6: 特征层纯函数设计

**User Story:** As a developer, I want feature calculations to be pure functions, so that I can test and debug them in isolation.

**Rationale:** 纯函数易于测试、缓存和并行化，是可靠特征计算的基础。

#### Acceptance Criteria

1. THE Feature Layer functions SHALL accept DataFrame input and return DataFrame output without side effects
2. THE Feature Layer functions SHALL NOT access external state (database, file system, global variables)
3. THE Feature Layer functions SHALL NOT modify the input DataFrame
4. THE Feature Layer functions SHALL preserve index alignment between input and output
5. THE Feature Layer functions SHALL preserve NaN values in output where input contains NaN
6. WHEN Feature Layer functions encounter division by zero THEN the functions SHALL return NaN (not zero, not infinity)
7. THE Feature Layer functions SHALL support `min_periods` parameter for rolling calculations with default equal to window size
8. WHEN rolling/percentile calculations have insufficient window data THEN the functions SHALL return NaN (not fill with 0)
9. THE Feature Layer functions SHALL operate only on data from the clean schema
10. THE Feature Layer output columns SHALL include metadata annotation (naming convention, unit, frequency, lag) in docstring or schema

### Requirement 7: 特征入库白名单与判定流程

**User Story:** As a system architect, I want clear criteria and process for which features to persist, so that we balance storage costs with computation efficiency.

**Rationale:** 并非所有特征都需要入库，需要明确判定标准避免存储膨胀。

#### Acceptance Criteria

1. THE system SHALL persist features that meet high-reuse criteria (used by 3+ strategies or reports)
2. THE system SHALL persist features that meet high-computation-cost criteria (window > 252 days or requiring full history scan)
3. THE system SHALL persist features required for real-time queries (latency requirement < 100ms)
4. THE system SHALL NOT persist strategy-specific composite scores
5. THE system SHALL NOT persist features computable in under 1 second from clean data
6. THE system SHALL review feature persistence decisions quarterly

#### Feature Persistence Decision Flow

```
1. 复用度评估：被 3+ 策略/报告使用？ → 是 → 入库
2. 重算成本评估：窗口 > 252 天或需全量扫描？ → 是 → 入库
3. 查询延迟要求：需 < 100ms 响应？ → 是 → 入库
4. 以上均否 → 不入库，现算或缓存
```

#### Update and Backfill Strategy

| 场景 | 策略 |
|------|------|
| 日常更新 | 增量计算当日特征，UPSERT 入库，SLA: T+1 09:00 前完成 |
| 历史回填 | 按月分区批量计算，分区替换，触发条件：数据修正/新增历史数据 |
| 参数变更 | 全量重算，版本号递增 |

#### Version Management

| 场景 | 版本管理方式 |
|------|--------------|
| 参数微调 | `_feature_version` 列递增（v1 → v2） |
| 算法升级 | 新建表（feature_xxx_v2）或分区（partition by version） |
| 历史版本保留 | 保留最近 2 个版本，超期归档 |

#### Data Volume Control

| 策略 | 说明 |
|------|------|
| 分区 | 按 trade_date 月分区 |
| 分表 | 超过 1 亿行考虑按年分表 |
| 归档 | 超过 5 年的历史数据归档到冷存储 |

#### Initial Whitelist (Draft)

| 类别 | 特征 | 入库理由 |
|------|------|----------|
| 估值/利差 | PE/PB 分位（10Y/12M）、ERP | 高复用、长窗口 |
| 波动 | RV 多窗、分位、短长比 | 高复用、风控必需 |
| IV | 期限结构关键点（7/30/60/90/180天） | 高复用、重算成本高 |
| 基差 | 基差/基差率及其分位/zscore | 高复用 |
| 资金流/宽度 | 主力净流入率、资金流分位/zscore | 高复用 |

### Requirement 8: 任务层编排

**User Story:** As a developer, I want tasks to orchestrate data flow without embedding feature logic, so that I can maintain clear separation of concerns.

**Rationale:** 任务层应专注编排，特征逻辑下沉到特征层便于复用和测试。

#### Acceptance Criteria

1. THE Task Layer SHALL follow the sequence: fetch → clean → feature (optional) → save
2. THE Task Layer SHALL NOT contain inline feature calculation logic (no rolling/zscore/percentile code in task files)
3. THE Task Layer SHALL call Feature Layer functions through explicit imports from `processors.operations`
4. WHEN a task requires features THEN the task SHALL declare feature dependencies in `feature_dependencies` attribute
5. THE Task Layer SHALL support skipping the feature step via `skip_features=True` parameter

### Requirement 9: 现有任务分类

**User Story:** As a tech lead, I want existing tasks classified by their refactoring direction, so that I can plan the migration incrementally.

**Rationale:** 渐进式迁移需要清晰的任务分类和优先级。

#### Acceptance Criteria

1. THE system SHALL produce a classification table for each existing task with columns: task_name, input_tables, output_table, primary_key, time_column, feature_columns, classification
2. THE classification SHALL use labels: "处理层保留", "特征下沉", "混合需拆分"
3. THE classification SHALL identify which columns belong to clean layer vs feature layer
4. THE classification SHALL document the target clean schema table name for each task
5. THE classification SHALL list the feature functions to extract for tasks marked "特征下沉" or "混合需拆分"
6. THE classification table SHALL be completed within 2 weeks and reviewed by tech lead

#### Classification Output Format

| 任务 | 输入表 | 输出表 | 主键 | 时间列 | 特征列 | 分类 | 目标 clean 表 | 待提取特征函数 |
|------|--------|--------|------|--------|--------|------|---------------|----------------|
| index_valuation | ... | ... | ... | ... | pe_pctl, pb_pctl, erp | 混合需拆分 | clean.index_valuation_base | rolling_percentile, erp_calc |

