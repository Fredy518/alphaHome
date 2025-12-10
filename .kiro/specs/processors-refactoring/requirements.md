# Requirements Document

## Introduction

本文档定义了 alphahome/processors 模块重构的需求规格。当前 processors 模块处于早期开发阶段，尚未投入使用，存在架构设计问题。本次重构旨在：

1. 建立清晰的三层架构（Engine → Task → Operation）
2. 从 data_infra 模块迁移成熟的数据处理逻辑和工具函数
3. 为未来的市场特征计算、技术指标计算等任务提供统一的处理框架

重构完成后，data_infra 模块将被废弃，其核心功能将整合到 processors 模块中。

## Glossary

- **Processor Engine**: 处理引擎，负责任务调度、并发控制和执行监控
- **Processor Task**: 处理任务，封装完整的业务处理流程（数据获取→处理→保存）
- **Operation**: 原子级数据处理操作，可复用的无状态数据转换函数
- **Operation Pipeline**: 操作流水线，将多个 Operation 组合成顺序执行的处理链
- **Cross-sectional Feature**: 横截面特征，同一时间点上多个股票的统计特征
- **Rolling Calculation**: 滚动计算，基于时间窗口的移动计算（如滚动均值、滚动标准差）
- **Transform Function**: 变换函数，对数据进行标准化、去极值等处理的工具函数

## Requirements

### Requirement 1: 操作层（Operations）重构

**User Story:** As a developer, I want a set of reusable atomic operations, so that I can compose complex data processing pipelines without duplicating code.

#### Acceptance Criteria

1. THE Processor Operations Layer SHALL provide transform operations including zscore, minmax_scale, rolling_zscore, rolling_percentile, winsorize, and quantile_bins
2. THE Processor Operations Layer SHALL provide rolling calculation operations including rolling_sum, rolling_rank, rolling_slope, and ema
3. THE Processor Operations Layer SHALL provide return calculation operations including diff_pct and log_return
4. THE Processor Operations Layer SHALL provide advanced operations including price_acceleration, rolling_slope_volatility_adjusted, and trend_strength_index
5. WHEN an Operation receives empty or null input data THEN the Operation SHALL return an empty DataFrame without raising exceptions
6. WHEN an Operation encounters zero variance in zscore calculation THEN the Operation SHALL return zeros instead of NaN or infinity values
7. THE Operation base class SHALL define an async apply method that accepts a DataFrame and returns a transformed DataFrame

### Requirement 2: 任务层（Tasks）重构

**User Story:** As a developer, I want a clear task structure for different data processing domains, so that I can organize and maintain processing logic efficiently.

#### Acceptance Criteria

1. THE Processor Task Layer SHALL organize tasks into domain-specific subdirectories: market, index, and style
2. THE ProcessorTaskBase class SHALL require subclasses to implement fetch_data, process_data, and save_result methods
3. WHEN a ProcessorTask executes THEN the ProcessorTask SHALL follow the sequence: fetch_data → process_data → save_result
4. THE ProcessorTaskBase class SHALL provide source_tables and table_name attributes for data lineage tracking
5. WHEN a ProcessorTask fails during execution THEN the ProcessorTask SHALL log the error with full context and raise the exception
6. THE ProcessorTaskBase class SHALL support async execution through the run method

### Requirement 3: 市场技术特征任务

**User Story:** As a quantitative analyst, I want to compute cross-sectional market technical features, so that I can analyze market-wide momentum, volatility, and volume patterns.

#### Acceptance Criteria

1. THE MarketTechnicalTask SHALL compute momentum distribution features including 5/10/20/60-day momentum median and positive ratio
2. THE MarketTechnicalTask SHALL compute volatility distribution features including 20/60-day realized volatility median and high/low volatility ratios
3. THE MarketTechnicalTask SHALL compute volume activity features including volume ratio median and expand/shrink ratios
4. THE MarketTechnicalTask SHALL compute price-volume divergence features including price_up_vol_down_ratio and vol_price_aligned_ratio
5. WHEN computing derived features THEN the MarketTechnicalTask SHALL apply rolling zscore normalization with a 252-day window
6. WHEN computing derived features THEN the MarketTechnicalTask SHALL apply rolling percentile calculation with a 500-day window

### Requirement 4: 变换工具函数迁移

**User Story:** As a developer, I want the proven transform functions from data_infra to be available in processors, so that I can use battle-tested utilities for data processing.

#### Acceptance Criteria

1. THE transforms module SHALL implement zscore function that handles zero variance by returning zeros
2. THE transforms module SHALL implement rolling_zscore function with configurable window and min_periods parameters
3. THE transforms module SHALL implement rolling_percentile function that returns values in range [0, 1]
4. THE transforms module SHALL implement winsorize function that clips values beyond n standard deviations
5. THE transforms module SHALL implement rolling_slope function with both OLS and ROC methods
6. THE transforms module SHALL implement price_acceleration function that returns slope_long, slope_short, acceleration, acceleration_zscore, and slope_ratio columns
7. THE transforms module SHALL implement trend_strength_index function that computes multi-period trend strength and consistency

### Requirement 5: 引擎层增强

**User Story:** As a system operator, I want the processor engine to handle task execution reliably, so that I can run batch processing jobs with proper error handling and monitoring.

#### Acceptance Criteria

1. THE ProcessorEngine SHALL support concurrent task execution with configurable max_workers
2. WHEN executing multiple tasks THEN the ProcessorEngine SHALL track execution status for each task
3. WHEN a task fails THEN the ProcessorEngine SHALL continue executing remaining tasks and report failures at the end
4. THE ProcessorEngine SHALL provide execute_task method that accepts task name and returns execution result
5. THE ProcessorEngine SHALL support task discovery through the unified task registry

### Requirement 6: 数据序列化与持久化

**User Story:** As a developer, I want consistent data serialization, so that processed results can be reliably stored and retrieved.

#### Acceptance Criteria

1. WHEN saving DataFrame results THEN the ProcessorTask SHALL serialize data to the configured database table
2. WHEN loading source data THEN the ProcessorTask SHALL deserialize data from database tables specified in source_tables
3. THE serialization process SHALL preserve DataFrame index and column types
4. WHEN serializing datetime index THEN the system SHALL use ISO 8601 format for consistency
