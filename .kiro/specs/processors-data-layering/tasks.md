# Implementation Plan

## Phase 1: 基础设施建立

- [x] 1. 实现 Clean Layer 核心组件
  - [x] 1.1 实现 TableSchema 和 ValidationResult 数据类
    - 定义 required_columns, column_types, nullable_columns, value_ranges
    - _Requirements: 1.1, 1.2, 1.4, 1.5_
  - [x] 1.2 编写 Property 1 属性测试：Column type validation
    - **Property 1: Column type validation**
    - **Validates: Requirements 1.1**
  - [x] 1.3 实现 DataValidator 类
    - 实现 validate(), validate_column_types(), detect_missing_columns()
    - 实现 detect_duplicates(), detect_nulls(), detect_out_of_range()
    - 实现 detect_dropped_columns()
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_
  - [x] 1.4 编写 Property 2-5 属性测试

    - **Property 2: Missing column detection**
    - **Property 3: Duplicate key deduplication**
    - **Property 4: Null value rejection**
    - **Property 5: Range validation flagging**
    - **Validates: Requirements 1.2, 1.3, 1.4, 1.5**

- [x] 2. 实现数据对齐组件






  - [x] 2.1 实现 DataAligner 类

    - 实现 align_date() 支持多种日期格式
    - 实现 align_identifier() 支持多种标的格式
    - 实现 build_primary_key()
    - 集成 security_master 依赖和 fallback 策略
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_
  - [x] 2.2 编写 Property 7-9 属性测试


    - **Property 7: Date format standardization**
    - **Property 8: Identifier mapping**
    - **Property 9: Primary key uniqueness enforcement**
    - **Validates: Requirements 2.1, 2.2, 2.4, 2.5, 2.6**

- [x] 3. 实现数据标准化组件






  - [x] 3.1 实现 DataStandardizer 类

    - 实现 convert_monetary() 支持万元/亿元转换
    - 实现 convert_volume() 支持手转股
    - 实现 preserve_unadjusted()
    - _Requirements: 3.1, 3.2, 3.4, 3.5_
  - [x] 3.2 编写 Property 10-11 属性测试


    - **Property 10: Unit conversion correctness**
    - **Property 11: Unadjusted price preservation**
    - **Validates: Requirements 3.1, 3.2, 3.4, 3.5**

- [x] 4. 实现血缘追踪组件





  - [x] 4.1 实现 LineageTracker 类


    - 实现 add_lineage() 添加 _source_table, _processed_at, _data_version, _ingest_job_id
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_
  - [x] 4.2 编写 Property 12 属性测试


    - **Property 12: Lineage metadata completeness**
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**

- [x] 5. 实现数据写入组件




  - [x] 5.1 实现 CleanLayerWriter 类


    - 实现 upsert() 默认仅支持 replace 策略
    - merge 策略需额外方案批准后再实现（参考 design.md 约束）
    - 实现事务管理和批量写入
    - 实现指数退避重试
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [x] 5.2 编写 Property 6 属性测试

    - **Property 6: Column preservation**
    - **Validates: Requirements 1.6**

- [x] 6. Checkpoint - 确保 Clean Layer 组件测试通过





  - 运行 `pytest alphahome/processors/tests/test_clean_layer/ -v`
  - 确保 Property 1-6, 7-12 相关测试通过
  - 参考 design.md 中的 Property Test Coverage Matrix 核对覆盖情况
  - Ask the user if questions arise.

## Phase 2: 特征层接口验证

- [x] 7. 验证现有特征函数符合接口契约





  - [x] 7.1 审查 operations/transforms.py 中的函数


    - 确认所有函数符合纯函数约束
    - 确认 min_periods 默认值为 window
    - 确认除零和 inf 处理返回 NaN
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8_

  - [x] 7.2 编写 Property 13-18 属性测试

    - **Property 13: Feature function immutability**
    - **Property 14: Index alignment preservation**
    - **Property 15: NaN preservation**
    - **Property 16: Division by zero handling**
    - **Property 17: min_periods default behavior**
    - **Property 18: Insufficient window NaN handling**
    - **Validates: Requirements 6.1, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8**

- [x] 8. Checkpoint - 确保特征层属性测试通过





  - 运行 `pytest alphahome/processors/tests/test_feature_layer/ -v`
  - 确保 Property 13-18 相关测试通过
  - 参考 design.md 中的 Property Test Coverage Matrix 核对覆盖情况
  - Ask the user if questions arise.

## Phase 3: 任务层增强

- [x] 9. 增强 ProcessorTaskBase





  - [x] 9.1 添加新属性


    - 添加 clean_table, feature_dependencies, skip_features 属性
    - _Requirements: 8.1, 8.4, 8.5_

  - [x] 9.2 实现 clean_data() 方法

    - 组合 DataValidator, DataAligner, DataStandardizer, LineageTracker
    - _Requirements: 8.1_

  - [x] 9.3 实现 compute_features() 方法框架

    - 添加 _validate_feature_dependencies() 校验
    - _Requirements: 8.2, 8.3_
  - [x] 9.4 更新 run() 方法流程


    - 实现 fetch → clean → feature → save 流程
    - 支持 skip_features 参数
    - 增量计算时遵循"回溯 max(window)"约定（参考 design.md）
    - _Requirements: 8.1, 8.5_

  - [x] 9.5 编写任务层单元测试

    - 测试执行流程
    - 测试 skip_features 行为
    - _Requirements: 8.1, 8.5_

- [ ] 10. Checkpoint - 确保任务层测试通过
  - 运行 `pytest alphahome/processors/tests/test_task_layer/ -v`
  - 确保任务执行流程和 skip_features 行为测试通过
  - Ask the user if questions arise.

## Phase 4: Clean Schema 建立

- [x] 11. 创建 clean schema 和核心表
  - [x] 11.1 创建 clean schema DDL
    - 创建 clean schema
    - **执行环境约束**：仅在 dev/staging 环境执行，生产环境需 DBA 审批
    - _Requirements: 成功标准_
  - [x] 11.2 创建 clean.index_valuation_base 表
    - 包含 trade_date, ts_code, pe_ttm, pb, 血缘列
    - _Requirements: 成功标准_
  - [x] 11.3 创建 clean.index_volatility_base 表
    - 包含 trade_date, ts_code, close, close_unadj, _adj_method, 血缘列
    - _Requirements: 成功标准_
  - [x] 11.4 创建 clean.industry_base 表
    - 包含 trade_date, ts_code, close, 血缘列
    - _Requirements: 成功标准_
  - [x] 11.5 创建 clean.money_flow_base 表
    - 包含 trade_date, total_net_mf_amount, total_circ_mv, 血缘列
    - _Requirements: 成功标准_
  - [x] 11.6 创建 clean.market_technical_base 表

    - 包含 trade_date, ts_code, close, vol, turnover_rate, 血缘列
    - _Requirements: 成功标准_

- [x] 12. Checkpoint - 确保 DDL 执行成功





  - 在 dev/staging 环境验证表结构
  - 确认所有表包含血缘列和正确的主键/索引
  - Ask the user if questions arise.

## Phase 5: 任务分类与迁移规划

- [x] 13. 完成任务分类表





  - [x] 13.1 分析现有任务并填写分类表


    - 覆盖 index_valuation, index_volatility, industry_return, industry_breadth
    - 覆盖 market_money_flow, market_technical, style_index, futures, option_iv
    - 标注分类：处理层保留/特征下沉/混合需拆分
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

  - [x] 13.2 输出任务分类表文档

    - 输出位置：`.kiro/specs/processors-data-layering/task-classification.md`
    - 包含 task_name, input_tables, output_table, primary_key, time_column, feature_columns, classification, target_clean_table, features_to_extract
    - _Requirements: 9.1_

- [x] 14. 完成特征入库白名单




  - [x] 14.1 审核初始白名单


    - 确认估值/利差、波动、IV、基差、资金流/宽度特征
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_
  - [x] 14.2 输出白名单文档


    - 输出位置：`.kiro/specs/processors-data-layering/feature-whitelist.md`
    - 包含：入库表名/列、更新频率、SLA、版本策略、回填策略
    - 与 design.md 中的"特征入库与版本管理"保持一致
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

- [x] 15. Final Checkpoint - 确保所有文档完成





  - 确认任务分类表（task-classification.md）已完成并评审通过
  - 确认特征入库白名单（feature-whitelist.md）已完成并评审通过
  - 运行全量测试 `pytest alphahome/processors/tests/ -v`
  - 参考 design.md 中的 Property Test Coverage Matrix 确认 18 个属性全部覆盖
  - Ask the user if questions arise.

