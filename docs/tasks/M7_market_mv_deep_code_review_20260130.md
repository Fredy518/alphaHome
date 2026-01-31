# M7 Market 域 MV 深度代码审查报告（2026-01-30）

范围：`alphahome/features/recipes/mv/market/*.py`（market 域全部 MV）及其导出/单测（`alphahome/features/recipes/mv/__init__.py`、`tests/unit/test_features/*`）。

> 审查目标：在“能跑”之外，进一步对齐 **单位口径/时间语义（排除当日）/PIT/质量监控/导入导出稳定性**，并把关键点落实为可执行测试或文档约束。

## 1. 已落地修复（本轮直接修改代码）

### 1.1 industry_features_daily：补齐偏度/峰度输出
- 文件：`alphahome/features/recipes/mv/market/industry_features_daily.py`
- 新增字段：
  - `industry_return_skew`
  - `industry_return_kurtosis_excess`（excess kurtosis，峰度减 3）
- 实现方式：两段聚合（先算均值/标准差，再算三四阶中心矩），避免聚合嵌套引用。

### 1.2 market_sentiment_daily：两融字段单位一致性
- 文件：`alphahome/features/recipes/mv/market/market_sentiment_daily.py`
- 发现：
  - `total_margin_balance/total_short_balance/total_margin_buy/total_margin_repay` 为“元”
  - `margin_net_buy_billion` 为“亿元”
  - 单位混用容易引发下游误读
- 修复：保留原“元”字段不破坏兼容，并新增 *_billion（亿元）字段：
  - `total_margin_balance_billion`
  - `total_margin_buy_billion`
  - `total_margin_repay_billion`
  - `total_short_balance_billion`

### 1.3 mv 包导出链路：移除失效的 MarketMarginMonthlyMV re-export
- 文件：`alphahome/features/recipes/mv/__init__.py`
- 发现：`MarketMarginMonthlyMV` 的模块文件已不存在，但顶层仍显式导入，导致 `import alphahome.features.recipes.mv` 直接报错。
- 修复：移除该 import 与 __all__ 导出，恢复 mv 包可用性。

### 1.4 market_stats：质量检查字段名滞后
- 文件：`alphahome/features/recipes/mv/market/market_stats_daily.py`
- 发现：`quality_checks.null_check.columns` 仍使用 `stock_count`（已不再输出）。
- 修复：改为 `total_stock_count/valid_stock_count`。

### 1.5 文档一致性
- 文件：`docs/tasks/M7_market_mv_audit_checklist.md`
- 更新项：
  - 行业偏度/峰度已实现
  - early_limit_count 不需要配置化（固定 10:00）
  - 两融字段单位说明（元 + *_billion 亿元）
  - market_margin_monthly 标记为“已废弃/已移除”并指向迁移路径

## 2. 测试增强（把审查点变成可回归）

- 文件：`tests/unit/test_features/test_mv_sql_patterns.py`
- 新增/增强断言：
  - `industry_return_skew`/`industry_return_kurtosis_excess` 字段存在
  - `total_margin_balance_billion`/`total_short_balance_billion` 字段存在

## 3. 关键语义审查结论（逐 MV）

### 3.1 时间语义（排除当日）
- ✅ `market_sentiment_daily`：52 周新高新低窗口使用 `... AND 1 PRECEDING`，避免自比较。
- ✅ `index_features_daily` / `macro_rate_daily`：分位数/历史统计使用 `trade_date < 当前日`（或等价逻辑），排除当日。
- ⚠️ `market_technical_daily`：动量/波动使用当日包含窗口是合理的；量比窗口显式排除当日（`... AND 1 PRECEDING`）已对齐。

### 3.2 PIT
- ✅ `index_fundamental_daily`：使用 `weight_date <= trade_date` 且取 `MAX(weight_date)`，确保 PIT 权重。

### 3.3 单位口径
- ✅ `margin_turnover_daily`：成交额 `amount` 千元→元（*1000），口径清晰。
- ✅ `market_sentiment_daily`：补齐两融字段单位对齐（元 + *_billion 亿元）。
- ⚠️ 建议后续（非必须）：对所有涉及“亿元/万元/元”的字段在字段名或文档中强制注明单位（例如 `_billion` / `_wan` / `_yuan`），避免隐式约定。

### 3.4 血缘字段一致性
- 当前存在两种分隔符：逗号与竖线（`,` vs `|`）。
- 这不会影响 SQL，但会影响后续解析的一致性。
- 建议后续统一为一种（推荐 `|`，更少与 SQL/CSV 冲突），并加一个测试锁定格式。

## 4. 后续建议（不强制，但高收益）

1. **Schema 参数一致性**：目前多数 MV 的 SQL 直接写死 `features.`，`schema=` 参数仅用于校验而不用于渲染。建议在 BaseFeatureView 层统一替换 schema（或彻底移除 schema 入参），避免误用。
2. **字段级契约测试**：对外部依赖更强的 MV（`market_sentiment_daily`、`etf_flow_daily`）可以补充“字段清单快照测试”（只验证输出列名集合，不跑 SQL）。
3. **质量监控覆盖面**：目前仅 `market_stats` 有 `quality_checks`。建议把 `market_sentiment_daily`（limit_up_count/new_high_count/margin_balance）也纳入最小化监控（NULL 比例 + 环比突变）。

## 5. 本轮验证

- 已执行：
  - `python -m pytest tests/unit/test_features/test_mv_recipes.py tests/unit/test_features/test_mv_sql_patterns.py -v`
- 结果：全部通过（31 passed）。
