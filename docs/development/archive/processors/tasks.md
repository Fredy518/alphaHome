#!/usr/bin/env markdown

# processors 任务梳理与拆分计划（Draft）

## 任务分类（初稿）
- 偏特征/混合（需拆分特征层）
  - `index/index_valuation.py`：估值分位、ERP
  - `index/index_volatility.py`：RV、分位、短长比
  - `index/industry.py`：`IndustryBreadthTask`（宽度/分位）；`IndustryReturnTask` 当前含 pct_change
  - `index/option_iv.py`：IV 反推、期限结构
  - `index/futures.py`：基差/基差率分位、MemberPosition 分位
  - `market/money_flow.py`：资金流分位、zscore
  - `market/market_technical.py`：量价技术特征
  - `style/style_index.py`：多周期收益
- 偏处理/基础（可作为 clean 基础表）
  - `index/industry.py`：`IndustryReturnTask`（若仅保留基础收益）
  - `style/style_index.py`：若仅保留基础收益则归处理层
  - `market/market_technical.py`：若仅保留基础行情/量价字段则归处理层

## 特征入库白名单（初稿）
- 估值/利差：PE/PB 分位（10Y/12M）、ERP
- 波动：RV 多窗、分位、短长比；IV 期限结构关键点（7/30/60/90/180 天）
- 基差：基差/基差率及其分位/zscore
- 资金流/宽度：主力净流入率、资金流分位/zscore；行业宽度（上涨占比、强弱占比、收益 std/skew、滚动均值）

## 拆分方向（通用）
- 处理层：保留数据校验/对齐/基础派生（如 pct_change 可视为基础），输出至 `clean` schema。
- 特征层：滚动/分位/去极值/插值/比值等迁移到 features/operations，任务调用。
- 任务层：仅编排 fetch → clean → feature → save，减少特征内嵌。

## 后续工作计划（建议顺序）
1) 定稿分层规范、命名（schema `clean`，主键 `trade_date + ts_code`）、量纲/复权口径。
2) 定稿特征入库白名单。
3) 为每个任务列出输入表、输出表、主键/时间列、当前特征点，标记“处理层保留/特征下沉”。
4) （执行阶段）拆分特征到 features/operations，调整任务仅做处理与编排；补充处理层防御性测试和特征性质测试。

