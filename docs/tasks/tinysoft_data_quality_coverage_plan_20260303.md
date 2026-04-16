# TinySoft 数据质量与完整度补齐计划（2026-03-03）

## 0. 阶段 B 开发进展（2026-03-03）

- 已落地任务级日检链路（`TinySoftStockMinuteTask._post_execute`）：
  - 覆盖率检查（分钟覆盖股票数 / 全市场股票数）
  - 异常值检查（负值、`high < low`）
  - 分钟条数检查（按周期推导理论条数，1 分钟默认 240）
  - 与 `tushare.stock_daily` 的聚合对账（OHLC/volume/amount）
- 质量检查结果写入任务执行结果 `tinysoft_quality_checks`，不影响主采集成功状态（失败仅告警）。
- 配置样例已新增质量检查参数：`enable_quality_checks`、`quality_check_days`、`quality_check_coverage_warn_threshold`、`quality_check_reconcile_days`。
- 单测已覆盖新增逻辑并通过。
- 实盘小样本验证（2026-02-26 ~ 2026-03-02，`max_symbols=200`）：
  - 成功入库 `144,000` 行（200 只股票 x 3 交易日 x 240 分钟）
  - 价量额对账：`open/high/low/close/volume/amount` 差异行数均为 0（对齐样本内）
- 覆盖率计算已升级为“按任务执行范围评估”，并支持阈值强约束：
  - 样本/限量运行不再误报全市场低覆盖
  - 全市场运行可启用 `enforce_coverage_threshold` + `coverage_fail_on_breach` 触发 `partial_success`
- 新增质量检查结果落库：默认写入 `tinysoft.stock_minute_quality_checks`，便于阶段 B 持续观测与审计；支持 `persist_quality_checks` 开关关闭。
- 实盘验证（2026-03-03，`20260302` 单日、`max_symbols=10`）：主任务成功写入 `2,400` 行，质量表新增 `1` 条检查记录。

## 0.1 阶段 C 开发进展（2026-03-03，持续）

- C1（停复牌 PoC）已启动并完成首版任务落地：
  - 新增任务：`TinySoftStockSuspendTask`（`tinysoft_stock_suspend`）
  - 新增表：`tinysoft.stock_suspend`（字段：`ts_code/trade_date/event_type/event_text/spec_field`）
  - 任务特性：全市场符号解析、按自然日分批、`spec_field_expr` 可配置、默认过滤空事件。
- 配置样例已新增 `tinysoft_stock_suspend` 任务段。
- 单测新增并通过：`tests/unit/test_tinysoft_stock_suspend_task.py`（5/5）。
- 实跑验证（2026-03-03）：
  - 运行窗口：`20260301~20260302`，`max_symbols=2`
  - 任务成功写入 `2` 行（开启 `include_empty_events=true` 验证链路）。
- 当前观察：
  - `spec()` 在小样本主动交易股票上多数为 `None`，需在 C1.1 做字段口径进一步校准（确认停复牌事件表达式/服务参数）。
- C1.1（字段口径校准）已完成（2026-03-03）：
  - 新增校准脚本：`scripts/analysis/tinysoft_suspend_field_calibration.py`
  - 输出报告：`docs/tasks/tinysoft_suspend_field_calibration_20260303.md`
  - 实测参数：`2024-01-01 ~ 2026-03-02`、样本 `20` 只股票、9 个候选表达式
  - 结论：所有候选表达式均为 0 命中（非空率/关键词命中率均为 0）；当前 `query` 口径不足以直接识别停复牌事件
  - 暂定默认表达式保持 `spec()`（基于一致性与兼容性，不代表可用性已满足生产）
- C1.2（二次校准）已完成（2026-03-03）：
  - 同一脚本已支持 `expression x service x viewpoint` 组合评估
  - 输出报告：`docs/tasks/tinysoft_suspend_field_calibration_c1_2_20260303.md`
  - 实测参数：`2024-01-01 ~ 2026-03-02`、样本 `5` 只股票、`spec()/isstop()`、`viewpoint in {none,1}`
  - 结论：`viewpoint=1` 全部报错；`viewpoint=none` 可查询但结果仍全空，继续指向“需专用函数/服务口径”
- C2（行业版本化 PoC）已完成首版实现（2026-03-03）：
  - 新增任务：`TinySoftStockIndustryVersionedTask`（`tinysoft_stock_industry_versioned`）
  - 新增表：`tinysoft.stock_industry_versioned`
  - 新增校准脚本与报告：
    - `scripts/analysis/tinysoft_industry_field_calibration.py`
    - `docs/tasks/tinysoft_industry_field_calibration_20260303.md`
  - 实跑验证：`20260301~20260302`、`max_symbols=3`、`include_empty_records=true`，任务成功写入 `3` 行。
- C3（财务 PIT 扩展 PoC）已完成首版实现（2026-03-03）：
  - 新增任务：`TinySoftStockFinaPitExtTask`（`tinysoft_stock_fina_pit_ext`）
  - 新增表：`tinysoft.fina_pit_ext`
  - 新增校准脚本与报告：
    - `scripts/analysis/tinysoft_fina_field_calibration.py`
    - `docs/tasks/tinysoft_fina_field_calibration_20260303.md`
  - 实跑验证：`20260301~20260302`、`max_symbols=3`、`include_empty_metrics=true`，任务成功写入 `12` 行。

## 0.2 阶段 C 验收摘要（C1-C3 全完成）

### C1 停复牌 PoC
- 字段字典（`tinysoft.stock_suspend`）：
  - 主键：`ts_code + trade_date + event_text`
  - 关键字段：`event_type/event_text/spec_field`
- 时间范围：默认 `2015-01-01` 起（任务 `default_start_date=20150101`）
- 与现有源差异：当前库缺失停复牌表；该任务已补齐链路，但 TinySoft query 口径命中率为 0（需后续专用函数口径）
- 入库成本评估：低到中（任务链路稳定，瓶颈在字段可用性而非工程实现）

### C2 行业版本化 PoC
- 字段字典（`tinysoft.stock_industry_versioned`）：
  - 主键：`ts_code + trade_date + industry_source`
  - 关键字段：`industry_l1/industry_l2/industry_l3/industry_code/field_map_json`
- 时间范围：默认 `2018-01-01` 起（任务 `default_start_date=20180101`）
- 与现有源差异：现有 `tushare` 行业表偏“当前态/快照”，该任务提供“按观察日重建口径”的扩展框架
- 入库成本评估：中（字段可配置，后续成本主要在 TinySoft 表达式有效性与口径收敛）

### C3 财务 PIT 扩展 PoC
- 字段字典（`tinysoft.fina_pit_ext`）：
  - 主键：`ts_code + trade_date + finance_source + metric_name`
  - 关键字段：`metric_expr/metric_value/metric_text/metric_map_json`
- 时间范围：默认 `2018-01-01` 起（任务 `default_start_date=20180101`）
- 与现有源差异：现有 `tushare/akshare` 财务口径在部分字段存在时点一致性争议；该任务可并行拉取 TinySoft 指标做 PIT 对账
- 入库成本评估：中（工程已完成，后续成本集中在“可用表达式池”和对账规则）

## 0.3 阶段 C 二轮完善（2026-03-03，数据字典口径）

- C1 停牌任务改造完成：
  - 取数从 `query + spec()` 切换为 `call(infoarray, 127)`
  - 字段按字典落地：`停牌开始日/时间`、`停牌截止日/时间`、`停牌期限`、`停牌原因`
  - 表结构补充 `suspend_start_date/suspend_end_date/suspend_reason/source_table_id`
- C2 行业版本化改造完成：
  - 取数从表达式快照切换为 `call(infoarray, 139)`
  - 基于 `入选日期/剔除日期/级数` 重建 `industry_l1/l2/l3` 历史快照
  - 新增 `level1_code/level2_code/level3_code/source_name/source_table_id`
- C3 财务 PIT 改造完成：
  - 默认使用 `call(infoarray, 42)`（主要财务指标）
  - 指标定义改为 `metric_defs`（`metric_name + field_id + field_name`）
  - 生成 `report(field_id, report_date)` 形式 `metric_expr`，并落地 `report_date/ann_date/metric_field_id/source_table_id`
- 底层能力补齐：
  - `TinySoftAPI` 新增 `call/call_value/call_dataframe`，统一鉴权重试、错误处理、DataFrame 解析。

## 1. 本轮已完成（口径统一）

### 1.1 分钟行情单位口径统一
- 已在 `TinySoftStockMinuteTask` 落地默认单位归一化：
  - `volume`: TinySoft 原始“股” -> AlphaDB 统一“手”（`* 0.01`）
  - `amount`: TinySoft 原始“元” -> AlphaDB 统一“千元”（`* 0.001`）
- 新增可配置参数（任务级）：
  - `normalize_units`（默认 `true`）
  - `volume_scale`（默认 `0.01`）
  - `amount_scale`（默认 `0.001`）
- 同步更新了 `config.example.json`。
- 单测已通过：`tests/unit/test_tinysoft_stock_minute_helpers.py`（9/9 通过）。

### 1.2 时间口径补强
- `trade_time` 若带时区，统一转换到 `Asia/Shanghai` 后去时区写库（保持数据库 `TIMESTAMP` 语义一致）。

## 2. 当前数据库基线（用于后续计划）

- `tinysoft.stock_minute` 现状：`720` 行、`1` 只股票。
- `tinysoft` schema 目前仅 `stock_minute` 一张表。
- `tushare/akshare/rawdata` 现状中，未发现以下关键表：
  - `stock_suspend`（停复牌）
  - `stock_hsgt_top10` / `stock_hk_hold`（沪深港通持股明细）

> 结论：当前 TinySoft 最大价值仍是“分钟覆盖扩容 + 非行情维度补齐”，而不是继续加细到 Tick。

## 3. 非行情数据补齐调研结论（本轮新增）

> 按你的约束，暂不推进 Tick 级实现，重点评估非行情。

### 3.1 TinySoft 可补齐的数据域（来自官方文档/帮助）

1. 财务/F10 型数据函数能力  
   - 文档可见 `StockReport`、`ReportValueByRight`、`GetLastReportFinance`、`GetReportFinanceByTime`、`GetReportFinanceByTime2`、`InfoIdExt` 等函数族。  
   - 适合做：财务指标时点一致性补校、公告期映射、跨报告期口径对齐。

2. 证券基础属性/分类属性  
   - 文档可见 `StockBase` / `StockBaseEx` / `StockInfoArrayEx`，可取证券基础元数据与扩展属性。  
   - 适合做：基础信息字段补全与多源冲突裁决（如上市地、行业标签、代码映射边界情况）。

3. 行业/板块“版本化”分类  
   - 文档可见 `getswhy1/2/3`、`getswindexcode`、`getbkbydate` 等能力（强调版本与生效日）。  
   - 适合做：行业分类历史回溯、板块变更的 PIT 对齐（避免只看当前口径导致前视偏差）。

4. 停复牌事件  
   - 官方 FAQ 明确有停复牌取数方案（示例函数 `Spec`）。  
   - 适合做：补齐当前库缺失的停复牌维度。

5. 指数成分历史  
   - 官方 FAQ 提供大量指数成分获取入口，覆盖中证/国证/申万/Wind 等口径。  
   - 适合做：指数成分回测一致性、跨指数体系统一层。

6. 全球宏观数据库  
   - TinySoft Deep Data（页面检索结果）显示可覆盖全球利率、国债收益率、主权 CDS、全球股指等。  
   - 适合做：当前宏观因子对海外风险变量的补充。

### 3.2 与当前 Tushare/AkShare 的关系判断

1. 明确“缺失可补”的优先域  
   - 停复牌（`stock_suspend`）  
   - 北向持股明细（`stock_hsgt_top10`/`stock_hk_hold`）

2. 明确“有数据但可提质”的优先域  
   - 行业/板块分类的历史版本一致性  
   - 财务指标跨公告日/报告期的时点一致性（PIT 对齐）

3. 暂不进入实现域  
   - Tick/逐笔数据（按当前策略粒度约束暂缓）

## 4. 下一步开发计划（含范围约束）

## 阶段 A（已完成）
- 分钟数据口径统一（单位 + 时间）  
- 输出：代码、测试、配置样例

## 阶段 B（本周，优先）
- 目标：全市场分钟覆盖扩容（不改粒度）  
- 动作：
  - 保持当前 1 分钟任务，扩充至全市场持续增量
  - 增加日级质量校验：覆盖率、异常值、分钟条数、日线聚合对账
- 验收：
  - 最近 20 个交易日，覆盖率目标 >= 95%
  - `volume/amount` 与日线聚合偏差在阈值内

## 阶段 C（已完成首轮 PoC）
- 目标：从 TinySoft 中确定 2-3 个最有收益的非行情任务
- 动作：
  - C1 停复牌数据 PoC（候选表：`tinysoft.stock_suspend`，已实现）
  - C2 行业分类版本化 PoC（候选表：`tinysoft.stock_industry_versioned`，已实现）
  - C3 财务时点一致性 PoC（候选表：`tinysoft.fina_pit_ext`，已实现）
- 验收：
  - 每个 PoC 输出：字段字典、时间范围、主键方案、与现有源差异分析、入库成本评估

## 阶段 D（评审后实施）
- 依据阶段 C 评审结果，仅落地 ROI 最高的 1-2 个非行情任务。
- 明确约束：不引入 Tick 级链路。

## 5. 具体执行清单（建议）

1. 先完成分钟全市场覆盖率提升（阶段 B）。  
2. 并行做停复牌 PoC（阶段 C1，优先最高）。  
3. 做行业版本化 PoC（阶段 C2）并评估是否替换/补充现有行业成分口径。  
4. 最后做财务时点一致性 PoC（阶段 C3）。

## 5.1 后续建议（进入阶段 D 评审）

1. 先做“表达式可用性治理”专项：沉淀 TinySoft 专用函数白名单与服务参数矩阵。
2. 选 ROI 最高的 1-2 个任务进入生产化（建议优先 C3，再评估 C2）。
3. C1 暂保留为“链路已通但口径待解”的候选任务，避免误用空值数据进入策略层。

## 6. 参考来源

- pyTSL 使用说明（query 参数与行为）：http://py3k.cn/pyTSL/usage.html  
- pyTSL API 参考（周期与返回字段）：http://py3k.cn/pyTSL/reference.html  
- TinySoft FAQ（行业分类版本）：http://www.tinysoft.com.cn/tsdn/helpdoc/display.tsl?id=32609  
- TinySoft FAQ（停复牌信息）：http://www.tinysoft.com.cn/tsdn/helpdoc/display.tsl?id=32790  
- TinySoft FAQ（指数成分获取）：http://www.tinysoft.com.cn/tsdn/helpdoc/display.tsl?id=31970  
- TinySoft 函数文档（财务函数族）：http://www.tinysoft.com.cn/tsdn/helpdoc/display.tsl?id=33607  
- TinySoft 函数文档（基础属性扩展）：http://www.tinysoft.com.cn/tsdn/helpdoc/display.tsl?id=33594  
