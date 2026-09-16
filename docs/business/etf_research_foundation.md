# ETF 研究底座统一维护

本模块把 ETF 候选身份，与 AlphaHome 可重算事实分层维护。初始工作簿作为结构基线；以后规模、成交、费率、折溢价等数值由 AlphaHome 计算，候选分类可由 DeepSeek 按严格 JSON 合同确认。模型确认与人工确认都保留来源和审计，不把 LLM 输出当作资金或下单授权。

## 数据对象

| 对象 | 粒度 | 用途与边界 |
| --- | --- | --- |
| `features.mv_etf_product_facts_current` | 每只上市 ETF 一行 | 当前产品事实；不是 PIT 历史表 |
| `fund_pool_on.etf_candidate_master_batch` | 每个工作簿版本一行 | 来源哈希、质量摘要、权限边界 |
| `fund_pool_on.etf_candidate_master_snapshot` | 工作簿版本 × ETF | 候选身份、人工分类和载入时事实快照 |
| `fund_pool_on.etf_candidate_ai_run` | 每次月度运行一行 | 计划哈希、模型、token、结果快照及失败信息 |
| `fund_pool_on.etf_candidate_ai_decision` | AI 运行 × ETF | 结构化决定、输入/输出哈希、证据与置信度 |
| `fund_pool_on.etf_candidate_confirmation_audit` | 每次人工确认一行 | 标签变更前后镜像、复核人和说明 |
| `fund_pool_on.etf_candidate_master_pit_history` | 候选状态有效区间 | 同时保留业务截止日和实际可用时间，人工复核按事件展开 |
| `fund_pool_on.etf_candidate_master_as_of(ts)` | 指定可用时点 × ETF | 返回该时点真正可见的候选池，默认排除当时已拒绝产品 |
| `fund_pool_on.etf_candidate_master_current_enriched` | 当前版本 × ETF | 原始快照与最新产品事实并列；不回写候选状态 |
| `features.mv_etf_exposure_technical_current_universe_daily` | 日期 × 当前候选跟踪指数 | 技术原子；当前宇宙，不可宣称无幸存者偏差 |
| `features.mv_index_direct_valuation_daily` | 日期 × 指数 | 数据商直接 PE/PB；不以重构值静默补缺 |
| `fund_pool_on.etf_candidate_index_coverage_current` | 当前候选跟踪指数 | 明示技术和直接估值是否覆盖 |
| `features.mv_industry_earnings_observation_monthly` | 月份 × 行业 | PIT 的 FAPI、预期 ROE、FTTM 和滚动原子；不含事后合成策略分数 |

这些对象只提供研究和候选池维护能力。候选载入合同强制 `capital_authority=false`、`order_authority=false`，不会赋予资金或下单权限。

## 维护入口

初始结构迁移时，可在个人研究工作区用 artifact-tool 校验并导出工作簿：

```powershell
node "D:\TradeHome\个人投资框架\ETF账户\功能模块\00_公共接口\src\export_etf_candidate_master.mjs"
```

四个 `features.mv_*` 对象都通过 `@feature_register` 注册。初始工作簿只用于一次性建立分类基线；正常月度维护不再读取 Excel 或外部 JSON，而由 AlphaHome GUI 的 **一键智能增量更新** 在 Features 之后直接完成事实门禁、DeepSeek 确认和候选快照入库。

需要重新建立初始基线时，仍可使用命令行入口显式载入：

```powershell
.venv\Scripts\python.exe scripts\curation\update_etf_research_foundation.py `
  --candidate-snapshot "D:\TradeHome\个人投资框架\ETF账户\功能模块\00_公共接口\outputs\current\etf_candidate_master_snapshot.json"
```

维护命令返回每个物化视图的行数、刷新耗时、候选载入计数、产品事实完整数，以及候选跟踪指数的技术/直接估值覆盖数。相同 `snapshot_id` 和来源哈希可重复执行；同一 `snapshot_id` 对应不同哈希时拒绝载入。

## AI 月度自动维护

### 一次性迁移

数据库结构变化必须先做只读 plan，再使用返回的同一哈希显式 apply：

```powershell
.venv\Scripts\python.exe scripts\curation\migrate_etf_candidate_confirmation.py

.venv\Scripts\python.exe scripts\curation\migrate_etf_candidate_confirmation.py `
  --apply --expected-plan-hash <上一步返回的 plan_hash>
```

迁移会给候选快照增加 `confirmation_status` 等确认元数据，并创建 AI 运行、逐产品决定和人工确认审计表。已有工作簿行统一标为 `LEGACY_IMPORTED`，不会追认成模型结果。

### GUI 一键月度维护

“日常更新”页固定显示 `ETF候选池月度维护` 更新域。每月 5 日前按规则跳过；5 日起只要当月还没有 `SUCCEEDED` 运行，就在数据采集和 Features 成功后重新生成候选计划。一个自然月最多成功一次。

全部逻辑位于 AlphaHome 包内：产品事实由 `alphahome.features` 计算，月度计划、模型门禁和候选入库由 `alphahome.curation` 执行。GUI 不调用外部 Excel、PowerShell 计划任务或候选维护子进程。运行时从用户环境读取 `DEEPSEEK_API_KEY`；可用 `DEEPSEEK_MODEL` 和 `DEEPSEEK_BASE_URL` 覆盖默认值，密钥不会写入数据库、计划或日志。

候选池与 FundPos 都依赖 Features，但彼此独立：候选模型失败不会阻止 FundPos 影子估算；Features 失败则两者都失败关闭。

### 诊断命令

只读计划不调用 DeepSeek、不写数据库：

```powershell
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_ai_automation.py
```

命令行只是同一 AlphaHome 内部服务的诊断门面，不再复制数据刷新或调度逻辑。若要严格执行刚审阅的计划，使用返回的哈希；通常应直接使用 GUI，让上游采集和 Features 先完成：

```powershell
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_ai_automation.py `
  --execute --expected-plan-hash <plan_hash>
```

模型仅处理三类对象：尚未确认的历史行、名称/跟踪指数发生结构变化的行、上次运行后新上市的 ETF。`HUMAN_CONFIRMED` 行不会被自动降级或覆盖。

模型输出必须通过以下本地门禁后才会和新快照在同一事务中入库：完整 JSON、每个基金代码恰好一条决定、动作与新旧身份相符、字段白名单、分类枚举、状态—研究权限一致、证据非空、产品事实覆盖和交易日新鲜度通过。结构或数据门禁失败时，本次候选快照不写入。置信度低于 0.75 或模型主动要求人工复核时，不伪装成已确认，而是进入 `AI_REVIEW_REQUIRED`；现有分类不自动改写，新产品即使入池也强制为“观察”。

产品事实门禁逐只执行，不能用全市场的最大日期代替单只 ETF 的新鲜度。现有候选（包括已人工确认的产品）必须具备完整 20 日成交记录、有效的规模/成交额/上市月数/费率，以及价格、净值、份额日期。价格至少覆盖运行日前一交易日；净值和份额允许再滞后至多 2 个 SSE 交易日，且不得晚于事实截止日。这是明确记录在计划中的本地运维容忍度，并非供应商 SLA。任何现有候选不满足门禁时，计划不可执行，原因按基金代码列在 `guards.current_product_fact_issues`。

新上市产品若暂不满足上述门禁，不调用模型、不入池，代码和原因保存在成功运行计划的 `guards.deferred_new_product_fact_codes` / `deferred_new_product_fact_issues`。下一次月度计划会继续检查这些代码，即使其上市日期已经早于新的发现截止日；临时缺失的事实行也不会丢失待补齐记录。被人工拒绝的产品不会被重新当成新产品发现。

模型调用期间不持有候选写锁，人工可正常复核。AI 发布前取得与人工复核、快照导入共用的事务锁，并重新生成计划；人工状态、来源批次、阈值或产品事实有变化时，旧计划失败关闭，不写新快照。复核先取得锁再解析最新批次，避免改到等待期间已被替代的快照；审计事件时间使用取得锁后实际改写时的时间。写入连接要求关闭自动提交并使用 `READ COMMITTED` 隔离级别，确保等待锁后读取的是已提交最新状态。

| 确认状态 | 含义 |
| --- | --- |
| `LEGACY_IMPORTED` | 旧工作簿导入，未经过本流程确认 |
| `AI_CONFIRMED` | DeepSeek 输出已通过本地合同和数据门禁；不等于人工背书 |
| `AI_REVIEW_REQUIRED` | 模型已给出结构化判断，但置信度不足或明确要求人工复核 |
| `HUMAN_CONFIRMED` | 人工复核后显式确认；原 AI 运行号与决策哈希仍保留 |
| `HUMAN_REJECTED` | 人工复核不通过；历史行、原 AI 证据和审计保留，但当前候选视图及后续月度候选宇宙排除该产品 |

### 人工复核

默认修改最新成功快照的一只 ETF，并写入不可覆盖的前后镜像审计：

```powershell
.venv\Scripts\python.exe scripts\curation\confirm_etf_candidate_human.py `
  --fund-code 510300.SH --decision approve --reviewer wuh `
  --review-note "已核对基金公告和指数归属"

.venv\Scripts\python.exe scripts\curation\confirm_etf_candidate_human.py `
  --fund-code 510300.SH --decision reject --reviewer wuh `
  --review-note "产品执行条件不满足，退出当前候选池"
```

`approve` 会把标签改为 `HUMAN_CONFIRMED` 并保留在当前候选池；`reject`
会把标签改为 `HUMAN_REJECTED`、将 `include_in_candidate_pool` 设为 false。
拒绝不是物理删除，底层快照行和前后镜像审计均保留。当前视图只返回仍在池内的产品，
因此最新批次的原始行数可以大于当前有效候选数。

## PIT 口径

候选池的 PIT 版本不把 `product_facts_as_of` 误当成“当时已经可用”。`business_as_of_date` 表示产品事实截止日，`available_from` 表示该快照或人工复核真正写入 AlphaHome 的时间；回测必须使用后者约束。`available_to` 是下一次候选状态或下一批快照实际可用的时间。已成功载入的 `snapshot_id` 不可变；同哈希重复载入直接 no-op，不改写原 `loaded_at` 或人工复核结果。

人工复核虽然更新当前快照行，但不可覆盖审计保存完整的 `before_record` 和 `after_record`；PIT 视图据此恢复人工操作前后的状态。首个工作簿版本在实际载入 AlphaHome 之前没有可用历史，系统不会把 2026 年建立的分类事后回填到更早日期。未来每月快照会自然形成前瞻可用的候选池时间序列。

## 查询示例

```sql
SELECT *
FROM fund_pool_on.etf_candidate_master_current_enriched
ORDER BY source_rank;

-- 以实际可用时间查询；请传入策略在当时真正能够读取数据的时间戳
SELECT *
FROM fund_pool_on.etf_candidate_master_as_of(
    TIMESTAMPTZ '2026-09-16 18:00:00+08'
);

SELECT *
FROM fund_pool_on.etf_candidate_index_coverage_current
WHERE NOT technical_available OR NOT direct_valuation_available
ORDER BY index_code;
```

直接估值缺失表示 AlphaDB 当前没有相同指数代码的直接提供口径，不代表估值为零，也不应自动切换到今日成分回构的历史估值。若后续增加重构路线，必须单列来源、时点和覆盖质量，并保留与直接口径的区别。
