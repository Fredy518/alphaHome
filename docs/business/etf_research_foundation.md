# ETF 研究底座统一维护

本模块把 ETF 候选身份，与 AlphaHome 可重算事实分层维护。初始工作簿作为结构基线；以后规模、成交、费率、折溢价等数值由 AlphaHome 计算，候选分类可通过公司 GLMS 网关按严格 JSON 合同确认。模型确认与人工确认都保留来源和审计，不把 LLM 输出当作资金或下单授权。

## 数据对象

| 对象 | 粒度 | 用途与边界 |
| --- | --- | --- |
| `features.mv_etf_product_facts_current` | 每只上市 ETF 一行 | 当前产品事实；不是 PIT 历史表 |
| `fund_pool_on.etf_candidate_master_batch` | 每个工作簿版本一行 | 来源哈希、质量摘要、权限边界 |
| `fund_pool_on.etf_candidate_master_snapshot` | 工作簿版本 × ETF | 候选身份、人工分类和载入时事实快照 |
| `fund_pool_on.etf_candidate_ai_run` | 每次月度或全量运行一行 | 计划哈希、全市场覆盖台账、模型、token、结果快照及失败信息 |
| `fund_pool_on.etf_candidate_ai_decision` | AI 运行 × ETF | 结构化决定、输入/输出哈希、证据与置信度 |
| `fund_pool_on.etf_candidate_confirmation_audit` | 每次人工确认一行 | 标签变更前后镜像、复核人和说明 |
| `fund_pool_on.etf_candidate_master_pit_history` | 候选状态有效区间 | 同时保留业务截止日和实际可用时间，人工复核按事件展开 |
| `fund_pool_on.etf_candidate_master_as_of(ts)` | 指定可用时点 × ETF | 返回该时点真正可见的候选池，默认排除当时已拒绝产品 |
| `fund_pool_on.etf_candidate_master_current_enriched` | 当前版本 × ETF | 原始快照与最新产品事实并列；不回写候选状态 |
| `fund_pool_on.etf_usable_pool_current` | 当前有效筛选 × ETF | 通过产品门槛后每暴露一主一同指数备份；当前产品可用池入口 |
| `fund_pool_on.etf_usable_pool_screening_current` | 最近一次筛选 × 全候选 | 逐只资格、排名和未选原因；`is_current=false` 表示需刷新 |
| `fund_pool_on.etf_usable_pool_batch` / `etf_usable_pool_snapshot` | 筛选版本 × ETF | 规则版本、源指纹、全量决定和不可覆盖的筛选快照 |
| `fund_pool_on.etf_usable_pool_as_of(ts)` | 指定实际可用时点 × ETF | 已发布的可用池历史，不将当前筛选回填过去 |
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

四个 `features.mv_*` 对象都通过 `@feature_register` 注册。初始工作簿只用于一次性建立分类基线；正常月度维护不再读取 Excel 或外部 JSON，而由 AlphaHome GUI 的 **一键智能增量更新** 在 Features 之后直接完成事实门禁、GLMS 模型确认和候选快照入库。

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

全部逻辑位于 AlphaHome 包内：产品事实由 `alphahome.features` 计算，月度计划、模型门禁和候选入库由 `alphahome.curation` 执行。GUI 不调用外部 Excel、PowerShell 计划任务或候选维护子进程。模型统一使用公司 GLMS，运行时读取 `GLMS_API_KEY`：优先读取进程环境，缺失时直接读取 Windows 用户环境，因此启动后新增的用户变量也可生效。密钥不会写入数据库、计划或日志。

默认 API 基址为 `https://models.glms.com.cn/ucloud/v1`，实际请求为其 `/chat/completions`；GLMS 站点首页不能直接当作完整 API 路径。月度分类默认 `deepseek-v4-flash`，证据增强复核和分类补全默认 `deepseek-v4-pro`。可通过 `GLMS_MODEL`、`GLMS_BASE_URL` 用户变量覆盖，CLI 的 `--model` 优先级更高。只填写 GLMS 首页作为基址时会自动补齐 `/ucloud/v1`。旧的 `DEEPSEEK_*` 变量不再参与解析，也不会在 GLMS 失败后回退直连。

新计划的 `llm_config` 冻结服务、基址和请求模型，参与计划及批次缓存哈希；切换连接后必须重新预览计划，旧批次缓存不会混用。模型返回的实际版本仍单独记录，新确认者标识为 `glms:<实际模型>`，历史 DeepSeek 审计保持原值。`deepseek_candidate_client.py` 和旧类名作为兼容入口保留。

候选池与 FundPos 都依赖 Features，但彼此独立：候选模型失败不会阻止 FundPos 影子估算；Features 失败则两者都失败关闭。

### 诊断命令

只读计划不调用 GLMS、不写数据库：

```powershell
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_ai_automation.py
```

命令行只是同一 AlphaHome 内部服务的诊断门面，不再复制数据刷新或调度逻辑。若要严格执行刚审阅的计划，使用返回的哈希；通常应直接使用 GUI，让上游采集和 Features 先完成：

```powershell
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_ai_automation.py `
  --execute --expected-plan-hash <plan_hash>
```

月度模型处理尚未确认或产品事实发生变化的候选，以及全市场中从未筛选或排除后发生重要变化的 ETF。发现范围不再依赖“上次运行后上市”，较早上市但遗漏的产品、较晚补齐的名录都能进入筛选。`HUMAN_CONFIRMED` 行不会被自动降级或覆盖。

首次启用应显式执行一次全量存量筛选，之后沿用 GUI 月度增量。`--scope full` 重新筛选所有具备完整事实的场内存量 ETF（包括此前 AI 排除项），复核已有非人工确认候选，保留历次人工拒绝。全量与月度的成功幂等键分开，每个范围每月最多成功一次；同月已经完成普通月度不会阻止首次全量，全量也不受每月 5 日窗口限制。

```powershell
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_ai_automation.py `
  --scope full --output-dir artifacts/etf-full
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_ai_automation.py `
  --scope full --output-dir artifacts/etf-full --batch-size 8 --workers 4 `
  --execute --expected-plan-hash <plan_hash>
```

全量计划包含 `coverage` 逐代码状态和 `coverage_counts` 汇总：已有候选、模型目标、人工拒绝、待补事实、截至事实日未上市/已退市、非场内代码。市场清单来自 `fund_etf_basic`，并用 `fund_basic` 的场内 ETF 补查缺口；`.OF` 等别名不会重复提交模型。名录缺失于产品事实时登记等待，不能把补查到的基金直接当作事实完整。默认新增目标上限为月度 50、显式全量 3000，只是防误执行门禁，超过上限会阻止整个计划而非截断清单。

`--output-dir` 保存完整计划、结果及模型批次检查点。失败重试仍需重新预览计划，只有模型、提示词、分类字典和整批事实完全相同才复用结果。并发仅用于模型请求，最终按计划顺序合并、重新验证后一次提交。AI 排除项的判断和筛选指纹持久化在决策表；月度不反复询问未变化的排除项，名称、指数、市场、费率或产品辅助状态变化会触发重筛。每日数值的小幅变化不触发重筛，显式全量始终可以重新审视全部存量。

分批模型输出还要经过全局暴露身份合并：同指数优先复用既有候选的完整分类和父工具；新指数的暴露 ID 由本地按指数代码稳定生成（`ETF_INDEX_*`），避免各批次独立生成的序号碰撞。原字典中的主工具/不同指数实施变体不会被模型自动拆分。同一新指数的跨批次分类不一致、缺少指数身份或模型试图改写原有暴露结构时，降为人工复核。原模型决定及本地修正保存在 `evidence_payload.model_decision` / `identity_adjustments`，不把本地合并伪装成模型原始输出。

升级已有数据库需先通过 `scripts/curation/migrate_etf_candidate_confirmation.py` 的只读计划和对应哈希显式应用 v5 迁移，将“每月一次”唯一索引改为“每月每个筛选范围一次”。直接 CLI 维护完成后，仍需按 Features 计划流程刷新 `etf_exposure_technical_current_universe_daily`；GUI 已负责该下游刷新。

模型输出必须通过以下本地门禁后才会和新快照在同一事务中入库：完整 JSON、每个基金代码恰好一条决定、动作与新旧身份相符、字段白名单、分类枚举、状态—研究权限一致、证据非空、产品事实覆盖和交易日新鲜度通过。结构或数据门禁失败时，本次候选快照不写入。置信度低于 0.75 或模型主动要求人工复核时，不伪装成已确认，而是进入 `AI_REVIEW_REQUIRED`；现有分类不自动改写，新产品即使入池也强制为“观察”。

产品事实门禁逐只执行，不能用全市场的最大日期代替单只 ETF 的新鲜度。现有候选（包括已人工确认的产品）必须具备完整 20 日成交记录、有效的规模/成交额/上市月数/费率，以及价格、净值、份额日期。价格至少覆盖运行日前一交易日；净值和份额允许再滞后至多 2 个 SSE 交易日，且不得晚于事实截止日。这是明确记录在计划中的本地运维容忍度，并非供应商 SLA。任何现有候选不满足门禁时，计划不可执行，原因按基金代码列在 `guards.current_product_fact_issues`。

尚未入池的产品若暂不满足上述门禁，不调用模型、不入池，代码和原因保存在成功运行计划的 `guards.deferred_new_product_fact_codes` / `deferred_new_product_fact_issues`。下一次月度计划会继续检查这些代码，临时缺失的事实行也不会丢失待补齐记录。人工拒绝从全部已载入批次中的最新产品状态读取，拒绝行即使未复制到后续快照也不会被重新当成新产品发现。

模型调用期间不持有候选写锁，人工可正常复核。AI 发布前取得与人工复核、快照导入共用的事务锁，并重新生成计划；人工状态、来源批次、阈值或产品事实有变化时，旧计划失败关闭，不写新快照。复核先取得锁再解析最新批次，避免改到等待期间已被替代的快照；审计事件时间使用取得锁后实际改写时的时间。写入连接要求关闭自动提交并使用 `READ COMMITTED` 隔离级别，确保等待锁后读取的是已提交最新状态。

| 确认状态 | 含义 |
| --- | --- |
| `LEGACY_IMPORTED` | 旧工作簿导入，未经过本流程确认 |
| `AI_CONFIRMED` | 模型输出已通过本地合同和数据门禁；不等于人工背书 |
| `AI_REVIEW_REQUIRED` | 模型已给出结构化判断，但置信度不足或明确要求人工复核 |
| `HUMAN_CONFIRMED` | 人工复核后显式确认；原 AI 运行号与决策哈希仍保留 |
| `HUMAN_REJECTED` | 人工复核不通过；历史行、原 AI 证据和审计保留，但当前候选视图及后续月度候选宇宙排除该产品 |

### 待复核记录的第二轮模型核对

可对当前 `AI_REVIEW_REQUIRED` 单独进行证据增强核对。输入同时包含第一轮模型判断与本地修正、基金名录、指数名录、指数类别、同指数候选和既有母暴露的指数变体，避免仅重复第一轮相同提示。独立入口默认通过 GLMS 使用 `deepseek-v4-pro`；只更新分类字段和 AI 确认状态，候选等级、成员集合、暴露 ID、产品角色及父工具关系冻结，结果不会标为人工确认。

```powershell
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_second_review.py `
  --expected-target-count 694 --output-dir artifacts/etf-second-review
.venv\Scripts\python.exe scripts\curation\run_etf_candidate_second_review.py `
  --expected-target-count 694 --output-dir artifacts/etf-second-review `
  --execute --expected-plan-hash <plan_hash>
```

`--expected-target-count` 可选，用于冻结本次明确授权的数量。第二轮在 `screening_scope=review` 单独记账，每月最多成功一次。默认启用模型深度思考（`thinking=enabled, reasoning_effort=high`）。置信度须达到 0.85，至少两条交叉核对证据，逐只引用实际基金代码、指数代码和完整指数名，且不得引入同指数分类冲突；否则保持待复核。规模小、历史短和仍未开展的交叉指数重合度研究不自动等于分类证据不足，但相关产品资格等级与风险限制保留。模型批次可断点复用，缓存绑定模型、提示版本、生成设置和输入资料；发布前会重新验证包括补充证据在内的完整计划。

### 独立可用池

候选母表是完整研究档案，可用池是按客观产品条件和同类排名选出的产品集合。`candidate_status` 和 `AI_REVIEW_REQUIRED` 不作为可用池资格门槛：规模/成交不足由数值规则处理，主备取舍由排序处理，重合度、风格边界及预算归属保留为研究注记。`classification_review_required=true` 表示原分类映射仍有疑点，不能据产品入选宣称这些标签已确认；原 AI/人工状态和候选等级不回写。

当前V4规则为：上市满3个完整日历月，规模至少5亿元，20日日均成交至少0.3亿元；产品事实完整、实际价格/净值/份额日期符合既有交易日新鲜度约束，基金代码、跟踪指数代码与源名录一致，已上市且未被人工拒绝。LOF另按下文定期规模口径。所有候选参与，包括条件候选和观察产品。原V1/V2的12个月版本及V3的6个月版本保留。

按现有 `exposure_id` 分组，依次比较20日日均成交额降序、总费率升序、规模降序、完整上市月数降序、代码升序。每组选择一只 `PRIMARY`，再从跟踪相同指数的产品中选一只 `BACKUP`。其余达标产品为 `RESERVE`，仍留在档案；门槛未达为 `INELIGIBLE`，核心事实错误为 `BLOCKED`。不同指数的实施变体不直接作为同指数备份。此分组沿用现有暴露字典，未新增跨指数持仓聚类。

可用池随 GUI 日常更新链在 Features 和候选月度检查之后重算，不需要再次调用大模型。原月度候选维护按政策跳过时，可用池仍运行；上游失败时按依赖阻断。首次必须先执行独立 schema 计划。CLI 操作分别预览并绑定计划哈希：

```powershell
.venv\Scripts\python.exe scripts\curation\run_etf_usable_pool.py schema --output-dir artifacts/etf-usable
.venv\Scripts\python.exe scripts\curation\run_etf_usable_pool.py schema --output-dir artifacts/etf-usable --execute --expected-plan-hash <schema_hash>
.venv\Scripts\python.exe scripts\curation\run_etf_usable_pool.py refresh --output-dir artifacts/etf-usable
.venv\Scripts\python.exe scripts\curation\run_etf_usable_pool.py refresh --output-dir artifacts/etf-usable --execute --expected-plan-hash <selection_hash>
```

同一计划重复执行为 no-op。源候选、产品事实或基金身份变更后，当前可用池关闭旧结果直至重算；同时按交易日检查过期，历史筛选快照保留。未来不达标或排名落后的产品自动离开当前可用池，候选档案不删除。`as_of` 使用实际发布记录时间约束，首次发布之前返回空集。此池仅提供研究产品选择，不赋予资金或下单权限。

```sql
SELECT fund_code, fund_name, exposure_name, selection_status,
       classification_review_required, research_notes
FROM fund_pool_on.etf_usable_pool_current;
```

### 月度可用池与2016年起的历史重建

`alphahome.curation.etf_usable_pool_monthly` 独立计算各月产品事实。历史范围从2016年1月开始，截止运行日之前最后一个完整自然月；2026-09-29执行时止于2026年8月，共128个月。原日常可用池继续提供每日筛选结果，月度成员通过以下入口读取：

| 对象 | 含义 |
| --- | --- |
| `fund_pool_on.etf_usable_pool_monthly_batch` | 维护月份、事实截止日、决策截止时间、生效区间、实际生成时间、规则及来源哈希 |
| `fund_pool_on.etf_usable_pool_monthly_snapshot` | 每月逐产品筛选结果，含未达标和数据阻断原因 |
| `fund_pool_on.etf_usable_pool_monthly_screening` | 各月最新版本的完整筛选记录 |
| `fund_pool_on.etf_usable_pool_monthly_membership` | 各月最新版本的入选成员 |
| `fund_pool_on.etf_usable_pool_monthly_current` | 当前处于有效区间内的月度成员，明确暴露 `record_kind` |
| `fund_pool_on.etf_usable_pool_monthly_reconstructed_as_of(ts, known_at)` | 允许使用历史重建的研究查询；第二参数限制可以看到的重建版本 |
| `fund_pool_on.etf_usable_pool_monthly_as_of(ts)` | 只返回该时点已经实际发布并生效的 `OBSERVED` 维护记录 |

月度V5规则为上市满3个完整日历月、规模至少5亿元、20日日均成交至少3000万元。每日和月度共用上市满月函数，按真实上市周年日判断，避免按30.4375天取整提前满足3个月；短月份按月末周年日处理。日常筛选的精确月龄保存在 `diagnostics.listing_age_months`，原 `product_facts.age_months` 保留源事实，供审计对照。成交额窗口为月末之前最近20个交易所交易日；缺失不能通过跳过该日或补入更早数据凑满20条。ETF规模使用当时已披露的净资产，或同一业务日单位净值乘场内份额；净值和份额不混用不同日期，规模最多落后月末两个交易日。LOF采用下文独立的定期规模披露口径。

每月最后一个交易日是 `facts_cutoff`，下一个交易日09:00是 `decision_cutoff`，同日09:30是计划生效时间，留半小时计算及发布。月末收盘行情形成快照事实，月末净值和同日份额按次一交易日开盘前可得处理，不再统一延迟至次一交易日收盘。只有公告日期的当日净值使用开盘前可得约定，逐行标记 `nav_same_day_publication_time_assumed_pre_open`；缺少公告时间的份额标记 `share_publication_time_assumed_t_plus_1_pre_open`。这些是当前回放时序约定，源数据没有精确公告时分。LOF定期规模仍按报告公告/概况快照可得日期筛选，不把未公告的报告提前。月度成员有效至下一月份的首个交易日开盘，日常行情刷新不会令整份月度名单失效。退市日期另外约束成员查询。实际维护若晚于计划时间发布，最早从发布之后的下一次开盘生效。

策略应以同一次月度建仓的开盘时点取月度成员，再按月末信号选择载体，避免先在月末瞬间取旧池、建仓时再用新池过滤。历史研究可查询 `monthly_reconstructed_as_of(建仓开盘时点, 已知重建版本时间)`；真实维护查询继续要求快照在开盘前已经实际发布。此前保存的V4研究池快照和策略结果不会随AlphaHome更新自动重算。

历史宇宙来自场内ETF名录及基金基础表补充，V2加入场内LOF，按历史上市、退市日期筛选，覆盖后来退市的产品；六位场内代码为唯一身份，排除`.OF`、`J`别名和旧分级基金。ETF分支继续排除场外联接份额；有场内上市身份的联接LOF通过LOF分支筛选。宇宙完整性、历史转换前产品形态及供应商历史修订尚无独立认证，不将当前名录宣称为完整历史档案。

只使用 `etf_candidate_master_as_of(decision_cutoff, true)` 当时已经发布的分类与人工拒绝记录。2016-01至2026-08没有当时发布的候选分类，因此达标产品保留为 `STANDALONE`（独立工具），不使用今天的指数代码或AI标签合并历史产品。未来存在当时映射的组按成交额、规模、上市时长、代码排序，一主、最多一只同指数备份；无历史费率版本，月度筛选不使用费率排序。每日版本的排序保持独立。

每日主备池与历史月度资格池不能直接按总数比较：前者先取当前候选档案，再做客观筛选和同类主备精简；后者从场内历史产品宇宙出发，未知历史分类的合格产品全部独立保留。比较资格覆盖时，每日应计入 `PRIMARY`、`BACKUP`、`RESERVE`；比较主备代表时，必须具有当时可用的分类映射并统一日期、宇宙与排序规则。按2026-09-28的旧12个月版本，每日509只达标、192只为同类储备、317只主备；2026-08月度为527只独立达标产品。差异不是简单的数据频率差异，也不代表月度成员全部进入当前候选档案。候选模型此前的 `EXCLUDE_NEW` 仍可能排除客观达标产品，其决定不能解释成规模或成交不达标。

首次回补全部标记 `HISTORICAL_RECONSTRUCTION`，`recorded_at` 保留真实写入时间。它是带可得性假设的历史产品资格重建，不能冒充AlphaHome当年的运行记录，也不是已经恢复的历史ETF聚类池。查询实际维护历史的 `monthly_as_of` 不返回这些回补记录。研究使用示例：

```sql
SELECT maintenance_month,fund_code,selection_status,aum_100m,amount_20d_100m,
       record_kind,recorded_at,quality_flags
FROM fund_pool_on.etf_usable_pool_monthly_reconstructed_as_of(
  '2020-09-15 09:30:00+08'::timestamptz
);
```

迁移、预览及执行分离，执行绑定保存的计划哈希。源事实、日历、分类、规则或结果改变则拒绝旧计划。相同月度哈希复用已有快照，补正使用新版本，真实生成时间不回写：

```text
python scripts/curation/run_etf_usable_pool_monthly.py schema --output-dir <目录>
python scripts/curation/run_etf_usable_pool_monthly.py schema --execute --expected-plan-hash <迁移哈希> --output-dir <目录>
python scripts/curation/run_etf_usable_pool_monthly.py backfill --start-month 2016-01 --as-of 2026-09-29 --output-dir <目录>
python scripts/curation/run_etf_usable_pool_monthly.py backfill --start-month 2016-01 --as-of 2026-09-29 --execute --expected-plan-hash <计划哈希> --output-dir <目录>
```

GUI日常更新新增“ETF/LOF可用池月度快照”，依赖Features，独立于候选AI维护的成败。历史基线完成后，每月首个交易日09:00后自动补建新完整月份为 `OBSERVED`；当月在当前规则版本下已经覆盖则跳过。要在当月首次开盘生效，须在09:30前完成该链；晚于开盘运行仍按实际发布后的下一次开盘生效。多年历史缺口要求显式回补，不在日常运行中悄悄回写历史。现有外部研究脚本未批量修改，应按研究目的显式选择每日、月度重建或实际维护查询入口。

### LOF扩展（2026-09-29）

候选池及每日、月度可用池统一支持ETF和LOF，原 `etf_*` 表名作为兼容接口保留。ETF原物化视图保持独立，新增 `features.mv_lof_product_facts_current`，统一事实入口为 `features.exchange_fund_product_facts_current`，其 `product_type` 区分ETF与LOF。新LOF事实recipe由Features注册表自动发现，日常刷新；后续候选增量维护会自动检查未覆盖LOF及此前缺数据的产品。

LOF发现依据 `fund_basic.market='E'`、深市16开头或沪市501/502开头场内代码，并排除名字含“分级”的旧产品。代码范围只用于发现，还须校验上市/退市日期及场内行情；名字没有“LOF”的兴全合润等也能被发现，`.OF`场外别名不重复入池。存续状态并不证明已上市，缺上市日期的产品保留缺数原因，不能入可用池。

分类模型接收基金类型、投资类型、业绩比较基准、真实日期及规模来源。业绩比较基准不等于跟踪指数，模型不能制造指数代码。尚无核实指数映射的LOF使用 `LOF_PRODUCT_<场内代码>` 独立身份，不作为其他ETF/LOF的同指数备份；主动管理风格、QDII时差、折溢价及申赎限制进入分类风险说明。首次引入使用独立 `lof_initial` 运行范围，不重跑或改写已完成的ETF分类。

LOF规模按份额类别已报告净资产筛选，使用 `fund_nav.net_asset` 或 `fund_overview_em.net_asset_size_text`（亿元口径，可能有展示舍入），不使用 `total_netasset` 代替类别规模，也不把多年未更新的场内份额乘最新净值。规模报告期最多落后183个自然日；公告/快照日期不得晚于事实或决策截止日。价格和净值仍检查新鲜度，成交仍要求连续20个交易所交易日。当前统一门槛为5亿元、3000万元及上市满3个完整日历月，动态更新每月实际指标。

部分供应商的季度规模事后填进日净值记录，`ann_date=nav_date` 无法证明该规模在当天已知，故不作为季度规模可得性证据。无公告日期的Tinysoft资产配置也不回填历史。概况快照最早从实际 `snapshot_date` 可见，不能按报告期末倒填。已下载且满足 `ann_date>report_date` 的报告可通过 `import_lof_aum_evidence.py` 以冻结计划追加到 `fund_pool_on.lof_aum_report_evidence`，保留源响应、哈希和真实入库时间；不改写日净值历史。该表是补充证据，当前LOF事实的日常维护仍来自已有净值及基金概况采集链。

部署顺序：先预览/执行 `extend_exchange_fund_sources.py --output-dir <目录>` 创建LOF物化视图和统一入口，再独立执行月度schema迁移；每次执行均需对应 `--execute --expected-plan-hash <哈希>`。之后使用 `run_etf_candidate_ai_automation.py --scope lof_initial` 首筛，再刷新每日池及显式重建2016起月度池。月度V2使用 `exchange_fund_usable_pool_monthly_v2` 纳入LOF；2026-09-29按用户要求将上市限制改为6个月后采用V3，进一步降至3个月后采用V4，再将月度池调整为首个交易日开盘生效，采用V5。原ETF-only、12个月、6个月及第二个交易日生效的版本均保留，新增历史版本仍标记 `HISTORICAL_RECONSTRUCTION`。每日政策为 `exchange_fund_usable_pool_v4`，月度政策为 `exchange_fund_usable_pool_monthly_v5`。V5只调整月度池时序与可得性标记，候选档案中的辅助等级及其历史分类不回写，也不重新调用大模型。

### 分类缺项补全（2026-09-29）

分类字典独立维护在 `alphahome/curation/etf_candidate_taxonomy.py`，不再从已有档案的非空值推断完整词表。一级/二级组合有明确约束，并补充主动股票、混合配置、全球权益及商品LOF适用类别。地区按合同投资范围或跟踪指数样本市场判断；业绩比较基准不能当成主动LOF的跟踪指数。新产品ADD须包含市场/地区及一级、二级分类，存量缺失这些字段也会触发后续增量维护，模型KEEP不能将缺项档案标记为完整确认。

已有缺项使用独立补全入口，接受逐产品的实际公开网页、基金公司披露或指数编制方案。资料在冻结计划前清除PDF提取控制字符，并保留来源URL和原始下载哈希。模型只可填写空的 `region_market`、`level1_group`、`level2_group`；置信度须至少0.85，三个字段完整、枚举和层级正确、同一核实跟踪指数分类一致才发布。证据不足保留缺项和具体原因。低置信度、待复核与高置信度均是模型输出，不能解释成人工评估的准确率。

补全不改变基金身份、暴露ID、候选等级、入池标志及原有整条档案的确认状态。原来的其他疑点不会因为三个分类字段补齐而自动消除；本次补全决策、证据和响应哈希单独保存在AI运行与逐产品审计表。补全按冻结计划去重，同月可在补充新证据后再次处理残余缺项，普通月度筛选仍保持同月每个范围最多一次成功。

```text
python scripts/curation/run_candidate_classification_completion.py --web-evidence <逐产品证据JSON> --output-dir <目录> --expected-target-count <预期数量>
python scripts/curation/run_candidate_classification_completion.py --execute --expected-plan-hash <计划哈希> --web-evidence <逐产品证据JSON> --output-dir <目录> --expected-target-count <预期数量>
```

成功补全发布新候选快照后须刷新每日可用池，才能让每日结果取得新分类。实际发布时间保留真实时间；2016年以来的月度重建记录不会使用今天补齐的分类回填历史。

### 人工确认入口

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
