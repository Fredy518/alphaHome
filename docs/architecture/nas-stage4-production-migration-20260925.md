# NAS 第四阶段：生产结构与历史数据迁移（2026-09-25）

## 范围与恢复点

目标是 `192.168.5.6:15432/alphadb`，计算与数据截止日为 **2026-09-24**。长期维护电脑 3900X 在本次执行时关机；本机执行数据库迁移，没有启动采集、部署服务或连接该电脑。第二阶段的隔离演练见 [演练记录](nas-stage2-rehearsal-20260925.md)，第一阶段的 `stock_daily_enriched` 与宏观日历映射见 [原生产记录](nas-production-migration-20260925.md)。

迁移前的整库逻辑归档位于 `Y:\AlphaHomeBackups\nas-alphadb-full-20260925.dir`，281 个文件、22,299,648,053 字节；文件清单 SHA-256 为 `c9a5ffe22657f693d857054d0cb980df60b1e3ffb908fc94121000fa653f4e0d`。Y 盘和数据库是同一台 NAS 的**不同硬盘**。用户选择以此为本次回退点并继续生产迁移；归档可列出 2,597 个 TOC 条目并提取结构，但**没有完成整库独立实例恢复**。它能支持 NAS 仍可用时的数据库回退，不能证明整机故障恢复。

执行前确认目标库为 PostgreSQL 17.11，只有 `postgres` 角色，没有其他活动会话；配套代码固定在 `codex/architecture-recovery-20260922` 的 `55bf0fe`。关键计划、执行日志和核验 JSON 保存在 `Y:\AlphaHomeBackups`，不包含连接凭据。`ALPHAHOME_DATABASE_URL` 被显式设置为 NAS 目标；旧 `DATABASE_URL` 不能可靠覆盖本机配置。

## 已提交的数据库变更

| 范围 | 结果与核验证据 |
| --- | --- |
| 四张既有 Features 表 | `scripts/database/nas_stage2_feature_key_contract_20260925.sql` 单事务成功，业务键设为 `NOT NULL` 并建立有效唯一索引。原 276,958、1,386,396、5,179、5,179 行均保留。执行日志：`nas-stage2-feature-key-apply-20260925.log`。 |
| Factors | 五张治理表及两张修复归档表建成；只读 `schema` 检查为 `ready`、`issues=[]`。没有执行任何修复任务。 |
| 独立 Features 结构 | 产品事实、指数估值、行业盈利观察、ETF 技术四个物化视图，以及 All-A 日度、月度两张表按计划建成。每个 `create` 计划均无阻断项、结果为 `success`。 |
| ETF 候选池 | 候选母表、确认层和依赖视图建成；确认层迁移计划哈希为 `bd4ddc6a07aa01f2d9196b926089a8abb981b81026a30870ed1d6bc9d0791113`。历史候选快照 292 行、AI 决定 146 行、人工确认审计 5 行。六个候选业务视图可查询；当前母表 142 行。核验：`nas-stage2-candidate-view-validation-20260925.json`。 |
| FundPos | 001–006 六个迁移全部 `applied`，再次只读规划仍全部 `applied`；没有执行仓位或估算任务。 |
| 本机独有的 48 张表 | 从隔离演练已核验的本机历史归档导入，排除由迁移器生成的 `fundpos.schema_migration` 数据，单事务 `pg_restore` 零错误。导入后 48 张表行数均与本机冻结快照相同；46 张普通历史表的内容多重集指纹一致，候选快照经列名规范化后哈希一致，FundPos 迁移账本版本和 SQL 哈希一致。核验：`nas-stage2-history-count-validation-20260925.json`、`nas-stage2-history-fingerprints-timezone-aligned-20260925.json`。 |

48 张表及四个物化视图、九个普通视图的关系类型、列名、数据类型、默认值和非空约束，与本机对应结构逐项一致。源数据归档 `local-48-tables-data-rehearsal-20260925.dump` 与排除 FundPos 迁移账本重复数据的 TOC 文件，已另存到 `Y:\AlphaHomeBackups` 并逐文件核对 SHA-256；归档 SHA-256 为 `213c8ece2b21aa0caf0388212e2c9a58a386c6a43b9be6a26f4d7260ae311b63`。NAS 会话在内容指纹核验时固定为 `Asia/Shanghai`，避免时区展示差异被误判为数据差异。

## NAS 来源刷新与最终验收

四个物化视图均通过无阻断计划和计划哈希复核后按 NAS 来源完整刷新。其最新成功日志、实际行数、日期范围、业务键非空及有效唯一索引，由 `nas-stage2-new-mv-validation-20260925.json` 独立核对：

| 目标 | 行数 | 日期范围 |
| --- | ---: | --- |
| `features.mv_etf_product_facts_current` | 3,094 | 2026-09-24 |
| `features.mv_index_direct_valuation_daily` | 2,244,829 | 2004-01-02 至 2026-09-24 |
| `features.mv_industry_earnings_observation_monthly` | 23,984 | 2014-02-28 至 2026-08-31 |
| `features.mv_etf_exposure_technical_current_universe_daily` | 268,139 | 2005-01-04 至 2026-09-24 |

ETF 产品事实的 CLI 包装器曾在数据库成功提交后因多段标准输出发生 `JSONDecodeError`；没有重复刷新。物化视图已填充、日志 `success=true`、实际为 3,094 行，故该包装器错误不是数据库失败。指数估值、行业观察及 ETF 技术的 CLI 退出码均为 0、结果均为 `success`。

All-A 日度来源 `rawdata.stock_daily` 和 `stock_dailybasic` 的水位均为 2026-09-24，原导入基线只到 2026-09-11。按正式依赖链重建后，`features.all_a_index_daily` 为 **17,448 行**，覆盖 1991-01-02 至 2026-09-24 的 8,724 个交易日、两个序列；`features.all_a_expma_monthly` 为 **854 行**，覆盖 1991-02-28 至 2026-08-31 的 427 个完整月份、两个序列。两表各只有一个计算批次和一个 `source_data_as_of=2026-09-24`，月度表全部引用最终日度批次；无无效价格、非完整月份或批次错链。核验：`nas-stage2-final-product-validation-20260925.json`。48 张迁入表中，除按 NAS 来源重算的两张 All-A 表外，另外 46 张的最终行数仍与冻结快照一致；核验：`nas-stage2-final-history-count-validation-20260925.json`。

FundPos 数据库 234 条历史证据路径均指向本机 `E:`；对应的 392 个文件、382,176,508 字节已按原相对目录复制并逐文件 SHA-256 核对到 `Y:\AlphaHomeBackups\fundpos-evidence-20260925`。清单 SHA-256 为 `ca4c78fb9b919114d8257558b98b796d22802acd29a017e42e62de9c8a284a08`。数据库保留原始 `local_path` 作为来源记录；3900X 若要直接读取旧文件，需使用该 Y 盘副本与前缀映射，不能假定原 `E:` 路径在其机器上存在。

## 边界与接管

本次完成的是 NAS 数据库结构、历史数据和指定派生产品的迁移。运行结果仍标记 `source_consumption=unverified`，All-A 还标记 `history_coverage=unverified`；行数、业务键与批次核验不证明历史供应商数据逐日真实可得。All-A 使用当前历史版本重算，不是逐日封存的 PIT。整库恢复到 NAS 独立 PostgreSQL 实例、3900X 实际部署提交及首次自然运行均未验证，不能写作完成。第一阶段的 `macro_release_calendar` 视图仍为 498 行；未运行生产采集。

代码进入远端 `main` 后，3900X 开机应按准确提交拉取，核对 NAS 连接、历史证据前缀及任务入口，再逐域恢复采集和观察首次自然运行。不能把数据库迁移成功等同于另一台电脑已经更新或调度器已经恢复。
