# NAS AlphaDB 生产迁移记录（2026-09-25）

> 本文记录先期授权范围。后来范围扩展到本机独有的 48 张表、相关结构与 NAS 来源刷新；生产结果见 [第四阶段记录](nas-stage4-production-migration-20260925.md)。

本次目标是 NAS PostgreSQL `192.168.5.6:15432/alphadb`。维护者选择与 2026-09-22 本机生产采用相同的范围：恢复账本、`stock_daily_enriched` 首次全量基线及固定截止日增量验收、`macro_release_calendar` 的精确映射。截止日固定为 **2026-09-24**。这不是本机与 NAS 的整库同构迁移：迁移前盘点发现 NAS 缺少本机的 48 张表，也没有逻辑复制订阅；这两项不在本次授权范围内。

生产证据和备份保存在仓库外的 `C:\Users\wuh\Documents\Codex\Reviews\AlphaHome-NAS-production-migration-20260925`。所有数据库校验均针对上述 NAS 目标；文件中不保存连接凭据。

## 迁移前保护和结构变更

| 对象 | 迁移前 | 本次处理及结果 |
|---|---|---|
| 数据库结构 | 迁移前结构已导出 | `nas-schema-before.sql`，SHA-256 `265dc3153079857762997d84e883d05f23095053fce8947fdfe2d483af6309a9` |
| `features.mv_stock_daily_enriched` | 普通表，429,638 行 | 迁移前自定义格式备份 `nas-stock-daily-enriched-before.dump`，40,171,834 字节，SHA-256 `b52d195e2f64ce1e94e551793a48a8e3c707c185af7e84cc05b75d1ec9cf17c7`；`pg_restore --list` 可读取 |
| `rawdata.macro_release_calendar` | 480 行实体表 | 迁移前备份 `nas-macro-release-calendar-before.dump`，SHA-256 `61123030618ae90b0955bb33f138837a43a3465f7971617e41309635b3469f1e` |
| `pit.task_run`、`features.refresh_checkpoint` | 两表及索引已存在、列定义与本机一致，均无记录 | 没有重复建表或伪造历史水位；迁移后前者仍为 0 行，后者有一条真实提交的目标检查点 |

先在单事务中给 Features 目标的 `ts_code`、`trade_date` 设置 `NOT NULL`，建立业务键唯一索引，并给 `features.mv_refresh_log` 增加缺少的 `details JSONB` 列。变更前 429,638 行得以保留，SQL SHA-256 为 `312e32e1a69bfdada027215ee9c547860ffca11cd474bf274cdb6420e0840d0b`；对象核验见 `nas-feature-target-prerequisites-result.json`。

宏观日历映射也在单事务中完成：旧实体表改名为 `rawdata.macro_release_calendar_legacy_20260925`，保留原 OID `18674` 和 480 行；新 `rawdata.macro_release_calendar` 是指向 `akshare.macro_release_calendar` 的视图。新视图和供应商源均为 498 行，双向 `EXCEPT ALL` 差异为 0，运行时 `verify_only` 检查通过。SQL SHA-256 为 `4376b2567d6e42edb51afc4f9b919a5bade9a82a04e9fc25f81aef83019b28e8`；见 `nas-macro-release-calendar-result.json`。没有运行生产采集。

## `stock_daily_enriched` 基线与恢复验收

首次全量尝试因 NAS 写入速度低于预估、将越过原有两小时客户端超时而被主动取消；结果为错误且 `committed_rows=null`。独立核对确认事务回滚后旧目标仍为 429,638 行、检查点仍为空。取消产生的大量死元组经单表 `VACUUM (ANALYZE)` 清理，随后对该表 `REINDEX`；见 `nas-rollback-vacuum-result.json`、`nas-rollback-reindex-result.json`。这次失败没有被记作迁移成功。

在将 SQL 全量刷新的客户端超时有界调整至四小时、来源检查超时固定为 180 秒后，重新冻结计划并执行。代码变更见提交 `2439749`。正式全量计划哈希为 `f69e2e209dda56c8c148c61e879c795cf7c78ad3f1efccee1069225225c8290d`，无阻断项，并显式批准首次基线的合法扩容。任务在约 10,523 秒后成功提交 **18,532,514 行**；目标 OID 仍为 `17051`，检查点覆盖 `1900-01-01` 至 `2026-09-24`。运行时空值门禁和源业务键集合校验通过。计划、执行及结果分别见 `nas-stock-daily-enriched-full-plan-retry.json`、`nas-stock-daily-enriched-full-execution-retry.log`。

独立只读校验确认目标和合格 `rawdata.stock_daily` 源各为 18,532,514 行，实际业务日期从 1991-01-02 至 2026-09-24，共 8,658 个交易日，必需列无空值。按 36 个年份首个交易日取样的 3,320 行逐一比较 24 个行情和估值字段：源缺键 0、字段差异 0。原先两种大范围抽样查询因 NAS 随机读取慢而达到只读超时，不能将它们写作已通过；改用逐年小批次后完成上述校验。证据为 `nas-stock-daily-enriched-post-full-validation-batched.json`。

随后以固定截止日重新冻结增量计划，计划哈希 `788fb8f1f55f7dddfdd5d489e96ea3e1f3451328e6f5573aad7a104a147be740`、文件 SHA-256 `8fcc87953c19ab39db11e988dea31a69142a7be5ad15bef30ac1fd586a566eb8`，无阻断项。它仅重算 **2026-08-25 至 2026-09-24**，原窗口 127,643 行。执行再次核对计划后成功提交 127,643 行，耗时约 265 秒；质量检查的窗口前后行数均为 127,643。见 `nas-stock-daily-enriched-incremental-plan.json`、`nas-stock-daily-enriched-incremental-execution.log`。

增量后的独立核验确认目标总量仍为 18,532,514 行、OID 仍为 `17051`；窗口内源和目标各有 127,643 行，两种独立种子的业务键指纹一致。23 个交易日各取 100 行，合计 2,300 行的 24 字段比对缺键和差异均为 0；增量前封存的 9 条窗口外完整记录未变化。最新刷新日志为成功的 `incremental (20260825-20260924)`，检查点仍从 `1900-01-01` 覆盖到 `2026-09-24`。证据见 `nas-stock-daily-enriched-post-incremental-validation.json`、`nas-incremental-before-sentinels.json`。

最终对象核对再次确认宏观视图与源 498 行完全一致、旧表 480 行仍可访问、恢复检查点一条、目标三个索引存在、无其他活动数据库会话。目标估计死元组 127,643，约为总行数的 0.7%，属于此次增量替换的正常结果；已完成全量后的 `VACUUM ANALYZE`，无需为这一步再次重写目标。见 `nas-final-object-audit.json`。

## 验证边界和后续运行

代码离线测试为 1,112 passed、109 skipped；与本次 Features 变更相关的隔离 PostgreSQL 集成测试 29 passed。扩大到全套隔离数据库集成测试时，两轮各有一项不同的既有测试因计划哈希在执行前变化而失败，因此不能声称全套集成测试全绿；这些失败均为写入前阻断。迁移判定另以 NAS 上的事务结果和独立只读核验为准。隔离测试集群已停止，未连接生产库运行测试。

本次证明按当前 NAS 来源建立了一个截止 2026-09-24 的可恢复 Features 基线；运行结果仍明确标记 `source_consumption=unverified`，不证明历史来源的真实可得时点。其他五个增量 Features 样板、PIT 业务产品、NAS 缺失的 48 张表和持续同步都没有因这次迁移而自动完成。代码和迁移记录已推送到 `codex/architecture-recovery-20260922`，截至本记录尚未进入 `main`；本次没有切换 NAS 所用的服务代码或调度入口。后续来源录入恢复后，应按正式入口执行新的计划和增量刷新，检查点不会自动追赶新数据。此前由维护者手动停止的录入进程未由本次数据库迁移重启。

48 张表及相关视图的下一阶段隔离演练见 [NAS 第二阶段演练记录](nas-stage2-rehearsal-20260925.md)；该演练不改变本节的生产验收范围。
