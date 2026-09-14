# 重构后的生产切换与恢复手册

本手册是待执行操作说明。2026-09-14 的重构只修改代码、创建独立候选环境、运行隔离测试并本地提交；没有执行生产 DDL、补数、角色变更、口令轮换、PG 服务升级或任务安装。

## 交付与验收顺序

1. 记录部署 commit、根 uv.lock SHA256、解释器路径、当前任务配置与禁用/启用状态。P/G 数学版本固定 P v2.0、G v1.1；fundpos 继续使用其独立冻结 wheel/runtime。
2. 在明确的生产维护窗口暂停对应领域写入者，确认无在途任务；保存原配置和任务定义。创建可恢复备份，先在独立实例验证恢复。
3. 审查并执行各领域独立 SQL，记录对象、行数、owner/ACL、索引、触发器、依赖视图和执行耗时。先补结构，后提交计划，避免把 `migration_required` 当作空数据。
4. 使用候选解释器做真实只读计划与审计；确认任务、范围、依赖和哈希。数据变化后重新预览。先选择有完整源覆盖的小范围执行，再审计已提交结果；PIT / Features 的 `consumption_unverified` 必须保留。
5. 新登录角色验证后再切换配置与计划任务解释器。记录自然运行窗口、迟到数据重试、停机恢复和 SLO 证据，完成消费者验收后才退役兼容入口。

## 结构迁移清单

| 对象 | 生成/准备方式 | 执行前门槛 |
|---|---|---|
| Factors schema / ledger | `python -m alphahome.factors schema --help` | 先核当前结构、备份与预计锁；正常 plan/audit 不执行安装 |
| PIT 13 张物理目标表及触发器 | `alphahome.pit.schema.render_schema_sql()` | 选择目标任务；已有表逐列和唯一键核验；重复/缺列先处理 |
| Features 元数据 | `python -m alphahome.features schema` 输出 SQL | 独立事务安装；明确运行日志结构 |
| 5 张 SQL 增量表唯一键 | `features.storage.table_contracts.unique_key_migration_sql(recipe)` | 生成器拒绝 NULL/重复；确认依赖、数据量和锁窗口 |
| Features 新对象 | `python -m alphahome.features create --task NAME --dry-run` | 预览依赖闭包后执行同计划；表、索引和元数据整体提交 |
| 历史 P 行业未来信息影响 | 先生成影响范围报告，再单独制定重算计划 | 本轮不改历史数据；P→G 同日验证、比对与回滚档案齐备 |
| 旧消费证明 / 删除源数据 | 完整受影响范围重新验证 | 不把旧 timestamp-only 水位或硬删除解释为自动已消费 |

每份 SQL 保存哈希与执行记录。任何失败退出非零，不把部分写入当作全批成功。普通表刷新已经原地事务替换，禁止恢复到 rename swap 或“先删后算”的旧持久化路径。

## 角色和兼容层

生成草案，不连接数据库：

```powershell
python scripts/database/plan_runtime_roles.py --fetch-schema tushare --fetch-schema akshare --fetch-schema tinysoft > runtime_roles_review.sql
```

按实际已安装来源补充 `csindex/excel/fred/nbs/pbc` 等 schema；不得把 `rawdata/pit/factors/features/pgs_factors/fundpos` 作为 fetch 写入 schema。脚本要求新角色名；已有同名角色会使事务失败，避免沿用未知继承权限。它不创建登录、不设口令、不转移既有对象所有权。

`ah_data_reader` 只读；4 个 writer 只对本领域既有表做 DML；`ah_schema_owner` 只用于维护。新对象由维护角色创建时应用对应 default privileges。既有 owner 迁移须另生成逐对象清单，并处理其函数、视图与默认权限；不能用 `REASSIGN OWNED` 泛化跨项目所有权。Features 的 PG17 `MAINTAIN` 权限支持刷新 MV，普通 writer 不获得 ALTER/DROP 权限。[PG17 GRANT](https://www.postgresql.org/docs/17/sql-grant.html)

生产登录必须为非超级用户、非对象 owner，且不继承维护/其它写入角色。只撤销 `pgs_factors` 表级权限不能限制超级用户、owner、其它继承角色或 SECURITY DEFINER 函数；需审查角色成员关系、PUBLIC/列级授权、函数执行权与 pg_hba。测试允许/拒绝矩阵：本领域 INSERT/UPDATE/DELETE 允许；跨域写入、兼容视图写入和 ALTER/DROP 拒绝；reader 全部写入拒绝。隔离库已验证该矩阵，生产登录尚未切换。新采集表和 schema 调整必须先由维护入口安装，不能为日常采集重新授予超级用户。

## 备份与恢复

凭据放在本地受 ACL 保护的 pgpass 文件，命令参数和日志不写口令。以下示例中的 HOST/USER/文件名必须替换为本次明确对象：

```powershell
& 'E:\PostgreSQL\17\bin\pg_dump.exe' --host HOST --port 5432 --username BACKUP_USER --dbname alphadb --format custom --file 'E:\AlphaHomeBackups\approved-backup.dump'
if ($LASTEXITCODE -ne 0) { throw 'pg_dump failed' }
& 'E:\PostgreSQL\17\bin\pg_restore.exe' --list 'E:\AlphaHomeBackups\approved-backup.dump'
```

全库之外，角色/成员关系与服务配置也要独立保管；含密码哈希的 globals 备份不得入 Git。只对新建、明确命名的独立恢复库执行 `pg_restore --exit-on-error`，恢复时保留 owner/ACL 并提供对应角色。不得对生产库执行 `--clean`、`DROP DATABASE` 或试恢复。[PG 备份说明](https://www.postgresql.org/docs/17/backup-dump.html)

恢复验收包括行数和业务键校验和、关键聚合、P/G 同日依赖、owner/ACL、索引/约束、触发器与依赖视图、读写角色拒绝矩阵、应用只读预览。保存 dump 哈希、恢复日志和耗时。只做 `pg_restore --list` 不算成功恢复；只使用 `--no-owner --no-acl` 的数据恢复不算权限验收。隔离样本演练不替代真实生产备份恢复。

## 凭据和 PostgreSQL

现用数据库口令曾进入公开 Git 历史，代码删除不代表撤销秘密。应轮换数据库及实际暴露的远端凭据、更新本地配置/凭据管理器、验证新凭据可用、撤销旧凭据并检查连接日志。历史清理、force push 与远端 URL 改动是独立操作；不得因回滚应用而恢复已暴露的旧秘密。

生产 PG 17.4 的已知版本风险仍待维护窗口处理。选择官方当前维护版本并先验证扩展、驱动与备份恢复；同主版本补丁通常不需 pg_upgrade，但必须按安装器与平台流程停止/替换/重启，并完成验证。不要把跨主版本升级与补丁更新混为一项。[PG 升级说明](https://www.postgresql.org/docs/17/upgrading.html)

候选根环境的公开依赖已按锁安装并查询 OSV；该结果不是漏洞可达性证明，也没有替换全局 Python 或生产计划任务解释器。
