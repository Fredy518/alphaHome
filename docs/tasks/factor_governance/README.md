# P/G 因子统一治理实施说明

## 冻结边界

- PIT 原始事实属于 `pit`；P/G 结果属于 `factors`。
- `pgs_factors.p_factor` 与 `pgs_factors.g_factor` 仅为兼容视图。
- P 公式保持 v2.0，G 公式保持 v1.1；黄金样本按数据库精度锁定。
- 生产日期是自然周五，节假日周五合法；自动截止日是运行日前最近一个完整周五。
- G 自动展开 P，P 只检查 PIT 就绪状态，任何因子流程都不会自动执行 PIT。
- P 的行业来源必须在计算日或之前：优先使用 PIT 的实际 `obs_date`，缺失股票仅可
  查询 `in_date <= calc_date` 的行业成员记录。成员回退保留实际 `in_date`、
  `source_table` 和 `source_method`；没有历史证据时按 `industry_history_missing`
  中止整日计算，不使用未来首次行业，也不将未知行业默认为普通行业。该输入修正
  不包含已有历史 P/G 数据重算。

修复前证据见 [baseline_20260914.json](baseline_20260914.json)，正式修复与独立验收
见 [repair_7d10a9ac-a628-403c-a0c0-4ed6409105d8.md](repair_7d10a9ac-a628-403c-a0c0-4ed6409105d8.md)。

## 运行架构

`FactorTaskContract` 声明来源、输出、主键、日期策略、依赖、资格口径、审计口径和
公式版本。`FactorCoordinator` 先展开因子依赖和生成完整日期计划，再为每个任务
开启独立的 PostgreSQL `REPEATABLE READ` 只读快照。G 的来源快照只会在 P 提交后
取得。

每个日期先通过资格集合、主键、PIT 边界、状态和分数范围校验，再在 advisory
lock 内 COPY 到临时 staging 表，并在一个事务中替换正式日期快照、更新运行日期
账本。取消仅在日期边界检查，不会中断正在提交的事务。

`smart` 模式处理缺失和来源变脏日期；P 的变化向 G 后续最多 730 天传播。单项
超过 26 个日期时返回 `needs_manual_backfill`，由 GUI 或显式 CLI 回补处理。

消费水位使用 `snapshot_consumed_v1` 契约：计划查询脏日期前先读取水位上限，计算在
独立只读快照中记录实际来源水位，成功完成范围后只推进两者中的保守边界。批末查询仅
记录为 `end_observed_watermarks`，不能作为下一次增量起点。失败、取消、无工作日期
以及只覆盖局部日期的 manual/repair 不推进全局水位。G 在 P 提交后获取新快照。
旧账本没有消费证明时，smart 会重新核验其声明的窗口，并受 26 日期自动规模门约束；
不会将既有成功记录自动升级成可信消费基线。

## 治理表

- `factors.factor_run`：模式、版本、配置哈希、来源水位、运行状态和起止时间。
- `factors.factor_run_date`：日期级输入/输出、覆盖率、校验和和当前归属。
- `factors.factor_audit_snapshot`：实时表审计快照，和执行时间严格分开。
- `factors.factor_repair_manifest` / `factor_repair_date`：可回滚修复清单。

业务 P/G 表没有增加治理列，也没有为历史约 730 万行做版本字段重写。

## CLI

`run --dry-run`、`FactorCoordinator.plan` 和 GUI 因子预检仅执行读取；参数校验在
数据库 I/O 前完成。治理表或所需列缺失时返回 `migration_required`，普通 `run`
也不会自动建表或写入运行账本。现有库没有迁移版本账本，此处只检查治理表列契约，
不替代列类型、约束、索引的完整结构审计。

```powershell
# 只读检查治理表结构
python -m alphahome.factors schema

# 显式维护操作：执行已有治理表 DDL；不会自动修复已有表的结构漂移
python -m alphahome.factors schema --apply

# 智能预检；不执行 DDL，也不写业务表或治理表
python -m alphahome.factors run --tasks p g --mode smart --dry-run

# 指定日期回补（G 会自动先运行 P）
python -m alphahome.factors run --tasks g --mode manual `
  --start-date 2026-09-04 --end-date 2026-09-11

# 实时审计与诊断
python -m alphahome.factors audit --tasks p g
python -m alphahome.factors diagnose --task g --date 2026-09-11
python -m alphahome.factors diagnose --stock 000001.SZ

# 修复默认只预览；--apply 才执行，--rollback REPAIR_ID 回滚
python -m alphahome.factors repair
python -m alphahome.factors repair --apply
python -m alphahome.factors repair --rollback REPAIR_ID
```

## Windows 周任务

```powershell
# 默认只输出 JSON 预览，不注册
scripts/production/factor_calculators/install_factor_weekly_task.ps1

# 只有明确部署时才注册
scripts/production/factor_calculators/install_factor_weekly_task.ps1 -Apply
```

默认任务名 `AlphaHome-Factor-Weekly`，每周六 08:00 执行 P、G、审计；不执行 PIT。

## 修复与回滚

`repair --apply` 为每次修复生成唯一 `repair_id`。所有待删除或替换的 P/G 行先写入
archive 表并记录旧行数和校验和；失败会自动按 `repair_id` 回滚。手工回滚会在单个
事务中删除修复后快照并恢复归档行。

修复验收要求：非周五为零；窗口周五均存在或有 `expected_no_data` 证据；无空键、
重复键、PIT 违规和越界分数；兼容视图指向 `factors` 基表；执行与审计记录可分别
追溯。修复结果和 `repair_id` 在正式执行后追加到本目录。
