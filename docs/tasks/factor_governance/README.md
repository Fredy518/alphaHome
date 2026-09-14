# P/G 因子统一治理实施说明

## 冻结边界

- PIT 原始事实属于 `pit`；P/G 结果属于 `factors`。
- `pgs_factors.p_factor` 与 `pgs_factors.g_factor` 仅为兼容视图。
- P 公式保持 v2.0，G 公式保持 v1.1；黄金样本按数据库精度锁定。
- 生产日期是自然周五，节假日周五合法；自动截止日是运行日前最近一个完整周五。
- G 自动展开 P，P 只检查 PIT 就绪状态，任何因子流程都不会自动执行 PIT。

修复前证据见 [baseline_20260914.json](baseline_20260914.json)。

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

## 治理表

- `factors.factor_run`：模式、版本、配置哈希、来源水位、运行状态和起止时间。
- `factors.factor_run_date`：日期级输入/输出、覆盖率、校验和和当前归属。
- `factors.factor_audit_snapshot`：实时表审计快照，和执行时间严格分开。
- `factors.factor_repair_manifest` / `factor_repair_date`：可回滚修复清单。

业务 P/G 表没有增加治理列，也没有为历史约 730 万行做版本字段重写。

## CLI

```powershell
# 智能预检；不写业务表
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
