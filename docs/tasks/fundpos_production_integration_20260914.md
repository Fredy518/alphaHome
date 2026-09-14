# 公募基金仓位测算 AlphaHome 生产接入记录

日期：2026-09-14

## 结论

公募基金仓位测算的生产代码、配置、固定范围、版本化证据种子、数据库迁移和测试已迁入
AlphaHome。旧“公募基金行业仓位测算”仓库不再参与运行、安装、调度或入库。

AlphaDB `fundpos` schema 是结果、诊断和标准化证据的唯一维护与消费层。定时生产链固定为：

1. 从 AlphaDB 只读获取源数据并冻结输入快照；
2. 在隔离的 Python 3.12 wheel 中计算；
3. 将结果、诊断、融资情景和标准化证据事务写入 `fundpos`；
4. 从数据库读回，与冻结 Parquet 逐行勾稽；
5. 影子模式记录观察日，发布模式另行检查批准验证记录。

定时任务不生成 Excel、HTML 或 JSON 业务报表。Parquet 只作为入库前的不可变事务证据、
幂等指纹和故障恢复输入，不是下游接口。人工显式调用的临时 HTML 检视产物可随时删除，
不参与生产验收。

## 冻结发布

| 项目 | 值 |
|---|---|
| AlphaHome 提交 | `3e6f8020d4a8037bd0bb9e88e6c1410fdf7cb5e0` |
| fundpos 版本 | `0.4.0` |
| Git 标签 | `fundpos-v0.4.0` |
| 引擎 Git tree | `1e0dbdaa3ba6267de15d16eac27f925018b36f44` |
| Python | `3.12.7` |
| CVXPY / OSQP | `1.9.2` / `1.1.3` |
| wheel SHA256 | `dee646330f2965dc62d1df11b4f3faade084c54445522258efe712f048ab4417` |
| 隔离运行时 | `E:\CodePrograms\alphaHome\.fundpos-runtime\v0.4.0` |
| 数据库迁移 | 6/6 已应用 |

隔离环境按子包 `uv.lock` 安装，运行时不存在 `openpyxl`。编排器每次运行前核对 AlphaHome
标签、提交、引擎 tree、受管路径工作树、包版本、Python 版本、wheel 哈希和全部迁移；
任一项漂移即停止。

## 数据库存储边界

正式结果及诊断位于：

- `fundpos.estimation_run`、`fundpos.estimation_attempt`：逻辑运行、版本和执行尝试；
- `fundpos.fund_estimate`、`fundpos.fund_exposure`：单基金诊断和正式资产长表；
- `fundpos.group_estimate`、`fundpos.group_exposure`：群体覆盖与资产长表；
- `fundpos.fund_scenario_exposure`：固收+逐基金、逐融资情景、逐资产诊断长表；
- `fundpos.evidence_snapshot`、`fundpos.run_evidence`：输入和运行证据；
- 产品范围、份额、分类、合同及披露标准化表；
- 验证、发布和撤回审计表。

迁移 `005_fund_scenario_exposure.sql` 补齐原先只在 `scenarios.parquet` 中存在的逐情景结果；
迁移 `006_fund_scenario_privileges.sql` 显式授予 reader、writer 和 migrator 权限。真实测试曾触发
writer 权限不足，事务完整回滚；权限迁移后同一运行成功提交，证明失败不会留下半套结果。

下游使用 `fundpos.latest_available`、`fundpos.published_current`、
`fundpos.published_group_current` 或带 `run_id` 的明细查询，不读取本地项目目录。

## 2026-09-11 冻结影子运行

信息截止为 2026-09-12。

| 模型族 | 固定范围 | 可估算 | 适用范围覆盖率 | 正式资产行 | 融资情景行 | 入库与勾稽 |
|---|---:|---:|---:|---:|---:|---|
| 固收+ | 90 | 70 | 100% | 3,240 | 9,856 | 通过 |
| 增强指数 | 20 | 19 | 95% | 680 | 0 | 通过 |
| 转债主导 | 10 | 10 | 100% | 360 | 1,628 | 通过 |

运行号：

- 固收+：`2026-09-11_fixed_income_plus_e39fa3ebdb076e31b1976c17`
- 增强指数：`2026-09-11_personalized_614273d34e5e20847dd88c8f`
- 转债主导：`2026-09-11_convertible_dominant_d07d116260aa28894980ff99`

每个模型族均完成 `fund_estimate`、`fund_exposure`、`group_estimate`、`group_exposure` 和
`fund_scenario_exposure` 五组核对，缺行、多行和数值差异均为 0。三份正式运行目录均没有
`report` 目录，Excel 和 HTML 文件数均为 0。

迁移前后对 2026-09-11 的基金明细、群体汇总和融资情景分别按稳定键对齐。三个模型族的
列集合、非数值字段和全部数值完全一致，最大绝对差异为 0。再次执行完整影子批次时，
三个模型族均返回 `reused`，运行号保持不变，数据库勾稽继续通过。

## 测试与运行状态

- fundpos 子包：`160 passed`；
- AlphaHome 编排与状态初始化：`10 passed`；
- Ruff：子包、编排器、生产脚本和相关测试全部通过；
- `uv lock --check`：通过；
- 数据库迁移：6/6 哈希一致且已应用；
- Windows 任务 `AlphaHome-Fundpos-Shadow`：`Ready`，工作日 09:00、12:00、18:00；
- 影子观察：1/10 个不同估值日。

当前仍为影子入库，不更新 `fundpos.published_current`。这是运行观察状态，不影响数据库作为
唯一事实层；完成 10 个不同估值日以及迟到恢复、失败重跑和周报检查后，再按已批准的验证
记录切换发布指针。

## 维护入口

- 引擎源码与锁文件：`packages/fundpos`
- 配置模板：`config/fundpos_production.example.json`
- 日更入口：`scripts/production/fundpos/run_fundpos_daily.py`
- 状态种子初始化：`scripts/production/fundpos/bootstrap_fundpos_state.py`
- 计划任务安装：`scripts/production/fundpos/install_fundpos_schedule.ps1`
- 编排实现：`alphahome/integrations/fundpos/production.py`
- 运行账本：`E:\CodePrograms\alphaHome\logs\fundpos`
- 不可变事务证据：`E:\CodePrograms\alphaHome\logs\fundpos-engine`

旧项目只保留历史研究、R 对照和迁移审计，不得再次作为生产代码源或调度工作目录。
