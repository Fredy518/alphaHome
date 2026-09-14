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
| AlphaHome 提交 | `f669679fa7e98156fcf4f3b00cda2f98f511ce19` |
| fundpos 版本 | `0.4.1` |
| Git 标签 | `fundpos-v0.4.1` |
| 引擎 Git tree | `9ac6d5cd4b0c8af78db3899dd839c4f291cf4e01` |
| Python | `3.12.7` |
| CVXPY / OSQP | `1.9.2` / `1.1.3` |
| wheel SHA256 | `fa9590dc67cc1259d3391889862a06341c22ae6ef5c675fce7dbb06fc4d4a78e` |
| 隔离运行时 | `E:\CodePrograms\alphaHome\.fundpos-runtime\v0.4.1` |
| 数据库迁移 | 6/6 已应用 |

隔离环境按子包 `uv.lock` 安装，运行时不存在 `openpyxl`。编排器每次运行前核对 AlphaHome
标签、提交、引擎 tree、受管路径工作树、包版本、Python 版本、wheel 哈希和全部迁移；
任一项漂移即停止。

v0.4.1 将证据内容身份与本地审计位置分开维护：内容身份和首次观察时间保持不变，
重复导入时把审计位置更新为当前 AlphaHome 冻结目录；复用逻辑运行也会回填运行清单及
标准化输入证据。同日同范围的产品成员记录同步引用当前 AlphaHome 运行证据。

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

信息截止为 2026-09-14。

| 模型族 | 固定范围 | 可估算 | 适用范围覆盖率 | 正式资产行 | 融资情景行 | 入库与勾稽 |
|---|---:|---:|---:|---:|---:|---|
| 固收+ | 90 | 70 | 100% | 3,240 | 9,856 | 通过 |
| 增强指数 | 20 | 19 | 95% | 680 | 0 | 通过 |
| 转债主导 | 10 | 10 | 100% | 360 | 1,628 | 通过 |

运行号：

- 固收+：`2026-09-11_fixed_income_plus_43eab696fec3232a36b9c24b`
- 增强指数：`2026-09-11_personalized_aad67787a2f8955b0baa40d0`
- 转债主导：`2026-09-11_convertible_dominant_8bce4095cb2ea5101b05cd8f`

每个模型族均完成 `fund_estimate`、`fund_exposure`、`group_estimate`、`group_exposure` 和
`fund_scenario_exposure` 五组核对，缺行、多行和数值差异均为 0。三份正式运行目录均没有
`report` 目录，Excel 和 HTML 文件数均为 0。

v0.4.0 与 v0.4.1 对 2026-09-11 的数据库长表按稳定键对齐，缺行、多行和状态差异均为 0。
增强指数正式暴露完全一致；固收+和转债主导的正式暴露最大浮点差异分别为 `5.38e-13`
和 `3.4e-13`，融资情景最大差异为 `1.253e-10`，均远低于 `1e-6` 的工程容差；群体暴露
完全一致。再次执行完整影子批次时，三个模型族均返回 `reused`，运行号保持不变，五组
数据库勾稽继续通过。

三个当前运行各关联 27 条输入、标准化披露及运行清单证据，共 81 条；空路径、旧项目路径
和 AlphaHome 之外路径均为 0。复用批次后再次核验结果不变。当前运行目录没有 `report`
目录，Excel 和 HTML 文件数均为 0。

迁移前已有的 120 条历史证据位置也已复制到
`E:\CodePrograms\alphaHome\logs\fundpos-engine\legacy-standalone` 并在同一数据库事务中更新。
共保留 212 个 Parquet、JSON/NDJSON 等机器可读文件，合计 208,940,933 字节；明确排除
153 个 Excel、HTML、图片及其他展示文件。迁移清单 SHA256 为
`dc266d129f60b0f8f191aa93d0be3f81ede4dd8980411ed9a04f7056d695e0da`。迁移后
`evidence_snapshot` 的全部非空 `local_path` 均位于 AlphaHome，旧项目路径和外部路径均为 0；
原路径只保存在证据元数据中用于来源追溯。

## 测试与运行状态

- fundpos 子包：`161 passed`；
- AlphaHome 编排与状态初始化：`10 passed`；
- Ruff：子包、编排器和生产脚本全部通过；
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
