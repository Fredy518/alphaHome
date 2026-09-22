# Features 计划与执行契约

GUI 的刷新、创建服务与 `python -m alphahome.features` 共用 `FeatureCoordinator`。领域内部依赖由注册配方的 `source_tables` 和物理输出名解析，自动展开并按拓扑顺序执行；不会启动 PIT 或 Factors。失败的上游阻止下游执行。

`refresh/create --dry-run --task NAME` 只读预览，返回不可变 `RunPlan` 和 SHA256。计划包括显式数据库目标、配方代码、目录结构、源观察、依赖、策略、替换范围和已存在行数；缺结构或不支持策略返回 blocker。执行支持 `--plan-file` 和 `--expected-plan-hash`，GUI 服务也接受同一序列化计划。整个批次持有 Features advisory session lock，在写入前重新核验计划。PG 快照令牌采取保守失效策略，无关事务的提交也可能要求刷新预览。

普通 SQL / Python 特征表固定 Asia/Shanghai 业务日和日期范围。全量计算从 1900-01-01 到截止日，原表内事务替换；增量以配方 `incremental_days` 为常规窗口，并根据已提交检查点和来源修订扩展范围，超过预算要求显式回补。普通表必须已有声明的 NOT NULL 唯一键，不自动安装约束。物化视图刷新覆盖定义中的全部行，业务日标签不表示历史快照重建，尤其解禁计划可能合法包含未来日期。并发 MV 刷新缺能力时默认阻断；只有显式 `--allow-blocking-fallback` 才允许预检中已记录的全量退化。

`schema` 只输出元数据安装 SQL；`create` 是独立维护操作。新对象的表或 MV、索引、键约束和元数据在一个事务提交。任何一步失败全部回滚，已有合法对象返回 `no_op`；存储类型或键不符要求单独迁移。旧表的键迁移生成器见 `features.storage.table_contracts`。

刷新批次不是跨表原子事务：每个成功单元可独立提交，后续失败返回部分成功。取消会回滚当前普通表事务并关闭连接释放锁，已提交单元保留在结果中。计划源观察与实际源消费证明分开，当前 Features 结果明确报告 `source_consumption=unverified`；不能用执行成功代替上游消费完整性验收。

隔离 PG 验证覆盖 GUI/CLI 共用计划、依赖顺序、源变化拒绝、创建索引/元数据故障回滚、父任务失败、停止请求、批次锁和取消释放。生产安装、迁移、历史重算与调度切换另行执行。

2026-09-22：刷新执行已声明的空值/键/行数质量规则；普通表的恢复检查点与结果同事务提交。未认证的描述性质量字段仍在 `advisory_keys` 报告。来源合同、合法空结果、生产切换和局限见 [恢复合同与切换顺序](recovery-contracts-20260922.md)。
