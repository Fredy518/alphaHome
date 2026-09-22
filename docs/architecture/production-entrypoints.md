# 生产入口与维护权威

注册事实由 `python scripts/generate_entrypoint_inventory.py --output docs/architecture/entrypoint-inventory.json` 生成。2026-09-14：188 fetch、15 PIT、2 factor、1 processor 映射任务，共 206；Features 为 43 个注册配方。完整名称、源表、目标和合同在 [机器可读清单](entrypoint-inventory.json)。`processor` 是现存代码映射任务标签，不代表恢复已删除的 processors 包。

| 领域 | 正式入口 | 契约与写入范围 |
|---|---|---|
| 采集 | `scripts/production/data_updaters/tushare/data_collection_smart_update_production.py` | 统一工厂，所有选中输入默认必需；optional 必须明确声明；`--dry-run` 只发现任务 |
| PIT | `python -m alphahome.pit.pit_data_update_production`；原 scripts 路径是兼容门面 | `RunPlan`、依赖闭包、固定截止、预检哈希；只写 pit，正常执行不安装表/触发器 |
| Factors | `python -m alphahome.factors` | P v2.0、G v1.1；自然周五、同日 P→G；只写 factors，不自动启动 PIT |
| Features | `python -m alphahome.features` | 注册依赖、冻结计划、整批互斥；只写 features；表内事务刷新保留 OID |
| GUI | `python run.py` / `alphahome` | “日常更新”按依赖图编排采集→PIT→Factors→Features，再分支执行 ETF 候选池月度维护与 FundPos；候选计算/DeepSeek/入库均在 AlphaHome 内部，FundPos 仅检查/影子模式 |
| Features 初始化 | `scripts/features_init.py` / `scripts/initialize_materialized_views.py` | 显式 schema 维护；创建配方委托同一 Features 协调器；普通刷新不调用初始化 |
| Factors 修复 / schema | Factors `repair` / `schema` 子命令 | 先预览；具体 apply 和 rollback 参数见 `--help`；回滚验证快照所有权 |
| PIT schema | `alphahome.pit.schema.render_schema_sql()` | 纯 SQL 生成器，独立审核执行 |
| 市场择时依赖 | `scripts/production/refresh_market_timing_dependencies.py` | 仅其 profile 定义的 fetch 输入和时间新鲜度；不代表策略信号有效或可下单 |
| fundpos | `scripts/production/fundpos/run_fundpos_daily.py` | 独立冻结 wheel/runtime/迁移；本次根环境及重构不替换 |
| 旧统一 ah CLI | `alphahome.cli.main` | 已下线，业务命令明确返回 unavailable=3；帮助仍可读 |
| 历史研究 P/G | `research/pgs_factor` | 历史研究消费者；正式公式和入库权威在 alphahome.factors |

退出状态中 `partial_success/error/blocked/cancelled` 不得被调度器当作完整成功。PIT 和 Features 尚无覆盖全部来源的消费认证账本，成功只表示本次单元按执行契约完成；readiness 保留 `consumption_unverified`，不以最后成功时间替代源消费证据。

GUI 的策略跳过不会切断依赖链：即使工作日不运行 Factors，采集或 PIT 失败仍会阻断 Features 及其下游。ETF 候选池与 FundPos 是 Features 后的独立分支，候选池模型/数据门禁失败不影响 FundPos 自身的检查或影子运行。

候选月度维护成功或已完成重试后，GUI 会再刷新 `etf_exposure_technical_current_universe_daily`，失败报告部分成功，不能沿用候选变更前的 Features 成功状态。日常采集仅核验 rawdata 映射；映射 DDL 与恢复表的纯 SQL 生成入口为 `python -m alphahome.common.maintenance_sql`。2026-09-22 本机陈旧 ETF 定时任务已禁用，新的生产调度尚未启用；采用步骤见 [恢复合同](recovery-contracts-20260922.md)。

旧 P/G 日、季度、年度脚本保留薄门面，服从同一日期和公式契约。删除旧入口前必须登记调用者、验证替代入口的目标/计划/退出码、保留可恢复版本，并完成约定观察窗口。两个未注册历史 MV、历史 seed、fundpos 资源不因目录整齐而删除。安装器只在独立授权的调度切换中运行，Git 远端和生产凭据不由代码发布顺带更改。
