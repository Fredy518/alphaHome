# AlphaHome fundpos engine

`packages/fundpos` 是 AlphaHome 内部维护的公募基金仓位测算引擎。它负责申万一级行业、
港股、转债、普通债券和非权益仓位的估算、验证与 `fundpos` schema 入库。

该子包使用 Python 3.12 和独立 `uv.lock`，避免与 AlphaHome 主环境的 Python 3.13、
pandas 3 及求解器依赖相互污染。生产编排入口位于
`scripts/production/fundpos/run_fundpos_daily.py`，不得从其他仓库直接导入或执行代码。

生产配置为 `config/v3.toml`。固定 110 只范围保存在
`resources/universe/pilot_110.parquet`；可变补充证据位于
`../../logs/fundpos-engine/supplements`，首次部署由 AlphaHome 的状态初始化命令从带哈希
的版本种子创建。行情和基金原始数据继续从 AlphaDB 只读获取。

```powershell
uv sync --frozen
uv run --frozen pytest
uv run --frozen fundpos estimate --config config/v3.toml `
  --family convertible_dominant --scope v3-pilot --date latest
```

数据库迁移只由显式 `db-migrate --plan/--apply` 执行。生产链为“估算冻结 Parquet →
事务入库 → 数据库勾稽 → 可选发布”。AlphaDB `fundpos` schema 是结果、诊断和证据的
唯一维护与消费层；定时任务不生成 Excel、HTML 或 JSON 业务报表。Parquet 只用于入库
前的不可变审计、幂等校验与故障恢复，不作为下游数据接口。

`report` 命令仅保留为人工显式调用的临时 HTML 检视工具，产物可随时删除，也不参与
生产验收。需要表格文件时，应从数据库按指定运行版本临时导出，不维护第二份结果。
