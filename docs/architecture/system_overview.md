# AlphaHome 系统架构概览

## 定位

AlphaHome 当前定位为离线金融数据和特征生产平台：

- 采集多源数据并写入 AlphaDB/PostgreSQL。
- 维护 rawdata 视图、PIT 表和离线 features。
- 为研究侧提供 `ResearchContext` / `AlphaDataTool`。
- 通过 `scripts/production/` 支持日常更新、回填、修复和因子补算。

统一运维 CLI 已下线，当前入口是 GUI、脚本和 `python -m` 模块。

## 模块边界

```text
alphahome/
├── common/
│   ├── config_manager.py      # ~/.alphahome/config.json + 环境变量
│   ├── db_manager.py          # PostgreSQL 同步/异步管理
│   ├── task_system/           # BaseTask、注册、工厂
│   └── planning/              # BatchPlanner / ExtendedBatchPlanner
├── fetchers/
│   ├── base/                  # FetcherTask
│   ├── sources/               # tushare / akshare / tinysoft / excel
│   └── tasks/                 # 具体采集任务
├── factors/
│   ├── core/                  # P/G factor calculators
│   └── pipelines/             # FactorEngine 与兼容 CLI 调度
├── features/
│   ├── cards/                 # feature card YAML
│   ├── recipes/               # MV/Python recipes
│   └── storage/               # MV 初始化、刷新、校验
├── pit/
│   ├── base/                  # PITConfig / PITTableManager / PITTask
│   ├── calculators/           # 财务指标 calculator
│   ├── tasks/                 # task_type="pit" 的统一任务注册入口
│   ├── audit_service.py       # 覆盖率、缺口、单股诊断和审计快照
│   └── database/              # PIT DDL SQL
├── integrations/              # 外部系统集成预留
├── providers/                 # AlphaDataTool
└── gui/                       # Tkinter GUI
```

## 数据流

```mermaid
flowchart LR
    Sources[Tushare / AkShare / Tinysoft / Excel] --> Fetchers[fetchers tasks]
    Fetchers --> AlphaDB[(PostgreSQL / AlphaDB)]
    AlphaDB --> Rawdata[rawdata views]
    AlphaDB --> PIT[alphahome.pit managers]
    PIT --> FactorPG[alphahome.factors FactorEngine]
    AlphaDB --> Features[features MV recipes]
    AlphaDB --> Providers[AlphaDataTool / ResearchContext]
```

## 任务系统

所有任务通过 `BaseTask.execute()` 进入统一生命周期：

```text
_pre_execute -> _fetch_data -> process_data -> _validate_data -> _save_data -> _post_execute
```

采集任务的继承层次：

```text
BaseTask
└── FetcherTask
    ├── TushareTask
    ├── AkShareTask
    ├── TinySoftTask
    └── ExcelTask
```

`UnifiedTaskFactory` 负责注册任务、初始化数据库连接、注入 token/config，并为 GUI 和脚本创建任务实例。

PIT 任务也是统一任务系统的一部分，`task_type="pit"`。`PITTask` 只负责把统一执行模式映射到现有 PIT manager：

| GUI/CLI 模式 | PIT 内部动作 |
| --- | --- |
| 智能增量 | `incremental_update` |
| 全量更新 | `full_backfill` |
| 手动增量 | 指定日期范围的 `full_backfill`，支持单股配置时走 `single_backfill` |
| 只审计 | `PITAuditService.audit_task`，不写业务 PIT 表 |

## 存储

| 层 | 说明 |
| --- | --- |
| 源 schema | `tushare`、`akshare`、`tinysoft` 等，保存原始或标准化后的采集数据 |
| `rawdata` | 由任务保存流程自动创建/更新的统一视图层，Tushare 同名表优先 |
| `features` | 离线特征 MV 输出 schema |
| `pit` | 规范化 PIT 会计层、行业快照等时点数据 |
| `factors` | P/G 等因子计算结果 |
| `pgs_factors` | 旧 schema 兼容视图层，不作为新数据落库位置 |

`pgs_factors` 的分拆迁移脚本是 `scripts/database/split_pgs_factors_schema.py`，可重复执行；它会把 PIT 基表迁入 `pit`，把 P/G 因子迁入 `factors`，并重建旧 schema 的兼容视图。

## 生产脚本

| 目录 | 用途 |
| --- | --- |
| `scripts/production/data_updaters/tushare/` | 所有 fetch 任务的生产级智能更新 |
| `scripts/production/data_updaters/pit/` | PIT 表更新与财务指标计算的兼容入口，真实实现位于 `alphahome.pit` |
| `scripts/production/factor_calculators/` | P/G 因子补算和并行计算的兼容入口，真实实现位于 `alphahome.factors` |
| `scripts/database/` | AlphaDB 到 NAS 的逻辑同步/恢复 |

## PIT 管理

`pit` schema 是标准 PIT 落库层，当前一等任务包括：

- `pit_income_quarterly`
- `pit_balance_quarterly`
- `pit_cashflow_quarterly`
- `pit_financial_indicators`
- `pit_industry_classification`

PIT 审计结果写入 `pit.pit_audit_snapshot`。GUI 的 `PIT 管理` 页签负责刷新任务状态、执行增量/全量、只审计、查看覆盖缺口和单股诊断；常规运行状态仍复用 `任务运行与状态` 页，不引入第二套日志系统。

`features` 和 `factors` 消费 PIT 输出，不直接承担 PIT 口径治理；`pgs_factors` 只保留旧查询兼容视图。

## 已下线组件

- `alphahome.processors` 已删除。
- 旧外部分钟线加速集成已删除。
- `ah` / `alphahome-cli` / `refresh-materialized-view` 不再安装。
- `alphahome.cli` 仅保留兼容空壳，不作为当前入口。

历史设计和验收记录保留在 `docs/development/archive/`、`docs/tasks/` 和 `PHASE*_COMPLETION_REPORT.md`。
