# AlphaHome 用户指南

## 使用前准备

1. 按 [安装指南](../setup/installation.md) 安装项目。
2. 按 [配置指南](../setup/configuration.md) 配置 `~/.alphahome/config.json`。
3. 确认 PostgreSQL 可连接，Tushare Token 已配置。

```bash
python -c "from alphahome.common.db_manager import create_sync_manager; db=create_sync_manager(); print(db.test_connection())"
```

## GUI

启动：

```bash
python run.py
```

GUI 首页用于日常更新，各领域页面用于查看详情、排查和手工运行：

| 标签页 | 用途 |
| --- | --- |
| 日常更新 | 预览并一键按依赖顺序执行智能增量；交易日自动跳过低频任务，非交易日自动纳入 |
| 数据采集 | 查看、筛选、选择已注册 fetch 任务 |
| PIT 管理 | 查看并运行 PIT 任务、审计覆盖率和诊断缺口 |
| 因子管理 | 预检、运行和审计 P/G 因子任务 |
| 特征更新 | 查看并按各配方声明的默认策略刷新 Features |
| FundPos 估算 | 查看三类 FundPos 估算任务，执行环境检查或影子估算；GUI 不提供正式发布 |
| 任务运行与状态 | 选择 SMART / MANUAL / FULL 模式并运行任务 |
| 任务日志 | 查看任务生命周期、批次执行、验证和保存日志 |
| 存储与设置 | 查看数据库信息、加载/保存 Tushare Token、测试连接 |

“一键智能增量更新”以 `rawdata.others_calendar` 的 SSE 日历判断交易日。日常任务每天纳入，周/月/季度及全量型任务只在周末或其他非交易日纳入；被 GUI 明确隐藏的分钟线、历史兼容等手工任务始终不会被一键运行，并会在计划详情中列明。更新按领域依赖顺序执行；上游发生真实失败时，下游会显示“前置更新域未成功”并停止启动，避免消费旧数据。FundPos 固定使用影子模式，结果会勾稽入库但不会更新正式发布指针。

点击“停止”后，正在提交的原子刷新会先在安全点完成，尚未启动的任务显示为“已停止/未执行”，不会计入失败；若停止前已经发生真实异常，异常数量仍会在详情中单独保留。

PIT 页把“当前实时表覆盖”和“最近一次执行记录”分开显示。若历史执行失败、但当前覆盖率和缺口检查均完整，状态显示为橙色“历史失败（覆盖完整）”；这不会抹掉失败历史，也不等同于来源消费已经认证，后者仍以审计状态为准。

执行模式：

| 模式 | 说明 |
| --- | --- |
| SMART | 根据目标表最新日期自动增量更新，并按 `smart_lookback_days` 回看 |
| MANUAL | 使用用户指定的开始/结束日期 |
| FULL | 从任务 `default_start_date` 到当前日期全量拉取 |

## 生产脚本

生产脚本需要在仓库根目录执行。

### 数据采集

```bash
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3 --dry-run
```

该脚本会自动发现所有 `task_type="fetch"` 的任务，并按数据源做并发控制。

### PIT 数据

```bash
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
python scripts/production/data_updaters/pit/pit_data_update_production.py --target balance income --mode incremental
python scripts/production/data_updaters/pit/pit_data_update_production.py --target financial_indicators --mode full
```

PIT 当前支持：

- `pit_balance_quarterly`
- `pit_income_quarterly`
- `pit_financial_indicators`
- `pit_industry_classification`

`financial_indicators` 依赖 `income` 和 `balance`，同批执行时脚本会保护依赖顺序。

### P/G 因子

```bash
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2026-05-08

python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10
python scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16
```

G 因子依赖同日期已有 P 因子数据。

### Features / MV

```bash
python scripts/initialize_materialized_views.py
python scripts/features_init.py --help
python scripts/features_validate_pit.py --help
```

当前 features 目录以 `features/cards/*.yaml` 和 `features/recipes/` 为准。

## 任务系统

采集任务统一走以下生命周期：

```text
BaseTask.execute()
  -> _pre_execute()
  -> _fetch_data()
  -> process_data()
  -> _validate_data()
  -> _save_data()
  -> _post_execute()
```

数据源任务分层：

```text
BaseTask
└── FetcherTask
    ├── TushareTask
    ├── AkShareTask
    ├── TinySoftTask
    └── ExcelTask
```

新增任务请参考 [新任务开发指南](../new_task_development_guide.md)。

## 研究侧数据访问

`AlphaDataTool` 是当前推荐的轻量研究入口：

```python
from research.tools.context import ResearchContext

with ResearchContext() as context:
    data = context.data_tool.get_stock_data(
        ["000001.SZ"],
        "2024-01-01",
        "2024-12-31",
    )
```

更多说明见 [providers README](../../alphahome/providers/README.md)。

## 故障排查

- 配置问题：先确认 `~/.alphahome/config.json` 路径和 JSON 格式。
- 数据库问题：先用 `create_sync_manager().test_connection()` 验证。
- Tushare 问题：检查 token、接口权限、限流日志。
- 大批量脚本问题：降低 `--workers` 或任务级 `concurrent_limit`。
- 数据缺失：用 MANUAL 模式指定日期补拉，再检查任务日志和目标表主键。
