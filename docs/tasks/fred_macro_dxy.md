# 上下文
文件名：fred_macro_dxy.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_dxy.py`，通过 FRED `DTWEXBGS` 序列获取美元指数，用于 CrossLens SPEC-015 `global_liquidity_metrics.usd_index` 与 global_score 外部因子。

# 项目概述
本任务及后续 FRED 系列任务复用新建的 `FredTask` 基类（`alphahome/fetchers/sources/fred/`），该基类封装 keyless FRED fredgraph.csv 端点，经 `asyncio.to_thread` 异步化，支持重试与限流。任务经 `@task_register()` 自动注册发现。

---

# 分析 (由 RESEARCH 模式填充)
FRED `fredgraph.csv?id=DTWEXBGS` 端点实测 keyless 可用（HTTP 200，无需 API key），返回两列 `observation_date, DTWEXBGS`，日频，2006 至今。**SPEC-015 目标为 ICE 美元指数（DXY），但 CBOE CSV 实测 403、yfinance 实测 429 限流**，故用 DTWEXBGS（贸易加权广义美元指数，含商品与服务，2006 基期=100）作代理。DXY 仅对 6 种发达货币加权，DTWEXBGS 对更广泛贸易伙伴加权，两者走势相关但口径与数值不同，下游需标注。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroDxyTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_dxy"`, `table_name="macro_dxy"`, `data_source="fred"`。
- `series_ids=["DTWEXBGS"]`, `column_mapping={"DTWEXBGS": "dxy_close"}`, `primary_keys=["date"]`, `date_column="date"`, `default_start_date="20060101"`。
- `schema_def`：`date DATE NOT NULL`, `dxy_close NUMERIC(12,4)`（注释标注 DTWEXBGS 非 ICE DXY）。
- 基类 `fetch_batch` 单序列拉取+重命名；`process_data` 做类型转换、日期规整、生效窗口过滤、去重。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_dxy.py`。
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加属性、process_data、MANUAL 窗口过滤测试。
3. 创建 `docs/tasks/fred_macro_dxy.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 单元测试通过。
3. 文档创建，注明 DTWEXBGS 代理口径。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_dxy.py`, `test_fred_macro_tasks.py`, `fred_macro_dxy.md`
    * 更改摘要：新增 FRED 美元指数采集任务，DTWEXBGS 代理，单元测试全绿。
    * 原因：补齐 AlphaDB 全球流动性缺口。
    * 阻碍：无（keyless 可得）。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
