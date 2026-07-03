# 上下文
文件名：fred_macro_fed_rate.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_fed_rate.py`，通过 FRED 三序列（DFEDTARU/DFEDTARL/DFF）获取美联储联邦基金目标利率区间及有效利率，用于 CrossLens SPEC-015 `global_liquidity_metrics.fed_target_rate`，美联储政策姿态主指标。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。本任务为多序列合并任务，基类 `fetch_batch` 按 `observation_date` 外连接三序列。

---

# 分析 (由 RESEARCH 模式填充)
三序列均经 fredgraph.csv 端点实测 keyless 可用（HTTP 200）：`DFEDTARU`（目标区间上限，2008-12-16 起）、`DFEDTARL`（下限）、`DFF`（有效联邦基金利率，市场成交加权利率）。三者日频，按 `observation_date` 外连接合并，缺失日补 NULL。无重合日期冲突。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroFedRateTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_fed_rate"`, `table_name="macro_fed_rate"`, `data_source="fred"`。
- `series_ids=["DFEDTARU","DFEDTARL","DFF"]`, `column_mapping={DFEDTARU→target_upper, DFEDTARL→target_lower, DFF→effective_rate}`, `primary_keys=["date"]`, `default_start_date="20081216"`。
- `schema_def`：`date DATE NOT NULL`, `target_upper/target_lower/effective_rate NUMERIC(10,4)`。
- `fetch_batch` 三序列合并；`process_data` 类型转换+日期规整+窗口过滤+去重。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_fed_rate.py`。
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加 fetch_batch 多序列合并测试（含 DFF 缺失日 NaN、空数据返回 None）。
3. 创建 `docs/tasks/fred_macro_fed_rate.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 多序列合并单元测试通过。
3. 文档创建。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_fed_rate.py`, `test_fred_macro_tasks.py`, `fred_macro_fed_rate.md`
    * 更改摘要：新增美联储联邦基金利率采集任务（三序列合并），单元测试全绿。
    * 原因：补齐 AlphaDB 美联储政策姿态缺口。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
