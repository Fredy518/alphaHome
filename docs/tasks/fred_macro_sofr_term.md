# 上下文
文件名：fred_macro_sofr_term.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_sofr_term.py`，通过 FRED 三序列获取 SOFR 30/90/180 天复合平均，补充 SOFR 期限结构。与隔夜 SOFR（macro_sofr）共同构成 SOFR 利率曲线，供 liquidity/global_score 使用。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。本任务为多序列合并任务，基类 `fetch_batch` 按 `observation_date` 外连接三序列，并对未返回数据的序列补 NaN 列以保证输出 schema 一致。

---

# 分析 (由 RESEARCH 模式填充)
三序列均经 fredgraph.csv 端点实测 keyless 可用（HTTP 200，实时更新至 2026-06）：`SOFR30DAYAVG`（2018-05 起）、`SOFR90DAYAVG`（2018-07 起）、`SOFR180DAYAVG`（2018-10 起）。SOFR 复合平均反映不同期限的融资成本，构成 SOFR 曲线的期限结构。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroSofrTermTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_sofr_term"`, `table_name="macro_sofr_term"`, `data_source="fred"`。
- `series_ids=["SOFR30DAYAVG","SOFR90DAYAVG","SOFR180DAYAVG"]`, `column_mapping={...→sofr_30d/sofr_90d/sofr_180d}`, `primary_keys=["date"]`, `default_start_date="20180502"`。
- `schema_def`：`date DATE NOT NULL`, `sofr_30d/sofr_90d/sofr_180d NUMERIC(10,4)`。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_sofr_term.py`。
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加属性与 fetch_batch 多序列合并测试。
3. 创建 `docs/tasks/fred_macro_sofr_term.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 单元测试通过。
3. 文档创建。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_sofr_term.py`, `test_fred_macro_tasks.py`, `fred_macro_sofr_term.md`
    * 更改摘要：新增 SOFR 期限结构采集任务（3 序列合并），单元测试全绿。
    * 原因：补齐 SOFR 体系期限结构。
    * 阻碍：无（依赖 FRED 可达）。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
