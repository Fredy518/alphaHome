# 上下文
文件名：fred_macro_ted.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_ted.py`，通过 FRED `TEDRATE` 序列获取 TED 利差，用于 CrossLens SPEC-015 可选 global_score 输入（MVP 非阻塞）。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。

---

# 分析 (由 RESEARCH 模式填充)
FRED `fredgraph.csv?id=TEDRATE` 端点实测 keyless 可用（HTTP 200），日频。**重要：FRED 已于 2022-01-21 停用 TEDRATE**（LIBOR 退出导致），该序列自此后不再更新，数据冻结。本任务仍建表以保留历史数据，下游需知晓数据冻结，不作实时信号使用。schema comment 中已标注 discontinued 与冻结日期。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroTedTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_ted"`, `table_name="macro_ted"`, `data_source="fred"`。
- `series_ids=["TEDRATE"]`, `column_mapping={"TEDRATE": "ted_spread"}`, `primary_keys=["date"]`, `default_start_date="20000101"`。
- `schema_def`：`date DATE NOT NULL`, `ted_spread NUMERIC(10,4)`（注释标注序列已于 2022-01-21 停用）。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_ted.py`。
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加属性测试（含 discontinued 标注断言）。
3. 创建 `docs/tasks/fred_macro_ted.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 单元测试通过。
3. 文档创建，注明序列已停用。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_ted.py`, `test_fred_macro_tasks.py`, `fred_macro_ted.md`
    * 更改摘要：新增 TED 利差采集任务（序列已停用，仅历史数据），单元测试全绿。
    * 原因：补齐 AlphaDB 可选 global_score 输入。
    * 阻碍：TEDRATE 已停用，数据冻结 2022-01-21。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
