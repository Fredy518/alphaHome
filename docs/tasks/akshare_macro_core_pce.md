# 上下文
文件名：akshare_macro_core_pce.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_core_pce.py`，封装 akshare `macro_usa_core_pce_price`，获取美国核心 PCE 物价指数年率。美联储通胀目标锚定核心 PCE 2%（非 CPI），是 FOMC 决策真正依据，比美国 CPI 更关键。

# 项目概述
继承新建的 `AkShareMacroEventTask` 基类（事件类宏观任务共享 process_data：仅保留 schema_def 列、规整日期、丢弃未发布 NaN 行、去重）。任务经 `@task_register()` 自动注册发现。

# 分析 (由 RESEARCH 模式填充)
`macro_usa_core_pce_price`（akshare 1.18.64 实测可用）返回 670 行，列 `商品/日期/今值/预测值/前值`，月频事件。核心 PCE 剔除食品能源，反映潜在通胀趋势。最新未发布事件「今值」为 NaN，由基类丢弃。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroCorePceTask(AkShareMacroEventTask)`：
- `domain="macro"`, `name="akshare_macro_core_pce"`, `table_name="macro_core_pce"`, `api_name="macro_usa_core_pce_price"`, `primary_keys=["date"]`。
- `column_mapping`：`日期→date, 今值→rate, 预测值→rate_forecast, 前值→rate_prev`。
- `schema_def`：`date/rate/rate_forecast/rate_prev`。process_data 由基类统一。

# 实施计划
1. 新增任务文件。2. 测试。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_core_pce.py`, `test_akshare_macro_tasks.py`, `akshare_macro_core_pce.md`
    * 更改摘要：新增核心 PCE 采集任务（事件类基类），单元测试全绿。
    * 原因：补齐美联储通胀锚（比 CPI 更关键）。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查
（待补充）
