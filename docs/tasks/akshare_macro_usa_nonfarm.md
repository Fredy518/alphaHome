# 上下文
文件名：akshare_macro_usa_nonfarm.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_usa_nonfarm.py`，封装 akshare `macro_usa_non_farm`，获取美国非农就业人数。美联储双目标的就业侧，FOMC 决策核心，利率预期重要驱动。

# 项目概述
继承 `AkShareMacroEventTask` 基类（事件类共享 process_data）。任务经 `@task_register()` 自动注册发现。

# 分析 (由 RESEARCH 模式填充)
`macro_usa_non_farm`（akshare 1.18.64 实测可用）返回 669 行，列 `商品/日期/今值/预测值/前值`，月频事件。非农为劳动力市场新增流量，与失业率（存量比率）互补。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroUsaNonfarmTask(AkShareMacroEventTask)`：
- `domain="macro"`, `name="akshare_macro_usa_nonfarm"`, `table_name="macro_usa_nonfarm"`, `api_name="macro_usa_non_farm"`, `primary_keys=["date"]`。
- `column_mapping`：`日期→date, 今值→rate, 预测值→rate_forecast, 前值→rate_prev`（rate 单位万人）。

# 实施计划
1. 新增任务文件。2. 测试。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_usa_nonfarm.py`, `test_akshare_macro_tasks.py`, `akshare_macro_usa_nonfarm.md`
    * 更改摘要：新增非农就业采集任务（事件类基类），单元测试全绿。
    * 原因：补齐美联储双目标就业侧。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查
（待补充）
