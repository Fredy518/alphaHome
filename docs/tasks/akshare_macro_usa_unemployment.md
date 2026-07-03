# 上下文
文件名：akshare_macro_usa_unemployment.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_usa_unemployment.py`，封装 akshare `macro_usa_unemployment_rate`，获取美国失业率。美联储双目标就业侧，与非农（新增流量）互补（失业率为存量比率）。

# 项目概述
继承 `AkShareMacroEventTask` 基类（事件类共享 process_data）。

# 分析 (由 RESEARCH 模式填充)
`macro_usa_unemployment_rate`（akshare 1.18.64 实测可用）返回 669 行，列 `商品/日期/今值/预测值/前值`，月频事件。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroUsaUnemploymentTask(AkShareMacroEventTask)`：
- `domain="macro"`, `name="akshare_macro_usa_unemployment"`, `table_name="macro_usa_unemployment"`, `api_name="macro_usa_unemployment_rate"`, `primary_keys=["date"]`。

# 实施计划
1. 新增任务文件。2. 测试。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_usa_unemployment.py`, `test_akshare_macro_tasks.py`, `akshare_macro_usa_unemployment.md`
    * 更改摘要：新增失业率采集任务（事件类基类），单元测试全绿。
    * 原因：补齐美联储双目标就业侧（存量比率）。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查
（待补充）
