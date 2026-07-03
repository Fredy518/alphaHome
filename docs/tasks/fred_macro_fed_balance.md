# 上下文
文件名：fred_macro_fed_balance.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_fed_balance.py`，通过 FRED `WALCL` 获取美联储资产负债表总资产规模。全球流动性总闸门——QT/QE 缩表扩表直接决定全球美元流动性，比利率更前沿。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。单序列任务。

# 分析 (由 RESEARCH 模式填充)
FRED `WALCL`（美联储总资产，百万美元，周频每周三发布，2003 至今）经 fredgraph.csv 端点实测 keyless 可用（HTTP 200）。是观察 QT 进程的核心指标。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroFedBalanceTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_fed_balance"`, `table_name="macro_fed_balance"`, `data_source="fred"`。
- `series_ids=["WALCL"]`, `column_mapping={"WALCL": "total_assets"}`, `primary_keys=["date"]`, `default_start_date="20030101"`。
- `schema_def`：`date DATE`, `total_assets NUMERIC(18,2)`（百万美元）。

# 实施计划
1. 新增任务文件。2. 测试。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_fed_balance.py`, `test_fred_macro_tasks.py`, `fred_macro_fed_balance.md`
    * 更改摘要：新增美联储资产负债表采集任务，单元测试全绿。
    * 原因：补齐全球流动性总闸门（QT/QE 观测）。
    * 阻碍：无（依赖 FRED 可达）。
    * 用户确认状态：待确认

# 最终审查
（待补充）
