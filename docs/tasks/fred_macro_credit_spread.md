# 上下文
文件名：fred_macro_credit_spread.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_credit_spread.py`，通过 FRED `BAAFF` 获取美国 Baa-Aaa 企业债信用利差。信用风险与风险偏好先行指标——流动性紧张最早在信用利差走阔体现。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。单序列任务。与 VIX（股市波动）、SOFR-IORB（融资压力）共同构成多维度风险监测。

# 分析 (由 RESEARCH 模式填充)
FRED `BAAFF`（Baa 级与 Aaa 级企业债收益率利差，日频，1986 至今，单位 pp）经 fredgraph.csv 端点实测 keyless 可用（HTTP 200，当前 2.36）。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroCreditSpreadTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_credit_spread"`, `table_name="macro_credit_spread"`, `data_source="fred"`。
- `series_ids=["BAAFF"]`, `column_mapping={"BAAFF": "credit_spread"}`, `primary_keys=["date"]`, `default_start_date="19860101"`。

# 实施计划
1. 新增任务文件。2. 测试。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_credit_spread.py`, `test_fred_macro_tasks.py`, `fred_macro_credit_spread.md`
    * 更改摘要：新增美国信用利差采集任务，单元测试全绿。
    * 原因：补齐信用风险先行指标。
    * 阻碍：无（依赖 FRED 可达）。
    * 用户确认状态：待确认

# 最终审查
（待补充）
