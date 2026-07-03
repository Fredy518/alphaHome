# 上下文
文件名：akshare_macro_cci.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_cci.py`，封装 akshare `index_cci_cx`，获取大宗商品指数 CCI。输入型通胀综合指标——单看铜/油等单一商品不够，CCI 综合反映原材料价格趋势，是国内 PPI 输入型通胀的前瞻信号。

# 项目概述
继承 `AkShareNoDateSingleBatchTask`，直接列映射（无 melt）。与 future_daily（单品种）互补。

# 分析 (由 RESEARCH 模式填充)
`index_cci_cx`（akshare 1.18.64 实测可用）返回 4234 行，列 `日期/大宗商品指数/变化值`，日频。直接映射即可。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroCciTask`：
- `domain="macro"`, `name="akshare_macro_cci"`, `table_name="macro_cci"`, `api_name="index_cci_cx"`, `primary_keys=["date"]`。
- `column_mapping`：`日期→date, 大宗商品指数→cci, 变化值→change`。
- `schema_def`：`date DATE`, `cci NUMERIC(12,4)`, `change NUMERIC(12,4)`。

# 实施计划
1. 新增任务文件。2. 测试。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_cci.py`, `test_akshare_macro_tasks.py`, `akshare_macro_cci.md`
    * 更改摘要：新增大宗商品指数 CCI 采集任务，单元测试全绿。
    * 原因：补齐输入型通胀综合指标。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查
（待补充）
