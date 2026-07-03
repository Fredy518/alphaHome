# 上下文
文件名：akshare_macro_cn_cb_balance.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_cn_cb_balance.py`，封装 akshare `macro_china_central_bank_balance`，获取中国央行资产负债表（外汇占款/储备货币/政府存款等）。银行体系流动性的根源。

# 项目概述
继承 `AkShareNoDateSingleBatchTask`，用 `melt_config` 将 26 个项目列转长表（value_vars=None 自动推断，兼容列子集）。长表 schema：date/item/value，PK=(date,item)。

# 分析 (由 RESEARCH 模式填充)
`macro_china_central_bank_balance`（akshare 1.18.64 实测可用）返回宽表：`统计时间` + 26 项目列（外汇/储备货币/政府存款/总资产/总负债等），353 行回溯 1993。`统计时间` 格式 `2026.5`，transformer 通过 schema_def DATE 自动解析为月初日。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroCnCbBalanceTask`：
- `domain="macro"`, `name="akshare_macro_cn_cb_balance"`, `table_name="macro_cn_cb_balance"`, `primary_keys=["date","item"]`。
- `melt_config`：`id_vars=["date"], value_vars=None, var_name="item", value_name="value"`。
- `schema_def`：`date DATE`, `item VARCHAR(64)`, `value NUMERIC(20,4)`。

# 实施计划
1. 新增任务文件。2. 测试（melt + date 解析）。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_cn_cb_balance.py`, `test_akshare_macro_tasks.py`, `akshare_macro_cn_cb_balance.md`
    * 更改摘要：新增中国央行资产负债表采集任务（melt 长表），单元测试全绿。
    * 原因：补齐流动性根源（外汇占款+储备货币）。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查
（待补充）
