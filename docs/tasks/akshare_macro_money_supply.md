# 上下文
文件名：akshare_macro_money_supply.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_money_supply.py`，封装 akshare `macro_china_money_supply`，获取中国 M0/M1/M2 货币供应量（数量+同比+环比）。M1-M2 剪刀差是流动性收紧最早信号。

# 项目概述
继承 `AkShareNoDateSingleBatchTask`，用 `melt_config` + `var_parser` 将宽表转长表（复用 rmb_fixing 模式）。长表 schema：month/aggregate/measure/value，PK=(month,aggregate,measure)。

# 分析 (由 RESEARCH 模式填充)
`macro_china_money_supply`（akshare 1.18.64 实测可用）返回宽表：`月份` + 9 列（M2/M1/M0 各 数量/同比/环比）。AlphaDB 现有 `macro_cn_m2` 仅含 M2 同比，本表补齐 M0/M1 全口径，使 M1-M2 剪刀差可算。`月份` 格式 `2026年05月份` → YYYYMM。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroMoneySupplyTask`：
- `domain="macro"`, `name="akshare_macro_money_supply"`, `table_name="macro_money_supply"`, `primary_keys=["month","aggregate","measure"]`。
- `melt_config`：`var_parser` 用正则解析 `货币和准货币(M2)-数量(亿元)` → aggregate=M2 + measure=amount。
- `schema_def`：`month VARCHAR(10)`, `aggregate VARCHAR(8)`, `measure VARCHAR(8)`, `value NUMERIC(20,4)`。

# 实施计划
1. 新增任务文件。2. 测试（melt + month 解析）。3. 文档。

# 当前执行步骤
> 正在执行: "无"

# 任务进度
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_money_supply.py`, `test_akshare_macro_tasks.py`, `akshare_macro_money_supply.md`
    * 更改摘要：新增中国货币供应量采集任务（melt 长表，M0/M1/M2 全口径），单元测试全绿。
    * 原因：补齐 M1-M2 剪刀差（流动性最早信号）。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查
（待补充）
