# 上下文
文件名：tushare_stock_limitprice.md  
创建于：2025-11-18  
创建者：GPT-5.1 Codex  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `tushare_stock_limitprice.py`，调用 Tushare `stk_limit` 接口获取每日涨跌停价格（含 A/B 股及基金），单次最多提取 5000 行，需按交易日增量更新，字段与官方文档保持一致（https://tushare.pro/document/2?doc_id=183）。

# 项目概述
AlphaHome 的 stock 域抓取任务统一继承 `TushareTask` 并通过 `BatchPlanner` 管理批处理。新增任务必须遵循既有结构：声明核心属性、字段、schema、索引、转换与验证，复用交易日批处理逻辑，并在 `stock/__init__.py` 中导出以供调度。

---

# 分析 (由 RESEARCH 模式填充)
`stk_limit` 接口 8:40 更新当日涨跌停价，参数支持 `ts_code`、`trade_date`、`start/end_date`，返回 `trade_date`, `ts_code`, `pre_close`, `up_limit`, `down_limit`，单次最多 5800 行，但用户要求限制为 5000 行以保持安全余量。[tushare.pro/document/2?doc_id=183](https://tushare.pro/document/2?doc_id=183)  
现有 `tushare_stock_daily.py`、`tushare_stock_st.py` 等任务均采用 `generate_trade_day_batches`，设置 `default_start_date`（多为 1990 年段）并以 `trade_date + ts_code` 作为联合主键；`smart_lookback_days=3` 保障增量完整。此任务字段均为数值/日期，可通过 `TushareDataTransformer` 完成类型转换。建议默认起始日为 `19901219`（A 股最早交易日，与日线任务一致），以保证历史数据覆盖。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `TushareStockLimitPriceTask`：  
- `api_name="stk_limit"`, `fields=["trade_date","ts_code","pre_close","up_limit","down_limit"]`  
- `default_start_date="19901219"`, `primary_keys=["trade_date","ts_code"]`, `smart_lookback_days=3`  
- `default_page_size=5000`, `default_concurrent_limit=3` 以符合接口限额  
- `transformations`: 将 `pre_close/up_limit/down_limit` 转成 float  
- `schema_def`: `trade_date DATE`, `ts_code VARCHAR(15)`, 其余 `NUMERIC(15,4)`  
- 验证：`trade_date/ts_code` 非空，`pre_close > 0`, `up_limit/down_limit > 0`, 并要求 `down_limit <= up_limit`  
- 批处理：复用 `generate_trade_day_batches`，单代码 240 天、全市场 5 天，附带 `fields` 以及 `ts_code` 过滤  
- 更新 `alphahome/fetchers/tasks/stock/__init__.py` 导出任务

# 实施计划 (由 PLAN 模式生成)
1. 新建 `docs/tasks/tushare_stock_limitprice.md`，记录背景与实施计划。  
2. 创建 `alphahome/fetchers/tasks/stock/tushare_stock_limitprice.py`，实现任务类及属性、schema、验证。  
3. 在任务内实现 `get_batch_list`，复用交易日批量逻辑，并在 `stock/__init__.py` 导出新任务。

实施检查清单：
1. 任务文档创建完毕，含分析、方案与计划。  
2. `TushareStockLimitPriceTask` 代码实现（属性、schema、验证、配置）。  
3. `get_batch_list` 完成并导出任务至 `__init__.py`。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2025-11-18 11:45
    * 步骤：1. 任务文档创建及计划确认。
    * 修改：`docs/tasks/tushare_stock_limitprice.md`
    * 更改摘要：记录接口背景、方案与实施计划。
    * 原因：执行计划步骤 [1]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 11:52
    * 步骤：2. `TushareStockLimitPriceTask` 代码实现（属性、schema、验证、配置）。
    * 修改：`alphahome/fetchers/tasks/stock/tushare_stock_limitprice.py`
    * 更改摘要：创建任务类，定义字段、默认配置、schema、校验与批处理常量。
    * 原因：执行计划步骤 [2]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 11:54
    * 步骤：3. `get_batch_list` 完成并导出任务至 `__init__.py`。
    * 修改：`alphahome/fetchers/tasks/stock/tushare_stock_limitprice.py`, `alphahome/fetchers/tasks/stock/__init__.py`
    * 更改摘要：实现交易日批次生成逻辑并将任务导出。
    * 原因：执行计划步骤 [3]
    * 阻碍：无
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）


