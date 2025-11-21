# 上下文
文件名：tushare_stock_st.md  
创建于：2025-11-18  
创建者：GPT-5.1 Codex  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `tushare_stock_st.py`，调用 Tushare `stock_st` 接口每天抓取 ST 股票列表，支持按交易日增量更新，确保字段及批处理逻辑与官方文档（https://tushare.pro/document/2?doc_id=397）一致。

# 项目概述
AlphaHome 的 Tushare 抓取任务统一继承 `TushareTask`，通过批处理生成器拆分查询窗口并将数据入库。ST 列表任务需要与现有 `stock` 域任务保持相同结构，包括默认配置、字段映射、schema 定义、增量策略及日志风格，方便调度和监控。

---

# 分析 (由 RESEARCH 模式填充)
`stock_st` 接口自 20160101 起提供每日 ST 股票名录，更新于每个交易日 09:20，响应字段包含 `ts_code`, `name`, `trade_date`, `type`, `type_name`，单次请求最多 1000 行（[tushare.pro/document/2?doc_id=397](https://tushare.pro/document/2?doc_id=397)）。仓库中股票类任务统一使用 `generate_trade_day_batches` 进行交易日切片，默认 `smart_lookback_days=3`，并依赖 `TushareDataTransformer` 自动处理日期、列映射。新任务需定义 `default_start_date="20160101"` 并以 `trade_date + ts_code` 作为联合主键。由于接口字段均为字符串，不需要额外的数值转换，但需验证主键字段与类型字段非空。批处理策略可沿用 `tushare_stock_daily.py` 的配置：单代码模式 240 天，全市场 5 天，以确保每日全市场 ST 列表在单次批次内返回。

# 提议的解决方案 (由 INNOVATE 模式填充)
创建 `TushareStockSTTask` 继承 `TushareTask`，核心属性设置为 `name="tushare_stock_st"`, `table_name="stock_st"`, `default_start_date="20160101"`, `primary_keys=["trade_date","ts_code"]`, `api_name="stock_st"`, `fields=["ts_code","name","trade_date","type","type_name"]`，并保持 `smart_lookback_days=3`。批处理逻辑复用 `generate_trade_day_batches`，根据是否指定 `ts_code` 调整批大小为 240/5 天，附带 `fields` 参数，必要时支持 `ts_code` 过滤。schema 使用 `VARCHAR`/`DATE`，验证规则确保交易日和代码非空、类型字段存在。错误与日志遵循 `TushareTask` 约定。可选地将任务导出至 `stock/__init__.py` 以便调度器动态加载。

# 实施计划 (由 PLAN 模式生成)
1. 新建 `docs/tasks/tushare_stock_st.md` 并填充上下文、分析、方案、计划及检查清单。
2. 创建 `alphahome/fetchers/tasks/stock/tushare_stock_st.py`，定义 `TushareStockSTTask` 的属性、schema、验证、批处理配置。
3. 在新任务中实现 `get_batch_list`，复用交易日批次逻辑；必要时更新 `alphahome/fetchers/tasks/stock/__init__.py` 导出任务。

实施检查清单：
1. 新建任务文档，记录分析与计划。
2. 添加并实现 `TushareStockSTTask` 类（属性、schema、验证、配置）。
3. 实现 `get_batch_list` 交易日批次逻辑并在 `__init__.py` 中导出新任务。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2025-11-18 11:05
    * 步骤：1. 新建任务文档，记录分析与计划。
    * 修改：`docs/tasks/tushare_stock_st.md`
    * 更改摘要：创建 ST 任务文档并填入上下文、分析、方案及实施计划。
    * 原因：执行计划步骤 [1]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 11:15
    * 步骤：2. 添加并实现 `TushareStockSTTask` 类（属性、schema、验证、配置）。
    * 修改：`alphahome/fetchers/tasks/stock/tushare_stock_st.py`
    * 更改摘要：创建任务类，定义字段、schema、验证及批处理配置。
    * 原因：执行计划步骤 [2]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 11:18
    * 步骤：3. 实现 `get_batch_list` 交易日批次逻辑并在 `__init__.py` 中导出新任务。
    * 修改：`alphahome/fetchers/tasks/stock/tushare_stock_st.py`, `alphahome/fetchers/tasks/stock/__init__.py`
    * 更改摘要：实现交易日批处理逻辑并将任务导出。
    * 原因：执行计划步骤 [3]
    * 阻碍：无
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）


