# 上下文
文件名：tushare_stock_ahcomparison.md  
创建于：2025-11-18  
创建者：GPT-5.1 Codex  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `tushare_stock_ahcomparison.py` 任务文件，封装 Tushare `stk_ah_comparison` 接口，批处理模式需参考 `tushare_stock_daily.py`，确保按 Tushare 官方文档（https://tushare.pro/document/2?doc_id=399）定义字段与参数。

# 项目概述
AlphaHome 数据抓取子系统以任务驱动的方式对接 Tushare API，各 `fetchers/tasks/stock` 模块通过继承 `TushareTask` 实现统一的增量、批处理、转换与校验逻辑。新任务需与现有任务保持一致的结构、索引、schema、验证及批调度策略。

---

# 分析 (由 RESEARCH 模式填充)
当前 stock 任务均继承 `alphahome.fetchers.sources.tushare.TushareTask`，该基类要求显式定义 `api_name`、`fields`、`schema_def`、`transformations`、`validations` 并实现 `get_batch_list`，内部通过 `TushareDataTransformer` 统一完成列映射、数据类型转换与增量控制（参考 `alphahome/fetchers/sources/tushare/tushare_task.py`）。`tushare_stock_daily.py` 展示了最新批处理规范：按交易日生成批次（`generate_trade_day_batches`），针对单一 `ts_code` 使用 `batch_trade_days_single_code=240`，全市场模式使用 `batch_trade_days_all_codes=5`，并注入 `fields` 字段控制（`alphahome/fetchers/tasks/stock/tushare_stock_daily.py`）。

根据 Tushare 官方文档 `stk_ah_comparison` 接口仅自 2025-08-12 起提供 AH 比价数据，每日 17:00 后更新，单次最多 1000 行，可通过 `hk_code`、`ts_code`、`trade_date`、`start_date`、`end_date` 过滤（[tushare.pro/document/2?doc_id=399](https://tushare.pro/document/2?doc_id=399)）。返回字段包括 `hk_code`, `ts_code`, `trade_date`, `hk_name`, `hk_pct_chg`, `hk_close`, `name`, `close`, `pct_chg`, `ah_comparison`, `ah_premium`，数值字段需转为浮点。表结构需要至少以 `ts_code`、`hk_code`、`trade_date` 唯一约束，保证 A/H 对应关系。

考虑到文档建议“批处理模式参考 @tushare_stock_daily.py”，新任务应沿用交易日批量策略与 `smart_lookback_days=3` 的增量模式，默认 `start_date` 应取 20250812 以避免冗余请求。schema 字段命名应与业务规范一致（如 `hk_pct_chg`、`ah_comparison` 等保持下划线风格），同时为高频查询添加 `trade_date`/`ts_code`/`hk_code` 索引。还需确认 1000 行限制下的批次大小不会超量，两层分页由基类 `page_size`=6000 控制，故单批可覆盖多日范围，保持与 `stock_daily` 相同配置最稳妥。

# 提议的解决方案 (由 INNOVATE 模式填充)
遵循现有 `stock` 任务模板，新增 `TushareStockAHComparisonTask` 继承 `TushareTask`，`api_name="stk_ah_comparison"`，字段集与官方文档一致，所有价格/涨跌幅/比价字段转为 `float`，默认 `start_date` 固定为 20250812 并维持 `smart_lookback_days=3`。批处理沿用 `tushare_stock_daily` 的交易日切分策略（单代码 240 天、全市场 5 天），确保同时兼容 `ts_code` 和全市场模式。表结构以 `trade_date + ts_code + hk_code` 作为联合主键并补充高频索引，验证规则覆盖主键字段非空以及关键数值字段非负或大于零。异常与日志处理沿袭基类行为，便于接入统一调度。

针对 ST 列表需求，可仿照上述模式建立 `TushareStockSTTask`：`api_name="stock_st"`、`default_start_date="20160101"`、`primary_keys=["trade_date","ts_code"]`，字段包含 `ts_code`, `name`, `trade_date`, `type`, `type_name`。批处理继续使用交易日切分（单代码 240 天、全市场 5 天），保证每日 09:20 更新数据可覆盖。校验重点为主键字段非空以及 `type/type_name` 字段存在。增量策略沿用 `smart_lookback_days=3` 与默认分页配置，以便与其他股票任务共享调度。

# 实施计划 (由 PLAN 模式生成)
1. 更新 `docs/tasks/tushare_stock_ahcomparison.md` 的“实施计划”内容，写明新任务文件、字段、批处理策略及联合主键（trade_date+ts_code+hk_code）。
2. 新建 `alphahome/fetchers/tasks/stock/tushare_stock_ahcomparison.py`，定义任务类属性：`default_start_date="20250812"`、`primary_keys=["trade_date","ts_code","hk_code"]`、`indexes`、`fields`、`transformations`、`schema_def`、`validations`、`smart_lookback_days=3` 等。
3. 在新任务内实现 `get_batch_list`，复制 `tushare_stock_daily` 的交易日批次逻辑并适配默认日期与 `stk_ah_comparison` 特定参数。

实施检查清单：
1. 更新 `docs/tasks/tushare_stock_ahcomparison.md` 的实施计划段落，使其反映最终方案和步骤。
2. 添加 `alphahome/fetchers/tasks/stock/tushare_stock_ahcomparison.py` 并实现任务类属性、schema、校验及配置。
3. 在新任务中实现与 `tushare_stock_daily` 一致的交易日批处理逻辑 (`get_batch_list`)。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2025-11-18 10:20
    * 步骤：1. 更新 `docs/tasks/tushare_stock_ahcomparison.md` 的实施计划段落，使其反映最终方案和步骤。
    * 修改：`docs/tasks/tushare_stock_ahcomparison.md`
    * 更改摘要：补充提议方案、细化实施计划与检查清单。
    * 原因：执行计划步骤 [1]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 10:28
    * 步骤：2. 添加 `alphahome/fetchers/tasks/stock/tushare_stock_ahcomparison.py` 并实现任务类属性、schema、校验及配置。
    * 修改：`alphahome/fetchers/tasks/stock/tushare_stock_ahcomparison.py`
    * 更改摘要：定义 TushareStockAHComparisonTask 的核心属性、字段、schema、验证与批处理配置。
    * 原因：执行计划步骤 [2]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 10:32
    * 步骤：3. 在新任务中实现与 `tushare_stock_daily` 一致的交易日批处理逻辑 (`get_batch_list`)。
    * 修改：`alphahome/fetchers/tasks/stock/tushare_stock_ahcomparison.py`
    * 更改摘要：完成 `get_batch_list` 的交易日批次生成逻辑，支持 ts_code/hk_code 过滤。
    * 原因：执行计划步骤 [3]
    * 阻碍：无
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）


