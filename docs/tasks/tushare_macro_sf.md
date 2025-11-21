# 上下文
文件名：tushare_macro_sf.md  
创建于：2025-11-18  
创建者：GPT-5.1 Codex  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `tushare_macro_sf.py`，封装 Tushare `sf_month` 接口，获取月度社会融资数据。由于总量远低于接口限额（单次最多 2000 条），任务需采用全量更新模式，每次执行传入空参数一次性获取全部记录，行为类似 `tushare_stock_basic.py`。文档参考：https://tushare.pro/document/2?doc_id=310 。

# 项目概述
AlphaHome 的 `macro` 域抓取任务继承 `TushareTask`，通过统一的批处理和转换框架与数据库同步。新任务需保持既有结构：定义核心属性、字段、schema、转换、验证，并在 `macro/__init__.py` 导出。由于 `sf_month` 是月度数据且量小，最佳实践是单批次全量更新，以保证数据一致性和实现简洁性。

---

# 分析 (由 RESEARCH 模式填充)
`sf_month` 接口返回字段 `month`, `inc_month`, `inc_cumval`, `stk_endval`，可按 `m`, `start_m`, `end_m` 过滤，但 API 总体数据量仅数百条，远低于 2000 条限额，因此完全可以在每次任务执行时传空参数一次性抓取全量数据。[tushare.pro/document/2?doc_id=310](https://tushare.pro/document/2?doc_id=310)  
现有宏观任务如 `tushare_macro_cpi.py`、`tushare_macro_yieldcurve.py` 均继承 `TushareTask` 并定义 `api_name/fields/schema/validations`。月度任务通常会生成 `month_end_date` 便于日期索引。`TushareTask` 基类支持 `single_batch=True` 与 `update_type="full"`，可用于全量模式。为减少资源占用，可设定 `default_concurrent_limit=1`、`default_page_size=2000`。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `TushareMacroSFTTask`（或命名 `TushareMacroSFTTask`）：  
- `domain="macro"`, `name="tushare_macro_sf"`, `table_name="macro_sf_month"`（名称可按规范调整）。  
- `api_name="sf_month"`, `fields=["month","inc_month","inc_cumval","stk_endval"]`。  
- 设置 `single_batch=True`，`update_type="full"`，`default_concurrent_limit=1`，`default_page_size=2000`。  
- `primary_keys=["month"]`（可选增加 `month_end_date`），`date_column=None`。  
- 在 `process_data` 中生成 `month_end_date`（类似 CPI 任务），并确保所有数值列转换为 float。  
- `schema_def`：`month VARCHAR(10)`, `month_end_date DATE`, `inc_month NUMERIC(20,2)`, `inc_cumval NUMERIC(20,2)`, `stk_endval NUMERIC(20,4)`。  
- `validations`: `month` 非空、`inc_month`/`inc_cumval`/`stk_endval` 为非负、`stk_endval` 合理范围内。  
- `get_batch_list` 返回单个批次 `{"fields": "...all fields..."}`，不传任何日期参数。  
- 在 `macro/__init__.py` 导出任务以供调度器自动加载。

# 实施计划 (由 PLAN 模式生成)
1. 创建 `docs/tasks/tushare_macro_sf.md`，记录背景、分析、方案与实施计划。  
2. 新增 `alphahome/fetchers/tasks/macro/tushare_macro_sf.py`，实现任务类属性、schema、转换与验证，并配置全量单批模式。  
3. 实现在 `get_batch_list` 中返回单批次空参数，并在 `macro/__init__.py` 导出任务。

实施检查清单：
1. 文档创建并填充分析/方案/计划。  
2. 完成 `TushareMacroSFTTask` 代码实现。  
3. `get_batch_list` 返回单批次并在 `macro/__init__.py` 导出。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2025-11-18 12:10
    * 步骤：1. 文档创建并记录方案。
    * 修改：`docs/tasks/tushare_macro_sf.md`
    * 更改摘要：添加上下文、分析、方案与实施计划。
    * 原因：执行计划步骤 [1]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 12:18
    * 步骤：2. 完成 `TushareMacroSFTTask` 代码实现。
    * 修改：`alphahome/fetchers/tasks/macro/tushare_macro_sf.py`
    * 更改摘要：新增任务文件，定义全量单批逻辑、schema、转换与验证。
    * 原因：执行计划步骤 [2]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 12:19
    * 步骤：3. `get_batch_list` 单批实现并在 `macro/__init__.py` 导出任务。
    * 修改：`alphahome/fetchers/tasks/macro/tushare_macro_sf.py`, `alphahome/fetchers/tasks/macro/__init__.py`
    * 更改摘要：实现单批空参数逻辑并添加任务导出。
    * 原因：执行计划步骤 [3]
    * 阻碍：无
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）


