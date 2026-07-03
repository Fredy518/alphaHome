# 上下文
文件名：akshare_macro_usa_cpi.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_usa_cpi.py`，封装 akshare `macro_usa_cpi_yoy` 接口，获取美国 CPI 同比数据（月频）。该数据用于 CrossLens SPEC-015 计算美实际利率 `us_real_yield_10y = 美债10Y名义 - 美国CPI同比`，支撑 P0 metric `us_real_yield_10y_change_6m`。

# 项目概述
AlphaHome 的 `macro` 域 akshare 抓取任务继承 `AkShareNoDateSingleBatchTask`（全历史、不接受 start/end 参数）。新任务需保持既有声明式结构：定义 `api_name/column_mapping/transformations/schema_def/validations`，并在 `process_data` 中做少量清洗。任务经 `@task_register()` 自动注册，由 `discover_tasks()` 自动发现，无需手动编辑 `__init__.py`。

---

# 分析 (由 RESEARCH 模式填充)
`macro_usa_cpi_yoy` 接口（akshare 1.18.64 实测可用）返回中文表头：`时间/发布日期/现值/前值`，共约 221 行，月频（`时间` 列固定为月初日，如 `2026-06-01`）。`发布日期` 为 BLS 实际公告日。最新未发布月份的 `现值` 为 NaN，需在 `process_data` 中丢弃。该接口仅提供同比（YoY），无环比（MoM），故目标表不含 `cpi_mom`。数据量小，单批全量获取即可。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroUsaCpiTask`：
- `domain="macro"`, `name="akshare_macro_usa_cpi"`, `table_name="macro_usa_cpi"`, `data_source="akshare"`。
- `api_name="macro_usa_cpi_yoy"`, `primary_keys=["date"]`, `date_column="date"`, `default_start_date="20080101"`。
- `column_mapping`：`时间→date, 发布日期→release_date, 现值→cpi_yoy, 前值→cpi_prev_yoy`。
- `transformations`：`cpi_yoy/cpi_prev_yoy → float`。
- `schema_def`：`date DATE NOT NULL`, `release_date DATE`, `cpi_yoy NUMERIC(10,4)`, `cpi_prev_yoy NUMERIC(10,4)`。
- `process_data`：规整日期、丢弃未发布月份（`cpi_yoy` 为 NaN）、按 date 去重。
- `get_batch_list` 返回 `[{}]`（接口不接受日期参数，生效窗口过滤由基类处理）。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/akshare_macro_usa_cpi.py`。  
2. 在 `tests/unit/test_akshare_macro_tasks.py` 增加属性与 process_data 测试。  
3. 创建 `docs/tasks/akshare_macro_usa_cpi.md`。

实施检查清单：
1. 任务代码实现并 `@task_register()` 注册成功。  
2. 单元测试通过。  
3. 文档创建。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_usa_cpi.py`, `test_akshare_macro_tasks.py`, `akshare_macro_usa_cpi.md`
    * 更改摘要：新增美国 CPI 同比采集任务，21 项宏观单元测试全绿。
    * 原因：补齐 AlphaDB 美国宏观缺口（SPEC-015 P0）。
    * 阻碍：无
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
