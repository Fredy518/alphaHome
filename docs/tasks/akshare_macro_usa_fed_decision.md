# 上下文
文件名：akshare_macro_usa_fed_decision.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_usa_fed_decision.py`，封装 akshare `macro_bank_usa_interest_rate` 接口，获取美联储利率决议（今值/预测值/前值）。与 `fred_macro_fed_rate`（FRED 有效利率+目标区间）口径互补：决议=政策动作时点，有效利率=市场实际成交。用于 CrossLens SPEC-015 `global_liquidity_metrics.fed_target_rate`。

# 项目概述
继承 `AkShareNoDateSingleBatchTask`，声明式结构。任务经 `@task_register()` 自动注册发现。

---

# 分析 (由 RESEARCH 模式填充)
`macro_bank_usa_interest_rate` 接口（akshare 1.18.64 实测可用，调研补充发现）返回 294 行，列 `商品/日期/今值/预测值/前值`，回溯 1982。`日期` 为 FOMC 决议日（事件频，非日频），`今值` 为决议利率，`预测值` 为市场预测，`前值` 为上期决议。最新未发布决议的 `今值` 为 NaN，需丢弃。该接口在初轮调研中漏掉，经用户验证补充。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroUsaFedDecisionTask`：
- `domain="macro"`, `name="akshare_macro_usa_fed_decision"`, `table_name="macro_fed_decision"`, `data_source="akshare"`。
- `api_name="macro_bank_usa_interest_rate"`, `primary_keys=["date"]`, `date_column="date"`, `default_start_date="19820928"`。
- `column_mapping`：`日期→date, 今值→rate, 预测值→rate_forecast, 前值→rate_prev`。
- `schema_def`：`date DATE NOT NULL`, `rate/rate_forecast/rate_prev NUMERIC(10,4)`。
- `process_data`：规整日期、丢弃未发布（rate 为 NaN）、按 date 去重。
- `get_batch_list` 返回 `[{}]`。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/akshare_macro_usa_fed_decision.py`。
2. 在 `tests/unit/test_akshare_macro_tasks.py` 增加属性与 process_data 测试。
3. 创建 `docs/tasks/akshare_macro_usa_fed_decision.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 单元测试通过。
3. 文档创建，注明与 FRED fed_rate 口径互补。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_usa_fed_decision.py`, `test_akshare_macro_tasks.py`, `akshare_macro_usa_fed_decision.md`
    * 更改摘要：新增美联储利率决议采集任务（决议口径，与 FRED 有效利率互补），单元测试全绿。
    * 原因：补齐美联储政策姿态的决议口径（用户验证发现的 akshare 接口）。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
