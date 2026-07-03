# 上下文
文件名：akshare_macro_lpr.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_lpr.py`，封装 akshare `macro_china_lpr` 接口，获取中国 LPR（1Y/5Y）及历史基准贷款利率。该数据用于 CrossLens SPEC-015 `macro_rate_metrics.policy_rate` → P0 metric `policy_rate_change_6m`。

# 项目概述
继承 `AkShareNoDateSingleBatchTask`，声明式结构。任务经 `@task_register()` 自动注册发现。

---

# 分析 (由 RESEARCH 模式填充)
`macro_china_lpr` 接口（akshare 1.18.64 实测可用）返回 1572 行，列 `TRADE_DATE/LPR1Y/LPR5Y/RATE_1/RATE_2`，日期为公告日（不定期/月频）。`LPR1Y/LPR5Y` 为贷款市场报价利率，2019-08 前为 NaN，对应历史基准贷款利率 `RATE_1/RATE_2`。**MLF（中期借贷便利）经 akshare 源码 grep + 运行时确认无对应接口**，目标表预留 `mlf_1y` 空列以避免后续接入数据源时的 schema 迁移。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroLprTask`：
- `domain="macro"`, `name="akshare_macro_lpr"`, `table_name="macro_policy_rate"`, `data_source="akshare"`。
- `api_name="macro_china_lpr"`, `primary_keys=["date"]`, `date_column="date"`, `default_start_date="19910421"`。
- `column_mapping`：`TRADE_DATE→date, LPR1Y→lpr_1y, LPR5Y→lpr_5y, RATE_1→benchmark_loan_1y, RATE_2→benchmark_loan_5y`。
- `schema_def`：含上述列 + 预留 `mlf_1y NUMERIC(10,4)`（恒 NULL，标注数据源暂不可得）。
- `process_data`：规整日期、补齐 `mlf_1y=pd.NA`、按 date 去重。
- `get_batch_list` 返回 `[{}]`。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/akshare_macro_lpr.py`。  
2. 在 `tests/unit/test_akshare_macro_tasks.py` 增加属性与 process_data（含 MLF 预留列、2019 前 NaN 填充）测试。  
3. 创建 `docs/tasks/akshare_macro_lpr.md`。

实施检查清单：
1. 任务代码实现并注册成功。  
2. 单元测试通过。  
3. 文档创建，注明 MLF 暂缓。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_lpr.py`, `test_akshare_macro_tasks.py`, `akshare_macro_lpr.md`
    * 更改摘要：新增 LPR/基准贷款利率采集任务，预留 MLF 空列，单元测试全绿。
    * 原因：补齐 AlphaDB 国内政策利率缺口（SPEC-015 P0）。
    * 阻碍：MLF 无免费数据源，暂缓。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
