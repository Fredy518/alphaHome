# 上下文
文件名：akshare_macro_repo_rate.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `akshare_macro_repo_rate.py`，封装 akshare `repo_rate_query` 接口，获取银行间回购定盘利率 FR001/FR007/FR014。SPEC-015 目标为 DR007，作 `liquidity_metrics.dr007` 代理。

# 项目概述
继承 `AkShareNoDateSingleBatchTask`，声明式结构。

---

# 分析 (由 RESEARCH 模式填充)
`repo_rate_query(symbol="回购定盘利率")` 接口（akshare 1.18.64 实测可用）返回列 `date/FR001/FR007/FR014`，约 747 行，回溯至 2023-06-19。**DR007 经 akshare 源码 grep + 运行时确认无直接接口**（仅 FR007 定盘利率与 FDR007 银银间定盘利率）。FR007 为定盘利率（报价撮合），DR007 为成交加权利率，走势相关但口径不同，下游 evidence confidence 需降权。`symbol` 参数通过 `api_params` 固定传入。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `AkShareMacroRepoRateTask`：
- `domain="macro"`, `name="akshare_macro_repo_rate"`, `table_name="macro_repo_rate"`, `data_source="akshare"`。
- `api_name="repo_rate_query"`, `api_params={"symbol": "回购定盘利率"}`, `primary_keys=["date"]`, `date_column="date"`, `default_start_date="20230619"`。
- `column_mapping`：`date→date, FR001→fr001, FR007→fr007, FR014→fr014`。
- `schema_def`：`date DATE NOT NULL`, `fr001/fr007/fr014 NUMERIC(10,4)`，`fr007` 注释标注"DR007 代理"。
- `process_data`：规整日期、按 date 去重。
- `get_batch_list` 返回 `[{}]`（symbol 由 `prepare_params` 合并）。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/akshare_macro_repo_rate.py`。
2. 在 `tests/unit/test_akshare_macro_tasks.py` 增加属性与 process_data 测试。
3. 创建 `docs/tasks/akshare_macro_repo_rate.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 单元测试通过。
3. 文档创建，注明 FR007 作 DR007 代理的口径差异。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`akshare_macro_repo_rate.py`, `test_akshare_macro_tasks.py`, `akshare_macro_repo_rate.md`
    * 更改摘要：新增回购定盘利率采集任务，FR007 作 DR007 代理，单元测试全绿。
    * 原因：补齐 AlphaDB 国内流动性缺口。
    * 阻碍：DR007 无免费数据源，用 FR007 代理需降权。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
