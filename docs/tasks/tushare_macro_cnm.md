# 上下文
文件名：tushare_macro_cnm.md  
创建于：2025-11-18  
创建者：GPT-5.1 Codex  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `tushare_macro_cnm.py`，封装 Tushare `cn_m` 接口，获取月度货币供应量数据（M0、M1、M2 及同比、环比）。由于总数据量远小于接口单次 5000 条上限，任务采用全量更新方式，每次执行不传过滤参数，直接拉取全部数据。接口文档参考：[tushare.pro/document/2?doc_id=242](https://tushare.pro/document/2?doc_id=242)。

# 项目概述
AlphaHome 的宏观任务统一继承 `TushareTask`，例如 `tushare_macro_cpi.py`、`tushare_macro_sf.py`，通过统一的字段定义、schema、转换与验证逻辑，将 Tushare 数据落入本地数据库。新任务需与现有宏观任务保持一致：使用月度字段 `month`，在处理阶段生成 `month_end_date` 便于时间查询，并在 `macro/__init__.py` 导出。

---

# 分析 (由 RESEARCH 模式填充)
`cn_m` 接口用于获取月度货币供应量数据，字段包括 `month`, `m0`, `m0_yoy`, `m0_mom`, `m1`, `m1_yoy`, `m1_mom`, `m2`, `m2_yoy`, `m2_mom`，支持 `m/start_m/end_m` 过滤，限制为单次最多 5000 条，一次即可提取全部数据。[tushare.pro/document/2?doc_id=242](https://tushare.pro/document/2?doc_id=242)  
宏观任务中，`tushare_macro_cpi.py` 已实现从 `month` 派生 `month_end_date` 并排序入库，`tushare_macro_sf.py` 则展示了全量单批模式（`single_batch=True`, `update_type="full"`，`get_batch_list` 返回单空批次）。考虑到 `cn_m` 数据量与 `sf_month` 类似，采用完全相同的全量更新策略最为简单可靠。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `TushareMacroCNMTask` 继承 `TushareTask`：  
- 核心属性：`domain="macro"`, `name="tushare_macro_cnm"`, `table_name="macro_cn_m"`, `primary_keys=["month"]`, `date_column=None`，`default_start_date="19900101"`（占位）。  
- 模式配置：`single_batch=True`, `update_type="full"`, `default_concurrent_limit=1`, `default_page_size=5000`。  
- Tushare 属性：`api_name="cn_m"`, `fields=["month","m0","m0_yoy","m0_mom","m1","m1_yoy","m1_mom","m2","m2_yoy","m2_mom"]`。  
- 转换：所有数值列（除 `month` 外）转为 `float`。  
- schema：`month VARCHAR(10) NOT NULL`, `month_end_date DATE NOT NULL`, 其余数值字段 `NUMERIC(20,2)` 或 `NUMERIC(20,4)`；添加 `month`、`month_end_date`、`update_time` 索引。  
- 验证：`month` 非空且格式为 `YYYYMM`；`m0/m1/m2` 与同比、环比字段允许为空但非负/在合理范围内（例如同比、环比在 ±100% 内）；`month_end_date` 必须生成成功。  
- 批处理：`get_batch_list` 仅返回一个字典 `{"fields": ",".join(self.fields or [])}`；`process_data` 中生成 `month_end_date` 并按其排序后返回。  
- 在 `alphahome/fetchers/tasks/macro/__init__.py` 导出 `TushareMacroCNMTask`。

# 实施计划 (由 PLAN 模式生成)
1. 创建 `docs/tasks/tushare_macro_cnm.md`，记录任务背景、分析、方案与实施计划。  
2. 新增 `alphahome/fetchers/tasks/macro/tushare_macro_cnm.py`，实现 `TushareMacroCNMTask` 的属性、schema、转换、验证与数据处理逻辑。  
3. 在 `get_batch_list` 中实现单批次全量抓取，并在 `macro/__init__.py` 中导出新任务。

实施检查清单：
1. 文档创建并填充分析/方案/计划。  
2. 完成 `TushareMacroCNMTask` 代码实现。  
3. 单批次逻辑与导出配置生效。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2025-11-18 12:30
    * 步骤：1. 文档创建并记录方案。
    * 修改：`docs/tasks/tushare_macro_cnm.md`
    * 更改摘要：添加 cn_m 任务的上下文、分析、方案与实施计划。
    * 原因：执行计划步骤 [1]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 12:38
    * 步骤：2. 完成 `TushareMacroCNMTask` 代码实现。
    * 修改：`alphahome/fetchers/tasks/macro/tushare_macro_cnm.py`
    * 更改摘要：新增任务文件，定义全量单批逻辑、schema、转换与验证。
    * 原因：执行计划步骤 [2]
    * 阻碍：无
    * 用户确认状态：待确认
* 2025-11-18 12:40
    * 步骤：3. 单批次逻辑与导出配置生效。
    * 修改：`alphahome/fetchers/tasks/macro/tushare_macro_cnm.py`, `alphahome/fetchers/tasks/macro/__init__.py`
    * 更改摘要：实现单批空参数逻辑并在宏观任务包中导出。
    * 原因：执行计划步骤 [3]
    * 阻碍：无
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）


