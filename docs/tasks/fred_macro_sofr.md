# 上下文
文件名：fred_macro_sofr.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_sofr.py`，通过 FRED `SOFR` 序列获取担保隔夜融资利率，用于 CrossLens SPEC-015 可选全球短期利率参考（MVP 非阻塞，补齐提升 global_score 覆盖）。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。

---

# 分析 (由 RESEARCH 模式填充)
FRED `fredgraph.csv?id=SOFR` 端点实测 keyless 可用（HTTP 200），日频，2018 至今。SOFR 为有担保隔夜融资利率，LIBOR 退出后成为美元短期利率基准。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroSofrTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_sofr"`, `table_name="macro_sofr"`, `data_source="fred"`。
- `series_ids=["SOFR"]`, `column_mapping={"SOFR": "sofr"}`, `primary_keys=["date"]`, `default_start_date="20180101"`。
- `schema_def`：`date DATE NOT NULL`, `sofr NUMERIC(10,4)`。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_sofr.py`。  
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加属性与 process_data 测试。  
3. 创建 `docs/tasks/fred_macro_sofr.md`。

实施检查清单：
1. 任务代码实现并注册成功。  
2. 单元测试通过。  
3. 文档创建。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_sofr.py`, `test_fred_macro_tasks.py`, `fred_macro_sofr.md`
    * 更改摘要：新增 SOFR 采集任务，单元测试全绿。
    * 原因：补齐 AlphaDB 全球短期利率参考。
    * 阻碍：无。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
