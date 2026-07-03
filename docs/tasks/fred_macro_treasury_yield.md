# 上下文
文件名：fred_macro_treasury_yield.md  
创建于：2026-06-21  
创建者：ZCode  
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_treasury_yield.py`，通过 FRED 两序列获取美国 1M/3M 短端国债收益率。用于算 SOFR-国债利差（经典 TED 利差的现代替代：原 TED = 3M LIBOR - 3M 国债，LIBOR 停用后可用 SOFR-3M国债 反映融资压力）。配合 macro_sofr / macro_us_short_rate 使用。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。多序列合并任务。

---

# 分析 (由 RESEARCH 模式填充)
两序列均经 fredgraph.csv 端点实测 keyless 可用（HTTP 200，实时更新至 2026-06）：
- `DGS1MO`：1 个月美国国债收益率（2001-07 起）。
- `DGS3MO`：3 个月美国国债收益率（1981-09 起，回溯最长）。
AlphaDB 现有 `macro_bond_rate` 含中美国债 2y/5y/10y/30y，但无 1M/3M 短端，本表补短端。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroTreasuryYieldTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_treasury_yield"`, `table_name="macro_treasury_yield"`, `data_source="fred"`。
- `series_ids=["DGS1MO","DGS3MO"]`, `column_mapping={...→yield_1m/yield_3m}`, `primary_keys=["date"]`, `default_start_date="19810901"`。
- `schema_def`：`date DATE NOT NULL`, `yield_1m/yield_3m NUMERIC(10,4)`，`yield_3m` 注释标注"算 SOFR-国债利差替代 TED"。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_treasury_yield.py`。  
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加属性与 fetch_batch 测试。  
3. 创建 `docs/tasks/fred_macro_treasury_yield.md`。

实施检查清单：
1. 任务代码实现并注册成功。  
2. 单元测试通过。  
3. 文档创建，注明 SOFR-国债利差替代 TED 用途。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_treasury_yield.py`, `test_fred_macro_tasks.py`, `fred_macro_treasury_yield.md`
    * 更改摘要：新增美国短端国债收益率采集任务（1M/3M），单元测试全绿。
    * 原因：补短端国债 + 提供 SOFR-国债利差替代 TED。
    * 阻碍：无（依赖 FRED 可达）。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
