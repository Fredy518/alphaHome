# 上下文
文件名：fred_macro_us_short_rate.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_us_short_rate.py`，通过 FRED 三序列获取美元隔夜短期利率集合（IORB/OBFR/ON RRP）。与 SOFR（担保）/ fed_rate（联邦基金）共同构成美元短端利率全貌。其中 IORB 用于算 SOFR-IORB 利差，替代已停用的 TED 利差，反映实时融资压力。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。多序列合并任务，三序列起点不一（2013/2016/2021），外连接后早期日期部分列为 NULL（基类已对未返回数据的序列补 NaN 列保证 schema 一致）。

---

# 分析 (由 RESEARCH 模式填充)
三序列均经 fredgraph.csv 端点实测 keyless 可用（HTTP 200，实时更新至 2026-06）：
- `IORB`：准备金利率（2021-07 起，政策利率，IOER 的后继）。用于 SOFR-IORB 利差替代 TED。
- `OBFR`：隔夜银行融资利率（2016-03 起，无担保口径，与 SOFR 担保口径对比）。
- `RRPONTSYAWARD`：隔夜逆回购 ON RRP（2013-09 起，联储政策利率下限）。
未纳入：IOER（IORB 前身，已停用）、EFFR（与已建 macro_fed_rate.effective_rate/DFF 重复）。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroUsShortRateTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_us_short_rate"`, `table_name="macro_us_short_rate"`, `data_source="fred"`。
- `series_ids=["IORB","OBFR","RRPONTSYAWARD"]`, `column_mapping={...→iorb/obfr/on_rrp}`, `primary_keys=["date"]`, `default_start_date="20130923"`。
- `schema_def`：`date DATE NOT NULL`, `iorb/obfr/on_rrp NUMERIC(10,4)`，`iorb` 注释标注"算 SOFR-IORB 利差替代 TED"。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_us_short_rate.py`。
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加属性与 fetch_batch 测试（含起点不一的 NaN 处理）。
3. 创建 `docs/tasks/fred_macro_us_short_rate.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 单元测试通过。
3. 文档创建，注明 IORB 算 SOFR-IORB 利差替代 TED。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_us_short_rate.py`, `test_fred_macro_tasks.py`, `fred_macro_us_short_rate.md`
    * 更改摘要：新增美元隔夜短期利率集合采集任务（IORB/OBFR/ON RRP），单元测试全绿。
    * 原因：补齐美元短端利率全貌 + 提供 SOFR-IORB 利差替代 TED。
    * 阻碍：无（依赖 FRED 可达）。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
