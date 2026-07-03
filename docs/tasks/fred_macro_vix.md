# 上下文
文件名：fred_macro_vix.md
创建于：2026-06-21
创建者：ZCode
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
新增 `fred_macro_vix.py`，通过 FRED `VIXCLS` 序列获取 CBOE VIX 波动率指数收盘价，用于 CrossLens SPEC-015 `market_regime_label` 分类辅助（risk_on/risk_off 信号）。

# 项目概述
复用 `FredTask` 基类（keyless FRED fredgraph.csv 端点）。

---

# 分析 (由 RESEARCH 模式填充)
FRED `fredgraph.csv?id=VIXCLS` 端点实测 keyless 可用（HTTP 200），日频，1990-01-02 至今。**SPEC-015 目标为 CBOE VIX（含 OHLC），但 CBOE 官方 VIX_History.csv 实测返回 403 AccessDenied（封锁程序化访问），yfinance ^VIX 实测被 429 限流**。故改用 FRED VIXCLS，仅含收盘价（无 OHLC）。下游 market_regime 仅需收盘价作风险偏好信号，口径可接受。

# 提议的解决方案 (由 INNOVATE 模式填充)
实现 `FredMacroVixTask(FredTask)`：
- `domain="macro"`, `name="fred_macro_vix"`, `table_name="macro_vix"`, `data_source="fred"`。
- `series_ids=["VIXCLS"]`, `column_mapping={"VIXCLS": "vix_close"}`, `primary_keys=["date"]`, `default_start_date="19900102"`。
- `schema_def`：`date DATE NOT NULL`, `vix_close NUMERIC(10,4)`（注释标注仅收盘价）。

# 实施计划 (由 PLAN 模式生成)
1. 新增 `alphahome/fetchers/tasks/macro/fred_macro_vix.py`。
2. 在 `tests/unit/test_fred_macro_tasks.py` 增加 process_data 测试（含日期解析失败丢弃、NaN 保留）。
3. 创建 `docs/tasks/fred_macro_vix.md`。

实施检查清单：
1. 任务代码实现并注册成功。
2. 单元测试通过。
3. 文档创建，注明仅收盘价口径。

# 当前执行步骤 (由 EXECUTE 模式维护)
> 正在执行: "无"

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
* 2026-06-21
    * 步骤：1-3. 任务实现、测试、文档。
    * 修改：`fred_macro_vix.py`, `test_fred_macro_tasks.py`, `fred_macro_vix.md`
    * 更改摘要：新增 VIX 收盘采集任务（FRED VIXCLS），单元测试全绿。
    * 原因：补齐 AlphaDB 全球风险偏好缺口。
    * 阻碍：CBOE/yfinance 不可得，仅收盘价。
    * 用户确认状态：待确认

# 最终审查 (由 REVIEW 模式填充)
（待补充）
