## Barra 归因模块（AlphaDB 落库）设计文档式计划

目标：新增一个可生产使用的 Barra 风格归因模块，输出并落库四类核心对象：

1) 因子暴露矩阵（按日截面，个股 × 因子）
2) 因子收益序列（按日，每因子一个收益）
3) 特质收益/残差（按日，每股票一个 residual / specific return）
4) （可选）风险预测：因子协方差 $F_t$ 与特质方差 $D_t$

---

## 🚀 实施进度 (Implementation Progress)

| 模块 | 状态 | 说明 |
|------|------|------|
| **Schema 初始化** | ✅ 已完成 | `barra` schema、31 个申万行业维表、PIT 视图、7 张表 |
| **barra_exposures_daily** | ✅ 已完成 | 行业 one-hot + 落库；已实现 style：Size/Value(BP)/Liquidity 的 winsorize+市值加权 zscore；Beta/Momentum/ResVol 目前为占位列（NULL） |
| **barra_factor_returns_daily** | ✅ 已完成 | WLS(√市值) + sum-to-zero 重参数化 + 回归诊断(R²/RMSE) + 落库 |
| **barra_specific_returns_daily** | ✅ 已完成 | 残差侧输出至 `specific_returns_daily` 表 |
| **barra_portfolio_attribution_daily** | ✅ 已完成 | 单期归因 Δx'f + a'u 框架就绪 |
| **批量日期运行** | ✅ 已完成 | `scripts/run_barra_batch.py` 支持并行与日期范围 |
| **多期归因链接** | ✅ 已完成 | Carino/Menchero linking 算法 (`alphahome/barra/linking.py`) |
| **风险模型** | ✅ 已完成 | 因子协方差 + 特质方差估计 (`alphahome/barra/risk_model.py`) |

**批量验证**：2025-11-20 ~ 2025-12-31（30 个交易日）全部成功
- `exposures_daily`: 每日约 5,440+ 行
- `factor_returns_daily`: R² 范围 0.17 ~ 0.32
- `specific_returns_daily`: 每日约 5,440+ 行
- 风险模型：37 因子协方差 + 5,453 股票特质方差

---

实现约束（已确认）：

- 行业约束：用“数学变换法”实现 sum-to-zero（代码实现由你补齐，这里固定为设计约束）
- 回归：WLS，权重 $w_i=\sqrt{\text{mcap}_i}$
- 清洗：对回归输入做 Winsorization（至少 returns 与 style exposures）
- 存储：PostgreSQL 宽表，按 `trade_date` 分区

---

## 一、逻辑提炼与口径清单

### 1.1 端到端流程（按日）

1) 取交易日 $t$ 与前一持仓日 $t-1$
2) 构建当日回归样本 universe（可交易/合规过滤）
3) 计算/读取 $X_{t-1}$（因子暴露：style + industry + intercept/国家项等）
4) 计算/读取 $r_t$（个股收益，口径需与交易可实现性一致）
5) 对回归输入做清洗（winsorize、缺失处理、标准化）
6) WLS 回归：$r_t = X_{t-1} f_t + u_t$
7) 产出并落库：$f_t$（因子收益）、$u_t$（特质收益/残差）、诊断信息
8) 若需要归因：读取组合/基准权重，计算 $\Delta x_{t-1}^\top f_t + a_{t-1}^\top u_t$

### 1.2 关键口径（必须在配置里固化）

| 编号 | 口径项 | 默认建议 | 说明 |
|---|---|---|---|
| A1 | Universe | 全A/指定股票池（含退市历史） | 需可回放历史，避免幸存者偏差 |
| A2 | Benchmark | HS300/ZZ500/自定义 | 必须支持历史成分与权重 |
| A3 | 收益口径 | close-to-close（后复权） | 与数据源/交易一致 |
| A4 | 可交易过滤 | 停牌剔除、上市未满N日剔除 | 避免不可交易收益干扰 |
| A5 | 市值口径 | free-float mcap | 与 Barra 思路一致 |
| A6 | 行业体系 | 中信/申万（一级优先） | 需 PIT 生效日期 |
| A7 | 回归权重 | $w_i=\sqrt{mcap_i}$ | 已确认 |
| A8 | 输入清洗 | winsorize + zscore | 已确认 winsorize |
| A9 | 行业约束 | sum-to-zero（数学变换） | 已确认 |

---

## 二、AlphaDB 数据层接口与字段要求

### 2.1 输入数据（基于当前 alphadb 现状）

通过 `mcp_postgres_query` 已确认：当前 alphadb 存在 `rawdata` / `tushare` / `pgs_factors` 三个 schema，但**尚未创建** `materialized_views` schema（`pg_matviews` 中也没有 `materialized_views.*`）。

因此本 Barra 模块 MVP 的数据入口先以 `rawdata.*` 为主（可直接 join），避免依赖 `pgs_factors`，并把需要的 PIT 视图放在 `barra` schema 内自管理；后续若要纳入统一 MV 管理，再启用 [scripts/initialize_materialized_views.py](scripts/initialize_materialized_views.py)。

已确认可用的关键输入表（含主要字段）：

- 行情收益：`rawdata.stock_daily`
	- `ts_code` (varchar), `trade_date` (date), `close` (numeric), `pct_chg` (numeric), `amount` (numeric)
	- 覆盖：1991-01-02 至 2025-12-29（按库内 min/max）
- 市值/换手/估值（用于 WLS 权重、Size/Liquidity/Value 等）：`rawdata.stock_dailybasic`
	- `ts_code`, `trade_date`, `turnover_rate`, `turnover_rate_f`, `pb`, `pe_ttm`, `total_mv`, `circ_mv`, `free_share`
	- 覆盖：1991-01-02 至 2025-12-29（按库内 min/max）
- 复权因子：`rawdata.stock_adjfactor`
	- `ts_code`, `trade_date`, `adj_factor`
- 申万行业成分（带进出）：`rawdata.index_swmember`（也存在 `tushare.index_swmember`，字段一致）
	- `ts_code`, `in_date`, `out_date`, `l1_code/l1_name`, `l2_code/l2_name`, `l3_code/l3_name`
- ST 标记：`rawdata.stock_st`
	- `ts_code`, `trade_date`, `type/type_name`
- 涨跌停（可选，做样本过滤/稳健性）：`rawdata.stock_limitlist`、`rawdata.stock_limitprice`

行业体系已确认使用申万（SW），并且尽量不依赖 `pgs_factors` schema。

建议新增一个基于 `rawdata.index_swmember` 的 PIT 视图（或物化视图），放在 `barra` schema（避免引入 `materialized_views` 依赖）：

- `barra.pit_sw_industry_member_mv`
- 字段：`ts_code, query_start_date, query_end_date, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name`
- 规则：`query_start_date = in_date`；`query_end_date = COALESCE(out_date, '2099-12-31')`
	- 口径确认：`out_date` 是最后有效日，因此 **不做** `- 1 day`。

最低输入集（MVP）：

- 交易日历：`trade_date`
- 行情：收盘价/复权因子/收益、成交额/成交量、停牌标记
- 市值：free-float shares & free-float mcap
- 行业：industry_code（带生效区间，PIT 展开）

（可选）风格因子需要的基本面：净资产、净利润、营收等（必须 PIT 对齐：公告/披露日）。

### 2.2 需要确认/补充的字段清单（用于 MVP 风格因子）

| 因子 | 最低字段 | PIT 要求 |
|---|---|---|
| Size | `rawdata.stock_dailybasic.circ_mv`（近似 free-float mcap） | 日频，$t-1$ 可得 |
| Beta | 个股/指数收益历史 | 停牌处理一致 |
| Momentum | 历史收益序列 | 12-1 窗口 |
| Value(BP) | book_equity / mcap | book 必须 PIT |
| Liquidity | `turnover_rate(_f)` / `amount` | 日频 |
| Residual Vol | 历史 residual 或收益波动 | 需回溯窗口 |

---

## 三、数据模型与落库设计（PostgreSQL，宽表 + trade_date 分区）

建议新建 schema：`barra`（与现有 schema 隔离，便于权限与迁移管理）。

### 3.1 表：`barra.exposures_daily`（宽表，分区）

用途：存 $X_{t-1}$ 的最终入模暴露（供回归与归因复用）。

建议字段（示例，按需扩展）：

- `trade_date` (date, partition key)
- `ticker` (text)
- `universe_flag` (bool) / `eligible_flag` (bool)
- `ff_mcap` (numeric)  # MVP 口径：取 `rawdata.stock_dailybasic.circ_mv`
- `weight_wls` (numeric)  # 预先存 $\sqrt{ff\_mcap}$ 也行

style 暴露列（宽表列，示例）：

- `style_size`
- `style_beta`
- `style_mom_12m1m`
- `style_value_bp`
- `style_liquidity`
- `style_resvol`

industry 暴露列（宽表列，固定行业集合；列名建议 `ind_{industry_code}`）：

- `ind_01` ... `ind_N`（one-hot，或按行业集合命名）

主键/唯一：(`trade_date`, `ticker`)

索引建议：

- 分区表本身按 `trade_date` 分区
- 分区内索引：(`ticker`), (`eligible_flag`)（可选）

### 3.2 表：`barra.factor_returns_daily`（宽表，分区）

用途：存每日因子收益 $f_t$ 与回归诊断。

字段：

- `trade_date` (date, partition key)
- `n_obs` (int)
- `r2` (numeric)
- `r2_adj` (numeric, optional)
- `rmse` (numeric, optional)

因子收益列（宽表列，必须与 exposures 列对齐）：

- `fr_style_size` ...
- `fr_ind_01` ... `fr_ind_N`

说明：行业收益列是 sum-to-zero 约束下的“全量行业收益”（不是去一列的参数），需要从你实现的数学变换里“回推出完整 K 维行业收益”。

### 3.3 表：`barra.specific_returns_daily`（按股票行表，分区）

用途：存残差/特质收益 $u_{i,t}$，用于归因的 $a^\top u$ 与后续特质风险。

字段（建议）：

- `trade_date` (date, partition key)
- `ticker` (text)
- `specific_return` (numeric)  # 即 residual
- `fitted_return` (numeric, optional)
- `raw_return` (numeric, optional)
- `weight_wls` (numeric)

主键：(`trade_date`, `ticker`)

### 3.4 表：`barra.portfolio_attribution_daily`（宽表，分区）

用途：存单期（按日）归因结果，便于报表与回测对齐。

字段（示例）：

- `trade_date` (date, partition key)
- `portfolio_id` (text)
- `benchmark_id` (text)
- `active_return` (numeric)
- `specific_contrib` (numeric)  # $a^\top u$

因子贡献列（宽表列）：

- `contrib_style_size` ...
- `contrib_ind_01` ... `contrib_ind_N`

主键：(`trade_date`, `portfolio_id`, `benchmark_id`)

### 3.5 宽表的可演进性（回答：新增因子是否要“重写表”）

结论：通常不需要“重写整张表”。

- PostgreSQL 中给宽表 `ALTER TABLE ... ADD COLUMN`（不带默认值）通常是元数据操作，不会重写/拷贝历史数据。
- 对分区表：对父表 `ADD COLUMN` 会向下传播到所有分区（新列在历史分区里默认为 NULL）。
- 新增因子时的工程动作通常是：
	- `exposures_daily` 新增 `style_*` 列
	- `factor_returns_daily` 新增对应 `fr_*` 列
	- `portfolio_attribution_daily` 新增对应 `contrib_*` 列
	- 需要历史值时再跑回填任务（可按日期分区逐段回填）

如果你希望“列集合完全不变”（避免频繁 DDL），可选折中：

- 版本化宽表：`barra.exposures_daily_v1/v2/...`，并用一个视图 `barra.exposures_daily` 指向最新版本；回归与归因任务固定读视图。

---

## 四、核心算法设计

### 4.1 输入清洗（Winsorization + 标准化）

按日截面处理：

- returns winsorize：对 $r_t$ 在截面上按分位数截断（例如 1%/99%）
- exposures winsorize：对 style exposures 在截面上按分位数截断（行业 one-hot 不处理）
- 缺失处理：style 暴露可用行业中位数填补；无法填补则剔除该股票当日回归样本
- 标准化：style 因子做 z-score（可用市值加权均值 + 截面标准差）

### 4.2 WLS 回归（权重=根号市值）

回归目标：

$$
r_t = X_{t-1} f_t + u_t
$$

权重矩阵：$W = \mathrm{diag}(w_i)$，其中 $w_i=\sqrt{ff\_mcap_i}$。

估计：

$$
\hat f_t = (X^\top W X)^{-1} X^\top W r
$$

### 4.3 行业 sum-to-zero 约束（数学变换法）

约束目标（行业因子）：

$$
\sum_{j=1}^{J} f_{ind,j,t} = 0
$$

实现思路（约束重参数化）：

- 令 $f_{ind} = C g$，其中 $g\in\mathbb{R}^{J-1}$，$C\in\mathbb{R}^{J\times(J-1)}$ 满足 $\mathbf{1}^\top C=0$。
- 一个常用的 $C$：

$$
C = \begin{bmatrix}
I_{J-1}\\
-\mathbf{1}^\top
\end{bmatrix}
$$

- 将原设计矩阵中的行业 one-hot 部分 $X_{ind}$ 替换为 $X_{ind} C$，对 $g$ 做无约束回归。
- 回归后再恢复 $f_{ind}=C\hat g$，从而拿到“全量行业收益”（可直接落宽表 `fr_ind_*` 列）。

注：如果需要“市值加权 sum-to-zero”版本，约束可改为 $\sum_j \pi_j f_{ind,j}=0$，对应的 $C$ 需按权重构造（本计划先固定为简单 sum-to-zero）。

---

## 五、任务拆分与工程落点（与现有任务系统对齐）

### 5.1 代码包结构建议

- `alphahome/barra/`
	- `__init__.py`
	- `config.py`（因子列表、winsorize 参数、窗口等）
	- `exposures.py`（暴露计算与清洗）
	- `regression.py`（WLS + sum-to-zero 变换）
	- `attribution.py`（组合/基准归因计算）
	- `schemas.py`（表结构/字段清单常量）

### 5.2 任务（可独立单测、可增量跑）

1) `barra_exposures_daily`：

- 输入：PIT 行业/基本面 + 行情/市值
- 输出：写 `barra.exposures_daily`（按 `trade_date` 分区，幂等 upsert）
- 产物：每 `trade_date` 全量截面（或 eligible 截面）

2) `barra_factor_returns_daily`：

- 输入：`barra.exposures_daily`（取 $t-1$） + 个股收益 $r_t$
- 过程：winsorize + WLS(√市值) + sum-to-zero 数学变换
- 输出：写 `barra.factor_returns_daily`、`barra.specific_returns_daily`

3) `barra_portfolio_attribution_daily`：

- 输入：组合权重/基准权重（$t-1$）+ `barra.exposures_daily`（$t-1$）+ `barra.factor_returns_daily`（$t$）+ `barra.specific_returns_daily`（$t$）
- 输出：写 `barra.portfolio_attribution_daily`

### 5.3 增量更新策略

- 以 `trade_date` 为最小重跑粒度
- exposures：可按日期区间回填（例如发现 PIT 数据修订）
- factor_returns：依赖 $t-1$ exposures，回填窗口至少要覆盖 $[start-1, end]$

---

## 六、验收标准（QA / Monitoring）

### 6.1 回归质量检查（每日）✅ 已实现

- `n_obs` 不低于阈值（如 > 500）→ 实测每日 5,440+ 观测
- `r2` 在合理区间并可监控漂移 → 实测 R² 范围 0.17 ~ 0.32
- 行业因子收益满足 sum-to-zero（数值误差在容忍范围内）→ 已通过数学变换法实现
- 残差分布：极端值占比、均值接近 0 → RMSE 约 1.7% ~ 2.0%

### 6.2 归因一致性检查 ✅ 框架就绪

- 单期：

$$
		\mathrm{active\_return}_t \approx \sum_k \mathrm{contrib}_{k,t} + \mathrm{specific\_contrib}_t
$$

- 误差监控：`recon_error` 的绝对值/分布

---

## 七、已完成落地清单 ✅

| 序号 | 任务 | 状态 | 实现位置 |
|------|------|------|----------|
| 1 | 行业体系确认（申万一级 31 个） | ✅ | `barra.industry_l1_dim` |
| 2 | 收益口径（close-to-close 后复权） | ✅ | `rawdata.stock_daily.pct_chg` |
| 3 | MVP 风格因子（6 列） | ✅ | 已实现：size/value_bp/liquidity；占位列：beta/mom/resvol |
| 4 | Schema 与 7 张表创建 | ✅ | `scripts/initialize_barra_schema.py` |
| 5 | 三条核心任务实现 | ✅ | `processors/tasks/barra/` |
| 6 | 批量运行脚本 | ✅ | `scripts/run_barra_batch.py` |
| 7 | 多期归因链接 | ✅ | `alphahome/barra/linking.py` |
| 8 | 风险模型估计 | ✅ | `alphahome/barra/risk_model.py` |

---

## 八、代码架构总览

### 8.1 核心模块

```
alphahome/barra/
├── __init__.py          # 导出所有公共 API
├── constants.py         # BARRA_SCHEMA, STYLE_FACTOR_COLUMNS
├── ddl.py               # 表结构 DDL 生成
├── linking.py           # 多期归因链接 (Carino/Menchero/Simple)
└── risk_model.py        # 风险模型估计 (因子协方差 + 特质方差)
```

### 8.2 任务模块

```
alphahome/processors/tasks/barra/
├── __init__.py
├── barra_exposures_daily.py         # 因子暴露计算
├── barra_factor_returns_daily.py    # WLS 回归 + 因子收益
├── barra_portfolio_attribution_daily.py  # 单期归因
├── barra_multi_period_attribution.py     # 多期链接归因
└── barra_risk_model_daily.py        # 风险模型估计任务
```

### 8.3 脚本

```
scripts/
├── initialize_barra_schema.py   # 初始化 schema 和表
├── run_barra_batch.py           # 批量运行 (支持 --parallel, --last-n)
├── debug_run_barra_day.py       # 单日调试脚本
├── test_linking.py              # 多期链接测试
└── test_risk_model.py           # 风险模型测试
```

### 8.4 数据库表

| 表名 | 主键 | 用途 |
|------|------|------|
| `barra.industry_l1_dim` | `l1_code` | 申万行业维表 |
| `barra.exposures_daily` | `(trade_date, ticker)` | 因子暴露矩阵 |
| `barra.factor_returns_daily` | `(trade_date)` | 因子收益 + 回归诊断 |
| `barra.specific_returns_daily` | `(trade_date, ticker)` | 特质收益/残差 |
| `barra.portfolio_attribution_daily` | `(trade_date, portfolio_id, benchmark_id)` | 单期归因 |
| `barra.multi_period_attribution` | `(start_date, end_date, portfolio_id, benchmark_id)` | 多期链接归因 |
| `barra.factor_covariance` | `(as_of_date, factor1, factor2)` | 因子协方差矩阵 |
| `barra.specific_variance_daily` | `(as_of_date, ticker)` | 股票特质方差 |

---

## 九、使用示例

### 9.1 批量运行

```bash
# 运行最近 30 个交易日
python scripts/run_barra_batch.py --last-n 30

# 指定日期范围 + 并行
python scripts/run_barra_batch.py 2025-01-01 2025-12-31 --parallel 4

# 单日调试
python scripts/debug_run_barra_day.py 2025-12-31
```

### 9.2 多期归因

```python
from alphahome.barra import MultiPeriodLinker

linker = MultiPeriodLinker(method="carino")
for date, ret, contribs in daily_attributions:
    linker.add_period(return_=ret, contributions=contribs)
result = linker.get_linked()
print(f"Total return: {result['total_return']:.2%}")
```

### 9.3 风险模型

```python
from alphahome.barra import RiskModel, RiskModelConfig

config = RiskModelConfig(cov_window=252, half_life=126)
model = RiskModel(config)
model.fit(factor_returns_df, specific_returns_df)

risk = model.compute_risk(portfolio_weights, exposures)
print(f"Portfolio volatility: {risk['total_vol']:.2%}")
print(f"Factor risk: {risk['factor_var_pct']:.1%}, Specific risk: {risk['specific_var_pct']:.1%}")
```

---

## 十、Post-MVP 规划

### 10.1 Phase 2A：生产化加固（优先级：高）

| 任务 | 说明 | 验收标准 | 状态 |
|------|------|----------|------|
| 调度自动化 | 在 `PROD_SCRIPTS` 注册 `barra-daily`，由外部调度器调用 `ah prod run barra-daily` | 可通过 `ah prod list` 查看；单命令完成全流程 | ✅ 已完成 |
| DAG 顺序封装 | 将 exposures → factor_returns → attribution → risk_model 封装为单入口 | 支持 `--step` 选择性执行；失败可重试不产生脏数据 | ⏳ 待开始 |
| 并发安全 | 按 `trade_date` 加 advisory lock，防止多实例重复计算同一天 | 并发运行同日期时第二个实例等待或跳过 | ⏳ 待开始 |
| 数据质量监控 | 必备检查 + 动态阈值告警 | 见下方详细验收项 | ⏳ 待开始 |
| 回填工具 | 支持日期范围幂等回填，明确上游修订触发条件 | 见下方详细验收项 | ⏳ 待开始 |

#### 10.1.1 调度自动化实现方式

**最小实现**（推荐先落地）：
```python
# 在 alphahome/cli/commands/prod.py 的 PROD_SCRIPTS 字典中添加：
'barra-daily': (
	'scripts/production/barra/barra_daily.py',
	'Barra 每日流水线（默认 --last-n 1，可传 --parallel/--no-lag 等参数）'
),
```

说明：`scripts/run_barra_batch.py` 需要显式传入日期区间或 `--last-n`，因此这里使用薄包装脚本来提供“无参即跑”的生产入口。

**稍正规**（后续优化）：
- 在 `PROD_MODULES` 注册包内模块入口
- 封装 DAG 顺序：exposures → factor_returns → attribution → risk_model
- 避免脚本参数不一致

#### 10.1.2 数据质量监控验收项

| 检查项 | 阈值策略 | 必备/可选 |
|--------|----------|----------|
| 回归样本数 `n_obs` | ≥ 500（硬阈值） | 必备 |
| 样本覆盖率 | ≥ 95% 可交易股票 | 必备 |
| R² 合理性 | rolling 20日 median ± 3×MAD | 必备 |
| sum-to-zero 数值误差 | 行业因子收益和 < 1e-10 | 必备 |
| 缺失暴露比例 | 单因子缺失 < 5% | 必备 |
| 行业覆盖率 | 31 个行业均有样本 | 必备 |
| 归因 recon_error | \|active - Σcontrib\| < 1e-6 | 必备 |
| 单因子收益极值 | \|fr\| > rolling 99%分位 时标记 | 可选 |

#### 10.1.3 回填工具验收项

| 项目 | 说明 |
|------|------|
| 触发条件 | 复权因子修订、行业 PIT 变更、dailybasic 修订 |
| 重算粒度 | 按 `trade_date` 分区，幂等 upsert |
| 窗口依赖 | **强约束**：回填 `[start, end]` 必须先计算 `start-1` 的 exposures |
| 日志追踪 | 记录回填原因、影响日期范围、耗时 |

### 10.2 Phase 2B：因子升级与扩展（优先级：高）

#### 10.2.0 设计原则：从 MVP 走向"更像 Barra"

当前 MVP 因子实现较为简化（单一指标 + 简单清洗），与学术/商用 Barra CNE5 存在差距。本阶段目标是**让每个因子都采用多维度指标组合 + 行业调整 + 稳健处理**，逼近真实 Barra 风格。

**数据源确认**（通过 `mcp_postgres_query` 探索 alphadb）：

| 数据表 | 主要用途 | 覆盖情况 |
|--------|----------|----------|
| `rawdata.fina_indicator` | 财务比率、增长指标 | 348k 行，6904 只股票，关键指标非空率 75-97% |
| `rawdata.fina_income` | 利润表原始项 | 340k 行，1991-2025 |
| `rawdata.fina_balancesheet` | 资产负债表原始项 | 333k 行 |
| `rawdata.fina_cashflow` | 现金流量表原始项 | 298k 行 |
| `rawdata.stock_dailybasic` | 日频估值/市值/流动性 | 1770 万行，1991-2025 |
| `rawdata.stock_daily` | 日频行情 | 1755 万行 |
| `rawdata.index_swmember` | 申万行业成分 PIT | 6034 只股票，31 个一级行业 |
| `rawdata.index_dailybasic` | 宽基指数日频（HS300/ZZ500/上证综指） | 2004-2025 完整覆盖 |
| `rawdata.index_swdaily` | 申万行业指数日频 | 2012-2025，可用于行业调整 Beta |

#### 10.2.1 现有因子升级路线（MVP → Full Barra）

| 因子 | MVP 版本 | Full Barra 版本 | 数据来源 | 难度 |
|------|----------|-----------------|----------|------|
| **Size** | `log(circ_mv)` | 对数市值 + **行业中性化** | `stock_dailybasic.circ_mv` | 🟢 低 |
| **Beta** | 简单 252 日 OLS（未实现） | EWMA 加权 + 行业调整市场 Beta + Bayesian shrinkage 向 1.0 收缩 | `stock_daily` + `index_dailybasic`(000300.SH) | 🟡 中 |
| **Momentum** | 12-1 累计收益（未实现） | 多窗口加权（252-21d, 126-21d）+ 短期反转调整 + 行业中性化 | `stock_daily.pct_chg` | 🟡 中 |
| **Value** | `1/pb` | **多指标组合**：E/P + B/P + S/P + CF/P + DY，等权或 PCA | 以 `stock_dailybasic.pe_ttm/pb` 为主；`ps_ttm/dv_ttm` 需先验证存在性与覆盖率（缺失则改走 PIT 财务/分红数据计算） | 🟡 中 |
| **Liquidity** | `turnover_rate_f` | 多窗口换手率（21d/63d/252d）+ 成交额/市值比 + Amihud 非流动性 | `stock_dailybasic.turnover_rate_f/amount/circ_mv` | 🟡 中 |
| **ResVol** | 历史 60 日波动（未实现） | EWMA 加权残差波动 + 行业调整 + regime 归一化 | 回归残差序列 | 🟡 中 |

#### 10.2.2 新增因子详细定义

| 因子 | Barra 标准定义 | 子指标 | 数据字段 | 非空率 | 难度 |
|------|----------------|--------|----------|--------|------|
| **Non-Linear Size** | $\text{resid}(\text{Size}^3 \sim \text{Size})$ | Size 立方对 Size 的正交残差 | 派生自 Size | 100% | 🟢 低 |
| **Dividend Yield** | 过去 12 个月现金股息 / 市值 | `dv_ttm`（如存在）或按分红/财务数据重建 | `stock_dailybasic.dv_ttm`（待验证） | 待验证 | 🟢 低 |
| **Leverage** | 多维杠杆组合 | 市场杠杆 `(D+E)/E`、账面杠杆 `D/E`、负债/资产比 | `fina_indicator.debt_to_assets` (96.4%)、`fina_balancesheet` | 95%+ | 🟡 中 |
| **Growth** | 多维增长组合 | (1) 净利润 YoY (92.8%)、(2) 营收 YoY (92.4%)、(3) 现金流 YoY (88.8%) | `fina_indicator.netprofit_yoy/or_yoy/ocf_yoy` | 88-93% | 🔴 高 |
| **Earnings Quality** | 应计质量 + 现金转换 | 经营现金流/营业收入 (74.9%)、应计比率 | `fina_indicator.ocf_to_opincome`、`fina_cashflow/fina_income` | 75%+ | 🔴 高 |
| **Earnings Variability** | 盈利稳定性 | 净利润变异系数（5 年）、ROE 标准差 | `fina_indicator` 历史序列 | 需验证 | 🔴 高 |

#### 10.2.3 因子计算统一框架

每个因子的计算应遵循以下标准化流程：

```
1. 原始指标提取（PIT 对齐，使用 f_ann_date 或 ann_date）
2. 子指标标准化（截面 winsorize 1%/99% + 市值加权 z-score）
3. 多指标合成（等权平均 / IC 加权 / PCA）
4. 行业中性化（可选：对行业 dummy 回归取残差）
5. 最终标准化（市值加权 z-score，均值=0，标准差≈1）
6. 缺失值处理（用行业中位数填充 / 标记为 NULL 剔除回归）
```

#### 10.2.4 因子升级优先级排序

| 优先级 | 因子 | 理由 | 状态 |
|--------|------|------|------|
| 1 | **Size 行业中性化** | 已有基础，改动最小 | ⏳ 待开始 |
| 2 | **Value 多指标组合** | 以 dailybasic 为主；`ps_ttm/dv_ttm` 等字段存在性与覆盖率需先验证 | ⏳ 待开始 |
| 3 | **Liquidity 多窗口** | 数据完备，计算简单 | ⏳ 待开始 |
| 4 | **Beta EWMA + 行业调整** | 需要宽基指数，已确认 index_dailybasic 可用 | ⏳ 待开始 |
| 5 | **Momentum 多窗口** | 需要历史回溯，已有数据 | ⏳ 待开始 |
| 6 | **Non-Linear Size** | 派生自 Size，一行公式 | ⏳ 待开始 |
| 7 | **Dividend Yield** | 直接取 `dv_ttm`，简单 | ⏳ 待开始 |
| 8 | **Growth 多维组合** | 需 PIT 财务，覆盖率 88-93% | ⏳ 待开始 |
| 9 | **Leverage 多维组合** | 需 PIT 财务，覆盖率 95%+ | ⏳ 待开始 |
| 10 | **Earnings Quality** | 需 PIT 现金流/利润表，覆盖率 75% | ⏳ 待开始 |
| 11 | **ResVol 行业调整** | 依赖回归残差序列 | ⏳ 待开始 |

#### 10.2.5 因子扩展验收标准（每新增/升级一个因子）

| 验收项 | 标准 | 说明 |
|--------|------|------|
| 定义文档 | ✓ | 明确计算公式、子指标列表、合成方法 |
| 数据覆盖率 | ≥ 90% | 非空比例（或明确缺失处理策略） |
| 清洗口径 | ✓ | winsorize 分位数、zscore 加权方式 |
| 分布检查 | 均值≈0，std≈1 | 截面分布正态性、偏度/峰度合理 |
| 相关性矩阵 | VIF < 10 | 与现有因子相关系数，确认多重共线性可控 |
| 回归贡献 | 有信息增量 | 加入后 R² 变化 ≥ 0.5%，或单因子 IC 显著 |
| 单元测试 | ✓ | 边界条件、缺失值、极端值处理 |
| 回测验证 | ✓ | 2010-2024 长周期 IC/IR 统计 |

### 10.3 Phase 2C：风险模型增强（优先级：中）

#### 10.3.1 已实现（需参数校准与监控）

| 功能 | 实现位置 | 当前配置 | 后续工作 |
|------|----------|----------|----------|
| Newey-West 调整 | `risk_model.py:55, 92-120` | `newey_west_lags=2` | 最优滞后阶数校准（1~5）+ 回测验证 |
| 指数衰减加权 | `risk_model.py:68-89` | `half_life=126` | 最优半衰期校准（63/126/252）+ 回测验证 |
| PSD 修正 | `risk_model.py` 特征值截断 | 截断负特征值到 0 | 监控修正幅度、占比 |
| 特质方差收缩 | `risk_model.py:60` | `specific_var_shrinkage=0.2` | 最优收缩强度校准（0.1~0.5） |
| 特质方差下限 | `risk_model.py:63` | `specific_var_floor=1e-6` | 确认合理性 |

#### 10.3.2 待实现

| 增强项 | 说明 | 与现有差异 | 评价指标 | 状态 |
|--------|------|------------|----------|------|
| Eigenfactor 调整 | Menchero 特征值偏差修正 | 当前是简单截断，非 eigen-adjust | Bias Stat 分布 | ⏳ 待开始 |
| Volatility Regime | 根据近期实现波动调整预测 | 需要额外的 regime 检测逻辑 | 预测/实现比稳定性 | ⏳ 待开始 |
| 特质方差结构化 | 用市值/行业/波动解释截面 | 当前是全局收缩，非结构化 | 截面 R² | ⏳ 待开始 |

### 10.4 Phase 3：应用层（优先级：低）

| 模块 | 说明 | 依赖 | 状态 |
|------|------|------|------|
| 组合优化器 | 均值-方差 / 风险预算 / 跟踪误差约束优化 | 风险模型 | ⏳ 待开始 |
| fund_backtest 集成 | 回测结果自动接入 Barra 归因 | 单期归因 | ⏳ 待开始 |
| 归因报告生成 | PDF/HTML 多期归因可视化报告 | 多期链接 | ⏳ 待开始 |
| 实时/盘中归因 | 日内持仓变动实时归因（高级场景） | 全部模块 | ⏳ 待开始 |

### 10.5 跨 Phase 主题：模型版本管理

因子集合会变（加列/改口径），需要明确版本策略避免"同一天不同版本结果"混淆：

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| **字段标记**（推荐） | 表中增加 `model_version` 字段 | 渐进式扩展，记录“最后一次运行使用的版本”（不做多版本共存） |
| 版本化表名 | `exposures_daily_v1/v2` + 视图指向最新 | 大版本升级，完全隔离 |
| 元数据表 | 独立表记录 `(version, factor_list, params, created_at)` | 精细化配置管理 |

**建议实现**：
```sql
-- 生产迁移建议（更稳）：避免大表重写/长锁
-- 1) 先加列（不带 DEFAULT）
ALTER TABLE barra.exposures_daily ADD COLUMN model_version VARCHAR(16);
ALTER TABLE barra.factor_returns_daily ADD COLUMN model_version VARCHAR(16);
ALTER TABLE barra.specific_returns_daily ADD COLUMN model_version VARCHAR(16);
ALTER TABLE barra.portfolio_attribution_daily ADD COLUMN model_version VARCHAR(16);

-- 2) 分批回填历史分区（示例：按日期分区逐段执行）
-- UPDATE barra.exposures_daily SET model_version='v1.0' WHERE trade_date BETWEEN '2025-01-01' AND '2025-03-31' AND model_version IS NULL;

-- 3) 最后再设置默认值（如确实需要）
ALTER TABLE barra.exposures_daily ALTER COLUMN model_version SET DEFAULT 'v1.0';
ALTER TABLE barra.factor_returns_daily ALTER COLUMN model_version SET DEFAULT 'v1.0';
ALTER TABLE barra.specific_returns_daily ALTER COLUMN model_version SET DEFAULT 'v1.0';
ALTER TABLE barra.portfolio_attribution_daily ALTER COLUMN model_version SET DEFAULT 'v1.0';

-- 备注：如果目标是“多版本共存”，必须把 model_version 纳入唯一约束/主键，并同步调整 upsert 的 conflict key。
```

### 10.6 附录：Full Barra 因子详细实现方案

本附录提供每个因子的详细计算公式和数据落地方案，供开发时参考。

#### 10.6.1 Size（规模）

**MVP 版本**：`style_size = log(circ_mv)`

**Full Barra 版本**：
```python
# 1. 对数变换
log_mcap = np.log(stock_dailybasic.circ_mv)

# 2. 行业中性化（可选，CNE5 不做，但有助于减少行业相关）
# 对 31 个行业 dummy 回归，取残差
residuals = OLS(log_mcap ~ industry_dummies).residuals

# 3. 市值加权 zscore
style_size = weighted_zscore(residuals, weights=sqrt(circ_mv))
```

**数据来源**：`rawdata.stock_dailybasic.circ_mv`

---

#### 10.6.2 Beta（市场敏感度）

**MVP 版本**：未实现（placeholder）

**Full Barra 版本**：
```python
# 1. 获取市场指数收益
market_returns = index_dailybasic['000300.SH'].pct_change  # 沪深300

# 2. 计算 252 日 EWMA 加权 Beta
half_life = 63  # 约 3 个月
weights = exponential_decay(half_life, window=252)
beta_raw = weighted_OLS(stock_return ~ market_return, weights).beta

# 3. Bayesian shrinkage 向 1.0 收缩
shrinkage_factor = 0.3
beta_shrunk = shrinkage_factor * 1.0 + (1 - shrinkage_factor) * beta_raw

# 4. 行业调整（可选）：用行业指数替代全市场指数
industry_index = index_swdaily[stock_industry_code]
beta_industry_adj = ...

# 5. 市值加权 zscore
style_beta = weighted_zscore(beta_shrunk, weights=sqrt(circ_mv))
```

**数据来源**：
- `rawdata.stock_daily.pct_chg`（个股收益）
- `rawdata.index_dailybasic`（沪深300：000300.SH，覆盖 2005-2025）
- `rawdata.index_swdaily`（申万行业指数，用于行业调整 Beta）

---

#### 10.6.3 Momentum（动量）

**MVP 版本**：未实现（placeholder）

**Full Barra 版本**：
```python
# 1. 多窗口动量
mom_252_21 = cumulative_return(t-252, t-21)  # 长期动量，剔除近 1 月
mom_126_21 = cumulative_return(t-126, t-21)  # 中期动量

# 2. 加权合成
momentum_raw = 0.5 * mom_252_21 + 0.5 * mom_126_21

# 3. 短期反转调整（可选）
short_term_reversal = cumulative_return(t-21, t-1)
momentum_adj = momentum_raw - 0.1 * short_term_reversal

# 4. 行业中性化
residuals = OLS(momentum_adj ~ industry_dummies).residuals

# 5. 市值加权 zscore
style_mom = weighted_zscore(residuals, weights=sqrt(circ_mv))
```

**数据来源**：`rawdata.stock_daily.pct_chg`（需累计 252 日历史）

---

#### 10.6.4 Value（价值）

**MVP 版本**：`style_value_bp = 1 / pb`

**Full Barra 版本**：
```python
# 1. 多维价值指标
ep = 1 / stock_dailybasic.pe_ttm      # 盈利收益率 E/P
bp = 1 / stock_dailybasic.pb          # 账面市值比 B/P
sp = 1 / stock_dailybasic.ps_ttm      # 销售市值比 S/P（如无该字段则需要改走财务口径重建）
cfp = calculate_cf_to_price(fina_cashflow, stock_dailybasic)  # 现金流市值比 CF/P
dy = stock_dailybasic.dv_ttm / 100    # 股息率 DY（字段存在性/口径需验证）

# 2. 各子指标单独 winsorize + zscore
ep_z = weighted_zscore(winsorize(ep))
bp_z = weighted_zscore(winsorize(bp))
sp_z = weighted_zscore(winsorize(sp))
cfp_z = weighted_zscore(winsorize(cfp))
dy_z = weighted_zscore(winsorize(dy))

# 3. 等权合成（或 IC 加权 / PCA）
value_composite = (ep_z + bp_z + sp_z + cfp_z + dy_z) / 5

# 4. 最终标准化
style_value = weighted_zscore(value_composite, weights=sqrt(circ_mv))
```

**数据来源**：
- `rawdata.stock_dailybasic.pe_ttm, pb`（已确认存在）
- `rawdata.stock_dailybasic.ps_ttm, dv_ttm`（存在性/覆盖率需用 SQL 验证；若缺失则按 Full Barra 口径从 PIT 财务/分红数据重建）
- `rawdata.fina_cashflow.n_cashflow_act`（经营现金流，用于 CF/P）

---

#### 10.6.5 Liquidity（流动性）

**MVP 版本**：`style_liquidity = turnover_rate_f`

**Full Barra 版本**：
```python
# 1. 多窗口换手率
turnover_21d = rolling_mean(turnover_rate_f, 21)   # 月均
turnover_63d = rolling_mean(turnover_rate_f, 63)   # 季均
turnover_252d = rolling_mean(turnover_rate_f, 252) # 年均

# 2. 成交额/市值比（Amihud 变体）
amount_to_mv = rolling_mean(amount / circ_mv, 21)

# 3. Amihud 非流动性指标（可选）
amihud = rolling_mean(abs(pct_chg) / amount, 21)  # 价格冲击

# 4. 各子指标 zscore
turn_21_z = weighted_zscore(winsorize(np.log(turnover_21d)))
turn_63_z = weighted_zscore(winsorize(np.log(turnover_63d)))
turn_252_z = weighted_zscore(winsorize(np.log(turnover_252d)))
amv_z = weighted_zscore(winsorize(np.log(amount_to_mv)))

# 5. 等权合成
liquidity_composite = (turn_21_z + turn_63_z + turn_252_z + amv_z) / 4

# 6. 最终标准化
style_liquidity = weighted_zscore(liquidity_composite, weights=sqrt(circ_mv))
```

**数据来源**：
- `rawdata.stock_dailybasic.turnover_rate_f, circ_mv`
- `rawdata.stock_daily.amount, pct_chg`

---

#### 10.6.6 Residual Volatility（残差波动）

**MVP 版本**：未实现（placeholder）

**Full Barra 版本**：
```python
# 1. 计算残差收益序列（需先跑 Beta/行业回归）
residuals = stock_return - predicted_return  # 过去 252 日

# 2. EWMA 加权波动率
half_life = 42  # 约 2 个月
weights = exponential_decay(half_life, window=252)
resvol_raw = np.sqrt(weighted_variance(residuals, weights))

# 3. 行业调整：对行业中位数回归取残差
resvol_adj = OLS(resvol_raw ~ industry_dummies).residuals

# 4. 市值加权 zscore
style_resvol = weighted_zscore(resvol_adj, weights=sqrt(circ_mv))
```

**依赖**：需要先计算 Beta，获取残差序列

---

#### 10.6.7 Growth（成长性）- 新增

**Full Barra 版本**：
```python
# 1. 多维增长指标（PIT 对齐：使用 f_ann_date）
netprofit_yoy = fina_indicator.netprofit_yoy   # 覆盖率 92.8%
revenue_yoy = fina_indicator.or_yoy            # 覆盖率 92.4%
ocf_yoy = fina_indicator.ocf_yoy               # 覆盖率 88.8%

# 2. 稳健处理：winsorize 极端值（-100% 和 +500% 以外截断）
netprofit_yoy_w = winsorize(netprofit_yoy, -100, 500)
revenue_yoy_w = winsorize(revenue_yoy, -100, 500)
ocf_yoy_w = winsorize(ocf_yoy, -100, 500)

# 3. 各子指标 zscore
np_z = weighted_zscore(netprofit_yoy_w)
rev_z = weighted_zscore(revenue_yoy_w)
ocf_z = weighted_zscore(ocf_yoy_w)

# 4. 等权合成
growth_composite = (np_z + rev_z + ocf_z) / 3

# 5. 最终标准化
style_growth = weighted_zscore(growth_composite, weights=sqrt(circ_mv))
```

**数据来源**：
- `rawdata.fina_indicator.netprofit_yoy, or_yoy, ocf_yoy`
- PIT 字段：`ann_date, f_ann_date, end_date`

---

#### 10.6.8 Leverage（杠杆）- 新增

**Full Barra 版本**：
```python
# 1. 多维杠杆指标
debt_to_assets = fina_indicator.debt_to_assets  # 资产负债率，覆盖率 96.4%
debt_to_equity = fina_indicator.debt_to_eqt     # 负债/净资产

# 市场杠杆（用市值替代账面权益）
# market_leverage = (total_debt + market_cap) / market_cap

# 2. 各子指标 zscore
da_z = weighted_zscore(winsorize(debt_to_assets))
de_z = weighted_zscore(winsorize(debt_to_equity))

# 3. 等权合成
leverage_composite = (da_z + de_z) / 2

# 4. 最终标准化
style_leverage = weighted_zscore(leverage_composite, weights=sqrt(circ_mv))
```

**数据来源**：
- `rawdata.fina_indicator.debt_to_assets, debt_to_eqt`
- PIT 字段：`ann_date, f_ann_date, end_date`

---

#### 10.6.9 Earnings Quality（盈余质量）- 新增

**Full Barra 版本**：
```python
# 1. 经营现金流质量
ocf_to_income = fina_indicator.ocf_to_opincome  # 覆盖率 74.9%

# 2. 应计比率（需从报表计算）
# accruals = (net_income - operating_cashflow) / total_assets
accruals = (fina_income.n_income - fina_cashflow.n_cashflow_act) / fina_balancesheet.total_assets

# 3. 各子指标 zscore
ocf_z = weighted_zscore(winsorize(ocf_to_income))
acc_z = weighted_zscore(winsorize(-accruals))  # 取负，因为低应计=高质量

# 4. 等权合成
eq_composite = (ocf_z + acc_z) / 2

# 5. 最终标准化
style_earnings_quality = weighted_zscore(eq_composite, weights=sqrt(circ_mv))
```

**数据来源**：
- `rawdata.fina_indicator.ocf_to_opincome`
- `rawdata.fina_income.n_income`
- `rawdata.fina_cashflow.n_cashflow_act`
- `rawdata.fina_balancesheet.total_assets`

---

### 10.7 下一步行动建议

#### Phase 2B 因子升级实施路线图（推荐顺序）

```
Sprint 1（1-2 周）：基础设施 + 快速收益
┌─────────────────────────────────────────────────────────────────┐
│ 1. [必做] Size 行业中性化                                        │
│    - 修改 barra_exposures_daily.py                              │
│    - 添加行业 dummy 回归取残差逻辑                               │
│    - 验证：相关性矩阵、R² 变化                                   │
├─────────────────────────────────────────────────────────────────┤
│ 2. [必做] Value 多指标组合                                       │
│    - 添加 E/P, S/P, CF/P, DY 子指标                             │
│    - 实现 composite_value = mean(子指标 zscore)                 │
│    - 数据源：stock_dailybasic（完备）                           │
├─────────────────────────────────────────────────────────────────┤
│ 3. [必做] Liquidity 多窗口                                       │
│    - 计算 21d/63d/252d 滚动换手率                               │
│    - 添加成交额/市值比                                          │
│    - 数据源：stock_dailybasic + stock_daily                     │
└─────────────────────────────────────────────────────────────────┘

Sprint 2（2-3 周）：核心因子完善
┌─────────────────────────────────────────────────────────────────┐
│ 4. [重要] Beta EWMA + shrinkage                                  │
│    - 获取沪深300日频收益（index_dailybasic.000300.SH）          │
│    - 实现 252 日 EWMA 加权 OLS                                   │
│    - Bayesian shrinkage 向 1.0 收缩                             │
├─────────────────────────────────────────────────────────────────┤
│ 5. [重要] Momentum 多窗口 + 反转调整                             │
│    - 计算 12-1 月 / 6-1 月动量                                  │
│    - 添加短期反转调整项                                         │
│    - 行业中性化                                                  │
├─────────────────────────────────────────────────────────────────┤
│ 6. [可选] Non-Linear Size                                        │
│    - 一行公式：resid(Size³ ~ Size)                              │
│    - 作为 Size 的补充因子                                       │
└─────────────────────────────────────────────────────────────────┘

Sprint 3（3-4 周）：PIT 财务因子
┌─────────────────────────────────────────────────────────────────┐
│ 7. [重要] Growth 多维增长                                        │
│    - PIT 对齐：使用 fina_indicator.f_ann_date                    │
│    - 子指标：netprofit_yoy + or_yoy + ocf_yoy                   │
│    - 覆盖率：88-93%，需处理缺失                                 │
├─────────────────────────────────────────────────────────────────┤
│ 8. [重要] Leverage 多维杠杆                                      │
│    - 子指标：debt_to_assets + debt_to_eqt                       │
│    - 覆盖率：95%+                                               │
├─────────────────────────────────────────────────────────────────┤
│ 9. [可选] Earnings Quality                                       │
│    - 需要联表：fina_income + fina_cashflow + fina_balancesheet  │
│    - 覆盖率：75%，需验证影响                                    │
└─────────────────────────────────────────────────────────────────┘

Sprint 4（1-2 周）：收尾 + 验证
┌─────────────────────────────────────────────────────────────────┐
│ 10. ResVol 行业调整                                              │
│     - 依赖 Beta 残差序列                                        │
│     - EWMA 加权波动率                                           │
├─────────────────────────────────────────────────────────────────┤
│ 11. 全因子回测验证                                               │
│     - 2010-2024 IC/IR 统计                                      │
│     - 相关性矩阵 + VIF 检查                                     │
│     - R² 贡献分析                                               │
├─────────────────────────────────────────────────────────────────┤
│ 12. 文档更新                                                     │
│     - 更新 docs/business/barra_risk_model.md                    │
│     - 添加因子定义文档                                          │
└─────────────────────────────────────────────────────────────────┘
```

#### 其他待办事项

```bash
# [生产化] 在 PROD_SCRIPTS 注册 barra-daily（Phase 2A）
# 修改 alphahome/cli/commands/prod.py:24

# [监控] 在 run_barra_batch.py 添加质量检查
# - 必备：n_obs、R² 范围、sum-to-zero、recon_error
# - 可选：rolling MAD 动态阈值

# [风险] 校准现有参数（Phase 2C）
# - half_life: 测试 63/126/252
# - newey_west_lags: 测试 1-5
# - shrinkage: 测试 0.1-0.5

# [版本] 表结构增加 model_version 字段（Phase 10.5）
```

---

## 十一、数据库字段覆盖率速查表

本节汇总 alphadb 中与 Barra 因子相关的关键字段覆盖情况（通过 `mcp_postgres_query` 验证）。

### 11.1 财务指标表 `rawdata.fina_indicator`

| 字段 | 说明 | 非空率 | 用于因子 |
|------|------|--------|----------|
| `roe` | 净资产收益率 | 97.2% | Profitability |
| `roa` | 总资产收益率 | 96.8% | Profitability |
| `debt_to_assets` | 资产负债率 | 96.4% | Leverage |
| `netprofit_yoy` | 净利润同比 | 92.8% | Growth |
| `or_yoy` | 营收同比 | 92.4% | Growth |
| `ocf_yoy` | 现金流同比 | 88.8% | Growth |
| `q_netprofit_yoy` | 单季净利润同比 | 79.6% | Growth（可选） |
| `q_sales_yoy` | 单季营收同比 | 79.3% | Growth（可选） |
| `ocf_to_opincome` | 经营现金流/营收 | 74.9% | Earnings Quality |

### 11.2 日频估值表 `rawdata.stock_dailybasic`

注：本表字段“是否存在/覆盖率”以 alphadb 实际查询结果为准；下表的 `ps_ttm`/`dv_ttm` 仅作为计划中候选字段。

| 字段 | 说明 | 用于因子 |
|------|------|----------|
| `pe_ttm` | 滚动市盈率 | Value (E/P) |
| `pb` | 市净率 | Value (B/P) |
| `ps_ttm` | 滚动市销率（候选） | Value (S/P) |
| `dv_ttm` | 滚动股息率（候选） | Value (DY) / Dividend Yield |
| `circ_mv` | 流通市值 | Size, 回归权重 |
| `turnover_rate_f` | 自由流通换手率 | Liquidity |

### 11.3 指数日频表

| 表名 | 内容 | 时间范围 | 用于 |
|------|------|----------|------|
| `rawdata.index_dailybasic` | 宽基指数（HS300/ZZ500/上证综指） | 2004-2025 | Beta 市场收益 |
| `rawdata.index_swdaily` | 申万行业指数 | 2012-2025 | 行业调整 Beta |

### 11.4 PIT 关键字段

所有财务表均包含以下 PIT 对齐字段：
- `ann_date`: 计划公告日
- `f_ann_date`: 实际公告日（推荐使用）
- `end_date`: 报告期末日
- `report_type`: 报告类型（合并/母公司/调整等）

