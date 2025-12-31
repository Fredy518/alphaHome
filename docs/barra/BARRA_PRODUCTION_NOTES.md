# Barra 模块生产运行注意事项

> 本文档记录 Barra 模块在生产环境中的关键口径、已知限制与运行建议。

## 一、回填与增量运行建议

### 1.1 首次回填流程

```bash
# Step 1: 初始化 schema 和表结构
python scripts/initialize_barra_schema.py

# Step 2: 创建按年分区（避免全进 DEFAULT 分区）
python scripts/create_barra_partitions.py --start-year 2015 --end-year 2026

# Step 3: 回填（默认使用 t-1 暴露，PIT 模式）
python scripts/run_barra_batch.py 2015-01-01 2025-12-31 --parallel 4
```

### 1.2 增量运行

每个交易日 T：
1. 计算 exposures(T)
2. 计算 factor_returns(T, exposure_date=T-1)

```bash
python scripts/run_barra_batch.py --last-n 1
```

### 1.3 t-1 对齐（避免前视偏差）

- 因子收益回归使用 **t-1 日暴露** 解释 **t 日收益**
- 批跑脚本默认启用 PIT 模式（`--no-lag` 可切换为同日暴露，仅调试用）
- 脚本会自动检查并补跑缺失的 exposure_date 暴露

---

## 二、关键口径说明

### 2.1 WLS 回归权重

$$w_i = \sqrt{\text{ff\_mcap}_i}$$

- 使用流通市值的平方根作为 WLS 权重
- 符合 Barra CNE5 / USE4 的标准做法

### 2.2 行业 sum-to-zero 约束

采用**数学变换法（C 矩阵重参数化）**：

$$f_{ind} = C \cdot g, \quad C = \begin{bmatrix} I_{J-1} \\ -\mathbf{1}^T \end{bmatrix}$$

- 回归时对 J-1 个自由参数 g 做无约束估计
- 回归后通过 $f_{ref} = -\sum_{j \neq ref} g_j$ 恢复全量 J 个行业收益
- 保证 $\sum_{j=1}^{J} f_{ind,j} = 0$

### 2.3 暴露标准化

- **Winsorize**: 1%/99% 分位截断
- **Z-score**: 市值加权均值 + 截面标准差

### 2.4 行业分类

- 申万一级行业（31 个）
- 使用 PIT 视图 `barra.pit_sw_industry_member_mv`
- `out_date` 为最后有效日（不做 -1 day）

---

## 三、已知限制与后续迭代

### 3.1 MVP 阶段限制

| 限制 | 影响 | 后续计划 |
|------|------|----------|
| 无 intercept/country 因子 | 市场共同变动可能被吸收到 residual | 下一版本加入 |
| 仅 mcap 过滤 eligible | ST/停牌/一字板未剔除 | 增加可交易过滤 |
| Beta/Momentum/ResVol 未实现 | 对应 style 列为 NULL | 逐步补齐 |
| 风险模型窗口按自然日 | 有效样本可能不稳定 | 改为交易日对齐 |

### 3.2 分区策略

- 所有日度表按 `trade_date` 做 RANGE 分区
- 默认创建 DEFAULT 分区兜底
- **强烈建议**：回填前先创建按年分区（见 1.1）

---

## 四、表结构概览

| 表名 | 主键 | 分区键 | 用途 |
|------|------|--------|------|
| `barra.industry_l1_dim` | `l1_code` | - | 申万行业维表 |
| `barra.exposures_daily` | `(trade_date, ticker)` | `trade_date` | 因子暴露 |
| `barra.factor_returns_daily` | `(trade_date)` | `trade_date` | 因子收益 + 回归诊断 |
| `barra.specific_returns_daily` | `(trade_date, ticker)` | `trade_date` | 特质收益 |
| `barra.portfolio_attribution_daily` | `(trade_date, portfolio_id, benchmark_id)` | `trade_date` | 单期归因 |
| `barra.multi_period_attribution` | `(start_date, end_date, portfolio_id, benchmark_id)` | - | 多期链接归因 |
| `barra.factor_covariance` | `(as_of_date, factor1, factor2)` | - | 因子协方差 |
| `barra.specific_variance_daily` | `(as_of_date, ticker)` | - | 特质方差 |

---

## 五、监控与诊断

### 5.1 回归质量监控

每日检查 `barra.factor_returns_daily`：

```sql
SELECT trade_date, n_obs, r2, rmse
FROM barra.factor_returns_daily
WHERE r2 < 0.10 OR n_obs < 1000
ORDER BY trade_date DESC
LIMIT 20;
```

- `n_obs` 应 > 3000（A股正常交易日）
- `r2` 通常在 0.15 ~ 0.35 之间
- `rmse` 通常在 1.5% ~ 2.5%

### 5.2 归因一致性校验

```sql
SELECT trade_date, portfolio_id,
       active_return,
       explained_return,
       recon_error,
       ABS(recon_error) as abs_error
FROM barra.portfolio_attribution_daily
WHERE ABS(recon_error) > 0.001
ORDER BY abs_error DESC;
```

- `recon_error` 应接近 0
- 较大误差可能来源：权重变化、交易成本、模型遗漏因子

---

## 六、常见问题

### Q: 回填时报列不存在？

确保先运行 `scripts/initialize_barra_schema.py`，它会根据当前行业列表动态生成宽表列。

### Q: 第一天回填失败？

PIT 模式下第一天需要 t-1 暴露。脚本会自动补跑，但如果 t-1 是非交易日会跳过。确保回填区间的前一个交易日有数据。

### Q: 并行回填性能不佳？

检查是否创建了按年分区。全进 DEFAULT 分区会导致 upsert 性能严重下降。

---

*文档更新日期：2025-12-31*
