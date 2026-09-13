# ETF 研究底座统一维护

本模块把个人研究工作区中的 ETF 候选身份，与 AlphaHome 可重算事实分层维护。候选身份和人工分类按工作簿版本留存；规模、成交、费率、折溢价、指数技术指标、直接估值和行业盈利观察由 AlphaHome 计算。

## 数据对象

| 对象 | 粒度 | 用途与边界 |
| --- | --- | --- |
| `features.mv_etf_product_facts_current` | 每只上市 ETF 一行 | 当前产品事实；不是 PIT 历史表 |
| `fund_pool_on.etf_candidate_master_batch` | 每个工作簿版本一行 | 来源哈希、质量摘要、权限边界 |
| `fund_pool_on.etf_candidate_master_snapshot` | 工作簿版本 × ETF | 候选身份、人工分类和载入时事实快照 |
| `fund_pool_on.etf_candidate_master_current_enriched` | 当前版本 × ETF | 原始快照与最新产品事实并列；不回写候选状态 |
| `features.mv_etf_exposure_technical_current_universe_daily` | 日期 × 当前候选跟踪指数 | 技术原子；当前宇宙，不可宣称无幸存者偏差 |
| `features.mv_index_direct_valuation_daily` | 日期 × 指数 | 数据商直接 PE/PB；不以重构值静默补缺 |
| `fund_pool_on.etf_candidate_index_coverage_current` | 当前候选跟踪指数 | 明示技术和直接估值是否覆盖 |
| `features.mv_industry_earnings_observation_monthly` | 月份 × 行业 | PIT 的 FAPI、预期 ROE、FTTM 和滚动原子；不含事后合成策略分数 |

这些对象只提供研究和候选池维护能力。候选载入合同强制 `capital_authority=false`、`order_authority=false`，不会赋予资金或下单权限。

## 维护入口

先在个人研究工作区用 artifact-tool 校验并导出当前工作簿：

```powershell
node "D:\TradeHome\个人投资框架\ETF账户\功能模块\00_公共接口\src\export_etf_candidate_master.mjs"
```

导出后可在 AlphaHome GUI 的 **ETF 研究底座** 页选择该 JSON，点击“校验并统一维护”。GUI 会依次刷新独立事实、幂等载入候选版本、刷新候选指数技术事实，并展示候选版本、事实水位与技术/直接估值覆盖率。它只接受标准化 JSON，不在 AlphaHome 内重复解析 Excel。

无 GUI 环境时，可执行同一套共享服务的命令行入口：

```powershell
.venv\Scripts\python.exe scripts\curation\update_etf_research_foundation.py `
  --candidate-snapshot "D:\TradeHome\个人投资框架\ETF账户\功能模块\00_公共接口\outputs\current\etf_candidate_master_snapshot.json"
```

维护命令返回每个物化视图的行数、刷新耗时、候选载入计数、产品事实完整数，以及候选跟踪指数的技术/直接估值覆盖数。相同 `snapshot_id` 和来源哈希可重复执行；同一 `snapshot_id` 对应不同哈希时拒绝载入。

## 查询示例

```sql
SELECT *
FROM fund_pool_on.etf_candidate_master_current_enriched
ORDER BY source_rank;

SELECT *
FROM fund_pool_on.etf_candidate_index_coverage_current
WHERE NOT technical_available OR NOT direct_valuation_available
ORDER BY index_code;
```

直接估值缺失表示 AlphaDB 当前没有相同指数代码的直接提供口径，不代表估值为零，也不应自动切换到今日成分回构的历史估值。若后续增加重构路线，必须单列来源、时点和覆盖质量，并保留与直接口径的区别。
