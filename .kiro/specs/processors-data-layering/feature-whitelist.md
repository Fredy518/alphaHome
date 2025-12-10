# 特征入库白名单

## 概述

本文档定义哪些特征需要持久化到数据库，以及相应的更新、回填、版本管理策略。

特征入库的核心原则：**平衡存储成本与计算效率**。并非所有特征都需要入库，只有满足特定标准的特征才应持久化。

## 判定标准

根据 Requirements 7.1-7.6，特征入库需满足以下至少一项标准：

### 1. 高复用标准
- **定义**: 被 3+ 策略或报告使用
- **理由**: 避免重复计算，提高系统整体效率

### 2. 高计算成本标准
- **定义**: 窗口 > 252 天或需要全量历史数据扫描
- **理由**: 重算成本高，入库后可显著降低查询延迟

### 3. 实时查询标准
- **定义**: 查询延迟要求 < 100ms
- **理由**: 现算无法满足延迟要求，必须预计算

### 4. 排除标准
以下特征**不应**入库：
- 策略特定的复合评分（strategy-specific composite scores）
- 可在 1 秒内从 clean 数据计算的特征
- 仅用于单次分析的临时特征

## 判定流程

```
┌─────────────────────────────────────┐
│  特征是否被 3+ 策略/报告使用？        │
└─────────────┬───────────────────────┘
              │ 是
              ▼
         ┌─────────┐
         │  入库   │
         └─────────┘
              │ 否
              ▼
┌─────────────────────────────────────┐
│  窗口 > 252 天或需全量扫描？         │
└─────────────┬───────────────────────┘
              │ 是
              ▼
         ┌─────────┐
         │  入库   │
         └─────────┘
              │ 否
              ▼
┌─────────────────────────────────────┐
│  查询延迟要求 < 100ms？              │
└─────────────┬───────────────────────┘
              │ 是
              ▼
         ┌─────────┐
         │  入库   │
         └─────────┘
              │ 否
              ▼
┌─────────────────────────────────────┐
│  不入库，现算或使用缓存              │
└─────────────────────────────────────┘
```

## 白名单特征列表

### 1. 估值/利差特征

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| PE 分位数（10年） | processor_index_valuation | *_PE_Pctl_10Y | 高复用 + 长窗口 | 2520天 | 日 |
| PB 分位数（10年） | processor_index_valuation | *_PB_Pctl_10Y | 高复用 + 长窗口 | 2520天 | 日 |
| PE 分位数（12月） | processor_index_valuation | *_PE_Pctl_12M | 高复用 | 252天 | 日 |
| PB 分位数（12月） | processor_index_valuation | *_PB_Pctl_12M | 高复用 | 252天 | 日 |
| 股权风险溢价（ERP） | processor_index_valuation | *_ERP | 高复用 + 风控必需 | - | 日 |

**说明**: 估值分位数是择时和风控的核心指标，10年窗口需要全量历史数据，重算成本高。

### 2. 波动率特征

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| 已实现波动率（20日） | processor_index_volatility | *_RV_20D | 高复用 + 风控必需 | 20天 | 日 |
| 已实现波动率（60日） | processor_index_volatility | *_RV_60D | 高复用 + 风控必需 | 60天 | 日 |
| 已实现波动率（252日） | processor_index_volatility | *_RV_252D | 高复用 + 风控必需 | 252天 | 日 |
| RV 分位数（20日） | processor_index_volatility | *_RV_20D_Pctl | 高复用 | 252天 | 日 |
| RV 短长比 | processor_index_volatility | *_RV_Ratio_20_60 | 高复用 | - | 日 |

**说明**: 波动率是风险管理的核心指标，多个策略依赖波动率信号。

### 3. 期权隐含波动率特征

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| IV 近月 | processor_option_iv | *_IV_Near | 高复用 + 重算成本高 | - | 日 |
| IV 次近月 | processor_option_iv | *_IV_Next | 高复用 + 重算成本高 | - | 日 |
| IV 30天插值 | processor_option_iv | *_IV_30D | 高复用 + 重算成本高 | - | 日 |
| IV 短期平均 | processor_option_iv | *_IV_ShortTerm | 高复用 | - | 日 |

**说明**: IV 计算涉及 BS 模型反推和方差插值，计算复杂度高，且是期权策略的核心输入。

### 4. 期货基差特征

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| 基差 | processor_futures_basis | *_Basis | 高复用 | - | 日 |
| 基差率 | processor_futures_basis | *_Basis_Ratio | 高复用 | - | 日 |
| 基差 ZScore | processor_futures_basis | *_Basis_ZScore | 高复用 | 252天 | 日 |
| 基差分位数 | processor_futures_basis | *_Basis_Pctl | 高复用 | 252天 | 日 |
| 基差率 ZScore | processor_futures_basis | *_Basis_Ratio_ZScore | 高复用 | 252天 | 日 |
| 基差率分位数 | processor_futures_basis | *_Basis_Ratio_Pctl | 高复用 | 252天 | 日 |

**说明**: 基差是期现套利和对冲策略的核心指标。

### 5. 资金流特征

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| 净流入率 | processor_market_money_flow | Net_MF_Rate | 高复用 | - | 日 |
| 净流入率 ZScore | processor_market_money_flow | Net_MF_Rate_ZScore | 高复用 | 252天 | 日 |
| 净流入率分位数 | processor_market_money_flow | Net_MF_Rate_Pctl | 高复用 | 252天 | 日 |
| 净流入 ZScore | processor_market_money_flow | Net_MF_ZScore | 高复用 | 252天 | 日 |

**说明**: 资金流是市场情绪和流动性分析的核心指标。

### 6. 市场宽度特征

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| 行业上涨比例 | processor_industry_breadth | Industry_Up_Ratio | 高复用 | - | 日 |
| 行业强势比例 | processor_industry_breadth | Industry_Strong_Ratio | 高复用 | - | 日 |
| 行业弱势比例 | processor_industry_breadth | Industry_Weak_Ratio | 高复用 | - | 日 |
| 行业收益标准差 | processor_industry_breadth | Industry_Return_Std | 高复用 | - | 日 |
| 行业收益偏度 | processor_industry_breadth | Industry_Return_Skew | 高复用 | - | 日 |
| 行业上涨比例（5日均） | processor_industry_breadth | Industry_Up_Ratio_5D | 高复用 | 5天 | 日 |

**说明**: 市场宽度是市场健康度和趋势强度的重要指标。

### 7. 市场技术特征（精选）

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| 动量中位数（多窗口） | processor_market_technical | Mom_*_Median | 高复用 | - | 日 |
| 动量正比例（多窗口） | processor_market_technical | Mom_*_Pos_Ratio | 高复用 | - | 日 |
| 强动量比例 | processor_market_technical | Strong_Mom_Ratio | 高复用 | - | 日 |
| 弱动量比例 | processor_market_technical | Weak_Mom_Ratio | 高复用 | - | 日 |
| 波动率中位数（多窗口） | processor_market_technical | Vol_*_Median | 高复用 | - | 日 |
| 高波动比例 | processor_market_technical | High_Vol_Ratio | 高复用 | - | 日 |
| 低波动比例 | processor_market_technical | Low_Vol_Ratio | 高复用 | - | 日 |
| 量比中位数（多窗口） | processor_market_technical | Vol_Ratio_*_Median | 高复用 | - | 日 |
| 量能扩张比例 | processor_market_technical | Vol_Expand_Ratio | 高复用 | - | 日 |
| 量能收缩比例 | processor_market_technical | Vol_Shrink_Ratio | 高复用 | - | 日 |
| 价量背离比例 | processor_market_technical | Price_Up_Vol_Down_Ratio, Price_Down_Vol_Up_Ratio | 高复用 | - | 日 |

**说明**: 市场技术特征涉及全市场横截面统计，计算成本高。仅入库核心特征，其他特征按需现算。

### 8. 期货持仓特征

| 特征名称 | 入库表 | 入库列 | 入库理由 | 窗口 | 更新频率 |
|---------|--------|--------|----------|------|----------|
| 净多头持仓 | processor_member_position | *_NET_LONG | 高复用 | - | 日 |
| 净持仓变化 | processor_member_position | *_NET_CHG | 高复用 | - | 日 |
| 多空比 | processor_member_position | *_RATIO | 高复用 | - | 日 |
| 净持仓 ZScore | processor_member_position | *_ZScore | 高复用 | 120天 | 日 |
| 净持仓分位数 | processor_member_position | *_Pctl | 高复用 | 120天 | 日 |

**说明**: 期货持仓是市场情绪和机构行为的重要指标。

## 不入库特征（现算或缓存）

以下特征**不入库**，按需现算或使用短期缓存：

### 1. 简单收益率特征
- **特征**: 行业收益率（pct_change）、风格指数收益率（多窗口）
- **理由**: 计算简单（< 1秒），无需持久化
- **策略**: 从 clean 层现算

### 2. 策略特定评分
- **特征**: 复合 alpha 评分、策略权重
- **理由**: 策略特定，不具备通用性
- **策略**: 策略层自行计算

### 3. 临时分析特征
- **特征**: 一次性研究使用的特征
- **理由**: 不具备复用价值
- **策略**: 研究脚本中临时计算

## 更新策略

### 日常更新

| 更新类型 | 触发时间 | 计算范围 | 写入策略 | SLA |
|---------|---------|---------|---------|-----|
| 增量更新 | T+1 日 08:00 | 当日数据 + 回溯 max(window) 天 | UPSERT | T+1 09:00 前完成 |
| 补充更新 | 数据源延迟到达 | 缺失日期 + 回溯 max(window) 天 | UPSERT | 数据到达后 1 小时内 |

**增量更新边界处理**:
```python
# 确保滚动窗口完整
lookback_days = max(feature.window for feature in features)
actual_start_date = requested_start_date - timedelta(days=lookback_days)
```

### 历史回填

| 回填场景 | 触发条件 | 计算范围 | 写入策略 | 验证要求 |
|---------|---------|---------|---------|---------|
| 数据修正 | clean 层数据更新 | 受影响日期 + 回溯 max(window) 天 | UPSERT | 抽样对比修正前后差异 |
| 参数变更 | 特征计算参数调整 | 全量历史 | 新版本号 + UPSERT | 新旧版本并行验证 |
| 算法升级 | 特征计算逻辑优化 | 全量历史 | 新版本号 + UPSERT | 新旧版本并行验证 |
| 新增特征 | 白名单新增特征 | 全量历史 | INSERT | 与研究脚本结果对比 |

**回填流程**:
1. 创建新版本号（如 v2）
2. 按月分区批量计算
3. 写入新版本分区
4. 验证新版本数据质量
5. 切换默认查询版本
6. 归档旧版本（保留最近 2 个版本）

## 版本管理

### 版本字段

所有特征表包含 `_feature_version` 列：

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| _feature_version | VARCHAR(10) | 特征版本号 | v1, v2, v3 |

### 版本策略

| 变更类型 | 版本策略 | 说明 |
|---------|---------|------|
| 参数微调 | 递增版本号 | 如窗口从 252 调整为 250 |
| 算法升级 | 递增版本号 | 如从简单移动平均改为指数移动平均 |
| 新增特征 | 使用 v1 | 新特征从 v1 开始 |
| Bug 修复 | 递增版本号 | 修复后作为新版本 |

### 版本查询

**默认查询**（最新版本）:
```sql
SELECT * FROM processor_index_valuation
WHERE _feature_version = (
    SELECT MAX(_feature_version) FROM processor_index_valuation
)
```

**指定版本查询**:
```sql
SELECT * FROM processor_index_valuation
WHERE _feature_version = 'v2'
```

### 版本清理

| 清理策略 | 说明 |
|---------|------|
| 保留最近 2 个版本 | 当前版本 + 上一版本 |
| 归档超期版本 | 按月分区归档到冷存储（如 S3） |
| 归档触发条件 | 新版本稳定运行 30 天后 |

## 分区策略

### 按时间分区

所有特征表按 `trade_date` 月分区：

```sql
CREATE TABLE processor_index_valuation (
    trade_date INTEGER NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    _feature_version VARCHAR(10) NOT NULL,
    ...
) PARTITION BY RANGE (trade_date);

-- 创建分区示例
CREATE TABLE processor_index_valuation_202501 
    PARTITION OF processor_index_valuation
    FOR VALUES FROM (20250101) TO (20250201);
```

### 分区管理

| 操作 | 策略 |
|------|------|
| 新建分区 | 每月初自动创建下月分区 |
| 回填分区 | 按分区替换（TRUNCATE + INSERT） |
| 归档分区 | 超过 5 年的分区归档到冷存储 |
| 删除分区 | 仅删除已归档的分区 |

## 数据质量监控

### 监控指标

| 指标 | 阈值 | 告警级别 |
|------|------|---------|
| 更新延迟 | > 1 小时（相对 SLA） | 警告 |
| 数据缺失 | > 5% 行缺失 | 严重 |
| 异常值比例 | > 10% 行包含 inf/NaN | 警告 |
| 版本不一致 | 同一 trade_date 存在多个版本 | 严重 |
| 分区缺失 | 缺少当月分区 | 严重 |

### 监控频率

| 监控类型 | 频率 |
|---------|------|
| 更新延迟 | 每小时 |
| 数据质量 | 每日 |
| 版本一致性 | 每日 |
| 分区完整性 | 每周 |

## SLA 定义

### 高优先级特征（P0）

| 特征类别 | SLA | 说明 |
|---------|-----|------|
| 估值/利差 | T+1 09:00 前 | 择时策略依赖 |
| 波动率 | T+1 09:00 前 | 风控必需 |
| 资金流 | T+1 09:00 前 | 流动性监控 |

### 中优先级特征（P1）

| 特征类别 | SLA | 说明 |
|---------|-----|------|
| 基差 | T+1 12:00 前 | 期现套利策略 |
| 期权 IV | T+1 12:00 前 | 期权策略 |
| 市场宽度 | T+1 12:00 前 | 市场情绪分析 |

### 低优先级特征（P2）

| 特征类别 | SLA | 说明 |
|---------|-----|------|
| 市场技术 | T+1 收盘前 | 技术分析 |
| 期货持仓 | T+1 收盘前 | 持仓分析 |

## 存储容量规划

### 容量估算

| 特征表 | 行数/天 | 列数 | 单行大小 | 日增量 | 年增量 |
|--------|---------|------|---------|--------|--------|
| processor_index_valuation | ~10 | 10 | ~200B | ~2KB | ~500KB |
| processor_index_volatility | ~10 | 10 | ~200B | ~2KB | ~500KB |
| processor_option_iv | ~500 | 15 | ~300B | ~150KB | ~37MB |
| processor_futures_basis | ~20 | 15 | ~300B | ~6KB | ~1.5MB |
| processor_market_money_flow | 1 | 10 | ~200B | ~200B | ~50KB |
| processor_industry_breadth | 1 | 10 | ~200B | ~200B | ~50KB |
| processor_market_technical | 1 | 20 | ~400B | ~400B | ~100KB |
| processor_member_position | ~50 | 10 | ~200B | ~10KB | ~2.5MB |

**总计**: 约 **50MB/年**（单版本）

### 多版本存储

假设保留 2 个版本，5 年历史数据：
- 存储需求: 50MB × 5 年 × 2 版本 = **500MB**
- 加上索引和分区开销: 约 **1GB**

**结论**: 存储成本可控，无需过度优化。

## 白名单评审流程

### 季度评审

根据 Requirements 7.6，每季度评审特征入库决策：

| 评审内容 | 评审标准 | 决策 |
|---------|---------|------|
| 复用度 | 实际使用策略/报告数量 | < 3 个则考虑下线 |
| 查询频率 | 过去 90 天查询次数 | < 10 次则考虑下线 |
| 计算成本 | 平均重算耗时 | < 1 秒则考虑下线 |
| 存储成本 | 表大小增长趋势 | 超预期则优化或下线 |

### 新增特征流程

1. **提案**: 提交特征入库申请，说明入库理由（复用度/计算成本/查询延迟）
2. **评审**: Tech Lead 评审是否满足入库标准
3. **试运行**: 在 dev/staging 环境试运行 2 周
4. **验证**: 对比研究脚本结果，验证正确性
5. **上线**: 生产环境上线，加入白名单
6. **监控**: 持续监控数据质量和使用情况

### 下线特征流程

1. **标记**: 将特征标记为 deprecated
2. **通知**: 通知所有使用方，提供替代方案
3. **观察期**: 30 天观察期，确认无依赖
4. **归档**: 将历史数据归档到冷存储
5. **删除**: 从白名单移除，停止更新

## 元数据管理

### 特征元数据

每个入库特征需维护以下元数据：

| 元数据字段 | 说明 | 示例 |
|-----------|------|------|
| feature_name | 特征名称 | PE_Pctl_10Y |
| table_name | 入库表名 | processor_index_valuation |
| column_name | 列名 | CSI300_PE_Pctl_10Y |
| unit | 单位 | 分位数（0-1） |
| frequency | 频率 | 日频 |
| window | 窗口 | 2520天 |
| min_periods | 最小周期 | 252天 |
| description | 描述 | 沪深300市盈率10年分位数 |
| dependencies | 依赖的 clean 表 | clean.index_valuation_base |
| owner | 负责人 | Data Team |
| created_at | 创建时间 | 2025-01-01 |
| updated_at | 更新时间 | 2025-12-10 |

### 元数据存储

元数据存储在 `metadata.feature_registry` 表：

```sql
CREATE TABLE metadata.feature_registry (
    feature_id SERIAL PRIMARY KEY,
    feature_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    unit VARCHAR(50),
    frequency VARCHAR(20),
    window INTEGER,
    min_periods INTEGER,
    description TEXT,
    dependencies TEXT[],
    owner VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'active',  -- active, deprecated, archived
    UNIQUE(table_name, column_name)
);
```

## 与 design.md 的一致性

本白名单文档与 design.md 中的"特征入库与版本管理"章节保持一致：

| design.md 配置项 | 本文档对应章节 |
|-----------------|---------------|
| 版本字段 | 版本管理 > 版本字段 |
| 分区策略 | 分区策略 |
| 回填触发 | 更新策略 > 历史回填 |
| 重算流程 | 更新策略 > 历史回填 > 回填流程 |
| SLA | SLA 定义 |
| 默认查询版本 | 版本管理 > 版本查询 |
| 旧版本清理 | 版本管理 > 版本清理 |

## 附录：特征函数映射

| 特征函数 | 使用特征 | 说明 |
|---------|---------|------|
| rolling_percentile | PE/PB 分位数、RV 分位数、基差分位数、资金流分位数、持仓分位数 | 滚动分位数计算 |
| rolling_zscore | 基差 ZScore、资金流 ZScore、持仓 ZScore | 滚动标准化 |
| rolling_std | RV 计算 | 滚动标准差 |
| bs_implied_vol | IV 计算 | BS 模型反推隐含波动率 |
| variance_interpolation | IV 30天插值 | 方差线性插值 |
| cross_section_stats | 市场宽度、市场技术 | 横截面统计 |

---

**文档版本**: v1.0  
**生成时间**: 2025-12-10  
**负责人**: Data Team  
**评审周期**: 季度  
**下次评审**: 2026-03-10

**Requirements**: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6
