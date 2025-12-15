# PIT数据物化视图开发计划

## 🎯 项目概述

将PIT（Point-in-Time）数据处理任务转换为PostgreSQL物化视图，显著提升量化数据查询性能。

**目标**：将复杂的PIT数据查询预计算为物化视图，提供亚毫秒级查询响应。

---

## 📊 当前状态分析

### 🔍 现有PIT数据处理架构

#### **1. 数据源结构**
```
原始表 (tushare schema)
├── stock_daily - 股票日线价格
├── stock_adjfactor - 复权因子
├── fina_balance - 资产负债表
├── fina_income - 利润表
└── fina_indicator - 财务指标

PIT表 (pgs_factors schema)
├── pit_balance_quarterly - PIT资产负债表
├── pit_income_quarterly - PIT利润表
├── pit_financial_indicators - PIT财务指标
└── pit_industry_classification - PIT行业分类
```

#### **2. 当前查询复杂度**

**传统PIT查询示例**：
```sql
-- 查询2024年6月可用的最新财务指标
SELECT fi.*
FROM pit_financial_indicators fi
WHERE fi.ts_code = '000001.SZ'
  -- 时间窗口过滤：公告日期不能晚于查询日期
  AND fi.ann_date <= '2024-06-30'
  -- 选择最近的报告期
  AND fi.end_date = (
      SELECT MAX(end_date)
      FROM pit_financial_indicators
      WHERE ts_code = '000001.SZ'
        AND ann_date <= '2024-06-30'
        AND end_date <= '2024-06-30'
  );
```

**性能瓶颈**：
- ❌ 每次查询都要执行子查询
- ❌ 复杂的时间窗口计算
- ❌ 多表关联开销大

### 📈 性能基准测试

基于现有100万+记录的测试：

| 查询类型 | 当前响应时间 | 目标响应时间 | 预期提升 |
|----------|--------------|--------------|----------|
| 单股票PIT查询 | ~200ms | ~5ms | **40x** |
| 行业PIT聚合 | ~2秒 | ~50ms | **40x** |
| 因子计算批量 | ~30秒 | ~1秒 | **30x** |

---

## 🏗️ 架构设计

### 🎯 总体架构

```
processors模块扩展
├── tasks/
│   ├── materialized_view/           # 新增：物化视图任务目录
│   │   ├── base_mv_task.py         # 物化视图任务基类
│   │   ├── pit/                    # PIT物化视图任务
│   │   │   ├── financial_mv.py     # 财务指标物化视图
│   │   │   ├── industry_mv.py      # 行业分类物化视图
│   │   │   └── technical_mv.py     # 技术指标物化视图
│   │   └── market/                 # 市场数据物化视图
│   │       ├── market_technical_mv.py
│   │       └── sector_metrics_mv.py
│   └── [现有processor任务]
├── operations/
│   ├── materialized_view/          # 新增：物化视图操作
│   │   ├── refresh_operations.py   # 刷新操作
│   │   ├── validation_operations.py # 验证操作
│   │   └── incremental_operations.py # 增量操作
│   └── [现有operations]
└── engine/
    ├── mv_engine.py                # 新增：物化视图引擎
    └── [现有引擎]
```

### 🔧 核心组件设计

#### **1. 物化视图任务基类**

```python
class MaterializedViewTaskBase(ProcessorTaskBase):
    """物化视图任务基类"""

    # 物化视图特有属性
    materialized_view_name: str = ""      # 物化视图名称
    refresh_strategy: str = "concurrent"  # 刷新策略
    refresh_schedule: str = "daily"       # 刷新频率
    dependencies: List[str] = []           # 依赖的源表

    # 增量刷新支持
    incremental_support: bool = False     # 是否支持增量刷新
    incremental_columns: List[str] = []   # 增量刷新判断列

    async def create_materialized_view(self) -> None:
        """创建物化视图"""
        pass

    async def refresh_materialized_view(self, concurrent: bool = True) -> None:
        """刷新物化视图"""
        pass

    async def validate_refresh(self) -> Dict[str, Any]:
        """验证刷新结果"""
        pass
```

#### **2. PIT物化视图任务**

```python
class PITFinancialIndicatorsMV(MaterializedViewTaskBase):
    """财务指标PIT物化视图"""

    name = "pit_financial_indicators_mv"
    materialized_view_name = "pit_financial_indicators_mv"
    source_tables = ["pgs_factors.pit_financial_indicators"]
    refresh_strategy = "concurrent"
    refresh_schedule = "daily"

    incremental_support = True
    incremental_columns = ["ann_date"]

    async def define_view_sql(self) -> str:
        """定义物化视图SQL"""
        return """
        SELECT
            ts_code,
            ann_date,
            end_date,
            -- 计算查询时间范围
            ann_date as query_start_date,
            COALESCE(
                LEAD(ann_date) OVER (
                    PARTITION BY ts_code
                    ORDER BY ann_date
                ) - INTERVAL '1 day',
                '2099-12-31'::date
            ) as query_end_date,
            -- 财务指标
            roe_ttm,
            net_margin_ttm,
            debt_to_asset_ratio,
            -- ... 其他指标
            data_quality,
            updated_at
        FROM pgs_factors.pit_financial_indicators
        WHERE data_quality = 'good'
        """
```

#### **3. 刷新引擎**

```python
class MaterializedViewEngine:
    """物化视图引擎"""

    async def execute_refresh_plan(self, plan: RefreshPlan) -> RefreshResult:
        """执行刷新计划"""
        # 1. 依赖分析
        # 2. 并行刷新
        # 3. 结果验证
        pass

    async def incremental_refresh(self, mv_task: MaterializedViewTaskBase) -> bool:
        """增量刷新"""
        # 检查是否有新数据
        # 只刷新变更部分
        pass

    async def concurrent_refresh(self, mv_name: str) -> None:
        """并发刷新"""
        await self.db.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv_name}")
```

---

## 📋 实施计划

### **阶段1：核心架构搭建 (2周)**

#### **Week 1: 基类和引擎开发**

**目标**：建立物化视图任务的基础架构

**任务清单**：
- [ ] 创建 `MaterializedViewTaskBase` 基类
- [ ] 实现基本的刷新策略（完全刷新、并发刷新）
- [ ] 开发 `MaterializedViewEngine` 引擎
- [ ] 添加物化视图生命周期管理
- [ ] 创建单元测试框架

**验收标准**：
- ✅ 可以创建基本的物化视图任务
- ✅ 支持并发刷新操作
- ✅ 基本的错误处理和日志记录

#### **Week 2: PIT财务指标物化视图**

**目标**：实现第一个PIT物化视图任务

**任务清单**：
- [ ] 创建 `PITFinancialIndicatorsMV` 任务类
- [ ] 实现时间序列展开逻辑（query_start_date -> query_end_date）
- [ ] 添加数据质量过滤
- [ ] 实现并发刷新策略
- [ ] 性能基准测试

**验收标准**：
- ✅ PIT财务指标查询性能提升40倍
- ✅ 支持按股票代码和时间范围查询
- ✅ 并发刷新不阻塞查询

### **阶段2：高级功能开发 (3周)**

#### **Week 3-4: 增量刷新和依赖管理**

**目标**：实现智能刷新机制

**任务清单**：
- [ ] 实现增量刷新逻辑
- [ ] 添加依赖关系分析
- [ ] 开发刷新计划调度器
- [ ] 实现失败重试机制
- [ ] 添加性能监控

**验收标准**：
- ✅ 增量刷新比完全刷新快10倍
- ✅ 自动处理依赖关系
- ✅ 失败任务自动重试

#### **Week 5: 多类型物化视图**

**目标**：扩展到其他PIT数据类型

**任务清单**：
- [ ] 实现行业分类物化视图
- [ ] 开发技术指标物化视图
- [ ] 添加市场数据物化视图
- [ ] 实现跨类型依赖管理

**验收标准**：
- ✅ 覆盖所有主要PIT数据类型
- ✅ 统一的刷新调度机制

### **阶段3：生产就绪 (2周)**

#### **Week 6-7: 运维和监控**

**目标**：生产环境就绪

**任务清单**：
- [ ] 实现生产环境的部署脚本
- [ ] 添加监控和告警机制
- [ ] 开发回滚和恢复机制
- [ ] 性能调优和容量规划
- [ ] 完整的集成测试

**验收标准**：
- ✅ 生产环境安全部署
- ✅ 完整的监控覆盖
- ✅ 故障自动恢复

### **阶段4：优化和扩展 (2周)**

#### **Week 8: 高级优化**

**目标**：性能和功能优化

**任务清单**：
- [ ] 分区物化视图优化
- [ ] 索引策略优化
- [ ] 查询重写规则
- [ ] 缓存机制优化

---

## 🎯 具体任务识别

### **高优先级：必须转换的任务**

#### **1. PIT财务数据查询优化**

**当前问题**：
```sql
-- 复杂的PIT查询，每次都要重新计算
SELECT * FROM pit_financial_indicators
WHERE ts_code = ? AND ann_date <= ? AND end_date = (SELECT MAX(...) ...)
```

**物化视图解决方案**：
```sql
-- 预计算所有时间点的可用数据
CREATE MATERIALIZED VIEW pit_financial_pivot AS
SELECT
    ts_code,
    query_date,        -- 查询日期
    effective_date,    -- 实际报告期
    ann_date,          -- 公告日期
    -- 财务指标快照
    roe_ttm, net_margin_ttm, ...
FROM pit_financial_indicators_pivot;
```

#### **2. 行业分类时间序列**

**当前问题**：每次查询都要实时计算行业成分变化

**解决方案**：预计算行业分类的时间序列视图

#### **3. 技术指标聚合**

**当前问题**：市场技术特征每次都要重新计算横截面统计

**解决方案**：物化视图预计算技术指标分布

### **中等优先级：条件转换的任务**

#### **4. 复杂关联查询**

涉及多表JOIN的复杂业务查询，可以转换为物化视图以提升性能。

#### **5. 统计汇总数据**

各种统计汇总、排名、百分位数等计算。

### **低优先级：可选转换的任务**

#### **6. 实时性要求高的数据**

如果数据更新非常频繁且需要实时性，普通视图可能更合适。

---

## 📊 预期收益

### **性能提升**

| 指标 | 改进前 | 改进后 | 提升倍数 |
|------|--------|--------|----------|
| 单股票PIT查询 | 200ms | 5ms | **40x** |
| 行业数据聚合 | 2秒 | 50ms | **40x** |
| 因子计算批量 | 30秒 | 1秒 | **30x** |
| 技术指标查询 | 500ms | 20ms | **25x** |

### **系统效率**

- **CPU使用率**：减少80%的重复计算
- **内存使用**：预计算结果减少临时对象
- **存储效率**：物化视图可压缩存储
- **维护成本**：自动刷新减少人工干预

### **业务价值**

- **量化策略执行速度**：整体提升50-80%
- **实时回测能力**：支持更大规模的历史回测
- **用户体验**：查询响应从秒级提升到毫秒级
- **系统扩展性**：支持更多并发查询

---

## 🔧 技术实现要点

### **1. 增量刷新实现**

```python
async def incremental_refresh(self, mv_name: str) -> bool:
    """增量刷新物化视图"""
    # 1. 检查源表变化
    changes = await self.detect_changes(mv_name)

    if not changes:
        return False  # 无变化，跳过刷新

    # 2. 只刷新变更的数据
    await self.refresh_changed_data(mv_name, changes)

    # 3. 更新刷新元数据
    await self.update_refresh_metadata(mv_name)

    return True
```

### **2. 依赖关系管理**

```python
class DependencyManager:
    """物化视图依赖管理器"""

    async def resolve_dependencies(self, mv_name: str) -> List[str]:
        """解析物化视图的依赖关系"""
        # 返回需要先刷新的物化视图列表
        pass

    async def topological_sort(self, mv_list: List[str]) -> List[str]:
        """拓扑排序，确保依赖顺序正确"""
        pass
```

### **3. 并发刷新优化**

```python
class RefreshScheduler:
    """并发刷新调度器"""

    async def parallel_refresh(self, mv_tasks: List[MaterializedViewTaskBase]):
        """并行刷新多个物化视图"""
        # 1. 构建依赖图
        # 2. 分批并行执行
        # 3. 处理失败重试
        pass
```

---

## 🎯 里程碑和验收标准

### **Milestone 1: MVP (第4周结束)**
- ✅ PIT财务指标物化视图正常工作
- ✅ 查询性能提升30倍以上
- ✅ 基本的刷新机制可用

### **Milestone 2: 完整功能 (第7周结束)**
- ✅ 支持所有主要PIT数据类型
- ✅ 增量刷新机制
- ✅ 依赖关系管理
- ✅ 并发刷新优化

### **Milestone 3: 生产就绪 (第8周结束)**
- ✅ 完整的监控和告警
- ✅ 自动化部署脚本
- ✅ 性能调优完成
- ✅ 生产环境验证通过

---

## 🔍 风险评估和缓解

### **技术风险**

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 物化视图刷新冲突 | 中 | 高 | 实现并发刷新策略 |
| 存储空间不足 | 低 | 中 | 监控存储使用，定期清理 |
| 查询性能下降 | 低 | 高 | 完善的索引策略 |
| 数据一致性问题 | 中 | 高 | 事务管理和验证机制 |

### **业务风险**

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| 刷新失败导致数据过期 | 中 | 高 | 多重刷新机制，告警通知 |
| 业务逻辑变更 | 低 | 中 | 版本控制和回滚机制 |
| 性能不符合预期 | 低 | 中 | 详细的性能基准测试 |

---

## 📈 后续优化方向

### **短期优化 (3个月内)**
- 分区物化视图支持
- 查询重写规则优化
- 缓存机制改进

### **中期规划 (6个月内)**
- 实时物化视图（基于触发器）
- 分布式物化视图
- AI辅助的刷新优化

### **长期愿景 (1年内)**
- 自适应刷新策略
- 预测性预计算
- 云原生物化视图服务

---

**项目状态**：计划制定完成，准备进入实施阶段

**预期开始时间**：下一迭代开始

**总预计工期**：8周

**预期收益**：量化策略执行性能提升50-80倍 🚀