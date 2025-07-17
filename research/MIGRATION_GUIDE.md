# AlphaHome Research模块迁移指南

## 从直接SQL查询迁移到Providers数据提供层

本指南帮助您将现有的research代码从直接SQL查询方式迁移到新的providers数据提供层。

## 🎯 迁移优势

- **简化代码**: 无需手写SQL，减少出错概率
- **自动优化**: 内置数据类型转换和错误处理
- **智能检测**: 自动检测可用数据表
- **统一接口**: 80/20原则，5个核心方法覆盖80%需求
- **向后兼容**: 保留原有功能，渐进式迁移

## 📋 迁移对照表

### 1. 股票行情数据

#### 🔴 旧方式（直接SQL）
```python
# 手写SQL查询
query = """
SELECT ts_code, trade_date, open, high, low, close, vol, amount, pct_chg
FROM stock_daily 
WHERE ts_code IN ('000001.SZ', '000002.SZ')
AND trade_date >= '2024-01-01'
AND trade_date <= '2024-01-31'
ORDER BY ts_code, trade_date
"""
df = context.query_dataframe(query)

# 手动数据类型转换
df['trade_date'] = pd.to_datetime(df['trade_date'])
df['close'] = pd.to_numeric(df['close'], errors='coerce')
```

#### 🟢 新方式（Providers）
```python
# 使用providers数据提供层
df = context.get_stock_data(
    symbols=['000001.SZ', '000002.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    adjust=True  # 自动使用复权价格
)
# 数据类型已自动转换，无需手动处理
```

### 2. 股票基本信息

#### 🔴 旧方式
```python
query = """
SELECT ts_code, name, industry, area, list_date
FROM stock_basic
WHERE list_status = 'L'
ORDER BY ts_code
"""
df = context.query_dataframe(query)
```

#### 🟢 新方式
```python
df = context.data_tool.get_stock_info(list_status='L')
```

### 3. 交易日历

#### 🔴 旧方式
```python
query = """
SELECT cal_date
FROM trade_cal
WHERE exchange = 'SSE'
AND is_open = 1
AND cal_date BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY cal_date
"""
df = context.query_dataframe(query)
trading_dates = df['cal_date'].tolist()
```

#### 🟢 新方式
```python
trading_dates = context.get_trading_dates(
    start_date='2024-01-01',
    end_date='2024-01-31',
    exchange='SSE'
)
```

### 4. 指数权重数据

#### 🔴 旧方式
```python
query = """
SELECT index_code, con_code, trade_date, weight
FROM index_weight
WHERE index_code = '000300.SH'
AND trade_date >= '2024-01-01'
AND trade_date <= '2024-01-31'
ORDER BY trade_date, weight DESC
"""
df = context.query_dataframe(query)
```

#### 🟢 新方式
```python
df = context.get_index_weights(
    index_code='000300.SH',
    start_date='2024-01-01',
    end_date='2024-01-31',
    monthly=False  # 获取所有数据，True=只获取月末
)
```

### 5. 行业分类数据

#### 🔴 旧方式
```python
query = """
SELECT ts_code, industry_code, industry_name
FROM stock_industry
WHERE level = 'sw_l1'
AND ts_code IN ('000001.SZ', '000002.SZ')
"""
df = context.query_dataframe(query)
```

#### 🟢 新方式
```python
df = context.get_industry_data(
    symbols=['000001.SZ', '000002.SZ'],
    level='sw_l1'
)
```

## 🔧 复杂查询迁移

对于复杂的联表查询，使用`custom_query()`方法：

#### 🔴 旧方式
```python
query = """
SELECT s.ts_code, s.close, w.weight, i.industry_name
FROM stock_daily s
JOIN index_weight w ON s.ts_code = w.con_code
JOIN stock_industry i ON s.ts_code = i.ts_code
WHERE w.index_code = '000300.SH'
AND s.trade_date = '2024-01-31'
"""
df = context.query_dataframe(query)
```

#### 🟢 新方式
```python
df = context.data_tool.custom_query("""
SELECT s.ts_code, s.close, w.weight, i.industry_name
FROM stock_daily s
JOIN index_weight w ON s.ts_code = w.con_code
JOIN stock_industry i ON s.ts_code = i.ts_code
WHERE w.index_code = %(index_code)s
AND s.trade_date = %(trade_date)s
""", {
    'index_code': '000300.SH',
    'trade_date': '2024-01-31'
})
```

## 📝 迁移步骤

### 步骤1：识别现有查询模式
扫描代码中的以下模式：
- `context.query_dataframe()`
- `context.db_manager.query_dataframe()`
- 直接SQL查询字符串

### 步骤2：选择对应的providers方法
根据查询内容选择合适的核心方法：
- 股票数据 → `get_stock_data()`
- 指数数据 → `get_index_weights()`
- 基本信息 → `get_stock_info()`
- 交易日历 → `get_trade_dates()`
- 行业分类 → `get_industry_data()`
- 复杂查询 → `custom_query()`

### 步骤3：更新代码
替换SQL查询为providers方法调用，移除手动数据处理代码。

### 步骤4：测试验证
确保迁移后的代码功能与原来一致。

## 🔄 渐进式迁移策略

1. **保持向后兼容**: 原有的`query_dataframe()`方法仍然可用
2. **逐步替换**: 可以在同一项目中混用新旧方式
3. **优先迁移**: 先迁移简单的单表查询，再处理复杂查询
4. **测试驱动**: 每次迁移后进行功能测试

## 📊 性能对比

| 方面 | 旧方式 | 新方式 | 改进 |
|------|--------|--------|------|
| 代码行数 | 10-15行 | 3-5行 | 减少60-70% |
| 出错概率 | 高（SQL语法错误） | 低（类型安全） | 显著降低 |
| 维护成本 | 高（需要SQL知识） | 低（Python接口） | 大幅降低 |
| 数据处理 | 手动转换 | 自动处理 | 零配置 |

## 🚀 最佳实践

1. **优先使用核心方法**: 80%的需求可通过5个核心方法满足
2. **参数化查询**: 使用`custom_query()`时采用参数化查询避免SQL注入
3. **错误处理**: 利用providers内置的错误处理机制
4. **数据缓存**: 利用providers的智能缓存提高性能
5. **类型安全**: 信任providers的自动类型转换

## 📚 更多资源

- [Providers完整API文档](../alphahome/providers/README.md)
- [使用示例](../alphahome/providers/examples/usage_example.py)
- [Jupyter Notebook示例](./templates/database_research/notebooks/02_providers_usage_example.ipynb)

## ❓ 常见问题

**Q: 迁移后性能会受影响吗？**
A: 不会。Providers在底层仍使用相同的数据库连接，并增加了智能缓存和优化。

**Q: 可以混用新旧方式吗？**
A: 可以。迁移是渐进式的，新旧方式可以在同一项目中共存。

**Q: 复杂查询怎么处理？**
A: 使用`custom_query()`方法，支持任意复杂的SQL查询。

**Q: 如何处理特殊的数据表？**
A: Providers会自动检测可用表名，如果有特殊需求可以通过`custom_query()`访问。

# 重构 research/tools/common_steps.py

**日期**: {datetime.now().strftime('%Y-%m-%d')}

## 摘要
旧的 `research/tools/common_steps.py` 文件已被重构，以解决职责不清和设计模式不一致的问题。

## 主要变更
1.  **文件重命名**: `research/tools/common_steps.py` 已被重命名为 `research/tools/legacy_step_helpers.py`。
2.  **移除Step类**: 文件中所有具体的 `Step` 子类（如 `MovingAverageStep`, `RSIStep` 等）已被完全移除。
3.  **废弃工厂函数**: 文件中保留的 `create_*_step` 工厂函数已被标记为“已废弃”，其实现已被替换为会引发 `NotImplementedError` 的代码，以防止被意外使用。

## 动机
-   **职责混淆**: `common_steps.py` 同时包含了 `Step` 类的定义和创建这些类的工厂函数，职责边界模糊。
-   **设计冲突**: 该文件中 `Step` 类的实现方式（将业务逻辑与执行器紧密耦合）与 `research/templates/database_research/` 模板所倡导的最佳实践（逻辑与执行分离）相冲突。
-   **代码冗余**: `common_steps.py` 中的因子计算逻辑与 `database_research` 模板中的 `src/factors.py` 功能重叠，但设计上更差。

## 对开发者的影响和指导
本次重构对 `research/templates/database_research` 项目模板没有直接的破坏性影响，因为它不依赖于被修改的文件。

**对于新项目**:
- **请勿使用** `legacy_step_helpers.py`。
- **请遵循** `research/templates/database_research/` 模板展示的设计模式：
    1.  在 `src/factors.py` 或类似的纯函数库中实现核心计算逻辑。
    2.  在 `src/steps.py` 中创建继承自 `research.tools.pipeline.Step` 的自定义步骤类。
    3.  在 `Step` 类的 `run` 方法中，调用 `factors` 中的纯函数来执行计算。

这种模式可以最大化代码的可重用性、可测试性和可维护性。
