# 财务数据处理技术指南

## 📋 概述

本文档详细记录了AlphaHome系统中财务数据处理的核心逻辑、边界情况处理策略、以及重要的技术细节。这些规则和方法经过实际验证，用于确保PIT（Point-in-Time）数据库的数据质量和准确性。

**⚠️ 重要提醒**: 本文档包含关键的数据处理逻辑，任何修改都可能影响因子计算的准确性，请谨慎操作。

---

## 🎯 核心原则

### 1. Point-in-Time (PIT) 数据库设计原理

**目标**: 确保历史数据查询只返回在特定时点已知的信息，避免前瞻偏差。

**实现方式**:
- 所有财务数据按`ann_date`（公告日期）存储
- 查询时使用`ann_date <= 查询日期`条件
- 集成多数据源：财务报告(report)、业绩快报(express)、业绩预告(forecast)

### 2. 数据一致性原则

**单位统一**: 所有PIT库中的财务数据统一为**元**单位
**时间语义**: `n_income`和`n_income_attr_p`等字段统一为**单季度值**，便于TTM计算
**数据完整性**: 优先级 report > express > forecast
**数据源特性**: 
- **Report**: 包含完整的损益表数据，`n_income`和`n_income_attr_p`均为单季值
- **Express**: 仅包含净利润数据，`n_income_attr_p`为单季值，`n_income`为NULL
- **Forecast**: 仅包含归母净利润预告，`n_income_attr_p`为单季值，`n_income`为NULL

---

## 💰 Tushare数据源单位差异

### 🚨 关键单位规则

| 数据源 | 表名 | 单位 | 转换需求 |
|--------|------|------|----------|
| 财务报告 | `tushare.fina_income` | **元** | ❌ 无需转换 |
| 业绩快报 | `tushare.fina_express` | **元** | ❌ 无需转换 |
| 业绩预告 | `tushare.fina_forecast` | **万元** | ✅ 需要 `× 10000` |
| 资产负债表 | `tushare.fina_balancesheet` | **元** | ❌ 无需转换 |

### 📝 单位转换代码示例

```sql
-- ✅ 正确：仅对forecast数据进行单位转换
-- Forecast数据处理
SELECT 
    ts_code,
    end_date,
    ann_date,
    CASE 
        WHEN net_profit_min IS NOT NULL AND net_profit_min != 'NaN' THEN 
            (net_profit_min + net_profit_max) / 2.0 * 10000  -- 万元 → 元
        WHEN last_parent_net IS NOT NULL AND last_parent_net != 'NaN' THEN
            last_parent_net * (1 + (p_change_min + p_change_max)/200.0) * 10000  -- 万元 → 元
    END as net_profit_mid
FROM tushare.fina_forecast

-- ✅ 正确：Report数据无需单位转换
SELECT 
    ts_code,
    end_date, 
    ann_date,
    n_income_attr_p  -- 已经是元，无需转换
FROM tushare.fina_income
```

---

## 🎛️ 边界情况处理策略

### 📅 不同历史时期的季报模式

中国上市公司的财务报告经历了不同的发展阶段：

#### 1. 早期阶段 (典型: 2011-2013)
**特征**: 仅有年报
**数据模式**: `quarters = [4]`
**处理策略**: 
```python
if quarters == [4]:  # 仅年报
    # 年报数据直接作为年度总值，无法拆分季度
    result.loc[mask & (result['quarter'] == 4), f'{field}_single'] = \
        group[group['quarter'] == 4][field].iloc[0]
    logger.debug(f"{ts_code} {year}年仅有年报，Q4作为年度值处理")
```

**⚠️ 注意事项**:
- Q4值代表**全年总值**，非单季度值
- 无法进行季度间拆分
- 适用于TTM计算时的年度数据

#### 2. 过渡阶段 (典型: 2014)
**特征**: 中报 + 年报
**数据模式**: `quarters = [2, 4]`
**处理策略**:
```python
elif quarters == [2, 4]:  # 中报+年报
    q2_cumulative = group[group['quarter'] == 2][field].iloc[0]  # 上半年累计
    q4_cumulative = group[group['quarter'] == 4][field].iloc[0]  # 全年累计
    
    # Q2无法拆分，只能作为上半年总值
    result.loc[mask & (result['quarter'] == 2), f'{field}_single'] = q2_cumulative
    # Q4 = 全年 - 上半年
    result.loc[mask & (result['quarter'] == 4), f'{field}_single'] = q4_cumulative - q2_cumulative
```

**⚠️ 关键问题解决**:
- **错误做法**: `Q4单季 = Q4累计 - Q2累计` （跨期计算错误）
- **正确做法**: `Q2 = 上半年总值`, `Q4单季 = 全年 - 上半年`
- **数据语义**: Q2代表上半年业绩，Q4代表下半年业绩

#### 3. 成熟阶段 (典型: 2015+)
**特征**: 完整季报
**数据模式**: `quarters = [1, 2, 3, 4]` 或 `len(quarters) >= 3`
**处理策略**:
```python
elif len(quarters) >= 3:  # 完整季报或接近完整
    prev_cumulative = 0.0
    for quarter in quarters:
        current_cumulative = group[group['quarter'] == quarter][field].iloc[0]
        if quarter == 1:
            single_value = current_cumulative  # Q1 = Q1累计
        else:
            single_value = current_cumulative - prev_cumulative  # Qn = Qn累计 - Q(n-1)累计
        
        result.loc[mask & (result['quarter'] == quarter), f'{field}_single'] = single_value
        prev_cumulative = current_cumulative
```

### 🚨 异常情况处理

#### 不规则季报模式
**场景**: `quarters = [1]`, `quarters = [1, 3]` 等异常情况
**处理策略**:
```python
else:  # 其他不规则情况
    # 保守处理：直接使用累计值，标记为异常
    for quarter in quarters:
        cumulative_value = group[group['quarter'] == quarter][field].iloc[0]
        result.loc[mask & (result['quarter'] == quarter), f'{field}_single'] = cumulative_value
    logger.warning(f"{ts_code} {year}年季报模式异常: {quarters}，使用累计值")
```

#### 数据质量问题
**零值处理**: 上市前数据可能存在大量零值，但不影响单季化逻辑
**缺失数据**: 使用`fillna(0.0)`确保数值计算的稳定性
**异常值**: 负的单季度值可能表明会计调整或重述

---

## 🔄 单季化转换逻辑

### 核心算法实现

```python
def _convert_to_single_quarter(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """将累计数据转换为单季数据 - 增强版，处理边界情况"""
    
    # 需要单季化的字段列表
    cumulative_fields = [value_col, 'revenue', 'operate_profit', 'total_profit', 
                         'income_tax', 'oper_cost', 'total_cogs', 'fin_exp', 'interest_expense']
    
    for field in cumulative_fields:
        if field in result.columns:
            # 按股票和年份分组处理
            for (ts_code, year), group in result.groupby(['ts_code', 'year']):
                quarters = sorted(group['quarter'].tolist())
                
                # 根据季报模式选择处理策略
                if quarters == [4]:           # 仅年报模式
                    # 实现细节见上文
                elif quarters == [2, 4]:      # 中报+年报模式  
                    # 实现细节见上文
                elif len(quarters) >= 3:      # 完整季报模式
                    # 实现细节见上文
                else:                         # 异常情况
                    # 实现细节见上文
```

### 🎯 字段处理优先级

1. **主要盈利指标**: `n_income_attr_p` (归母净利润)
2. **收入成本**: `revenue`, `oper_cost`, `total_cogs`
3. **利润层级**: `operate_profit`, `total_profit`
4. **税费支出**: `income_tax`, `fin_exp`, `interest_expense`

### 🎯 数据精度处理

**精度标准**: 所有财务数据保留**2位小数**，确保数据质量和一致性

```python
# 示例：数据入库时的精度处理
round(float(row.get('n_income_attr_p') or 0), 2)
round(float(row.get('revenue') or 0), 2)
round(float(row.get('operate_profit') or 0), 2)
```

**优势**:
- 减少浮点数精度误差
- 提高数据可读性
- 确保不同计算路径结果一致

---

## 📊 业绩预告特殊处理

### 预告数据的两种模式

#### 1. 直接净利润型 (Direct)
**特征**: `net_profit_min`, `net_profit_max` 有具体数值
**处理**:
```sql
CASE 
    WHEN net_profit_min IS NOT NULL AND net_profit_min != 'NaN' THEN 
        (net_profit_min + net_profit_max) / 2.0 * 10000  -- 万元转元
END as net_profit_mid
```

#### 2. 同比变化型 (Calculated)
**特征**: `net_profit_min/max` 为空，但有`p_change_min/max`和`last_parent_net`
**处理**:
```sql
CASE
    WHEN last_parent_net IS NOT NULL AND last_parent_net != 'NaN' THEN
        last_parent_net * (1 + (p_change_min + p_change_max)/200.0) * 10000
END as net_profit_mid
```

**⚠️ 单位注意**:
- `last_parent_net`: **万元**单位
- 计算结果需 `× 10000` 转换为元

### 🎯 Forecast单季化计算

**核心逻辑**: Forecast预告的是归母净利润，需要转换为单季值
```python
def _compute_forecast_single_quarter(self, forecast_df, report_df):
    """计算forecast的单季归母净利润数据
    
    重要概念修正：
    - forecast预告的net_profit范围指的是"归母净利润"，对应n_income_attr_p字段
    - 计算单季值时应该用forecast预告值减去report的n_income_attr_p累计值
    """
    
    # Q1特例：单季 = 预告中值
    # Q2/3/4：单季 = 预告中值 - 年内已披露report单季累计（至上一季）
    
    # 计算逻辑：
    # merged['n_income_attr_p_single'] = merged.apply(
    #     lambda row: (
    #         float(row['net_profit_mid']) if row['quarter'] == 1
    #         else float(row['net_profit_mid']) - float(row['ytd_sum_before'])
    #     ), axis=1
    # )
```

**数据源特性**:
- **Report**: 包含完整的损益表数据，`n_income`和`n_income_attr_p`均为单季值
- **Express**: 仅包含净利润数据，`n_income_attr_p`为单季值，`n_income`为NULL  
- **Forecast**: 仅包含归母净利润预告，`n_income_attr_p`为单季值，`n_income`为NULL

### PostgreSQL vs Pandas: NULL/NaN处理差异

#### 🚨 关键差异总结

| 系统 | NULL概念 | NaN概念 | 比较行为 | 处理建议 |
|------|----------|---------|----------|----------|
| **PostgreSQL** | `NULL` | `'NaN'`字符串 | `NaN IS NOT NULL` = True | 同时检查`IS NULL`和`!= 'NaN'` |
| **Pandas** | `None`/`NaN` | `np.nan` | `pd.isna(NaN)` = True | 使用`pd.isna()`或`pd.notna()` |

#### 📝 PostgreSQL NaN处理

**核心问题**: PostgreSQL将字符串`'NaN'`视为有效值，不等于`NULL`

**错误示例**:
```sql
-- ❌ 错误：NaN字符串不会被过滤
SELECT * FROM table WHERE value IS NOT NULL;
-- 结果：包含 'NaN' 字符串的记录

-- ❌ 错误：NaN字符串通过了检查
SELECT * FROM table WHERE value IS NOT NULL AND value > 0;
-- 结果：PostgreSQL报错 "invalid input syntax for type numeric"
```

**正确处理**:
```sql
-- ✅ 正确：同时排除NULL和NaN字符串
SELECT * FROM table 
WHERE value IS NOT NULL 
  AND value != 'NaN' 
  AND value::numeric > 0;

-- ✅ 正确：复杂条件的NaN处理
WHERE (net_profit_min IS NOT NULL AND net_profit_min != 'NaN')
   OR (last_parent_net IS NOT NULL AND last_parent_net != 'NaN' 
       AND p_change_min IS NOT NULL AND p_change_min != 'NaN')
```

#### 🐍 Pandas NaN处理

**核心概念**: Pandas使用`np.nan`表示缺失值，具有特殊的比较语义

**错误示例**:
```python
# ❌ 错误：NaN != NaN 总是返回True
df[df['value'] != np.nan]  # 永远返回所有行

# ❌ 错误：不会过滤NaN值
df[df['value'] is not None]  # None != np.nan
```

**正确处理**:
```python
# ✅ 正确：使用pandas的专用函数
df[df['value'].notna()]  # 过滤掉NaN值
df[df['value'].isna()]   # 只选择NaN值

# ✅ 正确：数值转换时的NaN处理
df['value'] = pd.to_numeric(df['value'], errors='coerce')  # 无效值转为NaN
df = df[df['value'].notna()]  # 移除转换失败的行

# ✅ 正确：替换NaN值
df['value'] = df['value'].fillna(0.0)  # 用0替换NaN
```

#### 🔄 跨系统数据传递

**Pandas → PostgreSQL**:
```python
# ❌ 问题：Pandas的NaN被转换为'NaN'字符串
df.to_sql('table', con, if_exists='append')  # NaN变成'NaN'字符串

# ✅ 解决：预处理NaN值
df_clean = df.copy()
df_clean = df_clean.where(df_clean.notna(), None)  # NaN → None → NULL
df_clean.to_sql('table', con, if_exists='append')
```

**PostgreSQL → Pandas**:
```python
# ✅ 正确：查询时过滤'NaN'字符串
query = """
SELECT * FROM table 
WHERE value IS NOT NULL AND value != 'NaN'
"""
df = pd.read_sql(query, con)

# ✅ 正确：加载后处理'NaN'字符串
df = pd.read_sql("SELECT * FROM table", con)
df = df.replace('NaN', np.nan)  # 字符串'NaN' → np.nan
df = df.where(df.notna(), None)  # 标准化缺失值处理
```

#### 🎯 实战案例：PIT数据处理

```python
def process_forecast_data(self):
    """业绩预告数据处理的NaN处理示例"""
    
    # SQL查询：正确过滤PostgreSQL中的'NaN'
    query = """
    SELECT ts_code, end_date, ann_date,
           net_profit_min, net_profit_max,
           p_change_min, p_change_max, last_parent_net,
           CASE 
               WHEN net_profit_min IS NOT NULL AND net_profit_min != 'NaN' THEN 'direct'
               WHEN last_parent_net IS NOT NULL AND last_parent_net != 'NaN' 
                    AND p_change_min IS NOT NULL AND p_change_min != 'NaN' THEN 'calculated'
               ELSE 'invalid'
           END as data_source_type
    FROM tushare.fina_forecast
    WHERE ts_code IN %s
      AND ((net_profit_min IS NOT NULL AND net_profit_min != 'NaN')
           OR (last_parent_net IS NOT NULL AND last_parent_net != 'NaN' 
               AND p_change_min IS NOT NULL AND p_change_min != 'NaN'))
    """
    
    # Pandas处理：标准化NaN处理
    df = pd.read_sql(query, self.connection, params=[tuple(stocks)])
    
    # 确保数值列的正确转换
    numeric_cols = ['net_profit_min', 'net_profit_max', 'p_change_min', 'p_change_max', 'last_parent_net']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 最终检查：移除转换失败的行
    df = df[df[numeric_cols].notna().any(axis=1)]
    
    return df
```

#### ⚠️ 常见陷阱

1. **字符串'NaN'陷阱**:
   ```sql
   -- PostgreSQL中'NaN'是有效字符串，不等于NULL
   SELECT 'NaN' IS NULL;  -- 返回False
   SELECT 'NaN' = 'NaN';  -- 返回True
   ```

2. **数值转换陷阱**:
   ```python
   # Pandas中字符串'NaN'不等于np.nan
   pd.Series(['1', '2', 'NaN']).astype(float)  # 'NaN' → NaN
   pd.Series([1, 2, 'NaN']).replace('NaN', np.nan)  # 显式替换
   ```

3. **条件判断陷阱**:
   ```python
   # 错误的NaN检查
   value = np.nan
   if value == np.nan:  # 永远为False
       print("This never executes")
   
   # 正确的NaN检查
   if pd.isna(value):   # 正确
       print("This works")
   ```

---

## 🗄️ 数据库架构设计

### PIT表结构

#### pit_income_quarterly
```sql
CREATE TABLE pgs_factors.pit_income_quarterly (
    ts_code VARCHAR(10) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    data_source VARCHAR(20) NOT NULL,  -- 'report', 'express', 'forecast'
    year INTEGER,
    quarter INTEGER,
    
    -- 核心盈利指标 (单季度值，元单位，保留2位小数)
    n_income DOUBLE PRECISION,          -- 净利润 (单季，仅report有值)
    n_income_attr_p DOUBLE PRECISION,   -- 归母净利润 (单季，所有数据源)
    net_profit_mid DOUBLE PRECISION,    -- 预告净利润中值 (仅forecast)
    conversion_status VARCHAR(20),      -- 单季化状态标记
    
    -- 收入成本指标 (单季度值，元单位，保留2位小数，仅report)
    revenue DOUBLE PRECISION,           -- 营业收入
    oper_cost DOUBLE PRECISION,         -- 营业成本
    total_cogs DOUBLE PRECISION,        -- 营业总成本
    
    -- 利润层级指标 (单季度值，元单位，保留2位小数，仅report)
    operate_profit DOUBLE PRECISION,    -- 营业利润
    total_profit DOUBLE PRECISION,      -- 利润总额
    
    -- 税费支出指标 (单季度值，元单位，保留2位小数，仅report)
    income_tax DOUBLE PRECISION,        -- 所得税费用
    fin_exp DOUBLE PRECISION,           -- 财务费用
    interest_expense DOUBLE PRECISION,  -- 利息支出
    
    PRIMARY KEY (ts_code, end_date, ann_date, data_source)
);
```

#### pit_balance_quarterly
```sql
CREATE TABLE pgs_factors.pit_balance_quarterly (
    ts_code VARCHAR(10) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    data_source VARCHAR(20) NOT NULL,
    year INTEGER,
    quarter INTEGER,
    
    -- 资产负债指标 (时点值，元单位)
    tot_assets DOUBLE PRECISION,        -- 总资产
    tot_liab DOUBLE PRECISION,          -- 总负债  
    tot_equity DOUBLE PRECISION,        -- 所有者权益
    total_cur_assets DOUBLE PRECISION,  -- 流动资产
    total_cur_liab DOUBLE PRECISION,    -- 流动负债
    inventories DOUBLE PRECISION,       -- 存货
    
    PRIMARY KEY (ts_code, end_date, ann_date, data_source)
);
```

### 视图设计

```sql
CREATE VIEW pgs_factors.v_pit_financial_quarterly AS 
SELECT 
    i.ts_code,
    i.end_date,
    i.ann_date,
    i.data_source,
    i.year,
    i.quarter,
    
    -- 损益表数据 (单季度值)
    i.n_income,
    i.n_income_attr_p,
    i.revenue,
    i.operate_profit,
    i.total_profit,
    
    -- 资产负债表数据 (时点值)
    b.tot_assets,
    b.tot_liab,
    b.tot_equity
    
FROM pgs_factors.pit_income_quarterly i
LEFT JOIN pgs_factors.pit_balance_quarterly b 
    ON i.ts_code = b.ts_code 
    AND i.end_date = b.end_date 
    AND i.ann_date = b.ann_date
    AND i.data_source = b.data_source;
```

---

## 🚀 Smart TTM 计算逻辑

### TTM (Trailing Twelve Months) 原理

**目标**: 计算最近12个月的净利润，支持ROE等因子计算
**数据源优先级**: Report > Express > Forecast
**计算方法**: 取最近4个单季度值求和

### Smart TTM实现

```python
def calculate_smart_ttm_profit(self, ts_code: str, as_of_date: str) -> float:
    """计算Smart TTM净利润"""
    
    # 获取最近4个季度的数据，按数据源优先级排序
    query = """
    WITH latest_data AS (
        SELECT DISTINCT ON (ts_code, end_date)
            ts_code, end_date, ann_date, 
            n_income_attr_p as profit,
            CASE data_source
                WHEN 'report' THEN 1
                WHEN 'express' THEN 2
                WHEN 'forecast' THEN 3
                ELSE 5
            END as priority
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = %s 
          AND ann_date <= %s
          AND n_income_attr_p IS NOT NULL
        ORDER BY ts_code, end_date, priority, ann_date DESC
    )
    SELECT * FROM latest_data 
    ORDER BY end_date DESC 
    LIMIT 4
    """
    
    # 执行查询并计算TTM
    quarters = self.pit_db.query(query, (ts_code, as_of_date))
    if len(quarters) >= 4:
        return sum(q['profit'] for q in quarters)
    else:
        return None  # 数据不足
```

---

## ⚠️ 常见陷阱与解决方案

### 1. 单位混淆问题

**❌ 错误做法**:
```python
# 对所有数据源都进行单位转换
forecast_value * 10000  # 错误：express和report也被转换了
```

**✅ 正确做法**:
```python
# 仅对forecast数据进行单位转换
if data_source == 'forecast':
    value_in_yuan = value_in_wan_yuan * 10000
else:  # report, express已经是元
    value_in_yuan = value
```

### 2. 边界情况处理不当

**❌ 错误做法**:
```python
# 简单diff，忽略边界情况
single_quarter = df.groupby(['ts_code', 'year'])['n_income'].diff()
```

**✅ 正确做法**:
```python
# 根据季报模式分别处理
if quarters == [4]:           # 仅年报
    # 特殊处理
elif quarters == [2, 4]:      # 中报+年报  
    # 特殊处理
elif len(quarters) >= 3:      # 完整季报
    # 标准处理
```

### 3. PostgreSQL与Pandas的NULL/NaN处理差异

**❌ 错误做法**:
```sql
-- PostgreSQL: NaN字符串不等于NULL
WHERE net_profit_min IS NOT NULL  -- NaN字符串不会被过滤
```
```python
# Pandas: 错误的NaN比较
df[df['value'] != np.nan]  # 永远返回所有行
df[df['value'] is not None]  # None不等于np.nan
```

**✅ 正确做法**:
```sql  
-- PostgreSQL: 同时检查NULL和'NaN'字符串
WHERE net_profit_min IS NOT NULL AND net_profit_min != 'NaN'
```
```python
# Pandas: 使用专用函数
df[df['value'].notna()]  # 正确过滤NaN
df['value'] = pd.to_numeric(df['value'], errors='coerce')  # 安全转换
```

**🔄 跨系统数据传递**:
```python
# Pandas → PostgreSQL: 预处理NaN
df_clean = df.where(df.notna(), None)  # NaN → None → NULL

# PostgreSQL → Pandas: 查询时过滤或加载后处理
df = df.replace('NaN', np.nan)  # 字符串'NaN' → np.nan
```

### 4. 无效字段清理

**❌ 问题场景**:
```python
# 保留无用的字段处理逻辑
cumulative_fields = ['n_income', 'revenue', 'n_income_attr_p_non_recurring']  # 扣非字段不存在
```

**✅ 正确做法**:
```python
# 只处理确实存在的有效字段
cumulative_fields = ['n_income', 'revenue', 'operate_profit', 'total_profit', 
                     'income_tax', 'oper_cost', 'total_cogs', 'fin_exp', 'interest_expense']
```

**🧹 字段清理原则**:
- 确认数据源中确实存在的字段
- 移除无用或不存在的字段引用
- 保持代码与实际数据结构一致
- 定期审查和清理过时的字段处理逻辑

### 5. 数据覆盖问题

**❌ 错误做法**:
```python
# 覆盖新数据
INSERT INTO pit_table VALUES (...)  -- 重复主键报错
```

**✅ 正确做法**:
```python
# 使用UPSERT
INSERT INTO pit_table VALUES (...) 
ON CONFLICT (ts_code, end_date, ann_date, data_source) 
DO UPDATE SET ...
```

---

## 📋 数据质量检查清单

### 完整性检查
- [ ] PIT表是否包含所有必要字段
- [ ] 单季度值是否合理（无异常负值）
- [ ] 单位是否统一为元
- [ ] 数据源标记是否正确

### 一致性检查  
- [ ] 同一期间不同数据源的一致性
- [ ] 边界情况处理是否符合预期
- [ ] TTM计算结果是否合理

### 性能检查
- [ ] 索引是否正确创建
- [ ] 查询性能是否满足要求
- [ ] 批量处理是否高效

---

## 🔄 维护和更新

### 定期维护任务

1. **数据源变更监控**: 定期检查Tushare数据格式是否变更
2. **边界情况审查**: 新发现的异常季报模式需要及时处理
3. **性能优化**: 随数据量增长优化查询和索引
4. **文档更新**: 发现新问题时及时更新本文档

### 故障排查指南

1. **数据异常**: 检查单位转换和边界情况处理
2. **性能问题**: 分析查询计划和索引使用
3. **逻辑错误**: 验证TTM计算和单季化逻辑
4. **数据缺失**: 检查数据源和ETL流程

---

## 📚 相关文档

- [新任务开发指南](../new_task_development_guide.md)
- [系统架构概览](../architecture/system_overview.md)  
- [任务系统设计](../architecture/task_system.md)
- [数据质量指南](../business/data_quality.md)

---

**📝 文档版本**: v1.1  
**📅 最后更新**: 2025-08-08  
**👤 维护者**: AlphaHome开发团队  
**🔄 更新频率**: 根据业务需求和问题发现情况

### 🆕 最新更新 (v1.1)
- **单季口径统一**: 明确所有数据源的`n_income`和`n_income_attr_p`均为单季值
- **数据源特性说明**: 详细说明Report/Express/Forecast各数据源的字段覆盖情况
- **Forecast单季化逻辑**: 新增Forecast预告数据的单季化计算方法和代码示例
- **数据库架构更新**: 更新PIT表结构，增加`conversion_status`字段和字段覆盖说明

---

> ⚠️ **重要提醒**: 本文档包含关键的财务数据处理逻辑，任何修改都必须经过充分测试。建议在修改前备份现有数据，并在测试环境中验证新逻辑的正确性。
