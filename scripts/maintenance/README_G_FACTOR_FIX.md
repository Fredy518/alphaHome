# G因子排名和评分修复工具

## 📋 概述

本工具用于修复数据库中G因子的排名和评分计算问题。当子因子计算结果正确但排名和G评分有误时，可以使用此工具重新计算并更新数据。

## 🔧 修复内容

### 问题描述
- 空值因子被错误排名到底部
- 固定权重计算不考虑空值因子的影响
- 导致G评分计算结果不合理

### 修复方案
1. **排名计算修复**：
   - 空值因子保持为NaN，不参与排名
   - 只对有效值进行百分位排名计算

2. **动态权重计算**：
   ```
   Final_G_Score = (w1×Rank_ES×logic_ES + w2×Rank_EM×logic_EM + w3×Rank_RM×logic_RM + w4×Rank_PM×logic_PM) / (w1×logic_ES + w2×logic_EM + w3×logic_RM + w4×logic_PM)
   ```
   其中 `logic_X = 1 if Rank_X is not null else 0`

3. **空值处理**：
   - 空值因子权重自动调整为0
   - 其他因子权重按比例重新分配
   - 确保计算结果的合理性

## 🚀 使用方法

### Python脚本（推荐）

```bash
# 批量修复指定日期范围
python scripts/maintenance/fix_g_factor_rankings_and_scores.py --start_date 2020-01-01 --end_date 2024-12-31

# 单日修复
python scripts/maintenance/fix_g_factor_rankings_and_scores.py --single_date 2024-01-01

# 试运行模式（不实际更新数据库）
python scripts/maintenance/fix_g_factor_rankings_and_scores.py --start_date 2020-01-01 --end_date 2024-12-31 --dry_run
```

### Windows批处理

```bash
# 批量修复
scripts\maintenance\fix_g_factor_rankings.bat 2020-01-01 2024-12-31
```

## 📊 参数说明

| 参数 | 说明 | 示例 |
|------|------|------|
| `--start_date` | 开始日期 (YYYY-MM-DD) | `2020-01-01` |
| `--end_date` | 结束日期 (YYYY-MM-DD) | `2024-12-31` |
| `--single_date` | 单日修复 (YYYY-MM-DD) | `2024-01-01` |
| `--dry_run` | 试运行模式 | 无值 |

## ⚠️ 注意事项

1. **数据备份**：运行前请确保已备份相关数据
2. **数据库连接**：确保数据库连接正常
3. **权限要求**：需要对 `pgs_factors.g_factor` 表有更新权限
4. **运行环境**：需要在项目根目录下运行

## 📈 修复效果

### 修复前
- 空值因子被排名到底部（0分）
- 固定权重计算：`0.25×Rank_ES + 0.25×Rank_EM + 0.25×Rank_RM + 0.25×Rank_PM`
- 空值因子影响最终评分

### 修复后
- 空值因子不参与排名（保持NaN）
- 动态权重计算：根据有效因子数量调整权重
- 空值因子不影响最终评分
- **重要修复**: 当部分子因子为空值时，G因子不会为0，而是基于有效子因子计算，确保评分的连续性

## 🔍 验证方法

### 1. 检查空值处理
```sql
-- 查看有空值因子的记录
SELECT 
    ts_code, 
    calc_date,
    g_efficiency_surprise,
    g_efficiency_momentum,
    rank_es,
    rank_em,
    g_score
FROM pgs_factors.g_factor 
WHERE (g_efficiency_surprise IS NULL OR g_efficiency_momentum IS NULL)
AND calc_date >= '2024-01-01'
LIMIT 10;
```

### 2. 验证权重计算
```sql
-- 检查权重分配是否合理
SELECT 
    ts_code,
    calc_date,
    CASE WHEN g_efficiency_surprise IS NULL THEN 0 ELSE 1 END as has_es,
    CASE WHEN g_efficiency_momentum IS NULL THEN 0 ELSE 1 END as has_em,
    CASE WHEN g_revenue_momentum IS NULL THEN 0 ELSE 1 END as has_rm,
    CASE WHEN g_profit_momentum IS NULL THEN 0 ELSE 1 END as has_pm,
    g_score
FROM pgs_factors.g_factor 
WHERE calc_date >= '2024-01-01'
LIMIT 10;
```

## 📝 日志说明

修复过程中会输出详细日志：
- 数据获取情况
- 排名计算进度
- 数据库更新结果
- 错误和警告信息

## 🛠️ 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查数据库配置
   - 确认网络连接
   - 验证用户权限

2. **更新失败**
   - 检查表结构
   - 确认字段类型
   - 查看错误日志

3. **性能问题**
   - 分批处理大量数据
   - 调整数据库连接池
   - 监控系统资源

## 📞 技术支持

如遇到问题，请：
1. 查看错误日志
2. 检查数据库状态
3. 联系技术支持团队
