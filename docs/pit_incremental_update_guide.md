# PIT数据增量更新指南

## 📋 概述

PIT数据增量更新系统提供了完整的增量更新解决方案，支持财务数据、财务指标和行业分类的自动化更新。

## 🏗️ 系统架构

```
PIT增量更新系统
├── 增量更新管理器 (incremental_update_manager.py)
│   ├── 统一调度和协调
│   └── 跨组件依赖管理
├── 财务数据同步器 (pit_financial_data_syncer.py)
│   ├── tushare数据增量同步
│   └── PIT表数据更新
├── 行业分类更新器 (pit_industry_updater.py)
│   ├── 月度快照生成
│   └── 行业变更检测
└── 更新调度器 (incremental_update_scheduler.py)
    ├── 日/周/月更新调度
    └── 手动更新支持
```

## 🔄 更新策略

### 1. 日更新 (Daily Update)
**频率**: 每日执行  
**内容**: 财务数据 + 财务指标  
**适用**: 日常维护，保持数据最新

```bash
# 执行日更新
python scripts/pit/incremental_update_scheduler.py --mode daily

# 指定日期的日更新
python scripts/pit/incremental_update_scheduler.py --mode daily --target-date 2025-08-11
```

### 2. 周更新 (Weekly Update)
**频率**: 每周执行  
**内容**: 全量检查 + 补充更新 + 数据验证  
**适用**: 全面检查，补充遗漏数据

```bash
# 执行周更新
python scripts/pit/incremental_update_scheduler.py --mode weekly
```

### 3. 月更新 (Monthly Update)
**频率**: 每月执行  
**内容**: 行业分类快照 + 全面验证 + 性能检查  
**适用**: 行业快照生成，系统健康检查

```bash
# 执行月更新
python scripts/pit/incremental_update_scheduler.py --mode monthly
```

### 4. 手动更新 (Manual Update)
**频率**: 按需执行  
**内容**: 指定范围的强制更新  
**适用**: 数据修复、历史回填、特殊需求

```bash
# 手动更新财务数据和指标
python scripts/pit/incremental_update_scheduler.py --mode manual \
    --start-date 2025-08-01 --end-date 2025-08-11 \
    --financial-data --financial-indicators

# 手动更新行业分类
python scripts/pit/incremental_update_scheduler.py --mode manual \
    --start-date 2025-08-01 --end-date 2025-08-11 \
    --industry-classification
```

## 📊 组件详解

### PIT财务数据同步器

**功能**: 从tushare增量同步财务数据到PIT表

**支持的表**:
- `pit_income_quarterly` (利润表)
- `pit_balance_quarterly` (资产负债表)  
- `pit_cashflow_quarterly` (现金流量表)

**使用示例**:
```python
from scripts.pit.pit_financial_data_syncer import PITFinancialDataSyncer

with PITFinancialDataSyncer() as syncer:
    # 增量同步单表
    result = syncer.sync_incremental('pit_income_quarterly', '2025-08-01', '2025-08-11')
    
    # 增量同步所有表
    result = syncer.sync_all_tables_incremental('2025-08-01', '2025-08-11')
    
    # 获取同步状态
    status = syncer.get_sync_status()
```

### PIT行业分类更新器

**功能**: 基于月度快照机制更新行业分类

**特点**:
- 月末快照生成
- 申万和中信双重分类
- 自动检测行业变更
- 金融行业特殊处理

**使用示例**:
```python
from scripts.pit.pit_industry_updater import PITIndustryUpdater

with PITIndustryUpdater() as updater:
    # 更新月度快照
    result = updater.update_monthly_snapshots()
    
    # 基于行业变更更新
    result = updater.update_industry_changes(since_date='2025-08-01')
    
    # 获取更新状态
    status = updater.get_update_status()
```

### PIT财务指标计算器

**功能**: 基于最新财务数据增量计算财务指标

**特点**:
- 使用MVP计算器高性能计算
- 自动识别需要更新的股票
- 支持批量计算和保存

## 🛠️ 配置和部署

### 1. 环境要求
- Python 3.8+
- PostgreSQL 12+
- 足够的磁盘空间 (建议预留100GB+)

### 2. 依赖安装
```bash
pip install pandas psycopg2-binary python-dateutil
```

### 3. 数据库配置
确保数据库连接配置正确，参考 `research/tools/context.py`

### 4. 日志配置
日志文件保存在 `logs/pit_incremental_update_YYYYMMDD.log`

## 📅 定时任务设置

### Linux/Mac (crontab)
```bash
# 每日凌晨2点执行日更新
0 2 * * * cd /path/to/alphahome && python scripts/pit/incremental_update_scheduler.py --mode daily

# 每周日凌晨3点执行周更新
0 3 * * 0 cd /path/to/alphahome && python scripts/pit/incremental_update_scheduler.py --mode weekly

# 每月1号凌晨4点执行月更新
0 4 1 * * cd /path/to/alphahome && python scripts/pit/incremental_update_scheduler.py --mode monthly
```

### Windows (任务计划程序)
1. 打开任务计划程序
2. 创建基本任务
3. 设置触发器 (每日/每周/每月)
4. 设置操作: 启动程序
   - 程序: `python`
   - 参数: `scripts/pit/incremental_update_scheduler.py --mode daily`
   - 起始位置: `C:\path\to\alphahome`

## 🔍 监控和维护

### 1. 日志监控
```bash
# 查看最新日志
tail -f logs/pit_incremental_update_$(date +%Y%m%d).log

# 搜索错误
grep -i error logs/pit_incremental_update_*.log
```

### 2. 数据验证
```python
# 检查数据完整性
from research.tools.context import ResearchContext

with ResearchContext() as ctx:
    # 检查最新数据日期
    result = ctx.query_dataframe("""
        SELECT 
            'pit_income_quarterly' as table_name,
            MAX(ann_date) as latest_date,
            COUNT(*) as record_count
        FROM pgs_factors.pit_income_quarterly
        UNION ALL
        SELECT 
            'pit_financial_indicators_mvp' as table_name,
            MAX(ann_date) as latest_date,
            COUNT(*) as record_count
        FROM pgs_factors.pit_financial_indicators_mvp
    """)
    print(result)
```

### 3. 性能监控
- 监控更新耗时
- 检查数据库连接数
- 观察磁盘空间使用

## ⚠️ 注意事项

### 1. 数据一致性
- 增量更新过程中避免并发修改
- 定期进行全量数据验证
- 保持tushare数据源的稳定性

### 2. 错误处理
- 网络异常时自动重试
- 数据异常时记录日志但继续处理
- 关键错误时发送告警通知

### 3. 资源管理
- 控制并发数量避免数据库压力过大
- 合理设置批次大小
- 定期清理过期日志文件

## 🚀 最佳实践

1. **渐进式部署**: 先在测试环境验证，再部署到生产环境
2. **监控告警**: 设置关键指标的监控告警
3. **备份策略**: 重要更新前进行数据备份
4. **文档维护**: 及时更新配置和流程文档
5. **定期检查**: 每月检查系统运行状态和数据质量

## 📞 故障排除

### 常见问题

**Q: 更新失败，提示数据库连接错误**  
A: 检查数据库连接配置，确认数据库服务正常运行

**Q: 财务指标计算很慢**  
A: 检查数据库索引，考虑调整批次大小

**Q: 行业分类快照生成失败**  
A: 检查tushare行业数据是否正常，验证日期范围设置

**Q: 内存使用过高**  
A: 减小批次大小，增加分批处理

### 联系支持
如遇到无法解决的问题，请提供:
1. 错误日志
2. 系统环境信息
3. 数据库状态
4. 复现步骤
