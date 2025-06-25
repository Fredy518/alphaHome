# 数据迁移脚本

本目录包含数据库迁移脚本，用于处理历史数据问题和表结构变更。

## fina_indicator_001_fix_primary_key.py

### 目的
修复财务指标表（fina_indicator）的历史数据问题，这些问题之前在任务执行时处理，现在移到专门的迁移脚本中。

### 处理的问题
1. **空的 ann_date 字段**：使用 end_date 填充空的 ann_date
2. **重复记录**：删除基于主键的重复记录，保留最新的记录
3. **主键约束**：更新主键约束以包含 ann_date 字段

### 使用方法

#### 方法1：直接运行脚本
```bash
export DATABASE_URL="postgresql://user:password@localhost/dbname"
python alphahome/migrations/fina_indicator_001_fix_primary_key.py
```

#### 方法2：在代码中调用
```python
from alphahome.migrations.fina_indicator_001_fix_primary_key import FinaIndicatorMigration

# 假设你有数据库连接
migration = FinaIndicatorMigration(db_connection)
await migration.migrate()
```

### 安全性
- 脚本会先检查表是否存在
- 使用临时表确保数据安全
- 包含详细的日志记录
- 出错时会回滚操作

### 注意事项
- 建议在生产环境运行前先在测试环境验证
- 大表迁移可能需要较长时间
- 建议在低峰期执行

## 重构说明

### 重构前的问题
`TushareFinaIndicatorTask` 类的 `pre_execute` 方法包含了176行复杂的数据库操作代码，违反了单一职责原则。

### 重构后的改进
1. **职责分离**：数据迁移逻辑移到专门的迁移脚本
2. **代码简化**：任务类从534行减少到294行
3. **架构一致**：与其他财务任务保持一致的实现方式
4. **维护性提升**：代码更清晰，易于理解和修改

### 功能保持
重构后所有原有功能都得到保持：
- ✅ ann_date 空值处理
- ✅ 数据类型转换
- ✅ 批次生成（使用标准化工具）
- ✅ 主键约束管理（通过迁移脚本）
