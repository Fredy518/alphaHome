# Schema迁移脚本使用指南

本脚本用于将 `tushare` 和 `system` schema 中的所有表迁移到 `public` schema。

## 功能特性

- **安全性**: 使用数据库事务确保操作的原子性
- **验证**: 迁移前检查表名冲突，迁移后验证结果
- **日志**: 详细的执行日志和进度提示
- **交互式**: 提供用户确认机制，支持强制执行模式
- **集成**: 使用项目现有的数据库管理架构

## 使用方法

### 1. 交互式执行（推荐）

```bash
python migrate_schemas.py
```

脚本会：
1. 检查当前数据库状态
2. 显示迁移计划
3. 检查表名冲突
4. 要求用户确认
5. 执行迁移并验证结果

### 2. 强制执行（跳过确认）

```bash
python migrate_schemas.py --force
```

适用于自动化脚本或确信操作安全的情况。

### 3. 指定数据库URL

```bash
python migrate_schemas.py --db-url "postgresql://username:password@localhost:5432/database"
```

覆盖配置文件中的数据库连接设置。

### 4. 组合参数

```bash
python migrate_schemas.py --force --db-url "postgresql://user:pass@host:port/db"
```

## 迁移过程

### 迁移前检查
- 获取各schema中的表列表
- 检查是否存在表名冲突
- 显示迁移计划和影响范围

### 迁移执行
- 在事务中执行所有 `ALTER TABLE ... SET SCHEMA` 命令
- 逐个迁移表并显示进度
- 发生错误时自动回滚

### 迁移后验证
- 验证表数量是否正确
- 确认原schema已清空
- 生成验证报告

## 安全保障

1. **事务保护**: 所有迁移操作在一个事务中执行，失败时自动回滚
2. **冲突检测**: 迁移前检查 public schema 中是否已存在同名表
3. **状态验证**: 迁移后验证结果的正确性
4. **详细日志**: 记录每个步骤的执行情况

## 错误处理

### 常见错误及解决方案

1. **表名冲突**
   ```
   发现表名冲突: ['table1', 'table2']
   ```
   解决：删除或重命名 public schema 中的冲突表

2. **数据库连接失败**
   ```
   数据库连接失败: connection refused
   ```
   解决：检查数据库服务状态和连接配置

3. **权限不足**
   ```
   permission denied for schema public
   ```
   解决：确保数据库用户有 public schema 的写权限

## 回滚说明

本脚本不提供自动回滚功能。如需回滚，可以手动执行相应的 ALTER TABLE 命令：

```sql
-- 将表移回原schema
ALTER TABLE public.table_name SET SCHEMA original_schema;
```

或者使用数据库备份进行恢复。

## 注意事项

1. **备份**: 执行迁移前建议备份数据库
2. **停机**: 迁移期间建议停止应用程序的数据库访问
3. **权限**: 确保数据库用户有足够的权限执行schema变更
4. **依赖**: 检查是否有外部系统依赖特定的schema名称

## 支持的数据库版本

- PostgreSQL 12+
- 兼容项目现有的数据库配置

## 日志文件

脚本执行日志会输出到控制台，建议将输出重定向到文件：

```bash
python migrate_schemas.py 2>&1 | tee migration.log
```

## 联系支持

如果遇到问题，请检查：
1. 数据库连接配置
2. 用户权限设置
3. 表依赖关系
4. 日志文件中的详细错误信息 