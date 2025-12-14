## rawdata Schema 自动视图映射系统 - 实施总结

**提交 ID**: 8d874e3

### 📋 实施清单

#### ✅ 已完成

1. **schema_management_mixin.py 增强** (30分钟)
   - ✓ 添加 `create_rawdata_view()` 方法
     - 支持跨 schema 创建视图
     - 自动添加 AUTO_MANAGED COMMENT 标记
     - 支持 OR REPLACE 模式（tushare 专用）
   - ✓ 添加 `check_table_exists()` 方法
     - 高效查询表存在性
     - 支持任意 schema
   - ✓ 添加 `get_tables_in_schema()` 方法
     - 获取 schema 中的所有表
     - 支持批量操作

2. **BaseTask 集成** (30分钟)
   - ✓ 修改 `_ensure_table_exists()` 方法
     - 表创建后自动调用视图创建
     - 保持现有的错误处理逻辑
   - ✓ 实现 `_create_rawdata_view_if_needed()` 方法
     - tushare 优先级覆盖策略（OR REPLACE）
     - 其他数据源的优先级保护（双重检查）
     - 完整的日志记录
     - 失败不中断数据采集（try-catch，只记录警告）

3. **系统初始化** (15分钟)
   - ✓ 在 DBManagerCore 的 `connect()` 方法中添加初始化
   - ✓ 新增 `_initialize_rawdata_schema()` 方法
     - 创建 rawdata schema
     - 添加 schema 文档说明
     - 系统启动时自动调用
     - 失败不中断数据库连接

4. **批量迁移脚本** (45分钟)
   - ✓ 创建 `scripts/migrate_existing_tables_to_rawdata.py`
     - 扫描所有数据源 schema（tushare, akshare, ifind, pytdx）
     - 按优先级创建视图
     - 生成详细迁移报告
     - 错误处理和恢复机制

5. **集成测试** (完成)
   - ✓ 创建 `tests/integration/test_rawdata_views.py`
   - ✓ 测试场景1：tushare 优先覆盖
   - ✓ 测试场景2：CASCADE 删除
   - ✓ 测试场景3：优先级保护
   - ✓ 测试场景4：COMMENT 标记验证

### 🎯 核心特性

#### 自动视图创建
- ✅ 表创建时自动在 rawdata schema 创建映射视图
- ✅ 无需开发者干预
- ✅ 完全自动化流程

#### 数据源优先级规则
- ✅ **tushare（优先级1）**：始终创建 OR REPLACE 视图
  ```sql
  CREATE OR REPLACE VIEW rawdata.{table} AS
  SELECT * FROM tushare.{table}
  ```
- ✅ **akshare（优先级2）**：仅当 tushare 不存在且视图不存在时创建
- ✅ **其他源**：最低优先级，tushare/akshare 存在时不创建

#### CASCADE 删除
- ✅ 使用 PostgreSQL 原生依赖管理
- ✅ `DROP TABLE ... CASCADE` 自动删除依赖的视图
- ✅ 无需额外代码实现

#### 隔离与管理
- ✅ 每个自动创建的视图都有 COMMENT 标记
  ```sql
  COMMENT ON VIEW rawdata.{table} IS 
  'AUTO_MANAGED: source={schema}.{table}'
  ```
- ✅ 管理工具可以识别和操作自动视图
- ✅ rawdata schema 仅包含自动视图

### 📝 关键代码位置

| 文件 | 行数 | 说明 |
|------|------|------|
| `alphahome/common/db_components/schema_management_mixin.py` | 382-512 | 新增3个方法 |
| `alphahome/common/task_system/base_task.py` | 656-677 | 修改 _ensure_table_exists |
| `alphahome/common/task_system/base_task.py` | 679-750 | 新增 _create_rawdata_view_if_needed |
| `alphahome/common/db_components/db_manager_core.py` | 405-448 | 新增 _initialize_rawdata_schema |
| `scripts/migrate_existing_tables_to_rawdata.py` | 全文 | 迁移脚本 |
| `tests/integration/test_rawdata_views.py` | 全文 | 集成测试 |

### 🔧 使用指南

#### 1. 系统自动初始化
```python
# 在数据库连接时自动调用
db_manager = create_async_manager(connection_string)
await db_manager.connect()  # 自动创建 rawdata schema
```

#### 2. 新表自动映射
```python
# 任何新创建的 FetcherTask 表都会自动映射
class MyTask(FetcherTask):
    data_source = 'tushare'  # 自动在 rawdata 创建视图
    table_name = 'my_table'
```

#### 3. 查询统一接口
```sql
-- 所有查询可以统一从 rawdata 查询，不用关心数据源
SELECT * FROM rawdata.stock_basic;  -- 自动指向 tushare.stock_basic
SELECT * FROM rawdata.fund_basic;    -- 如果 tushare 不存在，可能指向 akshare
```

#### 4. 批量迁移现有表
```bash
python scripts/migrate_existing_tables_to_rawdata.py
```

#### 5. 删除表
```sql
-- 必须使用 CASCADE，视图会自动删除
DROP TABLE tushare.stock_basic CASCADE;
-- PostgreSQL 自动处理：rawdata.stock_basic 被删除
```

### ⚙️ 系统架构

```
BaseTask._ensure_table_exists()
  ↓
_create_table()
  ↓
create_table_from_schema()
  ↓
_create_rawdata_view_if_needed()  [新]
  ↓
根据 data_source 优先级：
  ├─ tushare      → OR REPLACE VIEW rawdata.{table}
  ├─ akshare      → 检查 tushare→跳过；检查视图→跳过；否则CREATE
  └─ 其他         → 同 akshare 逻辑
```

### 📊 性能影响

- ✅ **额外开销极小**
  - 每次表创建时多一次视图创建（异步操作）
  - 只在首次创建表时执行
  - 后续表查询无额外开销

- ✅ **视图查询性能**
  - 视图是直接的 `SELECT * FROM`
  - PostgreSQL 优化器会下推到底层表
  - 查询性能等同于直接查询源表

### 🔍 监控与诊断

#### 查看所有 rawdata 视图
```sql
SELECT table_name, 
       obj_description((quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass) as comment
FROM information_schema.views
WHERE table_schema = 'rawdata'
ORDER BY table_name;
```

#### 查看视图映射源
```sql
SELECT pg_get_viewdef('rawdata.stock_basic'::regclass);
```

#### 检查视图依赖
```sql
SELECT * FROM pg_depend 
WHERE refobjid = (SELECT oid FROM pg_class WHERE relname = 'stock_basic' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'tushare'))
AND deptype = 'n';  -- 'n' 表示 normal dependency
```

### ✨ 后续优化方向

1. **监控告警**
   - 监控 rawdata schema 对象变更
   - 检测违规手动创建的对象

2. **诊断工具**
   - 检测"孤儿视图"（源表已删除但视图仍存在）
   - 自动修复工具

3. **性能优化**
   - 为高频查询的视图考虑物化视图
   - 定期刷新物化视图

4. **高级自动化**
   - 使用 PostgreSQL DDL event trigger 实现更高级的自动化
   - 自动记录视图创建/删除事件

### 🧪 测试覆盖

所有四个核心场景都有完整的集成测试：

```
✓ test_tushare_priority_coverage    - 优先级覆盖验证
✓ test_cascade_delete               - CASCADE 删除验证
✓ test_priority_protection          - 优先级保护验证
✓ test_comment_marking              - COMMENT 标记验证
```

运行测试：
```bash
pytest tests/integration/test_rawdata_views.py -v
```

### 📌 重要注意事项

1. **删除表必须使用 CASCADE**
   ```sql
   -- ✓ 正确
   DROP TABLE tushare.stock_basic CASCADE;
   
   -- ✗ 错误（会报错）
   DROP TABLE tushare.stock_basic;
   ```

2. **rawdata schema 隔离**
   - 禁止在 rawdata 中手动创建表
   - 仅允许自动生成的视图存在
   - 所有自动视图都有 AUTO_MANAGED 标记

3. **数据源变更流程**
   - 如果从 akshare 切换到 tushare：
     1. 创建 tushare 表
     2. 系统自动 OR REPLACE 视图指向 tushare
     3. 之后所有查询都使用 tushare 数据

4. **并发安全性**
   - 实现了双重检查机制防止竞态
   - 支持多个数据源并发创建表

---

**实施日期**: 2025-01-15  
**提交 ID**: 8d874e3  
**总耗时**: ~2.5 小时（包括测试和文档）
