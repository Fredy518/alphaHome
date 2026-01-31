# Phase 1: Features 基础设施实施任务书

> **状态**: 已实施（以当前仓库代码为准）  
> **创建日期**: 2026-01-27  
> **关联设计**: [features_module_design.md](../architecture/features_module_design.md)  
> **里程碑**: M1 (features 骨架) → M2 (物化视图迁移完成)

---

## 概述

本文档详细描述 Phase 1 的 A1-A6 任务，目标是建立 `alphahome/features/` 模块并完成物化视图子系统的迁移。

> 注：本仓库已完成 Phase 1 的关键落地，本任务书保留为“实施说明 + 验收清单”。如与代码不一致，以代码为准。

**完成标准**：
- `from alphahome.features.storage import MaterializedViewRefresh` 可正常导入
- 所有 MV **强制**创建在 `features` schema（不再支持 `materialized_views` 作为目标 schema）
- `features.mv_metadata` 与 `features.mv_refresh_log` **立刻落地**（A4 创建），且 A3/A5 刷新/创建流程会写入对应记录
- 至少 1 个 MV 可通过 `scripts/features_init.py` 创建并刷新
- 全程无旧 processors 依赖

---

## 任务依赖图

```
A1 ──► A2 ──► A3 ──► A5 ──► A6 ──► A7
        │            ▲
        └──► A4 ─────┘
```

| 任务 | 前置依赖 | 产出 |
|------|----------|------|
| A1 | 无 | 目录骨架 + `__init__.py` |
| A2 | A1 | `storage/sql_templates.py` |
| A3 | A2 | `storage/refresh.py` + `storage/validator.py` |
| A4 | A2 | `storage/database_init.py` |
| A5 | A3, A4 | `storage/base_view.py` |
| A6 | A5 | `recipes/mv/*.py` (4 个视图定义) |
| A7 | A6 | 更新 `scripts/initialize_materialized_views.py` |

---

## A1: 创建 features 模块目录结构

### 目标
创建 `alphahome/features/` 目录骨架，确保模块可被正确导入。

### 输入
无

### 产出文件

```
alphahome/features/
├── __init__.py
├── registry.py              # FeatureRegistry + discover(动态扫描) + @feature_register
├── storage/
│   └── __init__.py
├── pit/
│   └── __init__.py
├── recipes/
│   ├── __init__.py
│   ├── mv/
│   │   └── __init__.py
│   └── python/
│       └── __init__.py
└── cards/
    └── .gitkeep
```

### 实现步骤

1. **创建目录结构**
   ```powershell
   $base = "E:\CodePrograms\alphaHome\alphahome\features"
   New-Item -ItemType Directory -Force -Path "$base\storage"
   New-Item -ItemType Directory -Force -Path "$base\pit"
   New-Item -ItemType Directory -Force -Path "$base\recipes\mv"
   New-Item -ItemType Directory -Force -Path "$base\recipes\python"
   New-Item -ItemType Directory -Force -Path "$base\cards"
   ```

2. **创建 `__init__.py` 文件**

本仓库已完成落地，建议直接以当前代码为准（不再按文档手抄 `__init__.py`/占位类）：

- `alphahome/features/__init__.py`
- `alphahome/features/registry.py`：`FeatureRegistry` + `discover()`（动态扫描）+ `@feature_register`
- `alphahome/features/storage/__init__.py`：导出 `MaterializedViewSQL/Refresh/Validator/FeaturesDatabaseInit/BaseFeatureView`
- `alphahome/features/recipes/mv/__init__.py`：兼容性 re-export（可配合 discover 使用）
- `alphahome/features/pit/__init__.py`

3. **创建 `.gitkeep`**
   ```powershell
   New-Item -ItemType File -Force -Path "$base\cards\.gitkeep"
   ```

### 验收标准

```python
# 验收脚本
import sys
sys.path.insert(0, "E:/CodePrograms/alphaHome")

from alphahome.features import __version__
from alphahome.features.storage import __all__ as storage_all
from alphahome.features.recipes.mv import __all__ as mv_all
from alphahome.features.registry import FeatureRegistry

print(f"features version: {__version__}")
print(f"storage exports: {storage_all}")
print(f"registry methods: {dir(FeatureRegistry)}")
print("✅ A1 验收通过")
```

---

## A2: 迁移 sql_templates.py

### 目标
将 `MaterializedViewSQL` 类迁移到 `features/storage/`，移除对 processors 的依赖。

### 输入
- 源文件: `alphahome/processors/materialized_views/sql_templates.py` (344 行)

### 产出文件
- `alphahome/features/storage/sql_templates.py`

### 迁移要点

1. **直接复制**：该文件无外部 processors 依赖，可直接复制
2. **更新 docstring**：将模块描述从 "物化视图 SQL 模板" 改为 "features.storage SQL 模板"
3. **保留核心类**：
   - `MaterializedViewSQL` 类及其静态方法
   - `pit_template()`: PIT 时间序列展开模板
   - `aggregate_template()`: 聚合统计模板
   - `join_template()`: 多表关联模板

### 实现步骤

1. **复制源文件**
   ```powershell
   Copy-Item `
     "E:\CodePrograms\alphaHome\alphahome\processors\materialized_views\sql_templates.py" `
     "E:\CodePrograms\alphaHome\alphahome\features\storage\sql_templates.py"
   ```

2. **修改文件头部** (前 15 行)
   ```python
   """
   features.storage SQL 模板生成器
   
   提供 SQL 模板和生成器，用于创建不同类型的物化视图。
   支持三种主要模式：
   1. PIT（Point-in-Time）物化视图 - 时间序列展开
   2. 聚合物化视图 - 横截面统计
   3. JOIN 物化视图 - 多表关联
   
    迁移自: processors.materialized_views.sql_templates（已删除）
   """
   
   from typing import List, Dict, Any, Optional
   from textwrap import dedent
   ```

3. **更新 `storage/__init__.py`**
   ```python
   from .sql_templates import MaterializedViewSQL
   
   __all__ = [
       "MaterializedViewSQL",
   ]
   ```

### 验收标准

```python
from alphahome.features.storage import MaterializedViewSQL

# 测试 PIT 模板生成
sql = MaterializedViewSQL.pit_template(
    view_name="test_mv",
    source_table="rawdata.test",
    key_columns=["ts_code"],
    time_columns={"ann_date": "announcement_date", "end_date": "data_date"},
    value_columns=["value1", "value2"],
)
assert "CREATE MATERIALIZED VIEW" in sql
print("✅ A2 验收通过")
```

---

## A3: 迁移 refresh.py + validator.py

### 目标
将刷新执行器和数据质量检查器迁移到 `features/storage/`。

### 输入
- `alphahome/processors/materialized_views/refresh.py` (364 行)
- `alphahome/processors/materialized_views/validator.py` (513 行)

### 产出文件
- `alphahome/features/storage/refresh.py`
- `alphahome/features/storage/validator.py`

### 迁移要点

#### refresh.py
1. **需要小幅改造**：源文件虽不依赖 processors，但默认 schema 仍是 `materialized_views`，且“刷新元数据”未落库
2. **保留核心类**：
   - `MaterializedViewRefresh`
   - `refresh()` 方法：执行 REFRESH MATERIALIZED VIEW
   - `_validate_identifier()`: SQL 注入防护
3. **强制 schema**：将默认 schema 改为 `features`，并拒绝非 `features` 的入参
4. **立刻落地刷新日志**：`refresh()` 返回前将结果写入 `features.mv_refresh_log`
5. **更新 docstring**

#### validator.py
1. **无外部 processors 依赖**：可直接复制
2. **保留核心类**：
   - `MaterializedViewValidator`
   - `validate_null_values()`: 缺失值检查
   - `validate_outliers()`: 异常值检查
   - `validate_row_count_change()`: 行数变化检查
   - `validate_duplicates()`: 重复值检查
3. **更新 docstring**

### 实现步骤

1. **复制文件**
   ```powershell
   Copy-Item `
     "E:\CodePrograms\alphaHome\alphahome\processors\materialized_views\refresh.py" `
     "E:\CodePrograms\alphaHome\alphahome\features\storage\refresh.py"
   
   Copy-Item `
     "E:\CodePrograms\alphaHome\alphahome\processors\materialized_views\validator.py" `
     "E:\CodePrograms\alphaHome\alphahome\features\storage\validator.py"
   ```

2. **修改 refresh.py 头部**
   ```python
   """
   features.storage 物化视图刷新执行器
   
   实现物化视图的刷新操作，包括：
   - 执行 REFRESH MATERIALIZED VIEW 命令
   - 支持 FULL 和 CONCURRENT 刷新策略
   - 记录刷新元数据
   - 获取刷新状态
   
    迁移自: processors.materialized_views.refresh（已删除）
   """
   ```

3. **修改 validator.py 头部**
   ```python
   """
   features.storage 数据质量检查器
   
   实现最小的数据质量检查机制，包括：
   - 缺失值检查
   - 异常值检查
   - 行数变化检查
   - 重复值检查
   - 类型检查
   
    迁移自: processors.materialized_views.validator（已删除）
   """
   ```

4. **在 refresh.py 中做强制迁移改造（关键）**

    - 将 `refresh()` / `get_refresh_status()` 的默认 `schema` 从 `materialized_views` 改为 `features`
    - 增加 guardrail：如果传入 `schema != "features"`，直接 `raise ValueError`
    - 在刷新完成后写入 `features.mv_refresh_log`（A4 会创建该表）

5. **更新 `storage/__init__.py`**
   ```python
   from .sql_templates import MaterializedViewSQL
   from .refresh import MaterializedViewRefresh
   from .validator import MaterializedViewValidator
   
   __all__ = [
       "MaterializedViewSQL",
       "MaterializedViewRefresh",
       "MaterializedViewValidator",
   ]
   ```

### 验收标准

```python
from alphahome.features.storage import MaterializedViewRefresh, MaterializedViewValidator

# 验证类可实例化
refresh = MaterializedViewRefresh()
validator = MaterializedViewValidator()

assert hasattr(refresh, 'refresh')
assert hasattr(validator, 'validate_null_values')
print("✅ A3 验收通过")
```

---

## A4: 迁移 database_init.py

### 目标
将数据库初始化逻辑迁移到 `features/storage/`，并调整 schema 命名。

### 输入
- `alphahome/processors/materialized_views/database_init.py` (556 行)

### 产出文件
- `alphahome/features/storage/database_init.py`

### 迁移要点

1. **类名变更**：`MaterializedViewDatabaseInit` → `FeaturesDatabaseInit`
2. **强制迁移**：仅允许 `features` schema（不再兼容 `materialized_views` 作为目标 schema）
3. **立刻落地元数据表**：Phase1 直接创建并使用以下两张表：
    - `features.mv_metadata`
    - `features.mv_refresh_log`
4. **移除视图定义**：具体视图的 SQL 移到 `recipes/mv/`，此处只保留基础设施：
   - `create_schema_sql()`: 创建 features schema
   - `create_metadata_table_sql()`: 创建元数据表
   - `create_refresh_log_table_sql()`: 创建刷新日志表

### 实现步骤

1. **创建新文件** `alphahome/features/storage/database_init.py`:

   ```python
   """
   features.storage 数据库初始化
   
   负责创建 features schema 和相关的元数据表。
   
    迁移自: processors.materialized_views.database_init（已删除）
   """
   
   from typing import Any, Dict, Optional, List
   import logging
   
   logger = logging.getLogger(__name__)
   
   
   class FeaturesDatabaseInit:
       """Features 数据库初始化"""
       
       DEFAULT_SCHEMA = "features"
       
       @classmethod
       def create_schema_sql(cls, schema: str = None) -> str:
           """
           创建 features schema 的 SQL
           
           Args:
               schema: schema 名称，默认为 'features'
           
           Returns:
               str: CREATE SCHEMA SQL 语句
           """
           schema = schema or cls.DEFAULT_SCHEMA
           if schema != cls.DEFAULT_SCHEMA:
               raise ValueError(
                   f"Phase1 强制迁移：仅允许 schema={cls.DEFAULT_SCHEMA!r}，收到: {schema!r}"
               )
           return f"CREATE SCHEMA IF NOT EXISTS {schema};"
       
       @classmethod
       def create_metadata_table_sql(cls, schema: str = None) -> str:
           """
           创建物化视图元数据表的 SQL
           
           Args:
               schema: schema 名称
           
           Returns:
               str: CREATE TABLE SQL 语句
           """
           schema = schema or cls.DEFAULT_SCHEMA
           if schema != cls.DEFAULT_SCHEMA:
               raise ValueError(
                   f"Phase1 强制迁移：仅允许 schema={cls.DEFAULT_SCHEMA!r}，收到: {schema!r}"
               )
           return f"""
           CREATE TABLE IF NOT EXISTS {schema}.mv_metadata (
               view_name VARCHAR(128) PRIMARY KEY,
               schema_name VARCHAR(64) NOT NULL DEFAULT '{schema}',
               description TEXT,
               source_tables TEXT[],
               refresh_strategy VARCHAR(32) DEFAULT 'full',
               quality_checks JSONB,
               created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
               updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
           );
           """
       
       @classmethod
       def create_refresh_log_table_sql(cls, schema: str = None) -> str:
           """
           创建刷新日志表的 SQL
           
           Args:
               schema: schema 名称
           
           Returns:
               str: CREATE TABLE SQL 语句
           """
           schema = schema or cls.DEFAULT_SCHEMA
           if schema != cls.DEFAULT_SCHEMA:
               raise ValueError(
                   f"Phase1 强制迁移：仅允许 schema={cls.DEFAULT_SCHEMA!r}，收到: {schema!r}"
               )
           return f"""
           CREATE TABLE IF NOT EXISTS {schema}.mv_refresh_log (
               id SERIAL PRIMARY KEY,
               view_name VARCHAR(128) NOT NULL,
               schema_name VARCHAR(64) NOT NULL DEFAULT '{schema}',
               refresh_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
               refresh_completed_at TIMESTAMP WITH TIME ZONE,
               status VARCHAR(32) NOT NULL,  -- 'running', 'success', 'failed'
               row_count INTEGER,
               duration_seconds FLOAT,
               error_message TEXT,
               triggered_by VARCHAR(64)  -- 'manual', 'scheduled', 'script'
           );
           
           CREATE INDEX IF NOT EXISTS idx_mv_refresh_log_view_name 
           ON {schema}.mv_refresh_log (view_name, refresh_started_at DESC);
           """
       
       @classmethod
       def get_all_init_sqls(cls, schema: str = None) -> List[str]:
           """
           获取所有初始化 SQL
           
           Args:
               schema: schema 名称
           
           Returns:
               List[str]: SQL 语句列表
           """
           schema = schema or cls.DEFAULT_SCHEMA
           if schema != cls.DEFAULT_SCHEMA:
               raise ValueError(
                   f"Phase1 强制迁移：仅允许 schema={cls.DEFAULT_SCHEMA!r}，收到: {schema!r}"
               )
           return [
               cls.create_schema_sql(schema),
               cls.create_metadata_table_sql(schema),
               cls.create_refresh_log_table_sql(schema),
           ]
       
       @classmethod
       async def initialize(cls, db_connection, schema: str = None) -> Dict[str, Any]:
           """
           执行数据库初始化
           
           Args:
               db_connection: 数据库连接对象
               schema: schema 名称
           
           Returns:
               Dict: 初始化结果
           """
           schema = schema or cls.DEFAULT_SCHEMA
           if schema != cls.DEFAULT_SCHEMA:
               raise ValueError(
                   f"Phase1 强制迁移：仅允许 schema={cls.DEFAULT_SCHEMA!r}，收到: {schema!r}"
               )
           results = {"schema": schema, "tables_created": [], "errors": []}
           
           for sql in cls.get_all_init_sqls(schema):
               try:
                   await db_connection.execute(sql)
                   # 提取表名（简单解析）
                   if "CREATE TABLE" in sql:
                       table_name = sql.split("CREATE TABLE IF NOT EXISTS")[1].split("(")[0].strip()
                       results["tables_created"].append(table_name)
                   elif "CREATE SCHEMA" in sql:
                       results["schema_created"] = True
               except Exception as e:
                   logger.error(f"初始化失败: {e}")
                   results["errors"].append(str(e))
           
           return results
   ```

2. **更新 `storage/__init__.py`**
   ```python
   from .sql_templates import MaterializedViewSQL
   from .refresh import MaterializedViewRefresh
   from .validator import MaterializedViewValidator
   from .database_init import FeaturesDatabaseInit
   
   __all__ = [
       "MaterializedViewSQL",
       "MaterializedViewRefresh",
       "MaterializedViewValidator",
       "FeaturesDatabaseInit",
   ]
   ```

### 验收标准

```python
from alphahome.features.storage import FeaturesDatabaseInit

# 验证 SQL 生成
schema_sql = FeaturesDatabaseInit.create_schema_sql()
assert "CREATE SCHEMA" in schema_sql
assert "features" in schema_sql

metadata_sql = FeaturesDatabaseInit.create_metadata_table_sql()
assert "mv_metadata" in metadata_sql

all_sqls = FeaturesDatabaseInit.get_all_init_sqls()
assert len(all_sqls) == 3

print("✅ A4 验收通过")
```

---

## A5: 迁移 base_task.py → base_view.py

### 目标
创建独立的 `BaseFeatureView` 基类，移除对 `ProcessorTaskBase` 的依赖。

### 输入
- `alphahome/processors/materialized_views/base_task.py` (377 行)
- 参考: `alphahome/processors/tasks/base_task.py` (ProcessorTaskBase)

### 产出文件
- `alphahome/features/storage/base_view.py`

### 设计要点

1. **完全独立**：不继承 `ProcessorTaskBase`，直接继承 ABC
2. **保留核心属性**：
   - `name`: 视图名称
   - `description`: 视图描述
    - `materialized_view_name`: 可选覆盖 MV 名称（不含 schema）
    - `schema`: 目标 schema（强制为 `features`）
    - `view_name`: 物化视图名称（不含 schema，默认 `mv_{name}`）
    - `full_name`: 完整名称（`features.mv_{name}`）
   - `source_tables`: 数据源表
   - `refresh_strategy`: 刷新策略
   - `quality_checks`: 质量检查配置
3. **保留核心方法**：
    - `get_create_sql()`: 定义 MV SQL (抽象方法)
    - `exists()`: 检查是否存在
   - `create()`: 创建 MV
   - `refresh()`: 刷新 MV
   - `drop()`: 删除 MV

### 实现步骤

创建 `alphahome/features/storage/base_view.py`:

实现已在仓库代码中完成：
- [alphahome/features/storage/base_view.py](../../alphahome/features/storage/base_view.py)

与当前仓库一致的关键约定：
- 仅允许 `schema="features"`
- 默认 `view_name = "mv_{name}"`（可用 `materialized_view_name` 覆盖）
- `full_name = "{schema}.{view_name}"`
- create/refresh 会维护 `features.mv_metadata` 与 `features.mv_refresh_log`

更新 `storage/__init__.py`:
```python
from .sql_templates import MaterializedViewSQL
from .refresh import MaterializedViewRefresh
from .validator import MaterializedViewValidator
from .database_init import FeaturesDatabaseInit
from .base_view import BaseFeatureView

__all__ = [
    "MaterializedViewSQL",
    "MaterializedViewRefresh",
    "MaterializedViewValidator",
    "FeaturesDatabaseInit",
    "BaseFeatureView",
]
```

### 验收标准

```python
from alphahome.features.storage import BaseFeatureView

# 验证不依赖 processors
import sys
processors_imported = any("processors" in mod for mod in sys.modules if "alphahome" in mod)
# 注意：如果之前导入过 processors，这里会是 True，需要在干净环境测试

# 验证基类可继承
class TestView(BaseFeatureView):
    name = "test_view"
    materialized_view_name = "test_mv"
    source_tables = ["rawdata.test"]
    
    def get_create_sql(self) -> str:
        return "CREATE MATERIALIZED VIEW features.test_mv AS SELECT 1"

view = TestView()
assert view.full_name == "features.test_mv"
assert view.get_create_sql().startswith("CREATE")
print("✅ A5 验收通过")
```

---

## A6: 迁移视图定义到 recipes/mv/

### 目标
将 4 个 MV 定义文件迁移到 `features/recipes/mv/`，继承新的 `BaseFeatureView`。

### 输入（4 个源文件）

历史来源主要来自 `alphahome/processors/tasks/**` 下的 MV 任务实现；本阶段已完成迁移，本文档不再维护具体源文件清单（以当前仓库代码为准）。

### 产出文件

| 迁移后路径 |
|------------|
| `features/recipes/mv/stock/stock_fina_indicator.py` |
| `features/recipes/mv/stock/stock_sw_industry.py` |
| `features/recipes/mv/stock/stock_daily_enriched.py` |
| `features/recipes/mv/market/market_stats_daily.py` |

### 迁移要点

1. **继承变更**：
   - 旧: `class PITFinancialIndicatorsMV(MaterializedViewTask)`
    - 新: `class StockFinaIndicatorMV(BaseFeatureView)`

2. **方法重命名**：
   - 旧: `define_materialized_view_sql()` → 新: `get_create_sql()`

3. **移除依赖**：
    - 移除: `from processors...`（旧路径）
    - 添加: `from alphahome.features.storage.base_view import BaseFeatureView`

4. **Schema 调整**：
    - 强制：仅允许实例化参数 `schema="features"`（BaseFeatureView 会校验）
    - SQL 中必须创建到 `features` schema

### 实现步骤（以 stock_fina_indicator 为例）

创建 `alphahome/features/recipes/mv/stock/stock_fina_indicator.py`:

实现以当前仓库代码为准：
- `alphahome/features/recipes/mv/stock/stock_fina_indicator.py`

最小接口形态（示例）：

```python
from alphahome.features.storage import BaseFeatureView


class ExampleMV(BaseFeatureView):
    name = "example"
    description = "示例 MV"
    source_tables = ["rawdata.some_table"]

    def get_create_sql(self) -> str:
        return "CREATE MATERIALIZED VIEW features.mv_example AS SELECT 1;"
```
更新 `recipes/mv/__init__.py`:
```python
"""
recipes.mv - 物化视图特征配方

使用 SQL 物化视图实现的特征配方。
"""

from .stock.stock_sw_industry import StockSwIndustryMV
from .stock.stock_fina_indicator import StockFinaIndicatorMV
from .stock.stock_daily_enriched import StockDailyEnrichedMV
from .market.market_stats_daily import MarketStatsMV

__all__ = [
    "StockSwIndustryMV",
    "StockFinaIndicatorMV",
    "StockDailyEnrichedMV",
    "MarketStatsMV",
]
```

### 其余 3 个视图

按相同模式迁移：

1. **stock_sw_industry.py**: 申万行业分类（PIT 时间窗口）
2. **stock_daily_enriched.py**: 每日行情增强
3. **market_stats_daily.py**: 市场横截面统计

### 验收标准

```python
import importlib
from alphahome.features.registry import FeatureRegistry
from alphahome.features.recipes.mv import StockFinaIndicatorMV

view = StockFinaIndicatorMV(schema="features")

# 验证元数据
assert view.name == "stock_fina_indicator"

# 验证 SQL 生成
sql = view.get_create_sql()
assert "CREATE MATERIALIZED VIEW" in sql
assert "features." in sql

# 验证 discover 输出（动态扫描 + 注册）
FeatureRegistry.reset()
FeatureRegistry.discover(force_reload=True)
assert "stock_fina_indicator" in FeatureRegistry.list_all()

# 验证 recipes 代码无 processors 依赖
modules = [
    "alphahome.features.recipes.mv.stock.stock_fina_indicator",
    "alphahome.features.recipes.mv.stock.stock_sw_industry",
    "alphahome.features.recipes.mv.stock.stock_daily_enriched",
    "alphahome.features.recipes.mv.market.market_stats_daily",
]
for mod_name in modules:
    mod = importlib.import_module(mod_name)
    source = open(mod.__file__, encoding="utf-8").read()
    assert "processors." not in source

print("✅ A6 验收通过")
```

---

## A7: 更新初始化脚本

### 目标
更新 `scripts/initialize_materialized_views.py`，改为使用 `alphahome.features`。

### 输入
- `scripts/initialize_materialized_views.py` (126 行)

### 产出
- 更新后的 `scripts/initialize_materialized_views.py`
- 新增 `scripts/features_init.py`（可选，作为新入口）

### 迁移要点

1. **更改 import**：
    - 旧: `from processors.materialized_views.database_init import MaterializedViewDatabaseInit`
   - 新: `from alphahome.features.storage import FeaturesDatabaseInit`

2. **更改调用**：
   - 使用 `FeaturesDatabaseInit.initialize()` 替代原有逻辑

3. **添加视图创建**：
   - 遍历 `recipes/mv/` 下的所有视图并创建

4. **强制 schema**：
    - 仅允许 `features` schema，不提供任意 schema 切换能力（避免跑偏写回旧 schema）

### 实现步骤

本仓库已提供可用脚本 [scripts/features_init.py](../../scripts/features_init.py)，不建议在文档内维护脚本全文。

使用方法：
- 初始化 features schema 与元数据表：`python scripts/features_init.py`
- 同时创建物化视图：`python scripts/features_init.py --create-views`
- 仅检查初始化状态：`python scripts/features_init.py --check`

### 验收标准

```powershell
# 1. 语法检查
python -m py_compile scripts/features_init.py

# 2. 帮助信息
python scripts/features_init.py --help

# 3. 实际执行（需数据库连接）
# python scripts/features_init.py
```

```python
# 验证无 processors 依赖
source = open("scripts/features_init.py").read()
assert "processors." not in source
print("✅ A7 验收通过")
```

---

## 附录

### A. 文件清单汇总

| 任务 | 新建文件 | 修改文件 |
|------|----------|----------|
| A1 | `features/__init__.py`, `storage/__init__.py`, `pit/__init__.py`, `recipes/__init__.py`, `recipes/mv/__init__.py`, `recipes/python/__init__.py`, `registry.py`, `cards/.gitkeep` | - |
| A2 | `storage/sql_templates.py` | `storage/__init__.py` |
| A3 | `storage/refresh.py`, `storage/validator.py` | `storage/__init__.py` |
| A4 | `storage/database_init.py` | `storage/__init__.py` |
| A5 | `storage/base_view.py` | `storage/__init__.py` |
| A6 | `recipes/mv/stock/stock_fina_indicator.py`, `recipes/mv/stock/stock_sw_industry.py` (+ 2 个) | `recipes/mv/__init__.py` |
| A7 | `scripts/features_init.py` | - |

### B. 验收检查清单

- [ ] A1: `from alphahome.features import __version__` 可执行
- [ ] A2: `from alphahome.features.storage import MaterializedViewSQL` 可执行
- [ ] A3: `from alphahome.features.storage import MaterializedViewRefresh, MaterializedViewValidator` 可执行
- [ ] A4: `from alphahome.features.storage import FeaturesDatabaseInit` 可执行
- [ ] A5: `from alphahome.features.storage import BaseFeatureView` 可执行
- [ ] A6: `from alphahome.features.recipes.mv import StockFinaIndicatorMV` 可执行
- [ ] A7: `python scripts/features_init.py --help` 可执行
- [ ] 全局: 无文件包含旧 `processors.*` import

### C. 回滚策略

如任何步骤失败：
1. 删除 `alphahome/features/` 目录
2. 从 Git 恢复原始状态

```powershell
Remove-Item -Recurse -Force "E:\CodePrograms\alphaHome\alphahome\features"
git checkout -- .
```
