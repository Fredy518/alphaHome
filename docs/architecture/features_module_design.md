# 离线特征工程模块设计方案

> **状态**: 历史设计 + 当前 features 模块参考
> **创建日期**: 2026-01-27
> **最后更新**: 2026-05-12
> **作者**: alphahome team

> 当前说明：processors 删除、统一 CLI 下线、features/storage 与 recipes 迁移已完成。日常使用请优先看 [系统架构](system_overview.md)、[任务系统](task_system.md) 和根目录 README；本文保留为 features 迁移的设计与验收记录。

---

## 1. 背景与动机

### 1.1 问题陈述

当前 alphahome 项目存在以下架构问题：

| 问题 | 现状描述 |
|------|----------|
| **processors 过重** | processors 模块设计了完整的引擎层/任务层/操作层/Clean层/物化视图子系统，但对于个人量化平台而言过于复杂，实际投入生产使用的部分很少 |
| **特征模块空置** | `alphahome/factors/` 为空壳目录，未被实际使用 |
| **特征能力分散** | PIT 数据在 `research/pit_data/`、物化视图在 `processors/materialized_views/`，缺乏统一的特征工程入口 |
| **data_infra 定位模糊** | 外部 QuantLab 项目的 data_infra 模块有 35+ Fetcher，但其语义是"从 alphadb 读取"而非"写入 alphadb"，与 alphahome 的生产者定位不符 |

### 1.2 目标

1. **轻装上阵**：废弃并删除 processors 模块，降低维护成本
2. **明确定位**：alphahome 定位为"离线特征生产工厂"，只负责写入 alphadb，不做在线服务
3. **统一入口**：建立 `features` 模块作为离线特征工程的唯一入口
4. **特征甄别**：对 data_infra 做特征入库清单甄别，只把应沉淀的核心特征落库

### 1.3 非目标

- ❌ 不保留运维/生产 CLI（`refresh-materialized-view`、`ah`/`alphahome-cli` 全部下线；必要时以脚本或 Python API 替代，等 features 稳定后再考虑恢复）
- ❌ 不做独立风险模型或组合归因模块（如需此类能力，应拆分到独立项目）
- ❌ 不做在线特征服务层

### 1.4 CLI 边界说明

| 入口 | 类型 | 本次处置 | 说明 |
|------|------|----------|------|
| `alphahome` | GUI launcher | **保留** | 用于启动 GUI 应用，与运维 CLI 无关 |
| `refresh-materialized-view` | 运维 CLI | 下线 | 迁移到 `scripts/` 脚本 |
| `ah` / `alphahome-cli` | 统一 CLI | 下线 | 过重，features 稳定后按需恢复 |

---

## 2. 现状分析

### 2.1 processors 外部依赖点盘点

#### 2.1.1 CLI 入口依赖

| 入口类型 | 文件路径 | 依赖符号 | 处置方式 |
|----------|----------|----------|----------|
| console_script | `pyproject.toml` → `refresh-materialized-view` | 旧 `processors.materialized_views.cli:main_sync`（已删除） | 移除入口 |
| console_script | `pyproject.toml` → `ah` / `alphahome-cli` | `alphahome.cli.main:main_sync` | 下线统一 CLI（整套 cli/ 子包可移除） |
| CLI 子命令 | `alphahome/cli/commands/mv.py` | 动态 import 旧 `processors.materialized_views.cli`（已删除） | 下线命令组（不再提供 mv 管理） |
| CLI 注册 | `alphahome/cli/commands/registry.py` | `MVCommandGroup` 等 | 移除注册 / 移除整个 cli 子包 |

#### 2.1.2 scripts 依赖

| 脚本路径 | 依赖符号 | 处置方式 |
|----------|----------|----------|
| `scripts/initialize_materialized_views.py` | `MaterializedViewDatabaseInit` | 迁移到 features 或改为独立脚本 |

#### 2.1.3 tests 依赖

当前无额外测试依赖需要迁移。

### 2.2 processors 内部模块分类

| 分类 | 模块路径 | 处置决策 |
|------|----------|----------|
| **需迁移** | `processors/materialized_views/` | 迁移核心能力到 `features/storage/` |
| **可废弃** | `processors/engine/` | 过重的调度引擎，废弃 |
| **可废弃** | `processors/tasks/` | 未投入生产，废弃 |
| **可废弃** | `processors/clean/` | Clean Layer 概念过重，废弃 |
| **可废弃** | `processors/operations/` | 几乎无外部引用，废弃（如需保留可迁至 common） |
| **可废弃** | `processors/domain/` | 废弃 |

### 2.3 物化视图子系统核心能力

需要迁移到 `features/storage/` 的最小接口：

| 能力 | 源文件 | 迁移后位置 | 说明 |
|------|--------|------------|------|
| SQL 模板生成 | `sql_templates.py` | `features/storage/sql_templates.py` | PIT/聚合/JOIN 三种模板 |
| 刷新执行器 | `refresh.py` | `features/storage/refresh.py` | FULL/CONCURRENT 刷新 |
| 质量校验 | `validator.py` | `features/storage/validator.py` | 数据质量检查 |
| 数据库初始化 | `database_init.py` | `features/storage/database_init.py` | schema/元数据表创建 |
| 任务基类 | `base_task.py` | `features/storage/base_view.py` | 物化视图任务抽象 |

补充说明：
- 当前 `processors/materialized_views/cli.py` 内置了一个 `MATERIALIZED_VIEWS` 注册表，并直接 import 多个 `processors/tasks/*/*_mv.py` 视图定义类。
- 由于本方案不保留 CLI，注册表可直接移除，但"视图定义（view definitions）"本身必须迁移到 `features`（建议放在 `features/recipes/mv/`），否则迁移后的 storage 层只有基础设施没有产物。

**需迁移的 MV 定义文件**（共 4 个）：

| 源路径 | 迁移后路径 | 说明 |
|--------|------------|------|
| `processors/tasks/pit/pit_financial_indicators_mv.py` | `features/recipes/mv/stock/stock_fina_indicator.py` | 财务指标（PIT 时间窗口） |
| `processors/tasks/pit/pit_industry_classification_mv.py` | `features/recipes/mv/stock/stock_industry_monthly_snapshot.py` | 行业分类（月度快照，sw+ci） |
| `processors/tasks/market/market_technical_indicators_mv.py` | `features/recipes/mv/stock/stock_daily_enriched.py` | 每日行情增强（在 stock 域落库） |
| `processors/tasks/market/sector_aggregation_mv.py` | `features/recipes/mv/market/market_stats_daily.py` | 市场横截面统计 |

---

## 3. 目标架构

### 3.1 模块职责重新定义

```
alphahome/
├── common/                    # 基础设施（保持不变）
│   ├── db_manager.py
│   ├── config_manager.py
│   ├── task_system/
│   └── ...
├── fetchers/                  # 数据获取（保持不变）
│   └── ...
├── features/                  # 【新建】离线特征工程
│   ├── storage/               # 特征存储层（物化视图）
│   ├── pit/                   # PIT 时序特征
│   ├── recipes/               # 特征计算配方
│   └── registry.py            # 特征注册表
├── integrations/              # 外部集成（保持不变）
└── [processors/]              # 【废弃删除】
```

### 3.2 features 模块设计

```
alphahome/features/
├── __init__.py                # 统一导出
├── storage/                   # 特征存储层
│   ├── __init__.py
│   ├── base_view.py           # 物化视图任务基类
│   ├── refresh.py             # 刷新执行器
│   ├── sql_templates.py       # SQL 模板生成器
│   ├── validator.py           # 数据质量验证
│   └── database_init.py       # Schema 初始化
├── pit/                       # PIT 时序特征（当前仅占位；完整迁移见 Section 6）
│   └── __init__.py
├── recipes/                   # 特征计算配方
│   ├── __init__.py
│   ├── base_recipe.py         # 配方基类
│   ├── mv/                     # 用 SQL/MV 实现的特征配方（推荐）
│   ├── python/                 # 必须用 Python 计算的离线特征（谨慎使用，优先落库后再消费）
│   └── ... (按特征甄别清单逐步添加)
├── cards/                     # 入库卡片（YAML）
│   └── <feature_name>.yaml    # 每个入库特征一张卡片
└── registry.py                # FeatureRegistry 统一访问
```

#### 3.2.1 命名与目录规范（对齐 fetchers 的“可读性优先”）

> 目的：让新同学只看路径/文件名就能推断“这是什么特征、属于哪个域、如何落库、如何刷新”。

核心原则：
1. **目录表达语义，文件名保持短而稳定**：recipes 的类型（mv/python）由目录表达；业务域由子目录表达。
2. **一个特征配方 = 一个文件 = 一个类 = 一张卡片**：形成可追溯的最小闭环。
3. **稳定标识优先**：特征的 `recipe.name`（唯一标识）一旦发布应尽量不变；重命名视为破坏性变更。

目录结构规范（强制）：
```
alphahome/features/recipes/
├── mv/
│   ├── stock/
│   ├── market/
│   └── ...（允许新增域：index/fund/macro/option/future/...）
└── python/
    ├── stock/
    └── ...（谨慎使用，优先用 mv 落库）
```

命名三件套规范（强制）：

1) 文件名（module name）
- 规则：`{recipe_name}.py`
- 约束：`recipe_name` 必须是 `snake_case`，且在全仓库内建议唯一（至少在 features/recipes 范围内唯一）。
- 示例：
  - `recipes/mv/stock/stock_daily_enriched.py`
  - `recipes/mv/stock/stock_fina_indicator.py`
  - `recipes/mv/stock/stock_industry_monthly_snapshot.py`
  - `recipes/mv/market/market_stats_daily.py`

2) 类名（Recipe class name）
- 规则：`{RecipeName}{TypeSuffix}`，其中 `TypeSuffix` 在 mv 中固定为 `MV`。
- 示例：
  - `StockDailyEnrichedMV`
  - `StockFinaIndicatorMV`
  - `StockSwIndustryMV`
  - `MarketStatsMV`

3) 配方唯一标识（recipe.name）
- 规则：与文件名一致，取 `{recipe_name}`（不带 `.py`），例如 `market_stats`。
- 说明：避免把 `_mv` 写进 `recipe.name`，因为 MV 只是实现形态；未来迁移为 table 仍能保持 name 不变。

输出对象命名规范（强制）：
- MV 输出表名：`mv_{recipe.name}`，schema 固定为 `features`。
  - 例：`features.mv_market_stats`
- 非 MV（普通表）输出表名：默认 `features.{recipe.name}`；如需区分可引入前缀 `feat_`，但需在卡片中说明原因。

卡片文件规范（强制）：
- 路径：`alphahome/features/cards/{recipe.name}.yaml`
- 要求：卡片中的 `feature_name` 必须等于 `recipe.name`。

关于“是否给文件名加 mv_ 前缀”的决策：
- 理由：减少冗余；避免出现 `mv/mv_xxx.py` 的重复语义；与“目录表达语义”的原则一致。

#### 3.2.2 Recipe 的组织方式（借鉴 fetchers 的“分层 + 自动发现”）

- fetchers 的可读性来自于：`tasks/{domain}/` 分组 + 明确命名 + 装饰器注册。
- features 采取同样策略：`recipes/{type}/{domain}/` 分组 + 统一命名三件套 + 注册/发现机制。

features 不引入 fetchers 的点：
- 不引入“数据源前缀”（tushare/akshare），因为 features 的输入来自 alphadb 内部表，数据源差异通过 `source_tables` 表达即可。
- 不复用 fetchers 的 UnifiedTaskFactory；features 的生命周期更偏“定义/初始化/刷新/校验”，由 `FeatureRegistry` 统一管理。

### 3.3 features 核心接口契约

#### 3.3.1 BaseFeatureView（已落地：MV 配方基类）

当前仓库里“可运行的最小闭环”以 MV 为主，配方类统一继承 `BaseFeatureView`（位于 `alphahome/features/storage/base_view.py`）。

落地后的最小契约（与代码一致）：

```python
class BaseFeatureView:
  # === 元信息（必填，注册阶段校验）===
  name: str
  description: str
  source_tables: List[str]

  # === 可选配置 ===
  refresh_strategy: str = "full"        # full / concurrent
  materialized_view_name: str = ""      # 为空则默认 mv_{name}
  quality_checks: Dict[str, Any] = {}

  # === 命名约束（强制）===
  # schema 固定为 features；full_name = features.mv_{name}

  def get_create_sql(self) -> str:
    ...  # 返回完整 CREATE MATERIALIZED VIEW features.mv_{name} ...

  def get_post_create_sqls(self) -> List[str]:
    ...  # 可选：创建后附加 DDL（通常用于 CREATE INDEX IF NOT EXISTS ...）
```

说明：文档中的“时间语义/PIT 安全/质量规则”等更完整元信息，当前推荐通过 cards（YAML）承载；代码层的强校验以 `name/description/source_tables` 为主，避免在 import 阶段引入重逻辑。

#### 3.3.2 FeatureRegistry（特征注册表）

统一管理所有已入库特征的发现、刷新、校验：

```python
class FeatureRegistry:
    """特征注册表"""
    
    @classmethod
    def discover(cls) -> List[type]:
      """自动发现 alphahome/features/recipes/ 下所有 Recipe 类

      说明：实现上推荐返回“Recipe 类（可实例化）”或“Recipe 单例对象（二选一）”，但对外表现应稳定。
      """
    
    @classmethod
    def get(cls, name: str) -> type:
      """按名称获取特征配方类"""
    
    @classmethod
    def list_all(cls) -> List[str]:
        """列出所有已注册特征名"""
    
    # init/refresh/validate 的编排入口当前以脚本为主（见 3.3.3）。
    # 若后续需要对外提供统一 API，可在 registry 上层增加 orchestration 层，但避免把 DB 依赖引入注册表。
```

实现方案（写入为工程约束，减少后续分歧）：

1) 注册机制：装饰器 + 导入即注册（对齐 fetchers）
- 约束：每个 recipe 文件在模块顶层用 `@feature_register()` 注册（注册发生在 import 时）。
- 目的：避免在 `recipes/mv/__init__.py` 里维护越来越长的手工 import 列表。
- 兼容期：允许 `recipes/mv/__init__.py` 暂时保留导出（见 A6），但新增 recipe 不再要求修改该文件。

2) Registry 存储形态：存“类”优先，避免存“实例”
- 推荐：`FeatureRegistry` 存储“配方类”（例如 `Type[BaseFeatureView]`），需要执行 create/refresh/validate 时再实例化。
- 理由：
  - 避免 import 时就做重逻辑（例如读取配置/连接 DB）。
  - 更接近 fetchers 的“注册任务类”模型，便于按需构造。

3) discover 的实现策略（**默认采用动态扫描**）

> **决策**：默认采用动态扫描（方案 B），不再维护显式导入列表。

A. 显式导入（备选，不采用）
- `alphahome.features.recipes` 在 `__init__.py` 中显式 import 所有 recipe 子模块；import 触发注册；discover 只返回 registry 内容。
- 优点：行为确定、排错简单。
- 缺点：新增 recipe 仍需要维护导入列表（不符合"免维护"目标）。
- 结论：**不采用**。

B. 动态扫描（**默认采用**）
- `FeatureRegistry.discover()` 使用 `pkgutil.walk_packages` 扫描 `alphahome.features.recipes` 子包，按模块名排序后逐个 import。
- import 触发 `@feature_register()`；discover 返回 registry 内容。
- 约束：
  - 扫描仅在 scripts/工具入口调用，不在 `alphahome.features` 顶层 import 时自动触发。
  - 必须提供缓存（例如 `_discovered` 标志），并允许 `force_reload=True` 供开发调试。
  - 遇到重复 `recipe.name` 必须直接抛错（Fail Fast），避免静默覆盖。

4) 错误处理与可观测性
- 重复注册：抛出异常并打印冲突来源（module/class）。
- recipe 元信息缺失：在注册时校验 `name/description/source_tables` 等关键字段，不满足则报错。
- discover 导入异常：默认抛出（避免漏特征）；如需“容错模式”，仅允许在开发环境显式开启。

5) 与 storage 层的边界
- `BaseFeatureView.get_create_sql()` 只负责定义对象；执行/刷新/校验由 `features/storage/` 负责。
- `BaseFeatureView.get_post_create_sqls()` 用于声明“创建后 DDL”（例如索引），由 `BaseFeatureView.create()` 在 MV 创建成功后执行，推荐幂等（`IF NOT EXISTS`）。
- `FeatureRegistry` 是“配方发现与编排层”，不直接持有数据库连接或在 import 阶段触发数据库操作。

#### 3.3.3 运维脚本入口

替代 CLI，在 `scripts/` 下提供最小化入口：

| 脚本 | 功能 | 示例调用 |
|------|------|----------|
| `scripts/features_init.py` | 初始化 features schema 和所有 MV | `python scripts/features_init.py` |
| `scripts/features_refresh.py` | （规划）刷新指定或全部特征 | - |
| `scripts/features_validate.py` | （规划）校验特征数据质量 | - |

### 3.4 数据流向

```
┌─────────────────────────────────────────────────────────────────┐
│                        alphahome (生产者)                        │
├─────────────────────────────────────────────────────────────────┤
│  fetchers/ ──► rawdata schema                                   │
│      │                                                          │
│      ▼                                                          │
│  research/pit_data/ ──► rawdata.pit_* tables (当前实现)          │
│      │                                                          │
│      ▼                                                          │
│  features/pit/ ──► (规划) rawdata.pit_* tables                   │
│      │                                                          │
│      ▼                                                          │
│  features/storage/ ──► features schema (物化视图)                │
│      │                                                          │
│      ▼                                                          │
│  features/recipes/mv/ ──► features.mv_*                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (alphadb)
┌─────────────────────────────────────────────────────────────────┐
│                      QuantLab (消费者)                           │
├─────────────────────────────────────────────────────────────────┤
│  data_infra/fetchers/ ◄── 从 alphadb 读取离线特征               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. 迁移方案

采用**双线并行**策略，降低风险。

### 4.0 执行顺序（推荐）

为了最大程度降低风险，建议按以下顺序执行：

```
┌────────────────────────────────────────────────────────────────┐
│  Phase 1: features 基础设施就绪                                 │
│  ──────────────────────────────────────                        │
│  A1-A7 → 至少 1 个 MV 可 init/create/refresh/validate          │
│  P1-P3 → PIT 模块迁移完成                                       │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  Phase 2: CLI 下线 + processors 清零                           │
│  ──────────────────────────────────────                        │
│  C1-C4 → 运维 CLI 入口全部移除                                  │
│  D1    → grep 确认 0 引用                                       │
│  D2-D4 → 删除 processors 目录                                   │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  Phase 3: 首批特征入库                                          │
│  ──────────────────────────────────────                        │
│  基于甄别清单，逐批落库 valuation/breadth 等特征               │
└────────────────────────────────────────────────────────────────┘
```

**关键检查点**：
- Phase 1 完成后：`scripts/features_init.py` 可执行，至少 1 个 MV 可刷新
- Phase 2 完成后：仓库内无旧 processors 残留引用（建议用 PowerShell：`Select-String -Path alphahome/**,scripts/**,tests/** -Pattern "processors." -SimpleMatch`；如安装了 ripgrep/grep 也可用：`rg "processors\." alphahome scripts tests`）

### 4.1 Line A: 物化视图 → features/storage

**目标**: 把物化视图子系统迁移到 features/storage，作为特征存储的主实现方式。

| 阶段 | 任务 | 产出 | 验收标准 |
|------|------|------|----------|
| A1 | 创建 `features/storage/` 目录结构 | 空模块骨架 | import 不报错 |
| A2 | 迁移 `sql_templates.py` | PIT/聚合模板可用 | 单测通过 |
| A3 | 迁移 `refresh.py` + `validator.py` | 刷新与校验可用 | 手动验证 |
| A4 | 迁移 `database_init.py` | schema 初始化可用 | `features` schema 可创建 |
| A5 | 迁移 `base_task.py` → `base_view.py` | 去除对 ProcessorTaskBase 的依赖 | 独立运行 |
| A6 | 迁移"视图定义"到 `features/recipes/mv/{domain}/`（参见 Section 2.3 MV 定义清单，共 4 个） | 视图定义类/SQL 生成函数 | 至少 1 个视图可创建+刷新 |
| A7 | 更新 `scripts/initialize_materialized_views.py` | 改为 import features | 脚本可执行 |

补充约束（A6 的落地规范）：
1. 迁移时必须遵守 Section 3.2.1 的命名三件套（文件名 / 类名 / recipe.name）与输出表命名规则。
2. 迁移时按业务域分组落位：
  - stock 类 MV（可包含 PIT 时间窗口/安全语义）→ `features/recipes/mv/stock/`
    - market 类 MV → `features/recipes/mv/market/`
3. 命名变更与兼容：
  - 若需要重命名（例如从 `pit_*` 迁移到 `stock_*`），应视为破坏性变更：统一更新调用方与测试，并同步更新落库对象命名。
4. 路径迁移不应改变 `recipe.name`：
    - `recipe.name` 是下游契约（卡片、输出表名、refresh/validate 指令入参的基础），迁移目录结构时必须保持不变。

### 4.2 去 CLI

**目标**: 移除所有 CLI 入口（运维/生产命令行），等 features 稳定后再考虑恢复。

| 阶段 | 任务 | 产出 | 验收标准 |
|------|------|------|----------|
| C1 | 从 `pyproject.toml` 移除 `refresh-materialized-view` 入口点 | 无该入口 | `pip install -e .` 不报错 |
| C2 | 从 `pyproject.toml` 移除 `ah`/`alphahome-cli` 入口点 | 无统一 CLI | `pip install -e .` 不报错 |
| C4 | 移除 `alphahome/cli/` 子包（或最小化为空壳） | 无 cli 依赖 | import 不报错 |

替代方式：
- 运维与生产动作以 `scripts/*.py` 或 Python API 方式执行（例如初始化 features schema、刷新指定 MV）。

### 4.3 删除 processors

**目标**: 确认无外部依赖后，删除整个 processors 目录。

| 阶段 | 任务 | 产出 | 验收标准 |
|------|------|------|----------|
| D1 | 全局 grep 确认无旧 processors 残留引用 | 清零报告 | 0 匹配 |
| D2 | 删除 `alphahome/processors/` 目录 | 目录不存在 | - |
| D3 | 从 `alphahome/__init__.py` 移除 processors 导出 | 无导出 | import 不报错 |
| D4 | 更新文档（README、docs/）| 移除 processors 相关描述 | 文档一致 |

回滚策略（建议明确）：
- 在 D2 前保留一个短期兼容层（例如将旧路径 re-export 到新路径），以便逐步替换 scripts/tests 的 import；当 D1 清零后再删除。

---

## 5. 特征甄别清单（流程化方案）

> **甄别是本模块最核心的价值点。** alphahome 是 alphadb 的 "生产者"，QuantLab/data_infra 是 "消费者"。
> 落库前必须完成完整的甄别流程，避免把 QuantLab 端临时试验的 Fetcher 盲目搬运到 alphahome。

### 5.1 八步甄别流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        特征入库甄别流程（8 步）                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
│  │ Step 1  │───>│ Step 2  │───>│ Step 3  │───>│ Step 4  │                  │
│  │候选提名 │    │输入表映射│    │时间语义 │    │PIT安全  │                  │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘                  │
│       │                                              │                      │
│       v                                              v                      │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
│  │ Step 8  │<───│ Step 7  │<───│ Step 6  │<───│ Step 5  │                  │
│  │入库卡片 │    │下游验证 │    │落库形态 │    │质量规则 │                  │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

| 步骤 | 名称 | 必须回答的问题 | 产出 |
|------|------|----------------|------|
| 1 | **候选提名** | 谁提名？解决什么问题？已有多少策略复用？ | 提名表单 |
| 2 | **输入表映射** | 依赖 alphadb 哪些 schema/表？raw or clean？ | 依赖表清单 |
| 3 | **时间语义** | trade_date / ann_date / end_date？频率？ | 时间字段规范 |
| 4 | **PIT 安全** | 是否存在未来信息？available lag 多少天？ | PIT 安全声明 |
| 5 | **质量规则** | 缺失率阈值？范围校验？唯一性？ | 质量规则 YAML |
| 6 | **落库形态** | 物化视图 / 普通表 / PIT 表？刷新策略？ | 落库方案 |
| 7 | **下游验证** | data_infra 对应 Fetcher 能否无缝切换？ | 验证脚本 |
| 8 | **入库卡片** | 汇总所有信息，形成可追溯的入库记录 | YAML 卡片 |

### 5.2 入库卡片模板（YAML）

每个待入库特征必须提交以下卡片，存放于 `alphahome/features/cards/<feature_name>.yaml`：

```yaml
# 特征入库卡片
feature_name: valuation_daily
version: 1.0.0
created_by: <author>
created_at: <date>
status: pending  # pending | approved | rejected | deprecated

# Step 2: 输入表映射
source_tables:
  - schema: rawdata
    table: stk_valuation_daily
    key_columns: [stock_code, trade_date]

# Step 3: 时间语义
timestamp:
  field: trade_date
  semantic: trading_day  # trading_day | announcement_date | report_end_date
  frequency: daily       # daily | weekly | monthly | quarterly

# Step 4: PIT 安全
pit_safety:
  has_future_info: false
  available_lag_days: 0
  notes: "估值指标当日收盘后可得"

# Step 5: 质量规则
quality_checks:
  - type: null_rate
    column: pe_ttm
    threshold: 0.05
  - type: range
    column: pe_ttm
    min: -1000
    max: 10000
  - type: uniqueness
    columns: [stock_code, trade_date]

# Step 6: 落库形态
output:
  type: materialized_view  # materialized_view | table | pit_table
  schema: features
  name: mv_valuation_daily
  refresh: daily
  depends_on_mv: []

# Step 7: 下游验证
downstream_verification:
  quantlab_fetcher: valuation
  test_script: tests/features/test_valuation_parity.py
  expected_diff: 0

# 审批记录
approval:
  reviewer: null
  reviewed_at: null
  comments: null
```

### 5.3 决策树

```
是否应入库？
│
├── 口径是否稳定？
│   └── 否 → 不入库（保留 QuantLab）
│
├── 是否跨策略复用？
│   └── 否 → 评估后决定
│
├── 计算成本是否高？
│   └── 是 → 优先入库
│
├── PIT 语义是否清晰？
│   └── 否 → 需评审
│
└── 以上均满足 → 入库
```

### 5.4 data_infra Fetcher 甄别状态

> 以下为 data_infra 34 个 Fetcher 的完整甄别清单（2026-01-30 更新）。  
> **甄别状态**：⬜ 未开始 | 🟡 进行中 | ✅ 已入库 | ❌ 不入库 | 🔵 需评审

---

#### 第一批：推荐优先入库（口径稳定、跨策略复用、PIT 安全）

| Fetcher | 输出特征 | 依赖表 | 时间语义 | PIT 安全 | 甄别状态 | 入库卡片 |
|---------|----------|--------|----------|----------|----------|----------|
| `valuation` | PE/PB 分位数、ERP | tushare.index_dailybasic, akshare.macro_bond_rate | trade_date (日) | ✅ 无未来信息（收盘后可得） | ✅ | index_features_daily |
| `market_breadth` | MA60/MA90 占比、涨跌停比 | tushare.stock_factor_pro, tushare.stock_limitlist | trade_date (日) | ✅ 无未来信息 | ✅ | market_sentiment_daily |
| `new_high_new_low_diff` | 52 周新高新低比 | tushare.stock_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | market_sentiment_daily |
| `margin` | 融资余额占流通市值比 | tushare.stock_margin, tushare.stock_dailybasic | trade_date (月末) | ✅ 无未来信息 | ✅ | market_margin_monthly |
| `margin_turnover_ratio` | 两融成交占比 | tushare.stock_margin, tushare.stock_daily | trade_date (日) | ✅ 无未来信息 | ✅ | margin_turnover_daily |
| `index_volatility` | 指数实现波动率 20/60/252D | tushare.index_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | index_features_daily |
| `volatility_acceleration` | 波动率加速度 | tushare.index_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | index_features_daily |
| `index_fundamental` | 指数加权 PE/PB (PIT 权重) | tushare.index_weight, tushare.stock_dailybasic | trade_date (日) | ✅ PIT 权重已实现 | ✅ | index_fundamental_daily |
| `macro_bond_rate` | 中美国债收益率、期限利差 | akshare.macro_bond_rate | trade_date (日) | ✅ 无未来信息 | ✅ | macro_rate_daily |
| `macro_rate_percentile` | 利率历史分位数 | akshare.macro_bond_rate | trade_date (日) | ✅ 无未来信息 | ✅ | macro_rate_daily |
| `market_technical` | 市场动量/波动/量价分布 | tushare.stock_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | market_technical_daily |
| `market_return_distribution` | 全市场涨跌分布 | tushare.stock_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | market_technical_daily |
| `limit_updown_features` | 涨跌停家数/连板/炸板 | tushare.stock_limitlist, tushare.stock_st | trade_date (日) | ✅ 无未来信息 | ✅ | market_sentiment_daily |
| `industry_return` | 申万二级行业日收益 | tushare.index_swdaily | trade_date (日) | ✅ 无未来信息 | ✅ | industry_features_daily |
| `industry_breadth` | 行业宽度/分散度（申万二级） | tushare.index_swdaily | trade_date (日) | ✅ 无未来信息 | ✅ | industry_features_daily |
| `index_boll_signals` | 布林带突破信号 | tushare.index_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | index_technical_daily |
| `index_ma120_distance` | MA120 偏离度 | tushare.index_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | index_technical_daily |
| `etf_flow` | ETF 净申赎资金流 | tushare.fund_share, tushare.fund_nav, tushare.fund_etf_basic | trade_date (日) | ✅ 无未来信息 | ✅ | etf_flow_daily |
| `style_index_return` | 风格指数收益 | tushare.index_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | style_features_daily |
| `style_momentum` | 风格相对强弱 | tushare.index_factor_pro | trade_date (日) | ✅ 无未来信息 | ✅ | style_features_daily |

---

#### 第二批：评审完成（2026-01-30）

> 详细评审报告：[docs/tasks/M7_fetchers_review_phase2.md](../tasks/M7_fetchers_review_phase2.md)

| Fetcher | 评审结论 | 理由 | 甄别状态 |
|---------|----------|------|----------|
| `money_flow` | **入库** | 口径清晰（小单<5万/中单5-20万/大单20-100万/特大单≥100万）；字段完整（sm/md/lg/elg 买卖量价）；2010年起可用 | ✅ money_flow_daily |
| `futures_basis` | **入库** | 口径清晰（加权基差）；依赖表已有 | ✅ futures_features_daily |
| `member_position` | **入库** | 会员持仓净多空/多空比；future_holding 可用 | ✅ futures_features_daily |
| `pcr_weekly` | 保留评审 | 周频；ETF 期权覆盖率待验证 | 🔵 |
| `option_iv` | 不入库 | 强依赖 scipy；BS 反推极端 NaN 多；计算应留消费端 | ❌ |
| `iv_term_structure` | 不入库 | 依赖 option_iv；叠加复杂度 | ❌ |
| `rsrs` | 不入库 | 择时信号非特征；参数敏感（18/600 窗口） | ❌ |
| `market_industry_flow` | 保留评审 | 与 industry_features_daily 功能重叠待评估 | 🔵 |
| `cb_risk_appetite` | 保留评审 | cbond_daily 覆盖率待验证（2020 后数据较全） | 🔵 |
| `st_risk_appetite` | **入库** | stock_st 动态列表避免幸存者偏差；2016-08 起可用 | ✅ risk_appetite_daily |
| `bse_risk_appetite` | **入库** | 北交所 2021-11 开市；数据完整 | ✅ risk_appetite_daily |
| `microcap_risk_appetite` | 保留评审 | "微盘股"口径需确认（后 10% or 固定阈值） | 🔵 |
| `risk_appetite_composite` | 不入库 | 组合权重不宜固化；应落原子指标 | ❌ |
| `index_factor_pro` | 不入库 | 源表已落库（tushare.index_factor_pro）；无增量价值 | ❌ |
| `market_valuation_distribution` | **入库** | PE/PB 分布；与 market_stats 互补 | ✅ market_stats 或独立 |
| `market_turnover_distribution` | **入库** | 换手率分布/成交集中度 | ✅ market_technical_daily |
| `market_momentum_distribution` | **入库** | RSI/MA 分布 | ✅ market_technical_daily |
| `market_size_dispersion` | **入库** | 大小盘收益差；独立指标 | ✅ market_size_daily |
| `market_volatility_distribution` | **入库** | ATR/振幅分布 | ✅ market_technical_daily |
| `repurchase_weekly` | **入库** | 股票回购周频聚合（公告数/金额/进度分布）；2010年起可用 | ✅ repurchase_weekly |
| `holdertrade_weekly` | **入库** | 股东增减持周频（高管/公司/个人分层）；长期可用 | ✅ holdertrade_weekly |
| `limit_industry_distribution` | **入库** | 涨跌停行业分布（HHI/Top行业占比）；日频 | ✅ limit_industry_daily |

---

#### 不入库（保留 QuantLab 端消费）

| Fetcher | 原因 | 甄别状态 |
|---------|------|----------|
| `dragon_tiger` | 稀疏事件数据（日覆盖 <1%），口径不稳 | ❌ |
| `dragon_tiger_inst` | 同上 | ❌ |
| `blocktrade` | 稀疏事件数据（日覆盖 <1%），折溢价口径多变 | ❌ |
| `analyst_revision` | 强依赖外部预期数据，口径随券商变化 | ❌ |
| `consensus_eps` | 同上 | ❌ |
| `rating_momentum` | 同上 | ❌ |
| `global_index` | 外部数据源（非 alphadb 范畴），跨市场时区复杂 | ❌ |
| `global_index_momentum` | 同上 | ❌ |
| `option_iv` | 强依赖 scipy；计算应留消费端 | ❌ |
| `iv_term_structure` | 依赖 option_iv | ❌ |
| `rsrs` | 择时信号非特征 | ❌ |
| `risk_appetite_composite` | 组合权重不宜固化 | ❌ |
| `index_factor_pro` | 源表已落库；无增量价值 | ❌ |

---

#### 统计汇总（2026-01-30 更新）

| 类别 | 数量 |
|------|------|
| ✅ 已入库 | 20 + 11 + 3 = **34** |
| 🔵 保留评审 | 5 |
| ❌ 不入库 | 8 + 5 = **13** |
| **合计** | **52**（含子 Fetcher） |

### 5.5 甄别治理机制

1. **卡片驱动**：无卡片 = 不入库。所有特征必须有可追溯的 YAML 卡片。
2. **版本控制**：卡片随代码一起 Git 管理，变更需 PR 评审。
3. **状态流转**：`pending → approved → (deprecated)`，rejected 的卡片保留但不激活。
4. **定期审计**：每季度审计一次已入库特征的使用率，低使用率特征标记 deprecated。

---

## 6. PIT 数据迁移

### 6.1 现状

| 模块 | 位置 | 状态 |
|------|------|------|
| PITTableManager | `research/pit_data/base/pit_table_manager.py` | 生产可用 |
| pit_balance_quarterly | `research/pit_data/pit_balance_quarterly_manager.py` | 生产可用 |
| pit_income_quarterly | `research/pit_data/pit_income_quarterly_manager.py` | 生产可用 |
| pit_financial_indicators | `research/pit_data/pit_financial_indicators_manager.py` | 生产可用 |
| pit_industry_classification | `research/pit_data/pit_industry_classification_manager.py` | 生产可用 |

### 6.2 迁移计划

本项目**不再执行**“把 `research/pit_data/` 代码迁移进 `features/pit/`”的原计划（P1-P5）。

改为采用 **方案 D：MV PIT 化替代**：

- **定位调整**：PIT 是时间语义/安全原则，而不是必须独立维护的一套“PIT 表生产管道”。
- **落地方式**：以 `features` schema 下的 `mv_*` 物化视图作为对外稳定契约，直接提供可 PIT 消费的时间窗口字段。
- **取舍**：
  - `research/pit_data/` 继续保留为历史实现与研究工具链（必要时用于对比与回滚）。
  - `features/pit/` 保持占位（不再作为近期交付目标），避免引入职责边界与 schema 语义混乱。

#### 6.2.1 下一步计划（新增 income/balance 派生 MV）

在现有 `features.mv_stock_fina_indicator` / `features.mv_stock_industry_monthly_snapshot` 的基础上，新增两张“财报基础表”的 PIT 化 MV，结构与 `stock_fina_indicator.py` 对齐：

| 阶段 | 任务 | 产出 | 说明 |
|------|------|------|------|
| D0 | 明确 MV PIT 契约字段 | 文档/卡片约束 | 统一要求输出包含 `query_start_date/query_end_date/report_period/ann_date` |
| D1 | 新增 `features.mv_stock_income_quarterly` | MV 配方 + 可 init/refresh | 来源：`rawdata.fina_income`（利润表） |
| D2 | 新增 `features.mv_stock_balance_quarterly` | MV 配方 + 可 init/refresh | 来源：`rawdata.fina_balancesheet`（资产负债表） |
| D3 | 补齐校验与对比脚本 | scripts/验证报告 | 对齐校验见 D-1/D-2/D-3 |

> 说明：income/balance 两张 MV 先“低耦合落地”（以 `SELECT *` + PIT 窗口为主），后续再基于甄别流程收敛字段口径并补卡片。

#### 6.2.2 更严格验收（D-1 / D-2 / D-3）

为确保“MV PIT 化替代”达到生产可用标准，M6 的验收从“能跑”升级为以下三道门槛：

- **D-1：PIT 窗口正确性 + 数据契约**
  - 每张 MV 必须包含并满足：
    - `query_start_date = ann_date`
    - `query_end_date >= query_start_date`
    - `report_period = end_date`（财报类）
  - 同一 `ts_code` 下 PIT 窗口不允许“未来信息泄漏”：`query_end_date` 必须由下一条公告日推导（或封顶到远期日期）。
  - 输出必须可作为下游 join key（至少包含 `ts_code` + PIT 窗口字段），并通过唯一性/空值率/窗口一致性校验。

- **D-2：与既有 PIT 产出对比（可量化）**
  - 对 `income/balance`：抽样对比 `pgs_factors.pit_income_quarterly` / `pgs_factors.pit_balance_quarterly` 在最近 N 年的数据覆盖与关键字段一致性。
  - 对 `industry/fina_indicator`：抽样对比现有下游消费查询的结果（行数、分布、关键字段），明确可接受差异阈值并固化成脚本。

- **D-3：可运维性与幂等性**
  - `scripts/features_init.py` 能初始化并刷新相关 MV；连续执行 2 次无副作用。
  - 刷新策略、索引与并发刷新能力（如启用 concurrent）具备明确前置条件与失败回滚策略。
  - 输出包含基础血缘字段（如 `_source_table/_processed_at/_data_version`），便于追踪与审计。

---

## 7. 风险与缓解

### 7.1 风险清单

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 迁移过程中引入 bug | 物化视图刷新失败 | 迁移后对比原有产出 |
| CLI 移除后操作不便 | 日常运维需写脚本 | 提供最小化的 Python 脚本入口 |
| 特征甄别遗漏 | 重要特征未入库 | 保持甄别清单可扩展 |

### 7.2 验收指标

| 类别 | 验收指标 | 通过标准 |
|------|----------|----------|
| **功能验收** | 至少 1 个 features MV 可创建、刷新、校验、被下游读取 | 脚本运行无报错，数据一致 |
| **依赖验收** | 全项目无旧 processors import | PowerShell `Select-String -Path alphahome/**,scripts/**,tests/** -Pattern "processors." -SimpleMatch` 返回空（或 `rg "processors\."` 返回空） |
| **可运维性** | 初始化/刷新/校验均有脚本入口且可重复执行 | 连续运行 2 次无副作用 |
| **甄别完整性** | 首批特征全部有入库卡片 | `alphahome/features/cards/*.yaml` 与清单一一对应 |

### 7.3 回滚策略

| 阶段 | 回滚触发条件 | 回滚步骤 |
|------|--------------|----------|
| A（features 骨架） | 目录结构或接口设计严重缺陷 | 删除 `alphahome/features/`，重新设计 |
| B（物化视图迁移） | MV 刷新逻辑迁移后频繁失败 | 回退到迁移前实现（恢复 `processors/materialized_views/` 的使用路径），保留 features 但不启用 |
| D（CLI 下线） | 运维痛点超出预期 | 恢复 `cli/commands/mv.py`（从 Git 历史恢复） |
| E（processors 删除） | 发现遗漏依赖 | 从 Git 历史恢复整个目录 |

**回滚原则**：
- 每阶段完成后 **Git 打 tag**（如 `features-mig-A-done`），便于定点恢复。
- 数据库层面：物化视图删除前先 `DROP ... IF EXISTS`，新旧视图可共存过渡。

---

## 8. 里程碑

| 里程碑 | 包含阶段 | 预期产出 | 状态 |
|--------|----------|----------|------|
| M1: features 骨架 | A1 | `alphahome/features/` 可 import | ✅ 已完成 |
| M2: 物化视图迁移完成 | A2-A6 | features/storage 可独立运行 | ✅ 已完成 |
| M3: CLI 下线 | C1-C4 | 无 CLI 入口，cli/ 最小化 | ✅ 已完成 |
| M4: processors 删除 | D1-D4 | 目录已删除 | ✅ 已完成 |
| M5: PIT 替代完成（MV PIT 化） | D0-D3 | income/balance/industry/fina_indicator 等可 PIT 消费的 MV 达到 D-1/D-2/D-3 验收 | ✅ 已完成 |
| M6: 首批特征入库 | 基于甄别清单 | valuation/breadth/margin/volatility/limit 等 MV 可用 | 🔄 进行中 |

---

## 9. 附录

### 9.1 相关文档

- [系统架构概览](system_overview.md)
- [任务系统设计](task_system.md)
- [PIT 增量更新指南](../pit_incremental_update_guide.md)

### 9.2 待决事项

| 事项 | 选项 | 当前倾向 |
|------|------|----------|
| 物化视图 schema 命名 | `materialized_views` vs `features` | 渐进迁移到 `features` |
| operations 是否保留 | 迁到 common vs 废弃 | 废弃（外部无引用） |
| research/pit_data 迁移后是否保留 | 保留入口 vs 删除 | 待讨论 |

---

## 10. 变更记录

| 日期 | 版本 | 变更内容 |
|------|------|----------|
| 2026-01-27 | v0.1 | 初稿，基于讨论整理 |
| 2026-01-27 | v0.2 | 补强关键点：1) Section 1.4 CLI 边界澄清；2) Section 3.3 features 核心接口契约；3) Section 4.0 执行顺序流程图；4) Section 5 八步甄别流程 + 入库卡片模板；5) Section 7.2/7.3 验收指标 + 回滚策略 |
| 2026-01-27 | v0.3 | 可行性审查修正：1) Section 2.3 补充 4 个 MV 定义文件迁移清单；2) Section 3.2 补充 cards/ 目录；3) Section 4.1 A6 引用 MV 清单；4) Section 4.2 B5 测试路径修正 |
| 2026-01-29 | v0.4 | 补充 features 命名与目录规范（对齐 fetchers 的可读性原则）；明确 recipes 分域目录与输出对象命名；细化 A6 的落地约束与兼容策略 |
| 2026-01-29 | v0.5 | **代码落地**：1) 实现 @feature_register 装饰器 + FeatureRegistry.discover() 动态扫描；2) 重组 recipes/mv/ 为 {domain}/ 分组结构；3) 迁移 4 个 MV 到分域目录并添加装饰器；4) 保留兼容导出；5) 验证通过 |
| 2026-01-29 | v0.6 | 文档全面对齐现状：更新 MV 清单与目录规范（移除 mv/pit wrapper 描述）、修正数据流向（区分 research/pit_data 现状与 features/pit 规划）、同步 BaseFeatureView/FeatureRegistry 的已落地契约与里程碑状态 |
| 2026-01-29 | v0.8 | **Phase 3 进展**：完成 C1-C4 CLI 入口下线（pyproject.toml 移除 4 个入口点，cli/ 目录最小化）；完成 D1 验收（scripts/tests 无 processors 引用），待执行 D2-D4 删除 processors |
| 2026-01-29 | v0.9 | **Phase 3 完成**：执行 D2-D4，删除 `alphahome/processors/` 目录、移除顶层导出、同步 README/docs/repowiki 去除 processors/CLI 引用；全仓 grep 清零验收、pytest 104 passed |
| 2026-01-29 | v0.10 | **M6 策略调整**：6.2 改为"MV PIT 化替代（方案 D）"，新增 income/balance 两张派生 MV 的下一步计划，并引入更严格验收 D-1/D-2/D-3 |
| 2026-01-29 | v0.11 | **M6 D1/D2 完成**：落地 `stock_income_quarterly.py`、`stock_balance_quarterly.py`；FeatureRegistry 可发现 6 个配方；pytest 104 passed |
| 2026-01-29 | v0.12 | **M6 D3 验收通过**：修复财报类 MV 的 PIT 窗口逻辑（同一公告日多条记录问题：改用 DISTINCT + LEAD 三段 CTE）；创建 `scripts/features_validate_pit.py` 验收脚本；D-1/D-2/D-3 全部通过 |
| 2026-01-30 | v0.13 | **完全对标 PIT 表**：income MV 整合 report+express+forecast 三数据源（覆盖率 108%）；balance MV 整合 report+express（覆盖率 103%）；MV 完全覆盖 PIT 表（PIT 独有记录为 0）；验收全部通过 |
| 2026-01-30 | v0.14 | **行业分类对标完成**：`stock_industry_monthly_snapshot.py`（月度快照）整合 sw+ci 双数据源（1.68M 行），完全覆盖 pit_industry_classification（L1/L2 匹配率 100%，覆盖率 120%）；单元测试 7 passed |
| 2026-01-30 | v0.15 | **命名收敛完成**：行业 MV 最终命名为 `stock_industry_monthly_snapshot`（类名 `StockIndustryMonthlySnapshotMV`、MV 名 `mv_stock_industry_monthly_snapshot`）；全仓引用统一更新；兼容别名 `StockSwIndustryMV` 保留；单元测试 7 passed |
| 2026-01-30 | v0.16 | **M7 甄别清单完成**：全面分析 data_infra 34 个 Fetcher，完成 Section 5.4 甄别状态表（推荐入库 20 项、需评审 19 项、不入库 8 项）；明确依赖表、时间语义、PIT 安全性 |
| 2026-01-30 | v0.17 | **M7 首批 MV 落地**：新增 5 个 market 域 MV（`index_valuation_daily`、`market_breadth_daily`、`margin_ratio_monthly`、`index_volatility_daily`、`limit_updown_daily`）；创建 2 张入库卡片（`index_valuation_daily.yaml`、`market_breadth_daily.yaml`）；FeatureRegistry.discover() = 11 个 recipe；单元测试 7 passed |
| 2026-01-30 | v0.18 | **M7 完结**：新增 6 个 market 域 MV（`market_technical_daily`、`index_technical_daily`、`style_features_daily`、`margin_turnover_daily`、`etf_flow_daily`、`index_fundamental_daily`）；合并 8 个 Fetcher（margin_turnover_ratio、index_fundamental、market_technical、market_return_distribution、index_boll_signals、index_ma120_distance、etf_flow、style_index_return/style_momentum）；甄别状态表全部更新为 ✅；单元测试 18 passed |
