# AlphaHome: 自动化金融数据任务系统

## 项目简介

AlphaHome 是一个基于 Python 异步框架 (`asyncio`) 构建的、灵活且可扩展的金融数据获取、处理和存储系统。它旨在简化从 Tushare 等数据源自动同步金融数据的过程，并将其高效地存储到 PostgreSQL 数据库中。

系统核心采用模块化任务设计，通过 `TaskFactory` ([`alphahome/fetchers/task_factory.py`](alphahome/fetchers/task_factory.py:135)) 管理。每个数据任务（例如获取股票日线、财务指标）都继承自 `Task` ([`alphahome/fetchers/base_task.py`](alphahome/fetchers/base_task.py:10)) 或更具体的 `TushareTask` ([`alphahome/fetchers/sources/tushare/tushare_task.py`](alphahome/fetchers/sources/tushare/tushare_task.py:18)) 基类。

## 🚀 最新重大更新

**v2.0 架构升级** - 本版本引入了两项重大改进：

1. **双模式数据库管理器**: `DBManager` 已完全重构，支持异步 (`asyncpg`) 和同步 (`psycopg2`) 双模式操作，提供统一API的同时大幅提升性能和兼容性。

2. **回测扩展模块**: 新增 `bt_extensions` 模块，提供高性能回测基础设施，包括并行执行引擎、增强分析器、数据源适配器和性能监控等功能。

近期还将复杂的 Tushare 数据处理逻辑分解到 `TushareBatchProcessor` ([`alphahome/fetchers/sources/tushare/tushare_batch_processor.py`](alphahome/fetchers/sources/tushare/tushare_batch_processor.py:12)) 和 `TushareDataTransformer` ([`alphahome/fetchers/sources/tushare/tushare_data_transformer.py`](alphahome/fetchers/sources/tushare/tushare_data_transformer.py:10)) 中，以提高代码的可维护性和可扩展性。

## 主要特性

### 🔧 核心架构
*   **双模式数据库管理器**: `DBManager` ([`alphahome/common/db_manager.py`](alphahome/common/db_manager.py:23)) 支持异步 (`asyncpg`) 和同步 (`psycopg2`) 双模式操作，提供统一且高效的数据库交互接口，包括高效UPSERT、自动化表管理和索引创建。
*   **异步高效**: 基于 `asyncio` 和 `aiohttp` 构建，支持高并发数据获取和处理。
*   **模块化任务设计**: 通过 `Task` 和 `TushareTask` 基类，以及 `TushareBatchProcessor` 和 `TushareDataTransformer`，实现清晰的任务编排与数据处理。

### 📊 数据处理
*   **声明式任务定义**: 通过类属性清晰定义任务元数据（API名称、表名、主键、字段、Schema等）。
*   **自动化数据处理与验证**: 内置数据类型转换、列名映射、日期处理和基本的数据验证逻辑。
*   **灵活的更新策略**: 支持全量更新、多种增量更新模式（包括基于数据库最新日期的智能增量）。
*   **智能批处理**: 能够根据交易日历自动拆分大数据量任务为小批次执行。

### 🚀 回测与分析
*   **回测扩展模块 (`bt_extensions/`)**: 提供增强分析器、并行执行引擎、数据源适配器、缓存管理和性能监控等功能，为量化回测提供高性能基础设施支持。

### 🛠️ 工具与界面
*   **图形用户界面 (GUI)**: 提供基于 Tkinter 的用户界面，方便任务选择、配置、执行监控和日志查看。
*   **命令行工具集**: 提供丰富的脚本，用于单任务更新、批量任务更新及数据质量检查。
*   **数据质量检查**: 内置数据质量检查工具，可检查数据覆盖率和完整性。

### ⚙️ 配置与管理
*   **配置驱动**: 通过 `.env` 文件和用户特定目录下的 `config.json` 文件管理敏感信息和应用配置。
*   **API 速率限制管理**: 内置针对 Tushare API 的请求频率和并发控制机制。
*   **易于扩展**: 清晰的架构设计使得添加新的数据源适配器和数据任务相对简单。

## 系统架构

AlphaHome 采用分层和模块化的架构设计，主要包括以下几个核心层面：

*   **数据源层 (`alphahome/fetchers/sources/`)**: 负责封装与外部数据提供商（如 Tushare）的 API 交互逻辑，包括请求发送、数据解析和错误处理。
*   **任务层 (`alphahome/fetchers/tasks/`)**: 定义具体的数据获取和处理任务。每个任务都是一个独立的类，继承自通用的任务基类，并负责特定数据的完整生命周期管理。
*   **核心工具层 (`alphahome/fetchers/` & `alphahome/common/`)**: 提供项目共享的核心组件，例如：
    *   `DBManager` ([`alphahome/common/db_manager.py`](alphahome/common/db_manager.py:9)): 双模式数据库管理器，统一管理异步和同步数据库交互。
    *   `TaskFactory` ([`alphahome/fetchers/task_factory.py`](alphahome/fetchers/task_factory.py:135)): 负责任务的注册、发现和实例化。
    *   `BaseTask` 和 `TushareTask`: 提供任务定义的基础框架。
*   **回测扩展层 (`alphahome/bt_extensions/`)**: 提供量化回测基础设施，包括并行执行引擎、增强分析器、数据源适配器和性能监控等功能。
*   **图形用户界面 (GUI) 层 (`alphahome/gui/`)**: 基于 Tkinter 构建，提供用户友好的操作界面。
    *   它通过 `Controller` ([`alphahome/gui/controller.py`](alphahome/gui/controller.py:47)) 与后端逻辑进行异步通信（使用 Python 的 `queue.Queue`），确保界面操作的流畅性。
*   **脚本层 (`scripts/`)**: 包含一系列命令行脚本，用于自动化执行数据更新、数据质量检查等维护任务。

这种分层设计使得系统各部分职责清晰，易于维护和扩展。

## 🚀 快速开始

### 1. 克隆项目
```bash
git clone <your-repository-url>
cd alphaHome
```

### 2. 安装依赖
```bash
pip install -e .
```

### 3. 配置环境
创建 `.env` 文件并配置：
```bash
TUSHARE_API_TOKEN=your_tushare_token
DB_CONNECTION_STRING=postgresql+asyncpg://user:password@localhost:5432/database
```

### 4. 启动 GUI
```bash
python -m alphahome.gui.main_window
```

或使用命令行脚本：
```bash
python scripts/tasks/stock/update_daily.py --quarters 1
```

## 环境配置与安装

### 前提条件

*   **Python**: >=3.9 (根据 [`pyproject.toml`](pyproject.toml) 定义)
*   **PostgreSQL**: 12+ (推荐)
*   **Git**

### 安装步骤

1.  **克隆仓库**:
    ```bash
    git clone <your-repository-url> # 请替换为实际的仓库 URL
    cd alphaHome
    ```

2.  **创建并激活虚拟环境** (推荐):
    ```bash
    python -m venv venv
    # Windows
    venv\Scripts\activate
    # macOS/Linux
    source venv/bin/activate
    ```

3.  **安装依赖**:
    项目使用 [`pyproject.toml`](pyproject.toml) 管理依赖。推荐使用以下命令安装项目及其依赖：
    ```bash
    pip install .
    ```
    如果需要进行开发，可以使用编辑模式安装：
    ```bash
    pip install -e .
    ```
    或者，如果 [`requirements.txt`](requirements.txt) 包含了所有必要的运行时和开发依赖（请确认其与 [`pyproject.toml`](pyproject.toml) 的同步情况）：
    ```bash
    pip install -r requirements.txt
    # 如果有开发特定依赖，可能需要 pip install -r requirements-dev.txt
    ```

### 配置文件说明

AlphaHome 使用两种主要的配置文件：

1.  **`.env` 文件 (项目根目录)**:
    *   **用途**: 存储敏感信息和基础环境配置，如 API Token 和数据库连接字符串。
    *   **创建**: 此文件**必须创建**。您可以复制项目根目录下的 `.env.example` (如果提供) 并重命名为 `.env`，然后填入您的实际配置。
    *   **主要配置项**:
        *   `TUSHARE_API_TOKEN`: 您的 Tushare Pro API Token (必需)。
        *   `DB_CONNECTION_STRING`: PostgreSQL 数据库连接字符串 (必需)，格式为 `postgresql+asyncpg://<user>:<password>@<host>:<port>/<database>`。
            *   示例: `postgresql+asyncpg://alphahome_user:your_password@localhost:5432/alphahome_db`
    *   **优先级**: 此文件中的配置主要供后端脚本和作为系统默认值使用。

2.  **`config.json` (用户配置目录)**:
    *   **用途**: 存储用户特定的配置，例如通过 GUI 修改的设置（如 Tushare Token、数据库连接信息），以及覆盖任务默认参数（如并发限制、批次大小等）。
    *   **创建与位置**:
        *   项目根目录下提供了 `config.example.json` 作为模板。
        *   请将此模板复制到您的用户特定配置目录，并重命名为 `config.json`，然后根据需要修改。
        *   用户配置目录通常位于：
            *   Windows: `C:/Users/<YourUserName>/AppData/Local/AlphaHome/AlphaHome/config.json` (路径可能因 `appdirs` 库的具体实现略有不同，请以程序首次运行时的日志提示为准)
            *   macOS: `~/Library/Application Support/AlphaHome/config.json`
            *   Linux: `~/.config/AlphaHome/config.json` 或 `~/.local/share/AlphaHome/config.json`
    *   **优先级**: 此文件中的配置会覆盖 `.env` 文件中对应的设置（当通过 GUI 保存时）以及任务代码内定义的默认参数。

**配置优先级总结**: 用户 `config.json` > `.env` 文件 > 任务代码内默认值。

### (可选) 数据库初始化
如果项目包含数据库初始化脚本 (例如 `scripts/init_database.py`，请根据实际情况确认)，您可能需要在首次配置完成后运行它来创建必要的数据库表结构。
```bash
# 示例命令，请根据实际脚本路径和名称调整
python scripts/init_database.py
```

## 使用说明

AlphaHome 项目可以通过图形用户界面 (GUI) 或命令行脚本进行操作。

### 图形用户界面 (GUI)

GUI 提供了一个用户友好的方式来管理任务、配置设置和监控数据获取过程。

**启动 GUI:**

*   **从源码运行**:
    ```bash
    python -m alphahome.gui.main_window
    ```
*   **如果项目已通过 `pip install .` 安装**:
    ```bash
    alphahome
    ```

**GUI 主要功能简介:**

*   **任务列表**: 查看、选择、筛选和排序所有已注册的数据获取任务。
*   **存储设置**: 配置和保存 Tushare API Token。数据库连接信息目前主要通过配置文件管理。
*   **任务运行**: 选择任务执行模式（智能增量、手动增量、全量导入），指定日期范围（如果需要），并启动选定的任务。
*   **运行状态与日志**: 实时监控任务执行状态、进度以及详细的日志输出。

更详细的 GUI 操作指南请参阅 [用户指南](docs/user_guide.md)。

### 命令行脚本

项目在 `scripts/` 目录下提供了多种命令行脚本，用于自动化数据更新和维护任务。

**主要脚本类型:**

*   **单任务更新脚本**: 位于 `scripts/tasks/` 下，按数据类别组织 (如 `stock`, `finance`)，用于更新特定的数据任务。
*   **批量更新脚本**: 如 `scripts/batch/update_all_tasks.py`，用于一次性更新所有或指定的多个任务。
*   **数据质量检查脚本**: 如 `scripts/check_db_quality.py`，用于检查数据库中数据的完整性和覆盖情况。

**示例用法:**

*   更新最近4个季度的现金流量表数据:
    ```bash
    python scripts/tasks/finance/update_cashflow.py --quarters 4
    ```
*   更新指定日期范围的股票日线数据:
    ```bash
    python scripts/tasks/stock/update_daily.py --start-date 20230101 --end-date 20230331
    ```
*   运行全面的数据库质量检查:
    ```bash
    python scripts/check_db_quality.py
    ```

详细的脚本列表、参数说明和使用示例，请参阅 [`docs/user_guide.md`](docs/user_guide.md) 和 [`docs/developer_guide.md`](docs/developer_guide.md)。

## 项目结构

```
alphahome/
├── alphahome/                # AlphaHome 主包
│   ├── __init__.py
│   ├── bt_extensions/        # 回测扩展模块
│   │   ├── __init__.py
│   │   ├── README.md         # 模块说明文档
│   │   ├── analyzers/        # 分析器模块
│   │   │   └── enhanced_analyzer.py # 增强分析器
│   │   ├── execution/        # 执行引擎模块
│   │   │   ├── batch_loader.py    # 批量数据加载器
│   │   │   └── parallel_runner.py # 并行运行器
│   │   ├── data/             # 数据处理模块
│   │   │   ├── __init__.py
│   │   │   └── feeds.py      # 数据源适配器
│   │   └── utils/            # 工具模块
│   │       ├── __init__.py
│   │       ├── cache_manager.py      # 缓存管理器
│   │       ├── performance_monitor.py # 性能监控器
│   │       └── exceptions.py         # 异常定义
│   ├── factors/              # 因子计算模块 (当前为空或占位)
│   │   ├── __init__.py
│   │   ├── core/
│   │   ├── definitions/
│   │   ├── pipelines/
│   │   └── utils/
│   ├── fetchers/             # 数据获取与管理核心
│   │   ├── __init__.py
│   │   ├── base_task.py      # 任务基类 (Task)
│   │   ├── data_checker.py   # 数据质量检查工具
│   │   ├── task_decorator.py # 任务注册装饰器 (@task_register)
│   │   ├── task_factory.py   # 任务工厂
│   │   ├── sources/          # 数据源适配层
│   │   │   ├── __init__.py
│   │   │   └── tushare/      # Tushare 数据源实现
│   │   │       ├── __init__.py
│   │   │       ├── tushare_api.py
│   │   │       ├── tushare_batch_processor.py
│   │   │       ├── tushare_data_transformer.py
│   │   │       └── tushare_task.py (TushareTask 基类)
│   │   ├── tasks/            # 具体数据任务定义 (按数据类型组织)
│   │   │   ├── __init__.py   # 导入所有任务子模块
│   │   │   ├── finance/
│   │   │   ├── fund/
│   │   │   ├── hk/
│   │   │   ├── index/
│   │   │   ├── macro/
│   │   │   ├── option/
│   │   │   ├── others/
│   │   │   └── stock/
│   │   └── tools/            # 通用工具
│   │       ├── __init__.py
│   │       ├── batch_utils.py # 批处理工具
│   │       └── calendar.py    # 交易日历工具
│   ├── gui/                  # 图形用户界面 (Tkinter)
│   │   ├── __init__.py
│   │   ├── controller.py     # GUI 控制器
│   │   ├── event_handlers.py # 事件处理器和 UI 构建
│   │   └── main_window.py    # GUI 主窗口和入口
│   ├── common/               # 通用工具和基类
│   │   ├── __init__.py
│   │   ├── config_manager.py # 配置管理器
│   │   └── db_manager.py     # 数据库管理器 (PostgreSQL)
│   └── processors/           # 数据处理模块 (当前为空或占位)
│       └── __init__.py
├── docs/                     # 项目文档
│   ├── developer_guide.md
│   ├── tusharedb_usage.md
│   └── user_guide.md
├── scripts/                  # 命令行脚本 (具体内容参考文档)
├── tests/                    # 测试代码
├── .env.example              # .env 文件模板
├── .gitignore
├── config.example.json       # 用户 config.json 文件模板
├── pyproject.toml            # 项目构建和依赖配置文件
├── README.md                 # 本文档
├── README_old.md             # 旧版 README 备份
├── requirements.txt          # Python 依赖 (建议通过 pyproject.toml 管理)
└── run.py                    # GUI 启动脚本
```
*注：`logs/` 和 `venv/` 等自动生成或环境特定的目录未列出。用户 `config.json` 存储在用户特定的应用数据目录中。*

## 详细文档

更详细的关于项目使用、开发和数据库结构的信息，请参阅 `docs/` 目录下的相关文档：

*   **[用户指南](docs/user_guide.md)**: 详细介绍了系统的安装、配置、GUI 使用方法、命令行脚本操作以及常见问题解答。
*   **[开发者指南](docs/developer_guide.md)**: 提供了系统架构、代码组织、核心组件设计、开发流程、测试指南以及如何添加新功能（如数据源、任务）的详细说明。
*   **[TushareDB 使用文档](docs/tusharedb_usage.md)**: 详细描述了 `tusharedb` 数据库的表结构、各字段含义以及对应的数据来源 (Tushare API)。

## 贡献指南

欢迎对本项目做出贡献！请遵循标准的 GitHub Flow：

1.  Fork 本仓库。
2.  创建新的特性分支 (`git checkout -b feature/AmazingFeature`)。
3.  提交你的更改 (`git commit -m 'Add some AmazingFeature'`)。
4.  将更改推送到分支 (`git push origin feature/AmazingFeature`)。
5.  打开一个 Pull Request。

## 许可证

本项目采用 MIT 许可证。详情请见 `LICENSE` 文件 (如果存在)。