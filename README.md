# AlphaHome: 自动化金融数据任务系统

## 项目简介

AlphaHome 是一个基于 Python 异步框架 (`asyncio`) 构建的、灵活且可扩展的金融数据获取、处理和存储系统。它旨在简化从 Tushare 等数据源自动同步金融数据的过程，并将其高效地存储到 PostgreSQL 数据库中。

系统核心采用模块化任务设计，通过 `TaskFactory` ([`alphahome/fetchers/task_factory.py`](alphahome/fetchers/task_factory.py:135)) 管理。每个数据任务（例如获取股票日线、财务指标）都继承自 `Task` ([`alphahome/fetchers/base_task.py`](alphahome/fetchers/base_task.py:10)) 或更具体的 `TushareTask` ([`alphahome/fetchers/sources/tushare/tushare_task.py`](alphahome/fetchers/sources/tushare/tushare_task.py:18)) 基类。近期架构已重构，将复杂的 Tushare 数据处理逻辑分解到 `TushareBatchProcessor` ([`alphahome/fetchers/sources/tushare/tushare_batch_processor.py`](alphahome/fetchers/sources/tushare/tushare_batch_processor.py:12)) 和 `TushareDataTransformer` ([`alphahome/fetchers/sources/tushare/tushare_data_transformer.py`](alphahome/fetchers/sources/tushare/tushare_data_transformer.py:10)) 中，以提高代码的可维护性和可扩展性。

## 主要特性

*   **异步高效**: 基于 `asyncio` 和 `aiohttp` 构建，支持高并发数据获取和处理。
*   **模块化任务设计**: 通过 `Task` ([`alphahome/fetchers/base_task.py`](alphahome/fetchers/base_task.py:10)) 和 `TushareTask` ([`alphahome/fetchers/sources/tushare/tushare_task.py`](alphahome/fetchers/sources/tushare/tushare_task.py:18)) 基类，以及 `TushareBatchProcessor` ([`alphahome/fetchers/sources/tushare/tushare_batch_processor.py`](alphahome/fetchers/sources/tushare/tushare_batch_processor.py:12)) 和 `TushareDataTransformer` ([`alphahome/fetchers/sources/tushare/tushare_data_transformer.py`](alphahome/fetchers/sources/tushare/tushare_data_transformer.py:10))，实现清晰的任务编排与数据处理。
*   **声明式任务定义**: 通过类属性清晰定义任务元数据（API名称、表名、主键、字段、Schema等）。
*   **自动化数据处理与验证**: 内置数据类型转换、列名映射、日期处理和基本的数据验证逻辑。
*   **灵活的更新策略**: 支持全量更新、多种增量更新模式（包括基于数据库最新日期的智能增量）。
*   **配置驱动**: 通过 `.env` 文件和用户特定目录下的 `config.json` 文件管理敏感信息（API Token, DB连接）和应用配置（任务参数覆盖）。
*   **深度数据库集成 (PostgreSQL)**:
    *   使用 `DBManager` ([`alphahome/fetchers/db_manager.py`](alphahome/fetchers/db_manager.py:9)) 进行异步数据库操作。
    *   自动建表、创建索引，并支持在表定义中添加列注释。
    *   高效的 `UPSERT` 操作，确保数据唯一性并高效更新。
*   **API 速率限制管理**: 内置针对 Tushare API 的请求频率和并发控制机制。
*   **命令行工具集**: 提供位于 `scripts/` 目录下的丰富脚本，用于单任务更新、批量任务更新及数据质量检查。
## 系统架构

AlphaHome 采用分层和模块化的架构设计，主要包括以下几个核心层面：

*   **数据源层 (`alphahome/fetchers/sources/`)**: 负责封装与外部数据提供商（如 Tushare）的 API 交互逻辑，包括请求发送、数据解析和错误处理。
*   **任务层 (`alphahome/fetchers/tasks/`)**: 定义具体的数据获取和处理任务。每个任务都是一个独立的类，继承自通用的任务基类，并负责特定数据的完整生命周期管理。
*   **核心工具层 (`alphahome/fetchers/`)**: 提供项目共享的核心组件，例如：
    *   `DBManager` ([`alphahome/fetchers/db_manager.py`](alphahome/fetchers/db_manager.py:9)): 统一管理与 PostgreSQL 数据库的异步交互。
    *   `TaskFactory` ([`alphahome/fetchers/task_factory.py`](alphahome/fetchers/task_factory.py:135)): 负责任务的注册、发现和实例化。
    *   `BaseTask` ([`alphahome/fetchers/base_task.py`](alphahome/fetchers/base_task.py:10)) 和 `TushareTask` ([`alphahome/fetchers/sources/tushare/tushare_task.py`](alphahome/fetchers/sources/tushare/tushare_task.py:18)): 提供任务定义的基础框架。
*   **图形用户界面 (GUI) 层 (`alphahome/gui/`)**: 基于 Tkinter 构建，提供用户友好的操作界面。
    *   它通过 `Controller` ([`alphahome/gui/controller.py`](alphahome/gui/controller.py:47)) 与后端逻辑进行异步通信（使用 Python 的 `queue.Queue`），确保界面操作的流畅性。
*   **脚本层 (`scripts/`)**: 包含一系列命令行脚本，用于自动化执行数据更新、数据质量检查等维护任务。

这种分层设计使得系统各部分职责清晰，易于维护和扩展。
*   **智能批处理**: 能够根据交易日历 ([`alphahome/fetchers/tools/calendar.py`](alphahome/fetchers/tools/calendar.py)) 和任务特性自动拆分大数据量任务为小批次执行。
*   **图形用户界面 (GUI)**: 提供基于 Tkinter 的用户界面 ([`alphahome/gui/`](alphahome/gui/))，方便任务选择、配置、执行监控和日志查看。
*   **数据质量检查**: 内置数据质量检查工具 ([`alphahome/fetchers/data_checker.py`](alphahome/fetchers/data_checker.py:23))，可检查数据覆盖率和完整性。
*   **易于扩展**: 清晰的架构设计使得添加新的数据源适配器和数据任务相对简单。

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
│   ├── عوامل/              # 因子计算模块 (当前为空或占位)
│   │   ├── __init__.py
│   │   ├── core/
│   │   ├── definitions/
│   │   ├── pipelines/
│   │   └── utils/
│   ├── fetchers/             # 数据获取与管理核心
│   │   ├── __init__.py
│   │   ├── base_task.py      # 任务基类 (Task)
│   │   ├── data_checker.py   # 数据质量检查工具
│   │   ├── db_manager.py     # 数据库管理器 (PostgreSQL)
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
## 扩展性

AlphaHome 设计时考虑了良好的可扩展性，方便开发者添加新的数据获取任务或未来可能的因子计算、数据处理等功能。

### 添加新数据任务

添加一个新的数据获取任务（例如，针对 Tushare 的新接口或新的数据源）通常遵循以下步骤：

1.  **创建任务类**: 在 `alphahome/fetchers/tasks/` 下相应的子目录（如 `stock/`, `fund/`，或为新数据源创建新目录）中创建一个新的 Python 文件。在该文件中定义一个新的任务类，它应继承自 `TushareTask` ([`alphahome/fetchers/sources/tushare/tushare_task.py`](alphahome/fetchers/sources/tushare/tushare_task.py:18))（如果使用 Tushare 数据源）或通用的 `Task` 基类 ([`alphahome/fetchers/base_task.py`](alphahome/fetchers/base_task.py:10))（如果为其他数据源）。
2.  **定义任务属性**: 在新创建的类中，定义必要的类属性，例如：
    *   `name`: 任务的唯一标识符 (字符串)。
    *   `description`: 任务功能的简要描述。
    *   `table_name`: 数据将存储到的数据库表名。
    *   `primary_keys`: 表的主键列表 (用于 `UPSERT` 操作)。
    *   `date_column`: 用于增量更新的日期列名 (如果适用)。
    *   `api_name` (特指 `TushareTask`): 对应的 Tushare API 接口名称。
    *   `fields` (特指 `TushareTask`): 需要从 API 获取的字段列表。
    *   `schema`: 定义数据库表结构，包括列名、数据类型、约束以及可选的 `comment` (将作为数据库列注释)。
    *   可选属性如 `column_mapping` (API字段到数据库列的映射), `transformations` (数据转换规则), `validations` (数据验证规则), `indexes` (自定义数据库索引)。
3.  **实现核心方法**: 根据任务需求，实现或重写关键方法：
    *   `get_batch_list(self, **kwargs) -> List[Dict]`: (异步方法) 定义如何根据输入参数（如日期范围、代码列表）生成 API 调用所需的参数批次列表。可以使用项目提供的 `generate_trade_day_batches` 或 `generate_single_date_batches` 等工具函数 ([`alphahome/fetchers/tools/batch_utils.py`](alphahome/fetchers/tools/batch_utils.py)) 来辅助生成批次。
    *   `prepare_params(self, batch: Dict, **kwargs) -> Dict`: (异步方法) 准备每一次 API 调用所需的具体参数。
    *   可选地，可以重写 `process_data` (数据后处理) 或 `validate_data` (自定义验证) 等方法。
4.  **注册任务**: 使用 `@task_register()` 装饰器 ([`alphahome/fetchers/task_decorator.py`](alphahome/fetchers/task_decorator.py:13)) 标记你的任务类，使其能被 `TaskFactory` ([`alphahome/fetchers/task_factory.py`](alphahome/fetchers/task_factory.py:135)) 自动发现和注册。
5.  **导入任务**: (重要) 确保在相应的 `__init__.py` 文件中导入新创建的任务类，以便任务工厂能够扫描到。通常需要修改任务所在子目录的 `__init__.py` (例如 `alphahome/fetchers/tasks/stock/__init__.py`) 以及 `alphahome/fetchers/tasks/__init__.py`。

更详细的步骤和示例请参考 [开发者指南](docs/developer_guide.md)。

### 未来扩展方向

*   **因子计算 (`alphahome/factors/`)**: 此模块目前为占位结构，未来可以扩展用于定义和计算各种金融因子。
*   **数据处理器 (`alphahome/processors/`)**: 此模块目前也为占位结构，未来可以用于实现更复杂的数据处理流水线或衍生数据计算。
## 项目结构

```
alphahome/
├── alphahome/                # AlphaHome 主包
│   ├── __init__.py
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
│   │   ├── db_manager.py     # 数据库管理器 (PostgreSQL)
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

## 批处理功能说明
## 详细文档

更详细的关于项目使用、开发和数据库结构的信息，请参阅 `docs/` 目录下的相关文档：

*   **[用户指南](docs/user_guide.md)**: 详细介绍了系统的安装、配置、GUI 使用方法、命令行脚本操作以及常见问题解答。
*   **[开发者指南](docs/developer_guide.md)**: 提供了系统架构、代码组织、核心组件设计、开发流程、测试指南以及如何添加新功能（如数据源、任务）的详细说明。
*   **[TushareDB 使用文档](docs/tusharedb_usage.md)**: 详细描述了 `tusharedb` 数据库的表结构、各字段含义以及对应的数据来源 (Tushare API)。

系统提供了智能批处理功能，可以根据交易日历自动将数据获取任务分成多个批次进行处理。主要通过 `alphahome/fetchers/tools/batch_utils.py` 中的工具函数实现。

### 1. 按固定交易日数分批 (`generate_trade_day_batches`)

当API允许一次查询一定日期范围的数据时，可以使用此函数。

#### 批处理参数配置 (任务类中)

```python
# 批处理配置示例 (当使用 generate_trade_day_batches 时)
batch_trade_days_single_code = 240  # 单代码查询时，每个批次的交易日数量 (约1年)
batch_trade_days_all_codes = 15    # 全市场查询时，每个批次的交易日数量 (约3周)
```

#### 使用方法

```python
from ...tools.batch_utils import generate_trade_day_batches

async def get_batch_list(self, **kwargs) -> List[Dict]:
    start_date = kwargs.get('start_date', self.default_start_date)
    end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
    ts_code = kwargs.get('ts_code')
    exchange = kwargs.get('exchange', 'SSE')
    
    current_batch_size = self.batch_trade_days_single_code if ts_code else self.batch_trade_days_all_codes
    
    batch_list = await generate_trade_day_batches(
        start_date=start_date,
        end_date=end_date,
        batch_size=current_batch_size,
        ts_code=ts_code, # 如果提供，会加入到每个批次参数中
        exchange=exchange,
        logger=self.logger
    )
    # 返回的批次通常包含 'start_date', 'end_date', 和可选的 'ts_code'
    return batch_list
```
该工具函数会：
1.  根据交易日历自动识别指定日期范围内的实际交易日。
2.  根据提供的 `batch_size` 将交易日分批。
3.  如果提供了 `ts_code`，则将其包含在每个生成的批次参数字典中。

### 2. 按单个交易日分批 (`generate_single_date_batches`)

当API（尤其在全市场查询时）要求按单个交易日获取数据，或者业务逻辑需要逐日处理时，可以使用此函数。

#### 使用方法

```python
from ...tools.batch_utils import generate_single_date_batches

async def get_batch_list(self, **kwargs) -> List[Dict]:
    start_date = kwargs.get('start_date', self.default_start_date)
    end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
    ts_code = kwargs.get('ts_code') # 可选
    exchange = kwargs.get('exchange', 'SSE')
    
    # API cyq_perf 查询全市场时要求参数为 trade_date
    # date_field 指定了API调用时日期参数的键名
    batch_list = await generate_single_date_batches(
        start_date=start_date,
        end_date=end_date,
        date_field='trade_date', # API参数中的日期字段名
        ts_code=ts_code,         # 可选的股票代码
        exchange=exchange,
        logger=self.logger
    )
    # 返回的批次列表形如: [{'trade_date': 'YYYYMMDD', 'ts_code': 'xxxx'(可选)}, ...]
    # 每个字典代表一个API调用批次。
    return batch_list
```
该工具函数会：
1.  获取指定日期范围内的所有交易日。
2.  为每个交易日生成一个批次字典。
3.  批次字典中包含一个由 `date_field` 参数指定的键（值为该交易日），以及可选的 `ts_code`。
    例如，`TushareStockChipsTask` 获取每日筹码数据时，全市场查询 (`ts_code` 为 `None`) 就使用此方法，API参数为 `trade_date`。

## 脚本使用说明

本节包含了 `scripts` 目录下的脚本使用指南，提供详细的数据更新和质量检查工具使用方法。

### 脚本目录结构

```
scripts/
├── base/                    # 基础类文件
│   └── task_updater_base.py  # 更新任务的基类
├── tasks/                   # 任务更新脚本
│   ├── finance/            # 财务数据相关
│   │   ├── update_balancesheet.py  # 资产负债表更新
│   │   ├── update_cashflow.py      # 现金流量表更新
│   │   ├── update_express.py       # 业绩快报更新
│   │   ├── update_forecast.py      # 业绩预告更新
│   │   ├── update_income.py        # 利润表更新
│   │   └── update_indicator.py     # 财务指标更新
│   ├── stock/              # 股票数据相关
│   │   ├── update_adjfactor.py     # 复权因子更新
│   │   ├── update_daily.py         # 日线行情更新
│   │   └── update_dailybasic.py    # 每日指标更新
│   └── index/              # 指数数据相关（预留）
├── check_db_quality.py     # 数据库表质量全面检查工具
├── tools/                  # 工具脚本
│   └── check_stock_daily_quality.py  # 股票日线数据质量检查
└── batch/                  # 批量更新脚本
    └── update_all_tasks.py          # 全量更新脚本
```

### 单任务更新

每个更新脚本都支持以下参数：

- `--quarters`: 指定要更新的季度数
- `--years`: 指定要更新的年数
- `--report-period`: 指定要更新的报告期（如 20230331）
- `--start-date`: 指定更新的起始日期
- `--end-date`: 指定更新的结束日期
- `--full-update`: 执行全量更新

示例：
```bash
# 更新最近4个季度的现金流量表数据
python scripts/tasks/finance/update_cashflow.py --quarters 4

# 更新指定报告期的利润表数据
python scripts/tasks/finance/update_income.py --report-period 20230331

# 更新指定日期范围的股票日线数据
python scripts/tasks/stock/update_daily.py --start-date 20230101 --end-date 20230331
```

### 批量更新

使用 `update_all_tasks.py` 可以批量更新多个任务：

```bash
# 更新所有任务的最新数据
python scripts/batch/update_all_tasks.py

# 更新指定任务列表的数据
python scripts/batch/update_all_tasks.py --tasks "tushare_fina_cashflow,tushare_fina_income"
```

### 数据质量检查

#### 单表数据质量检查

使用 `check_stock_daily_quality.py` 可以检查股票日线数据的质量：

```bash
# 检查指定日期范围的数据质量
python scripts/tools/check_stock_daily_quality.py --start-date 20230101 --end-date 20230331
```

#### 全面数据库质量检查

使用 `check_db_quality.py` 可以对整个数据库或指定表进行全面质量检查：

```bash
# 运行全面的数据库表质量检查
python scripts/check_db_quality.py

# 检查特定表的数据质量
python scripts/check_db_quality.py -t tushare_stock_daily tushare_fund_daily

# 检查特定日期范围的数据质量
python scripts/check_db_quality.py -s 20230101 -e 20231231

# 指定输出目录（默认为logs/db_quality_时间戳）
python scripts/check_db_quality.py -o logs/custom_quality_check

# 启用详细日志输出
python scripts/check_db_quality.py -v
```

支持的参数：
- `-t, --tables`: 指定要检查的表名（多个表用空格分隔）
- `-s, --start-date`: 指定检查的起始日期（格式：YYYYMMDD）
- `-e, --end-date`: 指定检查的结束日期（格式：YYYYMMDD）
- `-o, --output-dir`: 指定输出目录路径
- `-v, --verbose`: 启用详细日志输出
- `-h, --help`: 显示帮助信息

检查完成后，脚本会生成包含以下内容的报告：
- HTML格式的综合质量报告
- 各表数据覆盖率的可视化图表
- 详细的JSON格式质量数据
- 按月数据覆盖分析

### 开发说明

1. 所有更新脚本都继承自 `TaskUpdaterBase` 类
2. 新增更新脚本时，请遵循现有的目录结构和命名规范
3. 请确保添加适当的日志记录和错误处理
4. 建议在更新脚本中添加数据质量检查逻辑

## 示例任务说明

本节介绍 `fetchers/tasks/examples` 目录下的示例任务，用于演示如何创建和注册自定义任务。

### 示例任务说明

- `custom_task_example.py`: 包含两个示例任务类
  - `CustomExampleTask`: 使用带参数的装饰器注册
  - `AnotherExampleTask`: 使用不带参数的装饰器注册

### 当前状态

这些示例任务已在 `__init__.py` 中被禁用，原因是它们在实际的全局更新脚本中会导致错误，因为它们需要 `api_token` 参数，而这个参数在当前的 `Task` 类实现中不支持。

### 使用方法

如果需要重新启用这些示例任务，请修改 `__init__.py` 文件，取消注释相关导入语句和 `__all__` 列表中的条目。同时，您需要更新 `custom_task_example.py` 中的任务实现，使其兼容当前的 `Task` 类接口。

### 错误信息参考

运行全局更新脚本时，这些示例任务会产生以下错误：

```
TypeError: Task.__init__() got an unexpected keyword argument 'api_token'
```

## 贡献指南

欢迎对本项目做出贡献！请遵循标准的 GitHub Flow：

1.  Fork 本仓库。
2.  创建新的特性分支 (`git checkout -b feature/AmazingFeature`)。
3.  提交你的更改 (`git commit -m 'Add some AmazingFeature'`)。
4.  将更改推送到分支 (`git push origin feature/AmazingFeature`)。
5.  打开一个 Pull Request。

## 许可证

本项目采用 MIT 许可证。详情请见 `LICENSE` 文件 (如果存在)。

## GUI 使用说明

除了命令行脚本，本项目还提供了一个图形用户界面 (GUI) 来管理和执行数据任务。

### 启动 GUI

在项目根目录下（或已安装的环境中），运行以下命令启动 GUI：

```bash
python -m gui.main
```

### 主要功能

GUI 界面通常包含以下几个主要部分：

1.  **任务列表**: 
    *   显示所有已发现的数据任务，包含名称、类型、描述、数据库表名和最后更新时间。
    *   每行开头有一个复选框，用于选择要执行的任务。
    *   提供按类型选择/取消选择的按钮。

2.  **执行控制**: 
    *   **运行模式**: 选择任务执行的方式：
        *   `全量导入`: 从最早日期（如 19900101）获取所有数据。
        *   `智能增量`: 自动从数据库中记录的最新日期开始更新。
        *   `手动增量`: 需要手动指定一个 `开始日期` 进行更新。
    *   **开始日期**: （仅在 `手动增量` 模式下可用）选择一个起始日期。
    *   **执行任务**: 启动选中任务的执行。
    *   **停止任务**: （在任务执行期间可用）请求停止当前正在运行的任务批次。

3.  **运行状态**: 
    *   显示当前执行批次中各个任务的状态（排队中、运行中、完成、失败、已取消等）、进度、开始/结束时间。

4.  **日志输出**: 
    *   实时显示来自后端任务和系统的日志信息，包括进度、警告和错误。

5.  **菜单栏**: 
    *   **文件 -> 存储设置**: 打开设置窗口，用于配置 PostgreSQL 数据库连接 URL 和 Tushare API Token。**这些设置将保存到用户目录的 `config.json` 文件中。**
    *   **文件 -> 退出**: 关闭应用程序。
    *   **操作**: 提供快速选择/取消选择所有任务或按类型选择任务的功能。

### 基本流程

1.  启动 GUI。
2.  (首次使用) 进入 `文件 -> 存储设置` 配置数据库和 Tushare Token 并保存。
3.  在任务列表中勾选你想要执行的任务。
4.  选择合适的 `运行模式`（和 `开始日期`，如果需要）。
5.  点击 `执行任务` 按钮。
6.  在 `运行状态` 和 `日志输出` 区域观察任务执行情况。
7.  如果需要，可以点击 `停止任务` 按钮来中断执行。