# AutoDatabase: 自动化金融数据任务系统

## 项目简介

AutoDatabase 是一个基于 Python 异步框架构建的、灵活且可扩展的金融数据获取、处理和存储系统。它旨在简化从 Tushare 等数据源自动同步金融数据的过程，并将其存储到 PostgreSQL 数据库中。

系统采用模块化任务设计，每个数据任务（例如获取股票日线、获取财务指标）都被封装为独立的类，易于管理和扩展。

## 主要特性

*   **异步高效**: 基于 `asyncio` 构建，支持高并发数据获取。
*   **声明式任务定义**: 通过类属性清晰地定义任务元数据（如 API 名称、表名、主键、字段等）。
*   **自动化数据处理**: 内置数据类型转换、列名映射和基本验证。
*   **灵活的更新模式**: 支持全量更新、增量更新（按天数、按日期范围、自动检测）。
*   **配置驱动**: 通过 `.env` 文件管理数据库连接和 API 密钥。
*   **易于扩展**: 可以方便地添加新的数据源和数据任务。
*   **数据库集成**: 自动处理数据库表创建和数据插入/更新（基于 `upsert`）。
*   **速率限制管理**: 支持按 Tushare API 接口设置不同的调用频率限制。
*   **命令行工具**: 提供方便的命令行脚本来执行数据更新任务。

## 环境配置

### 1. 克隆项目

```bash
git clone <your-repository-url>
cd autoDatabase
```

### 2. 安装依赖

建议使用虚拟环境：

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 3. 配置环境变量

在项目根目录下创建一个 `.env` 文件，并填入以下内容：

```dotenv
# .env 文件示例

# Tushare API Token (必需)
# 请替换为你的 Tushare Pro API Token
TUSHARE_API_TOKEN=your_actual_tushare_api_token

# PostgreSQL 数据库连接字符串 (必需)
# 格式: postgresql+asyncpg://<user>:<password>@<host>:<port>/<database>
# 例如: postgresql+asyncpg://user:password@localhost:5432/finance_db
DB_CONNECTION_STRING=your_database_connection_string

# 并发限制 (可选, 默认值在 config.py 中定义)
# 控制同时向 Tushare API 发送请求的数量
CONCURRENT_LIMIT=5

# Tushare API 速率限制 (可选, 默认值在 TushareAPI 类中定义)
# 可以为特定的 Tushare 接口设置每分钟的请求次数限制
# TUSHARE_API_RATE_LIMIT_DAILY=500
# TUSHARE_API_RATE_LIMIT_DEFAULT=50
```

**重要**:
*   确保已安装并运行 PostgreSQL 数据库。
*   将 `your_actual_tushare_api_token` 替换为你的 Tushare Pro 账户的有效 Token。
*   将 `your_database_connection_string` 替换为你的 PostgreSQL 数据库的实际连接信息。

## 使用方法

项目提供了两个主要的命令行脚本来更新数据，位于 `examples/` 目录下：

1.  `tushare_stock_daily_updater.py`: 用于更新股票日线行情数据 (`tushare_stock_daily` 表)。
2.  `tushare_stock_dailybasic_updater.py`: 用于更新股票每日基本面指标 (`tushare_stock_dailybasic` 表)。

这两个脚本支持多种更新模式，通过命令行参数进行控制：

*   `--auto` (默认): **自动增量更新**。脚本会自动检测数据库中对应任务的最新数据日期，并从下一个交易日开始更新到当前日期。这是最常用的模式。
*   `--days N`: **按天数增量更新**。更新最近 `N` 个交易日的数据。
*   `--start-date YYYYMMDD --end-date YYYYMMDD`: **按日期范围更新**。更新指定起始日期和结束日期之间的数据。`--end-date` 可选，默认为当前日期。
*   `--full-update`: **全量更新**。从 Tushare 支持的最早日期开始，更新到当前日期。**注意：全量更新可能需要较长时间并消耗大量 API 积分。**

### 示例命令

#### 更新股票日线数据 (`tushare_stock_daily`)

```bash
# 自动增量更新 (常用)
python examples/tushare_stock_daily_updater.py --auto

# 更新最近 5 个交易日的数据
python examples/tushare_stock_daily_updater.py --days 5

# 更新 2023 年 1 月份的数据
python examples/tushare_stock_daily_updater.py --start-date 20230101 --end-date 20230131

# 全量更新 (谨慎使用!)
python examples/tushare_stock_daily_updater.py --full-update
```

#### 更新股票每日指标 (`tushare_stock_dailybasic`)

```bash
# 自动增量更新 (常用)
python examples/tushare_stock_dailybasic_updater.py --auto

# 更新最近 10 个交易日的数据
python examples/tushare_stock_dailybasic_updater.py --days 10

# 更新 2023 年第一季度的数据
python examples/tushare_stock_dailybasic_updater.py --start-date 20230101 --end-date 20230331

# 全量更新 (谨慎使用!)
python examples/tushare_stock_dailybasic_updater.py --full-update
```

**提示**: 你可以在命令后添加 `--no-show-progress` 来禁用进度条显示。

## 添加新任务

要添加一个新的数据获取任务（例如，获取指数数据）：

1.  在 `data_module/tasks/` 下创建一个新的 Python 文件（例如 `data_module/tasks/index/tushare_index_daily.py`）。
2.  在该文件中创建一个新的类，继承自 `data_module.task.TushareTask` (或其他合适的数据源基类)。
3.  在类中定义必要的属性：
    *   `name`: 任务的唯一标识符 (例如 `"tushare_index_daily"`)。
    *   `description`: 任务描述。
    *   `table_name`: 对应的数据库表名。
    *   `primary_keys`: 数据表的主键列表。
    *   `date_column`: 用于增量更新的日期列名。
    *   `api_name`: 对应的 Tushare API 接口名称 (例如 `"index_daily"`)。
    *   `fields`: 需要从 API 获取的字段列表。
    *   `(可选)` `column_mapping`: API 字段名到数据库列名的映射。
    *   `(可选)` `transformations`: 列的数据类型转换规则。
    *   `(可选)` `validations`: 数据验证规则。
    *   `(可选)` `indexes`: 需要在数据库表中创建的自定义索引。
4.  根据需要实现或重写方法：
    *   `get_batch_list`: 定义如何根据输入参数（如日期范围、代码列表）生成 API 调用批次。
    *   `prepare_params`: 准备每次 API 调用所需的具体参数。
    *   `(可选)` `process_data`: 添加自定义的数据处理逻辑。
    *   `(可选)` `validate_data`: 添加自定义的数据验证逻辑。
5.  (重要) 在 `data_module/tasks/__init__.py` 文件中导入你新创建的任务类，以便 `TaskFactory` 能够发现它。
6.  现在你可以通过 `TaskFactory.get_task("your_new_task_name")` 来获取和使用你的新任务了。

## 项目结构

```
autoDatabase/
├── .env                    # 环境变量配置文件 (需手动创建)
├── .gitignore              # Git 忽略文件配置
├── config.py               # 应用配置 (如并发限制)
├── requirements.txt        # Python 依赖库
├── data_module/            # 数据模块核心
│   ├── __init__.py         # data_module 包初始化
│   ├── base_task.py        # 基础任务类 Task
│   ├── config.json         # (用途待定) 配置文件?
│   ├── db_manager.py       # 数据库交互 (连接, 表管理, CRUD)
│   ├── task_decorator.py   # (用途待定) 任务装饰器?
│   ├── task_factory.py     # 任务工厂，用于创建和管理任务实例
│   ├── sources/            # 数据源 API 封装
│   │   ├── __init__.py
│   │   └── tushare/        # Tushare 数据源
│   │       ├── __init__.py
│   │       ├── tushare_api.py  # Tushare API 客户端 (速率限制)
│   │       └── tushare_task.py # Tushare 任务基类 TushareTask
│   ├── tasks/              # 具体的业务数据任务实现
│   │   ├── __init__.py     # !! 需要在此导入所有任务类 !!
│   │   ├── examples/       # (待确认用途) 任务示例?
│   │   ├── index/          # (待确认用途) 指数相关任务?
│   │   └── stock/          # 股票相关任务
│   │       ├── __init__.py
│   │       ├── tushare_stock_daily.py
│   │       └── tushare_stock_dailybasic.py
│   └── tools/              # 通用工具
│       ├── __init__.py
│       └── calendar.py     # 交易日历工具 (异步)
├── examples/               # 使用示例脚本
│   ├── calendar_example.py # 交易日历工具使用示例
│   ├── check_stock_daily_quality.py # (待确认用途) 股票日线数据质量检查?
│   ├── stock_data.log      # (待确认用途) 示例脚本日志?
│   ├── tushare_stock_daily_updater.py      # 股票日线更新脚本
│   └── tushare_stock_dailybasic_updater.py # 股票每日指标更新脚本
├── docs/                   # 文档目录
│   └── user_guide.md       # 用户指南
├── stock_data.log          # 全局日志文件?
├── industry_crowd_factor.py # (用途待定) 行业拥挤度因子计算?
└── README.md               # 本文档

# 注： .git, __pycache__ 等目录已省略
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