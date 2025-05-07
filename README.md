# AlphaHome: 自动化金融数据任务系统

## 项目简介

AlphaHome 是一个基于 Python 异步框架构建的、灵活且可扩展的金融数据获取、处理和存储系统。它旨在简化从 Tushare 等数据源自动同步金融数据的过程，并将其存储到 PostgreSQL 数据库中。

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
*   **智能批处理**: 支持按交易日历智能分批获取数据，可针对单一股票和全市场设置不同批次大小。
*   **交易日历集成**: 内置交易日历工具，自动识别交易日并优化数据获取流程。

## 环境配置

### 配置文件说明
- 项目根目录下提供了 `config.example.json`，作为配置模板文件。
- **请将该文件复制到你的用户配置目录，并重命名为 `config.json`，然后根据实际情况填写内容。**
- 用户配置目录路径通常为：`C:/Users/<你的用户名>/AppData/Local/trademaster/alphahome/config.json`，也可参考日志中的路径提示。

### 1. 克隆项目

```bash
git clone <your-repository-url>
cd alphaHome
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
TUSHARE_API_RATE_LIMIT_DEFAULT=50
```

**重要**:
*   确保已安装并运行 PostgreSQL 数据库。
*   将 `your_actual_tushare_api_token` 替换为你的 Tushare Pro 账户的有效 Token。
*   将 `your_database_connection_string` 替换为你的 PostgreSQL 数据库的实际连接信息。

### 4. (可选) 从 Wheel 文件安装

如果你获得了项目的 `.whl` 分发包（例如 `dist/alphahome-1.0.0-py3-none-any.whl`），你可以直接使用 pip 安装：

```bash
# 确保你已经安装了所有非 Python 依赖（例如 PostgreSQL）
pip install dist/alphahome-1.0.0-py3-none-any.whl

# 安装后，你可能仍然需要创建和配置 .env 文件（见上文）
# 并根据你的安装方式运行 GUI 或脚本
```

## 添加新任务

要添加一个新的数据获取任务（例如，获取指数数据）：

1.  在 `fetchers/tasks/` 下创建一个新的 Python 文件（例如 `fetchers/tasks/index/tushare_index_daily.py`）。
2.  在该文件中创建一个新的类，继承自 `fetchers.task.TushareTask` (或其他合适的数据源基类)。
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
    *   `(可选)` `batch_trade_days_single_code`: 单代码查询时的批次大小（交易日数量）。
    *   `(可选)` `batch_trade_days_all_codes`: 全市场查询时的批次大小（交易日数量）。
4.  根据需要实现或重写方法：
    *   `get_batch_list`: 定义如何根据输入参数（如日期范围、代码列表）生成 API 调用批次。可使用系统提供的 `generate_trade_day_batches` 工具函数简化实现。
    *   `prepare_params`: 准备每次 API 调用所需的具体参数。
    *   `(可选)` `process_data`: 添加自定义的数据处理逻辑。
    *   `(可选)` `validate_data`: 添加自定义的数据验证逻辑。
5.  (重要) 在 `fetchers/tasks/__init__.py` 文件中导入你新创建的任务类，以便 `TaskFactory` 能够发现它。
6.  现在你可以通过 `TaskFactory.get_task("your_new_task_name")` 来获取和使用你的新任务了。

## 项目结构

```
alphaHome/
├── .env                    # 环境变量配置文件 (需手动创建)
├── .gitignore              # Git 忽略文件配置
├── config.py               # 应用配置 (如并发限制)
├── requirements.txt        # Python 依赖库
├── fetchers/               # 数据模块核心
│   ├── __init__.py         # fetchers 包初始化
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
│       ├── calendar.py     # 交易日历工具 (异步)
│       └── batch_utils.py  # 批处理工具 (包含交易日批次生成函数)
├── docs/                   # 文档目录
│   └── user_guide.md       # 用户指南
├── stock_data.log          # 全局日志文件?
├── industry_crowd_factor.py # (用途待定) 行业拥挤度因子计算?
└── README.md               # 本文档

# 注： .git, __pycache__ 等目录已省略
```

## 批处理功能说明

系统提供了智能批处理功能，可以根据交易日历自动将数据获取任务分成多个批次进行处理：

### 批处理参数配置

在任务类中，可以通过以下属性配置批处理：

```python
# 批处理配置示例
batch_trade_days_single_code = 240  # 单代码查询时，每个批次的交易日数量 (约1年)
batch_trade_days_all_codes = 15    # 全市场查询时，每个批次的交易日数量 (约3周)
```

### 批处理工具函数

系统提供了 `generate_trade_day_batches` 工具函数，可以简化批处理实现：

```python
from ...tools.batch_utils import generate_trade_day_batches

async def get_batch_list(self, **kwargs) -> List[Dict]:
    """生成批处理参数列表

    Args:
        **kwargs: 查询参数，包括start_date、end_date、ts_code等

    Returns:
        List[Dict]: 批处理参数列表
    """
    # 获取查询参数
    start_date = kwargs.get('start_date', '19910101')
    end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
    ts_code = kwargs.get('ts_code')  # 可选的股票代码
    exchange = kwargs.get('exchange', 'SSE')  # 可选的交易所参数
    
    # 使用批次生成工具
    batch_list = await generate_trade_day_batches(
        start_date=start_date,
        end_date=end_date,
        batch_size=self.batch_trade_days_single_code if ts_code else self.batch_trade_days_all_codes,
        ts_code=ts_code,
        exchange=exchange,
        logger=self.logger
    )
    return batch_list
```

该工具函数会：
1. 根据交易日历自动识别指定日期范围内的实际交易日
2. 根据是否提供了股票代码，选择合适的批次大小
3. 将交易日分批并生成适合API调用的参数列表

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
    *   **文件 -> 存储设置**: 打开设置窗口，用于配置 PostgreSQL 数据库连接 URL 和 Tushare API Token。
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