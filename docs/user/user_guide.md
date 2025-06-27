# 用户指南

本指南将帮助您了解自动金融数据库管理系统的基本使用方法和功能。

## 目录

- [用户指南](#用户指南)
  - [目录](#目录)
  - [系统概述](#系统概述)
  - [开始使用](#开始使用)
    - [安装配置](#安装配置)
    - [初始设置](#初始设置)
  - [图形用户界面 (GUI)](#图形用户界面-gui)
    - [启动 GUI](#启动-gui)
    - [1. 数据采集 (Data Collection)](#1-数据采集-data-collection)
    - [2. 数据处理 (Data Processing)](#2-数据处理-data-processing)
    - [3. 任务运行与状态 (Task Execution and Status)](#3-任务运行与状态-task-execution-and-status)
    - [4. 任务日志 (Task Log)](#4-任务日志-task-log)
    - [5. 存储与设置 (Storage and Settings)](#5-存储与设置-storage-and-settings)
  - [数据获取](#数据获取)
    - [支持的数据源](#支持的数据源)
    - [数据获取方式](#数据获取方式)
    - [数据更新策略](#数据更新策略)
  - [数据任务基类](#数据任务基类)
    - [任务基类概述 (BaseTask -\> FetcherTask -\> TushareTask)](#任务基类概述-basetask---fetchertask---tusharetask)
    - [关键可配置属性](#关键可配置属性)
    - [实现自定义任务](#实现自定义任务)
  - [BatchPlanner：声明式批处理系统](#batchplanner声明式批处理系统)
    - [为什么选择 BatchPlanner？](#为什么选择-batchplanner)
    - [BatchPlanner 核心组件](#batchplanner-核心组件)
      - [Source (数据源)](#source-数据源)
      - [Partition (分区策略)](#partition-分区策略)
      - [Map (参数映射器)](#map-参数映射器)
    - [如何使用 BatchPlanner](#如何使用-batchplanner)
  - [数据质量检查](#数据质量检查)
    - [运行质量检查](#运行质量检查)
    - [理解质量报告](#理解质量报告)
    - [图表生成](#图表生成)
    - [常见数据质量问题](#常见数据质量问题)
  - [常见使用场景](#常见使用场景)
    - [日常数据更新](#日常数据更新)
    - [新增数据类型](#新增数据类型)
    - [历史数据补充](#历史数据补充)
  - [开发指南：添加新的Tushare采集任务](#开发指南添加新的tushare采集任务)
    - [前置要求](#前置要求)
    - [步骤一：创建任务文件](#步骤一创建任务文件)
    - [步骤二：定义任务类（使用模板）](#步骤二定义任务类使用模板)
    - [步骤三：实现批处理逻辑 (get\_batch\_list)](#步骤三实现批处理逻辑-get_batch_list)
      - [模式A：按日期分批（最常见）](#模式a按日期分批最常见)
      - [模式B：按资产代码分批](#模式b按资产代码分批)
    - [步骤四：运行与测试](#步骤四运行与测试)
    - [高级定制：自定义智能增量逻辑](#高级定制自定义智能增量逻辑)

## 系统概述

AlphaHome 是一套用于自动化获取、处理、存储和验证金融数据的智能量化投研系统。本系统支持多种金融数据类型，包括股票、基金、指数和宏观经济数据等。系统设计为模块化架构，便于扩展和定制。

主要功能包括：

- 从多个数据源自动获取最新数据
- 自动数据清洗和标准化处理
- 增量数据更新以减少重复获取
- 数据质量验证和异常检测
- 生成数据质量报告和可视化图表
- 批量处理和定时任务支持

## 开始使用

### 安装配置

1.  确保已安装必要的依赖：
    -   Python 3.8+
    -   PostgreSQL 12+

2.  安装系统：
    ```bash
    git clone https://github.com/yourusername/alphaHome.git
    cd alphaHome
    pip install -r requirements.txt
    ```

3.  配置数据库连接：
    将 `config.example.json` 复制为 `config.json` 并根据需要修改，设置数据库连接 (`database.url`)、API Token (`api.tushare_token`) 以及可选的任务特定参数 (`tasks`)。

### 初始设置

1.  初始化数据库：
    系统在首次启动时会自动检查并根据任务定义创建所需的数据库表和索引。

2.  配置任务设置：
    在 `config.json` 中，您可以为特定任务覆盖默认的并发数、重试次数等参数，例如：
    ```json
    "tasks": {
      "tushare_stock_daily": {
        "concurrent_limit": 10,
        "rate_limit_delay": 45 
      },
      "tushare_index_weight": {
        "concurrent_limit": 5
      }
    }
    ```

## 图形用户界面 (GUI)

除了命令行工具，系统还提供了一个图形用户界面 (GUI) 来更方便地管理和执行任务。

### 启动 GUI

确保您位于项目的根目录 (`alphaHome`) 下，然后在命令行或终端中运行：

```bash
python run.py
```

GUI 窗口将会启动，包含以下几个标签页：

### 1. 数据采集 (Data Collection)

此标签页用于查看、选择和管理所有已注册的数据获取任务。

`[在此处插入数据采集标签页截图]`

-   **显示**: 以表格形式展示数据获取任务。
-   **列信息**:
    -   `选择`: 显示一个 '✓' 标记表示该任务已被选中。
    -   `类型`: 任务的分类 (如 `stock`, `fund`, `index`, `finance`)。
    -   `名称`: 任务的唯一标识符。
    -   `描述`: 任务功能的简要说明。
    -   `最新更新时间`: 显示任务最后执行的时间。
-   **刷新**: 点击"刷新列表"按钮从后台重新加载任务信息。
-   **过滤**:
    -   点击"类型过滤"下拉框，可以选择只显示特定类型的任务，或选择"所有类型"显示全部。
    -   在"名称过滤"输入框中输入关键词可以按任务名称进行筛选。
-   **排序**: 点击"类型"、"名称"、"描述"或"最新更新时间"的**表头**，可以按该列对当前显示的列表进行升序或降序排序。
-   **选择/取消选择**:
    -   点击列表中的**任意一行**可以切换该任务的选中状态。
    -   点击"全选"按钮会将**当前可见**的所有任务（经过滤和排序后）标记为选中。
    -   点击"取消全选"按钮会将**当前可见**的所有任务标记为未选中。

### 2. 数据处理 (Data Processing)

此标签页用于查看、选择和管理所有已注册的数据处理任务。

`[在此处插入数据处理标签页截图]`

-   **显示**: 以表格形式展示数据处理任务。
-   **任务依赖**: 显示任务之间的依赖关系，确保数据处理的正确顺序。
-   **选择操作**: 与数据采集标签页类似，支持任务的选择和批量操作。

### 3. 任务运行与状态 (Task Execution and Status)

此标签页用于控制和监控选中任务的执行。

`[在此处插入任务运行标签页截图]`

-   **任务执行历史**: 显示当前和历史任务的执行状态。
-   **历史模式切换**: 可以在当前任务和历史任务之间切换查看。

-   **运行控制**:
    -   `执行模式`: 下拉框选择任务运行模式：
        -   `智能增量` (SMART): 任务将自动查找数据库中每张表的最新日期，并从该日期的后一天开始获取新数据。**此模式会忽略"开始日期"选择器**。
        -   `手动增量` (MANUAL): 您需要通过旁边的日期选择器（如果安装了 `tkcalendar`）或手动输入（格式 YYYY-MM-DD）来指定一个"开始日期"和"结束日期"，任务将获取此指定范围内的数据。
        -   `全量导入` (FULL): 任务将尝试获取所有可用的历史数据，从任务本身定义的 `default_start_date` 开始，直到今天。**此模式会忽略"开始日期"选择器**。
    -   `开始日期`/`结束日期`: 仅在选择"手动增量"模式时可用，用于指定增量更新的日期范围。
    -   `运行选中任务`: 点击此按钮会根据当前选定的执行模式和日期范围（如果适用），启动在"数据采集"标签页中**已勾选**的任务。
    -   `停止执行`: 在任务运行时变为可用。点击此按钮会向正在运行的任务发送停止信号。系统会立刻停止创建新的批处理任务，并尽快中断已在运行的请求，包括那些因速率限制而长时间等待的请求。

-   **运行状态**:
    -   表格会显示本次运行所包含的任务及其状态。
    -   `类型`, `名称`: 任务信息。
    -   `状态`: 任务的当前状态（如 `排队中`, `运行中`, `完成`, `失败`, `已取消`等）。
    -   `进度`: 任务执行进度条。
    -   `开始时间`, `结束时间`: 任务的实际开始和结束时间。

### 4. 任务日志 (Task Log)

此标签页显示后台控制器和任务执行过程中产生的日志信息。

`[在此处插入任务日志标签页截图]`

-   日志会自动滚动，显示最新的信息。
-   可以用于查看任务执行的详细步骤、遇到的警告或错误信息。
-   支持清除日志功能。

### 5. 存储与设置 (Storage and Settings)

此标签页用于配置系统运行所需的数据库连接和API Token。

`[在此处插入存储设置标签页截图]`

-   **数据库设置**:
    -   **注意**: 数据库连接信息（主机、端口、用户名、密码、数据库名）**不能**通过此界面直接修改。
    -   如需修改数据库连接，请直接编辑项目根目录下的 `config.json` 文件，修改 `database` 部分下的 `url` 字段。
    -   界面上显示的数据库字段信息是从当前的 `database.url` 解析出来的，仅供参考，且处于**禁用**状态。
-   **Tushare 设置**:
    -   您可以在 "Tushare Token" 输入框中查看、输入或修改您的 Tushare API Token。
-   **操作按钮**:
    -   "加载当前设置": 点击此按钮会从 `config.json` 文件重新加载当前的 Tushare Token 并显示在输入框中（也会尝试解析并显示只读的数据库信息）。
    -   "保存设置": 点击此按钮会将您在 "Tushare Token" 输入框中输入的值保存回 `config.json` 文件。**保存操作仅针对 Tushare Token**，数据库设置不会被修改。
    -   "测试数据库连接": 验证当前数据库连接是否正常。

## 数据获取

### 支持的数据源

系统当前支持以下数据源：

1.  **Tushare**：中国金融数据服务商，提供股票、基金、指数等数据
    -   数据类型：日线数据、财务数据、指数数据、基金数据等

2.  **其他数据源**：可通过开发对应的适配器来支持更多数据源

### 数据获取方式

数据获取现在主要通过GUI界面进行：

1.  **通过GUI界面**（推荐）：
    -   启动GUI：`python run.py`
    -   在"数据采集"标签页中选择相应的任务
    -   在"任务运行与状态"标签页中选择执行模式并执行任务

### 数据更新策略

系统支持多种数据更新策略，对应GUI中的三种执行模式：

1.  **智能增量更新** (SMART)：自动从上次更新日期开始获取数据。
2.  **手动增量更新** (MANUAL)：指定开始和结束日期，获取该范围内的数据。
3.  **全量导入** (FULL)：重新获取所有历史数据。

## 数据任务基类

### 任务基类概述 (BaseTask -> FetcherTask -> TushareTask)

系统的任务架构采用三层继承设计，以实现最大程度的代码复用和清晰的功能划分。

1.  **`BaseTask` (位于 `common/task_system/base_task.py`)**
    -   **职责**: 定义所有任务（包括数据获取、数据处理等）的通用生命周期和核心功能。
    -   **核心功能**:
        -   `execute()`: 任务执行的总入口和流程控制器。
        -   数据库交互: 管理 `db_connection`。
        -   表管理: 自动创建表和索引。
        -   数据保存: `_save_data()` 方法，处理数据入库。
        -   `get_latest_date()`: 获取表中最新日期，是智能增量的基础。

2.  **`FetcherTask` (位于 `fetchers/base/fetcher_task.py`)**
    -   **职责**: 继承自 `BaseTask`，专门为**数据获取类任务**提供通用的并发下载和控制框架。
    -   **核心功能**:
        -   `_fetch_data()`: 实现了数据获取的通用逻辑，包括确定日期范围、生成批次、执行批次和聚合结果。
        -   `_execute_batches()`: 实现**并发控制** (Semaphore)、**进度条** (tqdm) 和**失败重试**。
        -   `_determine_date_range()`: 根据更新模式 (`manual`, `smart`, `full`) 确定日期范围的通用逻辑。
        -   **抽象方法**: 定义了 `get_batch_list`, `prepare_params`, `fetch_batch` 等抽象方法，强制子类实现与特定数据源相关的逻辑。

3.  **`TushareTask` (位于 `fetchers/sources/tushare/tushare_task.py`)**
    -   **职责**: 继承自 `FetcherTask`，为所有 **Tushare 数据源**的任务提供特有的实现。
    -   **核心功能**:
        -   管理 `TushareAPI` 实例。
        -   实现了 `fetch_batch` 方法，统一调用 `TushareAPI.query()`。
        -   将 `api_name` 和 `fields` 等 Tushare 特定属性作为子类的必需定义。
        -   处理 `rate_limit_delay` 等 Tushare 特有的配置。

### 关键可配置属性

在编写任务子类时（通常是继承 `TushareTask`），您可以通过定义类属性来配置任务行为，这些值可以被 `config.json` 文件覆盖。

-   `name`: `str`, 任务的唯一ID (必须)。
-   `table_name`: `str`, 存储数据的数据库表名 (必须)。
-   `description`: `str`, 任务的描述，会显示在GUI中 (建议)。
-   `task_type`: `str`, 任务分类，用于GUI中过滤 (建议)。
-   `date_column`: `str`, 表中的主要日期列名，用于智能增量 (必须)。
-   `primary_keys`: `List[str]`, 数据表的主键，用于去重 (建议)。
-   `api_name`: `str`, Tushare 的接口名, e.g., "daily" (Tushare任务必须)。
-   `fields`: `List[str]`, Tushare 接口返回的字段列表 (Tushare任务必须)。
-   `default_start_date`: `str`, 全量更新时的默认起始日期, 格式 "YYYYMMDD" (建议)。
-   `default_concurrent_limit`: `int`, 默认的并发请求数。
-   `default_retry_delay`: `int`, 失败重试的延迟时间（秒）。
-   `default_rate_limit_delay`: `int`, 触发Tushare速率限制后的等待时间（秒）。

### 实现自定义任务

见下一章节"开发指南"。

## BatchPlanner：声明式批处理系统

`BatchPlanner` 是系统核心的批处理规划工具，它提供了一种声明式、可组合的方式来定义如何将一个大的数据获取任务分解为一系列小的、可执行的批次。它位于 `alphahome/common/planning/batch_planner.py`。

### 为什么选择 BatchPlanner？

传统的批处理工具（如 `batch_utils.py` 中的函数）往往是针对特定场景硬编码的，导致：
-   **功能重复**：为类似的分批逻辑编写多个函数。
-   **参数僵化**：无法灵活适应不同数据源对参数命名（例如 `ts_code` vs `security_id`）的要求。
-   **扩展性差**：添加新的分批需求或数据源时，需要修改或复制现有逻辑。
-   **职责不清**：批处理逻辑与任务的业务逻辑混杂。

`BatchPlanner` 通过将批处理过程分解为三个独立的、可插拔的组件，彻底解决了这些问题：

1.  **Source (数据源)**：定义要进行批处理的原始元素列表（例如，所有股票代码，或一个日期范围内的所有交易日）。
2.  **Partition (分区策略)**：定义如何将 Source 提供的元素列表切分成更小的批次（例如，按固定大小切分，或按月份/季度分组）。
3.  **Map (参数映射器)**：定义如何将每个切分后的批次转换为最终API调用所需的参数字典。这解决了不同数据源对参数命名要求不同的问题。

这种声明式、组合式的设计极大地提升了系统的灵活性、可读性和可扩展性，使得添加新的数据源和批处理策略变得轻而易举。

### BatchPlanner 核心组件

#### Source (数据源)

`Source` 定义了批处理操作的原始数据来源。

-   **`Source.from_list(data: Iterable[T])`**:
    -   **用途**: 当您的批处理元素列表是已知且固定的（例如，硬编码的股票列表）时使用。
    -   **示例**: `Source.from_list(["000001.SZ", "600519.SH"])`

-   **`Source.from_callable(func: Callable[..., SourceData])`**:
    -   **用途**: 当批处理元素列表需要在运行时动态生成（例如，从数据库查询所有股票代码，或生成特定日期范围内的所有交易日）时使用。`func` 可以是一个同步或异步函数。
    -   **示例**:
        ```python
        # 生成交易日列表作为源
        async def get_trade_days(start, end):
            # 假设 get_trade_days_between 是一个已有的异步函数
            return await get_trade_days_between(start, end)
        Source.from_callable(get_trade_days)
        ```

#### Partition (分区策略)

`Partition` 定义了如何将 `Source` 提供的原始元素列表切分成更小的批次。

-   **`Partition.by_size(size: int)`**:
    -   **用途**: 将元素列表按指定大小进行等量切分。最常用。
    -   **示例**: `Partition.by_size(100)` （每100个元素一个批次）

-   **`Partition.by_month()`**:
    -   **用途**: 将日期列表（如 `YYYYMMDD` 格式字符串或 `datetime` 对象）按自然月进行分组。
    -   **示例**: `Partition.by_month()`

-   **`Partition.by_quarter()`**:
    -   **用途**: 将日期列表按自然季度进行分组。
    -   **示例**: `Partition.by_quarter()`

#### Map (参数映射器)

`Map` 定义了如何将 `Partition` 策略生成的每个批次（通常是一个元素列表）转换成最终 API 调用所需的参数字典。这是解决不同数据源参数名差异的关键。

-   **`Map.to_dict(field_name: str)`**:
    -   **用途**: 将只包含一个元素的批次，映射为 `{field_name: element}` 形式的字典。适用于需要单个ID作为参数的API。
    -   **示例**:
        -   对于Tushare API，如果需要 `ts_code`：`Map.to_dict("ts_code")`
        -   对于另一个API，如果需要 `security_id`：`Map.to_dict("security_id")`

-   **`Map.to_date_range(start_field: str, end_field: str)`**:
    -   **用途**: 将一个日期范围批次映射为包含开始和结束日期的字典。
    -   **示例**:
        -   如果API需要 `start_date` 和 `end_date`：`Map.to_date_range("start_date", "end_date")`
        -   如果API需要 `begin_date` 和 `end_date`：`Map.to_date_range("begin_date", "end_date")`

-   **`Map.with_custom_func(func: Callable[[List[T]], Dict[str, Any]])`**:
    -   **用途**: 提供最大的灵活性，允许您定义一个自定义函数来处理任何复杂的批次到参数的映射逻辑。
    -   **示例**: `Map.with_custom_func(lambda batch: {"codes": batch, "count": len(batch)})`

### 如何使用 BatchPlanner

在任务的 `get_batch_list` 方法中，您将按照以下模式使用 `BatchPlanner`：

```python
from alphahome.common.planning.batch_planner import BatchPlanner, Source, Partition, Map
# 假设 get_my_elements_func 是一个异步或同步函数，用于获取原始元素列表

async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
    # 1. 定义源（Source）
    my_source = Source.from_callable(get_my_elements_func) # 或者 Source.from_list([...])

    # 2. 定义分区策略（Partition）
    my_partition_strategy = Partition.by_size(self.my_batch_size) # 或者 Partition.by_month() 等

    # 3. 定义映射策略（Map）
    my_map_strategy = Map.to_dict("my_api_param_name") # 或者 Map.to_date_range()，或 Map.with_custom_func()

    # 4. 初始化 BatchPlanner
    planner = BatchPlanner(
        source=my_source,
        partition_strategy=my_partition_strategy,
        map_strategy=my_map_strategy
    )

    # 5. 生成批次列表
    # 可以通过 additional_params 传入任务级别的固定参数，它们会被合并到每个批次中
    return await planner.generate(start_date=kwargs.get("start_date"), 
                                  end_date=kwargs.get("end_date"),
                                  additional_params={"some_fixed_param": "value"})

```

通过这种方式，`get_batch_list` 方法变得高度可读、可维护，并且能够适应任何复杂的批处理场景，而无需修改 `BatchPlanner` 核心代码。

## 数据质量检查

### 运行质量检查

系统提供了全面的数据质量检查功能，可以通过以下方式运行：

**通过编程方式运行质量检查**：

```python
from alphahome.common.data_quality_checker import DataQualityChecker
from alphahome.common.db_manager import create_sync_manager

# 创建数据库连接和质量检查器
db_manager = create_sync_manager()
quality_checker = DataQualityChecker(db_manager)

# 检查所有表
results = await quality_checker.check_all_tables()

# 检查特定表
results = await quality_checker.check_tables(['tushare_stock_daily', 'tushare_fund_daily'])

# 生成报告
await quality_checker.generate_html_report(results, output_dir='./logs/quality_check')
```

质量检查功能包括：
- 检查数据完整性和空值率
- 分析日期覆盖情况
- 识别数据质量问题
- 生成可视化报告和图表

主要参数：
- `tables`: 指定要检查的表名列表
- `start_date`: 检查的开始日期
- `end_date`: 检查的结束日期
- `output_dir`: 输出目录路径

### 理解质量报告

执行检查后，脚本会在指定目录（默认为`logs/db_quality_<timestamp>`）生成以下文件：

1. `detailed_results.json` - 详细的检查结果，包含每个表的完整分析数据
2. `summary.json` - 摘要信息，包括问题表的列表和问题分类
3. `report.html` - 可视化HTML报告，包含图表和统计数据

HTML报告包括：
- 总体摘要（检查表总数、成功率、发现问题）
- 问题表列表及问题类型（如高空值率、低日期覆盖率）
- 每个表的详细检查结果
- 空值分析和统计
- 日期覆盖率分析
- 月度数据完整性分析
- 可视化图表

### 图表生成

对于带有日期列的表，系统会自动生成两种图表：

1. **日期覆盖图表** - 显示每个日期的记录数量，用于识别数据间隔和缺失
   - 文件命名：`<表名>_date_coverage.png`
   - 展示：条形图，x轴为日期，y轴为记录数

2. **月度覆盖率图表** - 显示每月的数据覆盖百分比，用于识别长期趋势和缺失
   - 文件命名：`<表名>_monthly_coverage.png`
   - 展示：条形图，x轴为月份，y轴为覆盖率百分比

这些图表保存在输出目录的`charts`子目录中，并在HTML报告中引用。通过这些图表，用户可以快速识别：
- 哪些日期完全缺失数据
- 哪些月份的数据覆盖率异常低
- 数据覆盖的长期趋势
- 数据更新的季节性模式

### 常见数据质量问题

系统可以识别的常见问题包括：

- **高空值率** - 当某列的空值比例超过10%（标记为警告）或20%（标记为严重）
- **低日期覆盖率** - 当日期覆盖率低于90%
- **月度数据不均衡** - 当某些月份的覆盖率明显低于其他月份
- **处理错误** - 检查过程中发生的任何错误

## 常见使用场景

### 日常数据更新

每日数据更新的推荐流程：

1.  **通过GUI执行增量更新**：
    -   启动GUI：`python run.py`
    -   在"数据采集"标签页选择需要更新的任务。
    -   在"任务运行与状态"标签页选择"智能增量"模式并执行。

### 新增数据类型

参考下一章"开发指南"。

### 历史数据补充

1.  **通过GUI执行全量或手动增量更新**：
    -   启动GUI。
    -   选择目标任务。
    -   **全量更新**: 在"任务运行与状态"标签页选择"全量导入"模式并执行。
    -   **补充特定时间段**: 选择"手动增量"模式，并设置需要补充的开始和结束日期，然后执行。

---

## 开发指南：添加新的Tushare采集任务

本指南将指导您如何为系统添加一个新的Tushare数据采集任务。

### 前置要求

在开始之前，请确保您已经明确：
1.  **Tushare接口名称**: 您要调用的Tushare API的名称，例如 `daily`, `adj_factor`, `fina_indicator`。
2.  **接口参数**: 调用该接口需要哪些参数，特别是与日期、资产代码相关的参数。
3.  **返回字段**: 该接口会返回哪些字段，您需要将它们全部或部分存储到数据库中。

### 步骤一：创建任务文件

根据您要获取数据的类型，在 `alphahome/fetchers/tasks/` 下的相应目录中创建一个新的Python文件。例如：
-   获取A股日线数据，则在 `stock` 目录下创建 `tushare_stock_daily.py`。
-   获取一个新的财务数据，则在 `finance` 目录下创建 `tushare_fina_new_indicator.py`。

### 步骤二：定义任务类（使用模板）

在新创建的文件中，使用以下模板定义您的任务类。请仔细阅读注释并修改所有标记为`(必须)`和`(建议)`的部分。

```python
# 路径: alphahome/fetchers/tasks/stock/tushare_my_new_task.py

from typing import Any, Dict, List
import pandas as pd
from alphahome.fetchers.sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from alphahome.fetchers.sources.tushare.batch_utils import generate_trade_day_batches

# (必须) 使用装饰器注册任务，使其能被系统和GUI发现
@task_register()
class TushareMyNewTask(TushareTask):
    """
    (建议) 任务描述，例如：获取我的新Tushare接口数据。这会显示在GUI中。
    """
    # 1. 任务元数据 (必须)
    name = "tushare_my_new_task"
    task_type = "stock"  # (建议) 任务分类，用于GUI过滤 (如: stock, fund, index, finance)

    # 2. 数据库相关定义 (必须)
    table_name = "tushare_my_new_task"
    date_column = "trade_date"  # (必须) 您的数据中代表日期的列名
    primary_keys = ["trade_date", "ts_code"]  # (建议) 用于数据去重的主键

    # 3. Tushare API 相关定义 (必须)
    api_name = "my_new_api"  # (必须) Tushare接口名, e.g., "daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "vol"]  # (必须) 您希望从API获取并存储的字段列表

    # 4. (可选) 任务默认配置
    default_concurrent_limit = 5      # 默认并发数
    default_rate_limit_delay = 65     # 默认速率限制等待时间（秒）
    default_start_date = "19901219" # (建议) 全量更新时的起始日期

    # 5. (必须) 实现批处理逻辑
    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        根据API的特点，定义如何将一个大的时间范围或资产列表分解成小的批次。
        这是编写新任务最核心的部分。
        """
        # 从kwargs获取由FetcherTask确定的起止日期
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 实现您的批处理逻辑 (见步骤三)
        # ...
        return [] # 返回一个批次列表
```

### 步骤三：实现批处理逻辑 (get_batch_list)

`get_batch_list` 方法的职责是：接收一个大的任务（例如，获取2020年到2024年所有股票的数据），并将其分解成一小批一小批可以被API处理的请求。

通常有两种模式：

#### 模式A：按日期分批（最常见）

如果API的主要查询参数是日期（例如 `trade_date`, `start_date`, `end_date`），您应该使用这种模式。系统提供了工具函数来简化这个过程。

**示例** (`tushare_stock_daily`):

```python
# ... 在 TushareMyNewTask 类内部 ...
async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
    # 适用于API接收 trade_date 作为参数的情况
    return await generate_trade_day_batches(
        start_date=kwargs.get("start_date"),
        end_date=kwargs.get("end_date")
    )
```
-   `generate_trade_day_batches`: 按**交易日**生成批次，每个批次是 `{"trade_date": "YYYYMMDD"}`。
-   `generate_natural_day_batches`: 按**自然日**生成批次。

#### 模式B：按资产代码分批

如果API的主要查询参数是资产代码（`ts_code`），而日期是次要参数，或者您需要为每个资产获取一个时间段的数据。

**示例** (`tushare_fina_indicator`):

```python
# ... 在 TushareMyNewTask 类内部 ...

# (可能需要一个辅助方法来获取所有股票代码)
async def get_all_stock_codes(self) -> List[str]:
    # 假设我们有一个 'tushare_stock_basic' 表存储了所有股票代码
    if not self.db.table_exists(self.get_full_table_name('tushare_stock_basic')):
        self.logger.error("依赖的 stock_basic 表不存在。")
        return []
    
    records = await self.db.fetch("SELECT ts_code FROM " + self.get_full_table_name('tushare_stock_basic'))
    return [rec['ts_code'] for rec in records] if records else []


async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
    stock_codes = await self.get_all_stock_codes()
    if not stock_codes:
        return []
        
    # 为每一个股票代码创建一个批次
    batches = [{"ts_code": code} for code in stock_codes]
    self.logger.info(f"为 {len(batches)} 个股票代码生成了批次。")
    return batches
```

### 步骤四：运行与测试

1.  保存您的新任务文件。
2.  重新启动GUI (`python run.py`)。
3.  您的新任务 (`tushare_my_new_task`) 应该会自动出现在"数据采集"标签页的列表中。
4.  选中您的任务，选择一个执行模式（建议先用"手动增量"并选择一个很短的日期范围进行测试），然后点击"运行选中任务"。
5.  在"任务日志"标签页观察任务的执行情况，检查是否有错误。
6.  执行成功后，使用数据库工具检查对应的数据表 (`tushare_my_new_task`) 是否已创建并且包含了正确的数据。

### 高级定制：自定义智能增量逻辑

在某些特殊情况下，通用的智能增量逻辑（从上一天开始）可能不适用。例如 `index_weight` 任务，它的数据是按月更新的，并且没有一个简单的日期字段。

在这种情况下，您可以重写 `_determine_date_range` 方法，实现您自己的日期确定逻辑。

**示例** (`tushare_index_weight`):

```python
# ... 在 TushareIndexWeightTask 类内部 ...

from datetime import datetime, timedelta

async def _determine_date_range(self) -> Optional[Dict[str, str]]:
    """
    重写此方法以实现无 look_back 的智能增量更新。
    它会从数据库中的最后日期开始，但不像通用实现那样回看N天。
    """
    self.logger.info(f"任务 {self.name}: 正在确定无 look_back 的智能增量日期范围...")
    
    last_date = await self.get_latest_date() # type: ignore
    
    if last_date:
        start_date = last_date + timedelta(days=1)
        end_date = datetime.now()
    else:
        # 如果没有数据，则从默认起始日期开始
        start_date_str = self.default_start_date
        start_date = datetime.strptime(start_date_str, "%Y%m%d")
        end_date = datetime.now()

    return {
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date": end_date.strftime("%Y%m%d"),
    }
```
通过这种方式，您可以为您的任务实现最符合其API特性的、最高效的智能更新策略。