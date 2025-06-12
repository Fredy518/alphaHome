# 开发者指南

本指南为开发者提供了有关自动金融数据库管理系统的架构详情、开发流程和扩展方法。

## 目录

- [开发者指南](#开发者指南)
  - [目录](#目录)
  - [系统架构](#系统架构)
    - [GUI交互与架构](#gui交互与架构)
  - [代码组织](#代码组织)
  - [核心组件](#核心组件)
    - [数据源适配器](#数据源适配器)
    - [任务管理器](#任务管理器)
    - [数据处理工具](#数据处理工具)
    - [DBManager](#dbmanager)
    - [图形用户界面 (GUI) - 重构后架构](#图形用户界面-gui---重构后架构)
  - [开发环境设置](#开发环境设置)
    - [前提条件](#前提条件)
    - [环境配置](#环境配置)
  - [添加新功能](#添加新功能)
    - [添加新数据源](#添加新数据源)
    - [添加新任务](#添加新任务)
    - [创建新工具](#创建新工具)
  - [测试指南](#测试指南)
    - [单元测试](#单元测试)
    - [运行测试](#运行测试)
  - [代码风格](#代码风格)
  - [提交指南](#提交指南)
  - [发布流程](#发布流程)

## 系统架构

自动金融数据库管理系统采用模块化架构，主要由以下几个部分组成：

1. **数据源适配器**：负责从各个数据提供商获取原始数据
2. **任务管理器**：定义和管理各类数据处理任务
3. **数据处理工具**：提供数据验证、转换和分析功能
4. **命令行接口**：为用户提供便捷的操作方式
5. **图形界面层**：提供一个基于 Tkinter 的用户界面，用于任务管理和执行

系统架构图：

```
                   ┌─────────────┐
                   │   用户交互  │
                   └──────┬──────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼───────┐ ┌───────▼───────┐ ┌───────▼───────┐
│   命令行工具  │ │  批处理脚本   │ │   任务调度器  │
└───────┬───────┘ └───────┬───────┘ └───────┬───────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │
                   ┌──────▼──────┐
                   │  任务管理器 │
                   └──────┬──────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼───────┐ ┌───────▼───────┐ ┌───────▼───────┐
│  数据源适配器 │ │  数据处理工具 │ │  验证与分析  │
└───────┬───────┘ └───────┬───────┘ └───────┬───────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │
                   ┌──────▼──────┐
                   │    数据库   │
                   └─────────────┘
```

系统架构基于分层设计，主要包括：

1.  **数据源层 (`fetchers/sources`)**: 负责与外部数据提供商（如Tushare）的API进行交互。
2.  **任务层 (`fetchers/tasks`)**: 定义具体的数据获取和处理任务，继承自基类。
3.  **工具层 (`fetchers/tools`)**: 提供通用的数据处理、数据库交互（`DBManager`）、日志记录等工具。
4.  **脚本层 (`scripts`)**: 包含用于执行任务、数据库初始化、质量检查等的命令行脚本。
5.  **图形界面层 (`gui`)**: 提供一个基于 Tkinter 的用户界面，用于任务管理和执行。

### GUI交互与架构

GUI 层采用重构后的模块化架构，通过 `Controller` (`gui/controller.py`) 与后端逻辑进行交互。新架构按功能域进行清晰分离：

**核心架构组件：**
- **主控制器** (`gui/controller.py`): 简化的协调器，委托具体业务逻辑给专门的处理器
- **后台逻辑层** (`gui/controller_logic/`): 包含异步业务逻辑处理器
  - `task_execution.py`: 任务执行逻辑
  - `storage_settings.py`: 配置管理
  - `data_collection.py` & `data_processing.py`: 数据处理逻辑
- **UI事件处理器** (`gui/handlers/`): UI事件响应逻辑
  - `task_execution.py`: 任务执行UI事件
  - `storage_settings.py`: 设置界面事件
  - 其他功能域的事件处理器
- **UI组件创建器** (`gui/ui/`): 纯UI组件创建逻辑
  - 各标签页的UI创建函数（如 `task_execution_tab.py`）
- **通用工具** (`gui/utils/`): 共享的工具函数

**通信机制：**
- **异步消息传递**: 通过 `asyncio` 和回调机制实现前后端通信
- **事件驱动**: UI事件通过专门的处理器响应，业务逻辑在后台异步执行
- **职责分离**: UI创建、事件处理、业务逻辑完全分离，便于维护和测试

这种重构后的设计实现了单一职责原则，提高了代码的可维护性和可扩展性。

## 代码组织

项目代码按照以下方式组织：

```
autoDatabase/
├── fetchers/           # 数据处理核心模块
│   ├── sources/           # 数据源适配器
│   │   ├── __init__.py
│   │   ├── base.py        # 基础适配器类
│   │   └── tushare/       # 特定数据源适配器
│   ├── tasks/             # 任务定义
│   │   ├── __init__.py
│   │   ├── base.py        # 基础任务类
│   │   ├── examples/      # 示例任务
│   │   ├── finance/       # 财务数据任务
│   │   ├── fund/          # 基金数据任务
│   │   ├── index/         # 指数数据任务
│   │   └── stock/         # 股票数据任务
│   └── tools/             # 工具库
│       ├── __init__.py
│       └── validators.py  # 数据验证工具
├── scripts/               # 脚本和命令行工具
│   ├── check_db_quality.py # 数据库质量检查工具
│   ├── base/              # 基础脚本组件
│   ├── batch/             # 批处理脚本
│   └── tasks/             # 任务执行脚本
├── examples/              # 使用示例
├── docs/                  # 文档
│   ├── user_guide.md      # 用户指南
│   └── developer_guide.md # 开发者指南
├── logs/                  # 日志输出
└── tests/                 # 测试代码
```

## 核心组件

### 数据源适配器

数据源适配器负责处理特定数据提供商的API调用、数据格式转换和错误处理。所有适配器都继承自`fetchers.sources.base.DataSourceBase`基类。

```python
# 适配器基类接口示例
class DataSourceBase:
    def __init__(self, config=None):
        self.config = config or {}
        self.initialize()
    
    def initialize(self):
        """初始化连接和配置"""
        pass
    
    def fetch_data(self, endpoint, params=None):
        """获取数据的通用方法"""
        raise NotImplementedError
    
    def check_availability(self):
        """检查数据源是否可用"""
        raise NotImplementedError
```

### 任务管理器

任务管理器定义了数据处理任务的结构和行为。所有任务类都继承自`fetchers.tasks.base.TaskBase`基类。

```python
# 任务基类接口示例
class TaskBase:
    def __init__(self, source=None, config=None):
        self.source = source
        self.config = config or {}
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """设置日志记录器"""
        return logging.getLogger(f"task.{self.__class__.__name__}")
    
    def validate_params(self, params):
        """验证任务参数"""
        raise NotImplementedError
    
    def execute(self, params):
        """执行任务"""
        raise NotImplementedError
    
    def handle_result(self, result):
        """处理任务结果"""
        raise NotImplementedError
```

### 数据处理工具

数据处理工具提供了各种数据转换、验证和分析功能。

```python
# 数据验证工具示例
class DataValidator:
    @staticmethod
    def validate_date_coverage(df, date_field, start_date, end_date):
        """验证数据的日期覆盖情况"""
        # 实现代码
        
    @staticmethod
    def check_duplicates(df, key_fields):
        """检查数据是否有重复项"""
        # 实现代码
```

### DBManager

(`fetchers/tools/db_manager.py`)

- 封装了与 PostgreSQL 数据库的交互。
- 使用 `SQLAlchemy` Core API 构建和执行 SQL 语句。
- 提供连接管理、事务处理、批量插入/更新等功能。
- 从 `config.json` 读取数据库连接 URL。

### 图形用户界面 (GUI) - 重构后架构

(`gui/` 目录) - **已完成模块化重构，采用功能域分离设计**

**主要组件：**

- **`main_window.py`**: 
  - Tkinter 应用的入口点和主窗口管理
  - 创建主窗口、Notebook (用于标签页)
  - 初始化 `Controller` 并设置异步通信机制
  - 调用模块化的UI创建器构建各标签页
  - 处理DPI感知和窗口生命周期管理

- **`controller.py`** *(已简化)*: 
  - 轻量级协调器，委托业务逻辑给 `controller_logic/` 模块
  - 管理与前端的异步通信
  - 处理请求路由和响应分发

- **`controller_logic/`** *(新增后台逻辑层)*:
  - `task_execution.py`: 任务执行的异步业务逻辑
  - `storage_settings.py`: 配置加载/保存逻辑  
  - `data_collection.py` & `data_processing.py`: 数据处理业务逻辑
  - 所有耗时操作在此层异步执行，保持UI响应性

- **`handlers/`** *(新增UI事件处理层)*:
  - `task_execution.py`: 任务执行相关的UI事件处理
  - `storage_settings.py`: 设置界面的事件响应
  - 其他功能域的专门事件处理器
  - 负责UI状态管理和用户交互逻辑

- **`ui/`** *(新增UI组件层)*:
  - `task_execution_tab.py`, `storage_settings_tab.py` 等: 各标签页的UI组件创建
  - 纯UI创建逻辑，无业务逻辑耦合
  - 使用 `tkinter.ttk` 组件构建界面布局

- **`utils/`**: 
  - `common.py`: 共享工具函数（状态映射、日期验证等）
  - 跨模块的通用功能

**重构优势：**
- **单一职责**: 每个模块职责明确，便于维护和测试
- **可扩展性**: 新功能可以独立添加而不影响现有模块  
- **代码复用**: UI组件和业务逻辑可独立复用
- **降低耦合**: 各层之间依赖关系清晰，减少相互影响

## 开发环境设置

### 前提条件

- Python 3.9 或更高版本
- PostgreSQL 数据库服务
- Git

### 环境配置

1. 克隆代码库
   ```bash
   git clone https://github.com/yourusername/autoDatabase.git
   cd autoDatabase
   ```

2. 创建虚拟环境
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   # 或者
   venv\Scripts\activate  # Windows
   ```

3. 安装开发依赖
   ```bash
   pip install -r requirements-dev.txt
   ```

4. 配置环境变量
   ```bash
   cp .env.example .env
   # 编辑.env文件，添加必要的配置
   ```

5. 创建开发数据库
   ```bash
   createdb auto_finance_dev
   ```

## 添加新功能

### 添加新数据源

1. 在`fetchers/sources/`目录下创建新的目录或模块
2. 创建一个继承自`DataSourceBase`的适配器类
3. 实现必要的方法，包括`fetch_data`和`check_availability`

示例：
```python
# fetchers/sources/newapi/adapter.py
from fetchers.sources.base import DataSourceBase

class NewAPIAdapter(DataSourceBase):
    def initialize(self):
        self.api_key = self.config.get('api_key')
        self.base_url = self.config.get('base_url', 'https://api.newprovider.com')
        # 初始化API客户端等
    
    def fetch_data(self, endpoint, params=None):
        # 实现API调用和数据获取
        pass
    
    def check_availability(self):
        # 检查API是否可用
        pass
```

4. 在源目录的`__init__.py`中注册新的适配器
5. 创建单元测试

### 添加新任务

1. 在 `fetchers/tasks/` 下合适的子目录（如 `stock/`, `fund/` 等）创建新的任务文件。
2. 创建一个继承自 `fetchers.tasks.base.Task` 或其特定子类（如 `TushareTask`）的任务类。
3. 实现必要的方法，通常是 `fetch_data()`，以及可能的 `process_data()`, `validate_data()` 等。
4. 定义任务的核心属性，如 `name`, `description`, `table_name`, `schema`, `primary_keys` 等。
5. 使用 `@task_register()` 装饰器将任务类注册到 `TaskFactory`。
    ```python
    from ...base_task import Task
    from ...task_decorator import task_register

    @task_register()
    class MyNewFetchTask(Task):
        name = "my_new_fetch"
        description = "获取我的新数据"
        table_name = "my_new_data"
        schema = { ... } # 定义表结构
        primary_keys = ["date", "code"]

        async def fetch_data(self, **kwargs):
            # 实现数据获取逻辑
            pass
    ```
6. 在任务目录的 `__init__.py` 文件中导入新创建的任务类，确保它能被 `TaskFactory` 发现。
7. 编写单元测试和集成测试。

### 创建新工具

1. 在`fetchers/tools/`目录下创建新的工具模块
2. 实现工具功能

示例：
```python
# fetchers/tools/analyzer.py
import pandas as pd
import numpy as np

class DataAnalyzer:
    @staticmethod
    def calculate_moving_average(data, window=5):
        """计算移动平均"""
        return data.rolling(window=window).mean()
    
    @staticmethod
    def detect_outliers(data, threshold=3):
        """检测异常值"""
        mean = np.mean(data)
        std = np.std(data)
        return [x for x in data if abs(x - mean) > threshold * std]
```

3. 创建单元测试

## 测试指南

### 单元测试

所有新功能都应编写单元测试。测试文件应放在`tests/`目录下，并遵循与源代码相同的目录结构。

```python
# tests/fetchers/sources/test_newapi.py
import unittest
from unittest.mock import patch, MagicMock
from fetchers.sources.newapi.adapter import NewAPIAdapter

class TestNewAPIAdapter(unittest.TestCase):
    def setUp(self):
        self.config = {
            'api_key': 'test_key',
            'base_url': 'https://test.api.com'
        }
        self.adapter = NewAPIAdapter(config=self.config)
    
    @patch('requests.get')
    def test_fetch_data(self, mock_get):
        # 设置mock返回值
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [{'id': 1, 'value': 100}]}
        mock_get.return_value = mock_response
        
        # 调用测试方法
        result = self.adapter.fetch_data('endpoint', {'param': 'value'})
        
        # 验证结果
        self.assertEqual(result['data'][0]['value'], 100)
        mock_get.assert_called_once()
```

### 运行测试

使用pytest运行所有测试：

```bash
pytest
```

运行特定测试：

```bash
pytest tests/fetchers/sources/test_newapi.py
```

## 代码风格

项目遵循PEP 8代码风格指南。使用flake8和black进行代码质量检查和格式化。

```bash
# 检查代码风格
flake8 fetchers scripts

# 格式化代码
black fetchers scripts
```

## 提交指南

1. 创建功能分支
   ```bash
   git checkout -b feature/new-feature
   ```

2. 提交更改
   ```bash
   git add .
   git commit -m "feat: add new feature"
   ```

   提交消息应遵循[约定式提交](https://www.conventionalcommits.org/)规范：
   - `feat`: 新功能
   - `fix`: 修复bug
   - `docs`: 文档更改
   - `style`: 不影响代码含义的修改（空格、格式化等）
   - `refactor`: 既不修复bug也不添加功能的代码重构
   - `perf`: 改进性能的代码更改
   - `test`: 添加或修改测试
   - `build`: 影响构建系统或外部依赖的更改
   - `ci`: 持续集成配置或脚本的更改
   - `chore`: 其他不修改src或test的更改

3. 推送分支
   ```bash
   git push origin feature/new-feature
   ```

4. 创建Pull Request

## 发布流程

1. 更新版本号
   ```bash
   # 在setup.py或version.py中更新版本号
   ```

2. 更新CHANGELOG.md
   
3. 创建发布分支
   ```bash
   git checkout -b release/vX.Y.Z
   git add .
   git commit -m "chore: prepare vX.Y.Z release"
   git push origin release/vX.Y.Z
   ```

4. 创建Pull Request并合并到主分支

5. 在主分支上创建标签
   ```bash
   git checkout main
   git pull
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

6. 构建并发布包
   ```bash
   python setup.py sdist bdist_wheel
   twine upload dist/*
   ``` 