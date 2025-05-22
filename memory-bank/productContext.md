# Product Context

This file provides a high-level overview of the project and the expected product that will be created. Initially it is based upon projectBrief.md (if provided) and all other available project-related information in the working directory. This file is intended to be updated as the project evolves, and should be used to inform all other modes of the project's goals and context.
2025-05-22 10:34:49 - Log of updates made will be appended as footnotes to the end of this file.

*

## Project Goal

*   AlphaHome 是一个基于 Python 异步框架构建的、灵活且可扩展的金融数据获取、处理和存储系统。
*   主要目标是简化从 Tushare 等数据源自动同步金融数据的过程，并将其存储到 PostgreSQL 数据库中。
*   系统采用模块化任务设计，每个数据任务都被封装为独立的类，易于管理和扩展。

## Key Features

*   **异步高效**: 基于 `asyncio` 构建，支持高并发数据获取。
*   **声明式任务定义**: 通过类属性清晰地定义任务元数据。
*   **自动化数据处理**: 内置数据类型转换、列名映射和基本验证。
*   **灵活的更新模式**: 支持全量更新和多种增量更新方式。
*   **配置驱动**: 通过 `.env` 和用户 `config.json` 文件管理配置。
*   **易于扩展**: 可以方便地添加新的数据源和数据任务。
*   **数据库集成**: 自动处理 PostgreSQL 数据库表的创建（含列注释）和数据的插入/更新（`upsert`）。
*   **速率限制管理**: 支持为 Tushare API 接口设置调用频率限制。
*   **命令行工具**: 提供脚本执行数据更新和质量检查任务。
*   **智能批处理**: 根据交易日历智能分批获取数据。
*   **交易日历集成**: 内置交易日历工具优化数据获取。
*   **GUI界面**: 提供图形用户界面进行任务管理和执行。

## Overall Architecture

*   **核心逻辑**: 主要位于 `alphahome/fetchers/` 目录。
    *   `base_task.py`: 定义基础任务类。
    *   `db_manager.py`: 处理与 PostgreSQL 数据库的交互。
    *   `task_factory.py`: 用于创建和管理任务实例。
    *   `sources/`: 封装不同数据源的 API 客户端和任务基类 (如 `tushare/`)。
    *   `tasks/`: 包含具体的业务数据任务实现，按数据类别 (如 `stock`, `fund`, `finance`) 组织。
    *   `tools/`: 提供通用工具，如 `calendar.py` (交易日历) 和 `batch_utils.py` (批处理)。
*   **配置管理**: 使用根目录的 `.env` 文件和用户特定目录的 `config.json`。
*   **脚本接口**: `scripts/` 目录包含用于执行更新、批量处理和数据质量检查的命令行脚本。
*   **图形用户界面 (GUI)**: `alphahome/gui/` 目录包含 GUI 应用代码，提供图形化操作界面。
*   **依赖管理**: 通过 `requirements.txt` 管理 Python 依赖。
*   **文档**: `docs/` 目录存放项目文档。
*   **测试**: `tests/` 目录包含测试代码。
2025-05-22 10:42:10 - Updated Project Goal, Key Features, and Overall Architecture based on README.md and project structure.