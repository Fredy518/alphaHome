# AlphaHome 投研工作台 (`research/`)

## 1. 核心理念

`research/` 目录是 AlphaHome 的心脏，一个为专业量化研究员设计的结构化工作环境。它旨在解决从快速原型验证到可复现的、可固化的研究项目之间的鸿沟。

其核心工作流是 **"探索 -> 固化 -> 集成"**:
1.  **探索 (Explore)**: 在 `notebooks/` 或 `prototypes/` 中进行自由、快速的想法验证和数据探索。
2.  **固化 (Solidify)**: 当一个想法被证明有价值时，使用 `tools/project_manager.py` 将其创建为一个标准化的研究项目，存放在 `projects/` 中。这强制执行了版本控制、配置管理和可复现性。
3.  **集成 (Integrate)**: 成熟的研究项目可以被重构，其核心逻辑可以被提取并集成到 AlphaHome 主系统的 `factors/` 或 `processors/` 模块中，成为生产级代码。

---

## 2. 目录结构详解

-   **/archives/**: 存放已归档或不再活跃的研究项目。
-   **/backtest_lab/**: 专门用于存放和管理回测结果、性能报告和分析图表的区域。
-   **/data_sandbox/**: 一个用于存放临时、共享或小型数据集的沙盒环境。此目录被 `.gitignore` 排除，不应提交大型文件。
-   **/docs/**: 本模块的文档所在地。
-   **/notebooks/**: 用于存放独立的、探索性的 Jupyter Notebooks。适合用于初步的数据分析和可视化。
-   **/projects/**: **核心目录**。每个子目录都是一个独立、完整的投研项目，拥有自己的代码、数据、配置和文档。
-   **/prototypes/**: 存放比 Notebook 更完整一些，但还未成为正式项目的原型代码（例如，单个 Python 脚本）。
-   **/templates/**: 存放用于创建新项目的模板。`default_project` 是标准模板。
-   **/tools/**: 包含用于管理 `research/` 环境的辅助工具，例如 `project_manager.py`。

---

## 3. 快速开始

### 创建你的第一个研究项目

1.  确保你位于项目根目录 (`alphaHome/`)。
2.  运行以下命令来创建一个新的研究项目（例如，名为 "my_awesome_strategy"）：

    ```bash
    python research/tools/project_manager.py
    ```
    *(注意：当前示例脚本会创建一个名为 `my_first_sentiment_analysis` 的项目并随后删除它。你可以修改脚本以创建你自己的项目。)*

    或者，在 Python 中调用：
    ```python
    from research.tools.project_manager import create_project
    
    create_project("my_awesome_strategy")
    ```

3.  创建成功后，你的新项目将位于 `research/projects/my_awesome_strategy`。
4.  进入该目录，阅读其 `README.md`，然后开始你的研究！

---

## 4. 最佳实践

-   **配置优于硬编码**: 始终将参数、文件路径和密钥放在 `config.yml` 中。
-   **代码入 `src`**: 将所有可重用的函数和类放入 `src/` 目录中，保持 `main.py` 和 notebooks 的整洁。
-   **版本控制**: 积极使用 Git 对你的每个项目进行版本控制。
-   **清理与归档**: 定期将完成或废弃的项目移动到 `archives/` 目录中，保持 `projects/` 的清晰。 