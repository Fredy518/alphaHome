# 上下文
文件名：enrich_templates_task.md
创建于：2024-08-01 11:00:00
创建者：AI
关联协议：RIPER-5 + Multidimensional + Agent Protocol

# 任务描述
基于已搭建完成的 `research/` 投研工作台，通过增加多样化的项目模板（如因子研究、策略研究）并丰富其示例代码与注释，来提升工作台的实战价值和易用性。

# 项目概述
此任务是"凤凰计划"的第二阶段，旨在将基础设施建设的成果转化为直接的研究生产力，为研究员提供高质量、开箱即用的研究起点，加速从想法到验证的过程。

---
*以下部分由 AI 在协议执行过程中维护*
---

# 分析 (由 RESEARCH 模式填充)
[待填充]

# 提议的解决方案 (由 INNOVATE 模式填充)
[待填充]

# 实施计划 (由 PLAN 模式生成)
实施检查清单：

**阶段一：创建 `factor_research` 模板**
1.  **创建模板目录**: 在 `research/templates/` 下创建 `factor_research/` 目录。
2.  **复制基础结构**: 将 `default_project` 的内容完整复制到 `factor_research` 作为起点。
3.  **特化配置文件**: 修改 `factor_research/config.yml`，添加因子分析特有的配置项（如 IC 计算参数、分层收益分析参数等）。
4.  **特化主逻辑**: 重构 `factor_research/main.py`，加入因子研究的标准流程示例代码和详细注释，包括数据加载、因子计算、因子标准化、IC 分析和分层回测的伪代码框架。
5.  **特化文档**: 更新 `factor_research/README.md`，详细说明因子研究项目的目标、流程和如何解读结果。
6.  **添加示例 Notebook**: 在 `factor_research/notebooks/` 中创建一个 `01_factor_eda.ipynb`，展示如何对因子进行探索性数据分析。

**阶段二：创建 `strategy_research` 模板**
7.  **创建模板目录**: 在 `research/templates/` 下创建 `strategy_research/` 目录。
8.  **复制基础结构**: 将 `default_project` 的内容完整复制到 `strategy_research`。
9.  **特化配置文件**: 修改 `strategy_research/config.yml`，添加策略回测特有的配置（如回测周期、手续费、滑点、基准）。
10. **特化主逻辑**: 重构 `strategy_research/main.py`，加入策略回测的框架，并提供与 `backtrader` 集成的清晰示例和注释。
11. **特化文档**: 更新 `strategy_research/README.md`，说明策略回测项目的流程和关键性能指标。
12. **添加示例 Notebook**: 在 `strategy_research/notebooks/` 中创建一个 `01_strategy_performance_analysis.ipynb`，展示如何分析回测结果。

**阶段三：升级核心工具**
13. **升级 `project_manager.py`**:
    *   引入 `argparse` 使其成为一个功能更完整的命令行工具。
    *   添加 `--template` 参数，允许用户在创建项目时通过 `factor_research` 或 `strategy_research` 来指定模板。
    *   添加 `--list-templates` 功能，用于列出所有可用的模板。

# 当前执行步骤 (由 EXECUTE 模式在开始执行某步骤时更新)
> 所有步骤已执行完毕。

# 任务进度 (由 EXECUTE 模式在每步完成后追加)
*   [2024-08-01 11:05:00] - 步骤 1-13 均已成功按计划执行。
    *   **阶段一: `factor_research` 模板创建完毕**
        *   步骤 1 & 2: 目录创建并从 `default_project` 复制。
        *   步骤 3: `config.yml` 已特化，加入了 `factor_analysis_settings`。
        *   步骤 4: `main.py` 已重构为因子分析管道。
        *   步骤 5: `README.md` 已更新为因子研究指南。
        *   步骤 6: `01_factor_eda.ipynb` 已创建。
    *   **阶段二: `strategy_research` 模板创建完毕**
        *   步骤 7 & 8: 目录创建并从 `default_project` 复制。
        *   步骤 9: `config.yml` 已特化，加入了 `backtest_settings`。
        *   步骤 10: `main.py` 已重构为 `backtrader` 启动器。
        *   步骤 11: `README.md` 已更新为策略回测指南。
        *   步骤 12: `01_strategy_performance_analysis.ipynb` 已创建。
    *   **阶段三: 核心工具升级完毕**
        *   步骤 13: `project_manager.py` 已升级为支持 `create` 和 `list` 子命令的完整 CLI 工具。
    *   **总体状态**: 所有修改均已完成，无阻碍。用户确认状态：成功。

# 最终审查 (由 REVIEW 模式填充) 