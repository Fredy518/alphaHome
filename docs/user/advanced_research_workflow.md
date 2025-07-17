# 高级研究工作流指南

## 1. 引言

### 目的
欢迎来到 AlphaHome 的高级研究工作流。本指南旨在引导您从传统的、基于本地文件的研究方法，过渡到一个全新的、与 AlphaHome 核心系统深度集成的数据库驱动的研究框架。

这个新框架的核心目标是解决研究孤岛问题，让您的每一个研究项目都能无缝利用系统强大的数据获取 (`fetchers`) 和批量处理 (`BatchPlanner`) 能力，从而实现**可复现、可自动化、可扩展**的量化研究。

### 新旧工作流对比

| 特性 | 旧工作流 (基于文件) | 高级工作流 (数据库驱动) |
| :--- | :--- | :--- |
| **数据源** | 手动下载的 CSV/Excel 文件 | 直连 `alphadb` 数据库，数据实时同步 |
| **可复现性** | 低，依赖手动操作和特定的文件 | 高，研究过程由代码化的流水线定义 |
| **自动化** | 困难，脚本分散 | 简单，`ResearchPipeline` 实现端到端自动化 |
| **集成度** | 低，与系统核心功能脱节 | 高，通过 `ResearchContext` 无缝访问核心服务 |
| **扩展性** | 有限，难以处理大规模数据 | 优秀，可利用数据库和批处理器的性能优势 |

---

## 2. 两大核心概念

新的工作流建立在两个核心组件之上，它们位于 `research/tools/` 目录下。

### 2.1. `ResearchContext`: 通往AlphaHome核心的钥匙

`ResearchContext` (`research/tools/context.py`) 是您在研究项目中与 AlphaHome 核心系统交互的**统一入口**。

当您在项目中实例化它时，它会自动读取项目根目录下的 `config.yml` 文件，并为您准备好两个关键服务：

1.  **数据库管理器 (`db_manager`)**: 一个配置好的 `DBManager` 实例，让您可以直接查询 `alphadb` 数据库中的任何数据，无需关心连接细节。
2.  **批处理计划器 (`planner`)**: 一个与数据库关联的 `ExtendedBatchPlanner` 实例，让您可以方便地为后续的数据处理任务（如因子计算）生成批量计划。

简而言之，`ResearchContext` 将复杂的后端配置抽象为一个简单的对象，让您可以专注于研究逻辑本身。

### 2.2. `ResearchPipeline`: 让研究过程自动化

`ResearchPipeline` (`research/tools/pipeline.py`) 是一个声明式的流水线框架，它将一个完整的研究过程定义为一系列可编程的**步骤 (`Step`)**。

每个 `Step` 都是一个独立的 Python 类，负责执行研究流程中的一个具体环节，例如：

-   `LoadDataStep`: 从数据库加载原始数据。
-   `FactorCalculationStep`: 计算技术指标或因子。
-   `AnalysisStep`: 执行统计分析或回测。
-   `SaveResultsStep`: 将结果存回数据库或保存为文件。

通过将这些 `Step` 按顺序组织到一个 `ResearchPipeline` 中，您的整个研究流程就变成了一段清晰、可读、可执行的代码。这确保了您的研究是**100% 可复现的**——任何人，在任何时间，只要运行相同的流水线代码，就能得到完全相同的结果。

---

## 3. 快速上手：创建你的第一个数据库驱动的研究项目

我们提供了一个专门的 `database_research` 模板来帮助您快速启动项目。

**步骤：**

1.  打开终端，确保当前目录位于 `alphaHome` 项目根目录。
2.  运行 `project_manager.py` 脚本来创建一个新项目。它会提示您输入项目名称。

    ```bash
    # 在 alphaHome/ 根目录下运行
    python research/tools/project_manager.py
    ```

3.  假设您输入了 `my_first_db_research`作为项目名，脚本会自动在 `research/projects/` 目录下创建一个包含以下结构的新项目：

    ```
    research/
    └── projects/
        └── my_first_db_research/
            ├── config.yml         # 项目配置文件
            ├── main.py            # 流水线执行入口
            ├── README.md          # 项目说明
            ├── notebooks/
            │   └── 01_interactive_analysis.ipynb  # 交互式分析 Notebook
            └── src/
                ├── __init__.py
                ├── factors.py     # 因子计算逻辑
                └── steps.py       # 流水线步骤定义
    ```

---

## 4. 分步实践：构建一个完整的研究流水线

现在，让我们以上一步创建的 `my_first_db_research` 项目为例，深入了解如何构建和运行一个完整的研究流水线。

### 第一步：配置环境 (`config.yml`)

打开 `config.yml` 文件。这是您连接 AlphaHome 核心的枢纽。

```yaml
# research/projects/my_first_db_research/config.yml

# 数据库管理器配置
db_manager:
  # 确保这里的配置与你的 alphadb 数据库一致
  db_type: 'postgresql'
  host: 'localhost'
  port: 5432
  user: 'your_db_user'
  password: 'your_db_password'
  db_name: 'alphadb'

# 批处理计划器配置
# 通常，计划器会直接使用上面定义的数据库连接，所以这里留空即可
planner:
  # batch_size, start_date 等参数可以在这里覆盖
```
**关键操作**：请务必将 `db_manager` 部分的 `user` 和 `password` 修改为您的数据库实际凭据。

### 第二步：定义流水线步骤 (`src/steps.py`)

打开 `src/steps.py`。这里是您定义研究流水线具体操作的地方。每个类都继承自 `Step` 基类，并实现 `run()` 方法。

```python
# research/projects/my_first_db_research/src/steps.py

import pandas as pd
from alphahome.research.tools.pipeline import Step
from .factors import calculate_moving_average

class LoadStockDataStep(Step):
    """从数据库加载股票日线数据。"""
    def run(self, stock_list, start_date, end_date):
        print("开始执行：加载股票数据...")
        # self.context 由 Step 基类提供，可直接访问 db_manager
        db = self.context.db_manager
        
        # 构建并执行查询
        query = f"""
        SELECT trade_date, ts_code, open, close, high, low, vol
        FROM stock_daily
        WHERE ts_code IN ({str(stock_list)[1:-1]})
          AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        """
        data = db.query_dataframe(query)
        print(f"成功加载 {len(data)} 条数据。")
        return {'stock_data': data} # 返回的数据将传递给下一步

class CalculateFactorsStep(Step):
    """计算技术因子。"""
    def run(self, stock_data):
        print("开始执行：计算因子...")
        # 从上一步获取数据
        if stock_data.empty:
            print("数据为空，跳过因子计算。")
            return {'factor_data': pd.DataFrame()}
            
        # 调用 src/factors.py 中的函数
        factor_data = calculate_moving_average(stock_data, window=20)
        print("因子计算完成。")
        return {'factor_data': factor_data}

class PrintResultsStep(Step):
    """打印最终结果。"""
    def run(self, factor_data):
        print("开始执行：打印结果...")
        print("最终带有因子的数据预览：")
        print(factor_data.head())
        # 此步骤不返回任何内容，流水线在此结束
```

### 第三步：编排与执行 (`main.py`)

打开 `main.py`。这是您将所有步骤串联起来，构建并运行 `ResearchPipeline` 的地方。

```python
# research/projects/my_first_db_research/main.py

from pathlib import Path
from alphahome.research.tools.context import ResearchContext
from alphahome.research.tools.pipeline import ResearchPipeline
from src.steps import LoadStockDataStep, CalculateFactorsStep, PrintResultsStep

def main():
    print("初始化高级研究工作流...")
    
    # 1. 创建 ResearchContext，自动加载 config.yml
    #    我们传入项目路径，以便它能找到配置文件
    project_path = Path(__file__).parent
    context = ResearchContext(project_path)
    
    # 2. 定义流水线步骤
    pipeline_steps = [
        LoadStockDataStep(context),
        CalculateFactorsStep(context),
        PrintResultsStep(context),
    ]
    
    # 3. 创建并配置 ResearchPipeline
    pipeline = ResearchPipeline(steps=pipeline_steps)
    
    # 4. 定义流水线运行参数
    run_params = {
        'stock_list': ['000001.SZ', '600519.SH'],
        'start_date': '2023-01-01',
        'end_date': '2023-03-31'
    }
    
    # 5. 运行流水线
    print("\\n" + "="*20 + " 开始运行流水线 " + "="*20)
    pipeline.run(initial_params=run_params)
    print("="*20 + " 流水线运行结束 " + "="*20 + "\\n")


if __name__ == "__main__":
    main()
```
**如何运行？**
在终端中，直接执行 `main.py` 文件：
```bash
# 确保终端位于 my_first_db_research 项目目录下
python main.py
```

### 第四步：交互式探索与验证 (`notebooks/01_interactive_analysis.ipynb`)

有时，您需要验证流水线产生的数据，或在将其固化为 `Step` 之前进行一些探索性分析。这正是 Jupyter Notebook 的用武之地。

我们无法在此处直接渲染 Notebook，但您可以手动创建 `research/projects/my_first_db_research/notebooks/01_interactive_analysis.ipynb` 文件，并填入以下代码。它展示了如何独立使用 `ResearchContext` 来查询数据并进行可视化。

```python
# --- 单元格 1: Imports 和路径设置 ---
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px

# 将项目根目录添加到 Python 路径中
# 注意：此处的路径回溯层级取决于你的 alphaHome 根目录与 research 目录的相对位置
project_root = Path.cwd().parent.parent.parent
sys.path.append(str(project_root.parent))
print(f"项目根目录已添加至 sys.path: {project_root.parent}")


# --- 单元格 2: 导入和实例化 ResearchContext ---
from alphahome.research.tools.context import ResearchContext

# 实例化 ResearchContext，传入当前 notebook 所在项目的路径
context = ResearchContext(project_path=Path.cwd().parent)


# --- 单元格 3: 使用 DBManager 查询数据 ---
stock_code = '000001.SZ'
table_name = 'stock_daily'
sql_query = f"SELECT trade_date, open, close FROM {table_name} WHERE ts_code = '{stock_code}' ORDER BY trade_date ASC"

try:
    price_df = context.db_manager.query_dataframe(sql_query)
    price_df['trade_date'] = pd.to_datetime(price_df['trade_date'])
    print(f"成功查询到 {stock_code} 的 {len(price_df)} 条数据。")
    display(price_df.head())
except Exception as e:
    print(f"查询失败: {e}")


# --- 单元格 4: 数据可视化 ---
if 'price_df' in locals() and not price_df.empty:
    fig = px.line(price_df, x='trade_date', y='close', title=f'{stock_code} 收盘价走势')
    fig.show()

```

---

## 5. 结论

通过采用以 `ResearchContext` 和 `ResearchPipeline` 为核心的高级研究工作流，您将获得前所未有的研究体验：

-   **效率提升**：将重复的数据操作自动化，让您专注于策略和思想。
-   **结果可靠**：代码化的流水线确保了研究的完全可复现性，告别“无法重现”的窘境。
-   **无缝集成**：让您的研究项目不再是孤岛，而是成为 AlphaHome 生态系统中有机的一部分。

我们鼓励您基于 `database_research` 模板开始新的研究项目，并充分享受这一新框架带来的便利与强大功能。

---

## 附录：Research 模块目录结构详解

`research/` 目录是为专业量化研究员设计的结构化工作环境，其目录结构如下：

-   **/archives/**: 存放已归档或不再活跃的研究项目。
-   **/backtest_lab/**: 专门用于存放和管理回测结果、性能报告和分析图表的区域。
-   **/data_sandbox/**: 一个用于存放临时、共享或小型数据集的沙盒环境。此目录被 `.gitignore` 排除，不应提交大型文件。
-   **/notebooks/**: 用于存放独立的、探索性的 Jupyter Notebooks。适合用于初步的数据分析和可视化。
-   **/projects/**: **核心目录**。每个子目录都是一个独立、完整的投研项目，拥有自己的代码、数据、配置和文档。
-   **/prototypes/**: 存放比 Notebook 更完整一些，但还未成为正式项目的原型代码（例如，单个 Python 脚本）。
-   **/templates/**: 存放用于创建新项目的模板。`database_research` 是推荐使用的最新模板。
-   **/tools/**: 包含用于管理 `research/` 环境的辅助工具，例如 `project_manager.py`。 