# 高级研究工作流指南

## 当前定位

研究侧代码位于 `research/`。它不替代 AlphaHome 的生产数据采集和 features 生产流程，而是在已有 AlphaDB 数据之上做研究、验证、回测原型和因子分析。

当前可用入口：

- `research.tools.context.ResearchContext`
- `research.tools.pipeline.ResearchPipeline`
- `alphahome.providers.AlphaDataTool`
- `research/templates/database_research/`
- `research/templates/strategy_research/`

## ResearchContext

`ResearchContext` 是研究脚本访问 AlphaHome 数据库和工具的统一入口。

```python
from research.tools.context import ResearchContext

with ResearchContext() as context:
    df = context.query_dataframe("SELECT now()")
    stock = context.data_tool.get_stock_data(
        ["000001.SZ"],
        "2024-01-01",
        "2024-12-31",
    )
```

它会复用 AlphaHome 的配置体系，默认读取 `~/.alphahome/config.json`。

## AlphaDataTool

`context.data_tool` 提供常用研究查询：

```python
with ResearchContext() as context:
    data_tool = context.data_tool
    prices = data_tool.get_stock_data(["000001.SZ"], "2024-01-01", "2024-12-31")
    adj = data_tool.get_adj_factor_data(["000001.SZ"], "2024-01-01", "2024-12-31")
    weights = data_tool.get_index_weights("000300.SH", "2024-01-01", "2024-12-31")
    info = data_tool.get_stock_info(active_only=True)
    cal = data_tool.get_trade_dates("2024-01-01", "2024-12-31")
```

复杂查询用 `custom_query()`：

```python
df = context.data_tool.custom_query(
    """
    SELECT ts_code, trade_date, close
    FROM tushare.stock_daily
    WHERE ts_code = %s AND trade_date BETWEEN %s AND %s
    """,
    ("000001.SZ", "2024-01-01", "2024-12-31"),
)
```

## ResearchPipeline

`ResearchPipeline` 把研究流程拆成可复用步骤，适合需要可复现的研究报告或批量实验。

```python
from research.tools.context import ResearchContext
from research.tools.pipeline import ResearchPipeline, Step


class LoadDataStep(Step):
    def run(self, symbols, start_date, end_date):
        data = self.context.data_tool.get_stock_data(symbols, start_date, end_date)
        return {"price_data": data}


class PrintStep(Step):
    def run(self, price_data):
        print(price_data.head())
        return {}


with ResearchContext() as context:
    pipeline = ResearchPipeline([
        LoadDataStep(context),
        PrintStep(context),
    ])
    pipeline.run({
        "symbols": ["000001.SZ"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
    })
```

## 创建研究项目

使用模板时，优先从 `research/templates/database_research/` 或 `research/templates/strategy_research/` 复制项目结构到 `research/projects/`。

推荐结构：

```text
research/projects/my_project/
├── README.md
├── main.py
├── notebooks/
└── src/
    ├── factors.py
    └── steps.py
```

研究项目应记录：

- 数据区间和输入表。
- 依赖的 features/PIT/因子版本。
- 运行命令。
- 输出路径和评估口径。

## 与生产链路的边界

- 需要沉淀为长期复用特征的逻辑，应迁移到 `alphahome/features/recipes/`。
- 需要每日/定期更新的数据，应做成 `fetchers` 任务或 `scripts/production/` 脚本。
- 一次性分析、探索性 Notebook 和研究报告数据准备可以留在 `research/projects/`。
