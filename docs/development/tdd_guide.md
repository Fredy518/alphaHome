# 测试与 TDD 指南

本文档说明当前仓库的测试入口和新增代码时的 TDD 实践方式。旧版“分阶段推进计划”已不再反映当前仓库状态；实际执行以 `pyproject.toml`、`tests/` 目录和本文命令为准。

## 测试环境

安装测试依赖：

```bash
pip install -e ".[test]"
```

当前测试配置在 `pyproject.toml`：

- 测试目录：`tests`
- 文件匹配：`test_*.py`
- pytest 默认参数：`--strict-markers --tb=short -v`
- marker：`unit`、`integration`、`e2e`、`slow`、`requires_db`、`requires_api`

注意：仓库里多数单元测试按目录归类，并未逐个标记 `unit`。日常运行应按目录选择，不要用 `-m "unit"` 作为唯一过滤条件。

## 常用命令

本地快速回归：

```bash
pytest tests/unit -v -m "not requires_db and not requires_api"
```

运行全部不依赖外部服务的测试：

```bash
pytest tests -v -m "not requires_db and not requires_api"
```

运行指定模块：

```bash
pytest tests/unit/test_fetcher_task_failures.py -v
pytest tests/unit/test_features/test_mv_recipes.py -v
```

需要数据库或外部 API 的测试必须显式确认环境后运行：

```bash
pytest tests -v -m requires_db
pytest tests -v -m requires_api
```

## 目录约定

| 目录 | 用途 |
| --- | --- |
| `tests/unit/` | 无真实外部服务依赖的单元测试和轻量组件测试 |
| `tests/integration/` | PIT、数据库交互等集成测试 |
| `tests/cli/` | 兼容性 CLI parser 测试；不代表 `ah` 等 console script 当前已安装 |
| `tests/conftest.py` | 共享 fixture、mock 数据库、mock API 和测试配置 |

## 新功能的 TDD 流程

1. 先写一个失败测试，明确接口、输入、输出和边界条件。
2. 写最小实现让测试通过。
3. 在测试保护下整理命名、分支和错误处理。
4. 对涉及数据库、API、文件系统的代码，用 fixture 或 mock 隔离外部依赖。
5. 只有确实需要真实服务时才加 `requires_db` 或 `requires_api` marker。

示例：

```python
def test_calculate_stock_return():
    result = calculate_stock_return([100, 110, 105])
    assert result == [0.1, -0.045454545454545456]
```

实现：

```python
def calculate_stock_return(prices: list[float]) -> list[float]:
    if len(prices) < 2:
        return []
    return [(curr - prev) / prev for prev, curr in zip(prices[:-1], prices[1:])]
```

## 测试数据和 Mock

优先使用 `tests/conftest.py` 中已有 fixture：

- `mock_db_manager`
- `mock_tushare_api`
- `sample_stock_data`
- `sample_calendar_data`
- `temp_data_dir`

新增 fixture 时保持小而明确，避免让一个 fixture 同时承担数据库、API 和业务语义。

## Marker 使用

需要真实数据库：

```python
import pytest


@pytest.mark.requires_db
def test_query_real_database():
    ...
```

需要外部 API：

```python
@pytest.mark.requires_api
def test_fetch_real_api():
    ...
```

耗时较长：

```python
@pytest.mark.slow
def test_large_backfill_case():
    ...
```

## 代码质量检查

格式化和导入排序配置在 `pyproject.toml`：

```bash
black alphahome tests
isort alphahome tests
```

如需只检查不改写：

```bash
black --check alphahome tests
isort --check-only alphahome tests
```

`mypy` 配置已存在，但当前是渐进式引入，是否作为强制门禁由具体任务决定。

## 新增测试的判断标准

- 修复 bug：必须先复现 bug，再验证修复。
- 新增 fetcher/task：至少覆盖批次生成、参数传递、异常重试或保存路径之一。
- 新增 PIT/Features 逻辑：覆盖时间窗口、去重、空值和边界日期。
- 新增生产脚本：将核心逻辑放进可测试函数，CLI 解析只做薄封装。
- 文档-only 变更：通常不需要跑 pytest，但应做链接和格式检查。
