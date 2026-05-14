# AlphaHome 贡献指南

本文档描述当前仓库的开发、测试和文档维护约定。命令以 `pyproject.toml` 和当前目录结构为准。

## 环境准备

```bash
git clone <repo-url>
cd alphaHome

python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .
pip install -e ".[test]"
```

可选依赖按需要安装：

```bash
pip install -e ".[akshare]"
pip install -e ".[research]"
```

配置文件默认放在 `~/.alphahome/config.json`，不要提交真实 token、密码或本机路径。

## 开发流程

```bash
git checkout -b feature/your-change

# 开发和测试
pytest tests/unit -v -m "not requires_db and not requires_api"
black alphahome tests
isort alphahome tests

git status
git diff
git commit -m "feat(scope): describe change"
```

`ah`、`alphahome-cli` 和 `refresh-materialized-view` 当前不是已安装入口。涉及 CLI 的改动要确认是维护兼容 parser，还是新增真实入口，并同步更新 [CLI 下线说明](../CLI_USAGE_GUIDE.md)。

## 代码风格

- Python 代码使用 `black`，行宽配置为 88。
- 导入排序使用 `isort`，profile 为 `black`。
- 类型检查配置在 `pyproject.toml`，当前为渐进式引入；只有任务明确要求时才把 `mypy` 作为门禁。
- 不新增全局状态、隐式网络访问或硬编码本机路径。
- 数据库、API、文件系统副作用应尽量隔离在可 mock 的边界。

只检查格式：

```bash
black --check alphahome tests
isort --check-only alphahome tests
```

## 测试要求

日常快速回归：

```bash
pytest tests/unit -v -m "not requires_db and not requires_api"
```

全量本地无外部依赖测试：

```bash
pytest tests -v -m "not requires_db and not requires_api"
```

真实数据库或 API 测试必须显式使用 marker：

```python
import pytest


@pytest.mark.requires_db
def test_database_case():
    ...


@pytest.mark.requires_api
def test_external_api_case():
    ...
```

运行真实依赖测试前确认 `~/.alphahome/config.json` 已配置：

```bash
pytest tests -v -m requires_db
pytest tests -v -m requires_api
```

新增代码的测试范围：

- 新 fetcher/task：覆盖参数构造、批次边界、保存路径或错误处理。
- 新 PIT/Features 逻辑：覆盖时间窗口、去重、缺失值和边界日期。
- 新生产脚本：核心逻辑放入可测试函数，CLI 解析保持薄封装。
- 修复 bug：先增加能复现问题的测试，再修改实现。

更多细节见 [测试与 TDD 指南](tdd_guide.md)。

## 文档维护

- 当前入口文档只写仍然存在且可运行的命令、路径和 API。
- 历史材料放在 `archive/`、`docs/development/archive/` 或 `docs/tasks/`，不要改写成当前手册。
- 新增或移动脚本时，同步更新对应目录的 `README.md`。
- 文档中引用文件时使用真实相对路径，避免链接到不存在的旧设计文档。

文档-only 变更通常不需要跑 pytest，但应至少检查链接和 `git diff --check`。

## 提交信息

使用 Conventional Commits：

```bash
git commit -m "feat(fetchers): add new data task"
git commit -m "fix(pit): handle missing forecast values"
git commit -m "docs(setup): update import workflow"
git commit -m "test(features): cover mv refresh edge case"
```

常用类型：

- `feat`: 新功能
- `fix`: 修复
- `docs`: 文档
- `test`: 测试
- `refactor`: 重构
- `chore`: 依赖、配置或维护

## Review 自查

提交前确认：

- 变更范围与任务目标一致。
- 没有改动无关文件或格式化整仓。
- 新增命令和路径已在文档中同步。
- 需要外部服务的测试已加 marker。
- `git diff --check` 无空白问题。
