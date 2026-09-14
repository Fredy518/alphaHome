# AlphaHome 安装指南

## 环境要求

- Python >= 3.10，推荐 3.11 或 3.12。
- PostgreSQL >= 12。
- Windows PowerShell、macOS shell 或 Linux shell。

## 安装

在已检出的仓库根目录，创建专用候选环境：

```powershell
$env:UV_PROJECT_ENVIRONMENT = 'E:\AlphaHomeRuntimes\candidate-20260914'
uv sync --locked --python 3.12 --extra test --extra akshare
uv lock --check --offline
& "$env:UV_PROJECT_ENVIRONMENT\Scripts\python.exe" scripts/verify_runtime.py
```

依赖权威为根 `pyproject.toml` 和 `uv.lock`。可选组 `tinysoft/research/backtest` 按用途安装；不要将根环境同步到 fundpos 独立环境。完整说明见[可复现运行环境](../development/reproducible-runtime.md)。

### Tinysoft（pyTSL）

Tinysoft 有两个后端：`api.tinysoft.mode="pytsl"` 使用 pyTSL 原生模块；`mode="opi"` 使用 TS-OPI HTTP 接口和基础依赖 `aiohttp`。只有 pyTSL 后端需要 `tinysoft` 可选依赖。

在运行 AlphaHome 的 Python 环境中，从仓库根目录安装：

```bash
uv sync --locked --extra tinysoft
```

仅在独立供应商诊断环境中单独安装 pyTSL；正式候选环境保持锁定：

```bash
python -m pip install --upgrade "tspytsl>=1.9"
python -c "import pyTSL; from importlib.metadata import version; print(version('tspytsl')); print(pyTSL.__file__)"
```

官方安装包名为 `tspytsl`，Python 导入名为 `pyTSL`。[官方安装文档](https://py3k.cn/pyTSL/setup.html)提供 pip 安装和按 Python 版本手工安装的方法；[PyPI](https://pypi.org/project/tspytsl/)提供对应系统及 Python 版本的二进制包。

Windows 还需要 Visual C++ 运行库，64 位 Python 对应官方文档中的 `VC_redist.x64.exe`。若出现 `DLL load failed`，检查 Python 位数、二进制包版本和运行库；若 pip 提示没有匹配版本，先检查当前 Python 版本和系统架构是否有官方包。

导入验证不连接天软服务。实际取数仍需配置已有天软账号及 `api.tinysoft.mode`、`user`、`password`；安装可选依赖不会切换当前后端。

## 数据库

数据库创建和 schema 安装由维护角色执行，日常作业使用按领域授权的非超级用户。既有数据库先运行只读计划；缺表返回 `migration_required` 后单独准备迁移。不要用生产配置运行测试。

角色 SQL 生成、备份恢复、凭据轮换与切换顺序见[生产切换手册](production-cutover-runbook.md)。

## 配置

AlphaHome 默认读取：

```text
~/.alphahome/config.json
```

Windows PowerShell：

```powershell
New-Item -ItemType Directory -Force $HOME\.alphahome
Copy-Item config.example.json $HOME\.alphahome\config.json
notepad $HOME\.alphahome\config.json
```

macOS/Linux：

```bash
mkdir -p ~/.alphahome
cp config.example.json ~/.alphahome/config.json
${EDITOR:-vi} ~/.alphahome/config.json
```

最小配置：

```json
{
  "database": {
    "url": "postgresql://alphahome:your_password@localhost:5432/alphadb"
  },
  "api": {
    "tushare_token": "your_tushare_token_here"
  }
}
```

## 验证

```bash
python -c "from alphahome.common.config_manager import load_config, redact_url; print(redact_url(load_config()['database']['url']))"
python -c "from alphahome.common.db_manager import create_sync_manager; db=create_sync_manager(); print(db.test_connection())"
pytest tests/unit/ -v -m "not requires_db and not requires_api"
```

启动 GUI：

```bash
python run.py
```

## 常见问题

- `ModuleNotFoundError`: 确认使用候选环境解释器，且已执行 `uv sync --locked` 和所需 extra。
- 配置不生效：确认文件在 `~/.alphahome/config.json`，不是仓库根目录的 `config.json`。
- Tushare 任务无法启动：确认 `api.tushare_token` 或环境变量 `TUSHARE_TOKEN` 已设置。
- 数据库连接失败：先用 `psql` 验证连接串，再检查 PostgreSQL 服务和防火墙。
