# AlphaHome 安装指南

## 环境要求

- Python >= 3.9，推荐 3.11 或 3.12。
- PostgreSQL >= 12。
- Windows PowerShell、macOS shell 或 Linux shell。
- 可选：DolphinDB 单机服务，用于 5 分钟 K 线高速查询层。
- 可选：Hikyuu HDF5 数据目录，用于 DolphinDB 导入。

## 安装

```bash
git clone https://github.com/your-repo/alphahome.git
cd alphahome

python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .
```

可选依赖：

```bash
pip install -e ".[akshare]"
pip install -e ".[research]"
pip install -e ".[test]"
```

本仓库以 `pyproject.toml` 管理依赖，当前没有 `requirements.txt`。

## 数据库

创建 PostgreSQL 数据库：

```sql
CREATE USER alphahome WITH PASSWORD 'your_password';
CREATE DATABASE alphadb OWNER alphahome;
GRANT ALL PRIVILEGES ON DATABASE alphadb TO alphahome;
```

如果你使用既有数据库，只需要确保配置中的 `database.url` 指向正确库。

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
python -c "from alphahome.common.config_manager import load_config; print(load_config()['database']['url'])"
python -c "from alphahome.common.db_manager import create_sync_manager; db=create_sync_manager(); print(db.test_connection())"
pytest tests/unit/ -v -m "not requires_db and not requires_api"
```

启动 GUI：

```bash
python run.py
```

## 常见问题

- `ModuleNotFoundError`: 确认已在仓库根目录执行 `pip install -e .`。
- 配置不生效：确认文件在 `~/.alphahome/config.json`，不是仓库根目录的 `config.json`。
- Tushare 任务无法启动：确认 `api.tushare_token` 或环境变量 `TUSHARE_TOKEN` 已设置。
- 数据库连接失败：先用 `psql` 验证连接串，再检查 PostgreSQL 服务和防火墙。
