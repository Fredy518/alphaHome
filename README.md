# AlphaHome

AlphaHome 是一个面向个人和内部投研环境的量化数据与离线特征生产平台。当前重点是稳定地采集多源金融数据、写入 PostgreSQL/AlphaDB、生成离线特征和 PIT 数据，并通过生产脚本支持日常更新、修复和因子补算。

## 当前能力

| 模块 | 状态 | 说明 |
| --- | --- | --- |
| `alphahome.fetchers` | 可用 | 统一任务框架，已覆盖 Tushare、AkShare、Tinysoft、Excel 等数据源任务 |
| `alphahome.common` | 可用 | 配置、数据库、日志、任务生命周期、批处理规划等基础设施 |
| `alphahome.features` | 可用 | 离线特征和物化视图，当前有 36 张 feature card、36 个 MV recipe、1 个 Python recipe |
| `alphahome.integrations.dolphindb` | 可用 | Hikyuu 5 分钟 HDF5 导入 DolphinDB，用作分钟线高速查询层 |
| `alphahome.providers` | 部分可用 | 面向研究脚本的简化数据访问工具 `AlphaDataTool` |
| `alphahome.gui` | 可用但偏内部工具 | Tkinter GUI，用于查看配置、选择并运行数据采集任务 |
| `alphahome.cli` | 已下线 | 仅保留空壳包兼容导入；不再安装 `ah` / `alphahome-cli` / `refresh-materialized-view` |
| `alphahome.processors` | 已删除 | 历史 processors 能力已迁移到 `features`、`scripts` 或 research 侧 |

## 快速开始

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -e .

# 可选依赖
pip install -e ".[akshare]"
pip install -e ".[research]"
pip install -e ".[test]"
```

复制配置模板到用户配置目录并编辑：

```powershell
New-Item -ItemType Directory -Force $HOME\.alphahome
Copy-Item config.example.json $HOME\.alphahome\config.json
notepad $HOME\.alphahome\config.json
```

至少需要配置：

```json
{
  "database": {
    "url": "postgresql://username:password@localhost:5432/alphadb"
  },
  "api": {
    "tushare_token": "your_tushare_token_here"
  },
  "backtesting": {
    "hikyuu_data_dir": "E:/stock"
  },
  "dolphindb": {
    "host": "localhost",
    "port": 8848,
    "username": "admin",
    "password": "123456"
  }
}
```

也可以用环境变量兜底：`DATABASE_URL`、`TUSHARE_TOKEN`、`HIKYUU_DATA_DIR`、`TINYSOFT_*`。

## 常用入口

```bash
# GUI
python run.py
# 或安装后
alphahome

# 数据采集生产更新
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3

# PIT 数据更新
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental

# Features / MV 初始化
python scripts/initialize_materialized_views.py
python scripts/features_init.py --help
python scripts/features_validate_pit.py --help

# DolphinDB / Hikyuu 5min
python -m alphahome.integrations.dolphindb.cli init-kline5m
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min --codes-file scripts/tickers/all.txt --incremental
.\scripts\import_all_hikyuu_to_ddb.ps1 -Incremental

# P/G 因子补算
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2026-05-08
```

## 项目结构

```text
alphahome/
├── common/          # 配置、数据库、日志、任务系统、批处理规划
├── fetchers/        # 数据源基类与具体采集任务
├── features/        # feature cards、MV recipes、存储/刷新/校验工具
├── integrations/    # DolphinDB 等外部系统集成
├── providers/       # 研究侧简化数据访问
└── gui/             # Tkinter GUI

scripts/
├── production/      # 生产数据更新、PIT、因子、数据库维护脚本
├── database/        # 本地/NAS 同步与数据库维护脚本
├── maintenance/     # 数据修复脚本
└── tickers/         # Hikyuu 5min 导入用 ts_code 清单

research/
├── pgs_factor/      # P/G 因子研究与历史生产实现
├── pit_data/        # PIT 研究侧实现
└── tools/           # ResearchContext / ResearchPipeline
```

## 测试

```bash
pytest tests/unit/ -v -m "not requires_db and not requires_api"
```

需要数据库或外部 API 的测试请显式启用对应 marker，并确认 `~/.alphahome/config.json` 已配置。

## 文档

- [文档中心](docs/README.md)
- [安装指南](docs/setup/installation.md)
- [配置指南](docs/setup/configuration.md)
- [用户指南](docs/user/user_guide.md)
- [任务开发指南](docs/new_task_development_guide.md)
- [系统架构](docs/architecture/system_overview.md)
- [PIT 增量更新](docs/pit_incremental_update_guide.md)
- [DolphinDB 集成](docs/business/dolphindb_integration.md)
- [CLI 下线说明](docs/CLI_USAGE_GUIDE.md)

## 许可证

MIT License，详见 [LICENSE](LICENSE)。
