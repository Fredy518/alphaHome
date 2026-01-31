# AlphaHome CLI 使用说明（已下线）

> 说明：本项目已在 Phase 3 中下线“统一 CLI”（例如 `ah` / `alphahome-cli` / `refresh-materialized-view` 等入口点已从发布配置中移除）。
> 该文件保留为迁移提示与历史参考，避免新用户误以为这些命令仍可用。

## 现在怎么做

### 1) GUI

- 仍可使用 `python run.py` 启动 GUI。

### 2) DolphinDB / Hikyuu 5min 导入

推荐使用脚本与模块入口（无需安装 console script）：

- PowerShell 一键导入：`./scripts/import_all_hikyuu_to_ddb.ps1`
- 或直接运行模块：

```bash
python -m alphahome.integrations.dolphindb.cli --help
python -m alphahome.integrations.dolphindb.cli init-kline5m --help
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min --help
```

### 3) 物化视图（MV）初始化

使用脚本初始化（已替代旧的 `refresh-materialized-view` / `ah mv`）：

```bash
python scripts/initialize_materialized_views.py --help
```

## 旧命令映射（仅供参考）

- `ah` / `alphahome-cli`：已下线（不再安装）
- `alphahome-ddb`：已下线（不再安装），替代为 `python -m alphahome.integrations.dolphindb.cli ...`
- `refresh-materialized-view`：已下线（不再安装），替代为脚本/Features 模块入口

