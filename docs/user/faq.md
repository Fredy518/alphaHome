# AlphaHome FAQ

## 安装与配置

### 现在应该怎么安装依赖？

使用 `pyproject.toml`：

```bash
pip install -e .
pip install -e ".[akshare]"   # 可选
pip install -e ".[research]"  # 可选
pip install -e ".[test]"      # 可选
```

当前仓库没有 `requirements.txt`。

### 配置文件在哪里？

默认路径是 `~/.alphahome/config.json`。可用下面命令确认：

```bash
python -c "from alphahome.common.config_manager import ConfigManager; c=ConfigManager(); print(c.config_file)"
```

### 支持哪些环境变量？

常用环境变量：

- `DATABASE_URL`
- `TUSHARE_TOKEN`
- `TINYSOFT_USER`
- `TINYSOFT_PASSWORD`
- `TINYSOFT_HOST`
- `TINYSOFT_PORT`
- `TINYSOFT_INI`

### 数据库连接失败怎么办？

先独立验证连接串，再验证 AlphaHome：

```bash
psql "postgresql://user:password@localhost:5432/alphadb"
python -c "from alphahome.common.db_manager import create_sync_manager; db=create_sync_manager(); print(db.test_connection())"
```

## GUI

### GUI 怎么启动？

```bash
python run.py
```

安装后也可以运行：

```bash
alphahome
```

### GUI 任务列表为空？

常见原因：

- 数据库配置无效，`UnifiedTaskFactory` 初始化失败。
- 任务模块未成功导入，查看启动日志中的 import error。
- 依赖缺失，例如可选数据源包未安装。

## CLI

### `ah`、`alphahome-cli`、`refresh-materialized-view` 为什么没有了？

统一 CLI 已下线。现在使用 GUI、`scripts/` 脚本或 `python -m` 模块入口。详见 [CLI 下线说明](../CLI_USAGE_GUIDE.md)。

## 数据采集

### Tushare API 调用失败怎么办？

检查三件事：

1. `api.tushare_token` 或 `TUSHARE_TOKEN` 是否设置。
2. Tushare 账号是否有对应接口权限。
3. 是否触发限流，必要时降低 `concurrent_limit` 或 `--workers`。

### 数据更新不完整怎么办？

先用 GUI 或生产脚本执行 SMART 更新；若只缺某个日期段，用 MANUAL 或具体脚本补：

```bash
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 2
```

对 PIT 或因子数据，使用对应生产脚本补算。

### 任务执行后返回 `partial_success` 是什么？

说明数据保存成功，但 `_validate_data()` 中有验证规则未完全通过。应查看日志中的 `validation_details` 和目标表数据质量。

## PIT 与因子

### PIT 日常更新命令是什么？

```bash
python scripts/production/data_updaters/pit/pit_data_update_production.py --target all --mode incremental
```

### 为什么 G 因子没有结果？

G 因子脚本会先查询同日期 `pgs_factors.p_factor` 中已有 P 因子的股票。如果该日期没有 P 因子，G 因子会跳过。

### 单日补 P/G 因子怎么做？

```bash
python scripts/production/factor_calculators/p_factor/calculate_p_factor_for_specific_dates.py --dates 2026-05-08
python scripts/production/factor_calculators/g_factor/calculate_g_factor_for_specific_dates.py --dates 2026-05-08
```

## 测试与开发

### 如何只跑不依赖数据库/API 的测试？

```bash
pytest tests/unit/ -v -m "not requires_db and not requires_api"
```

### 出现 `ModuleNotFoundError`？

在仓库根目录执行：

```bash
pip install -e .
```

### 如何定位慢任务？

先降低并发确认是否是外部 API 或数据库压力：

```bash
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 1 --log_level DEBUG
```

再检查 PostgreSQL 慢查询、目标表索引和任务批次大小。
