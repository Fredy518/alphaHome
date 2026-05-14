# Hikyuu 集成 FAQ

## 总体状态

### Q: 当前 AlphaHome 和 Hikyuu 的主要集成点是什么？

A: 当前主线最稳定的路径是把 Hikyuu 5 分钟 HDF5 导入 DolphinDB，用 DolphinDB 作为分钟线高速查询层。日线 HDF5 导出器和内存适配器仍保留在 `alphahome.providers.tools`，适合研究脚本或后续封装生产脚本。

### Q: 旧文档里的日线生产导出脚本在哪里？

A: `scripts/production/exporters/hikyuu_day_export.py` 当前不存在。请直接使用 `HikyuuH5Exporter`，或基于它新增生产脚本。示例见 [Hikyuu 集成指南](hikyuu_integration_guide.md)。

### Q: Hikyuu 是否是必装依赖？

A: 不是。`pyproject.toml` 当前没有固定安装 `hikyuu`。5 分钟 HDF5 到 DolphinDB 的导入依赖 `h5py` 和 DolphinDB 客户端，不需要导入 Hikyuu Python 包；只有使用 Hikyuu 原生对象、原生回测或 `HikyuuDataAdapter` 时才需要额外安装。

## 配置

### Q: Hikyuu 数据目录在哪里配置？

A: 推荐写入 `~/.alphahome/config.json`：

```json
{
  "backtesting": {
    "hikyuu_data_dir": "E:/stock"
  }
}
```

也可以用环境变量：

```powershell
$env:HIKYUU_DATA_DIR = "E:/stock"
```

命令行导入还可以传 `--hikyuu-data-dir` 覆盖。

### Q: 为什么配置了仓库根目录的 `config.json` 仍然不生效？

A: 当前 `ConfigManager` 默认读取 `~/.alphahome/config.json`。仓库根目录的模板或旧配置不会作为新安装的主配置路径使用。

### Q: `~/.hikyuu/hikyuu.ini` 和 `~/.alphahome/config.json` 有什么区别？

A: `~/.alphahome/config.json` 是 AlphaHome 配置，包含数据库、Tushare、DolphinDB 和 Hikyuu 数据目录。`~/.hikyuu/hikyuu.ini` 是 Hikyuu 自身初始化配置，只在你直接使用 Hikyuu 原生回测或交互式环境时需要。

## 5 分钟导入 DolphinDB

### Q: 标准导入流程是什么？

A:

```powershell
python scripts/generate_hikyuu_5min_tickers.py --hikyuu-dir E:/stock --output-dir scripts/tickers
python -m alphahome.integrations.dolphindb.cli init-kline5m
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min --codes-file scripts/tickers/all.txt --incremental
```

也可以使用 PowerShell 封装：

```powershell
.\scripts\import_all_hikyuu_to_ddb.ps1 -Incremental
```

### Q: `scripts/tickers/*.txt` 是什么？

A: 这些文件是从 Hikyuu 5 分钟 H5 文件中读取 `/data` keys 后生成的 `ts_code` 清单。`all.txt` 是全市场清单，`sh_all.txt`、`sz_all.txt`、`bj_all.txt` 是分市场清单，`sh_6.txt` 这类文件用于手动分批或断点续跑。

### Q: Hikyuu 数据更新后要不要重新生成清单？

A: 建议重新生成。新上市、退市或 H5 文件变化都可能改变可导入代码集合。

### Q: DolphinDB 出现重复数据怎么办？

A: 如果历史上误重复导入，最简单的修复是备份后重建库表并全量导入：

```powershell
.\scripts\import_all_hikyuu_to_ddb.ps1 -ResetDb -InitTable
```

增量导入的正常路径使用 `--incremental` 或 `-Incremental`。

## 日线 HDF5 导出

### Q: 日线导出 API 需要什么输入？

A: `HikyuuH5Exporter.export_day_incremental()` 需要标准 OHLCV DataFrame：

```text
ts_code, trade_date, open, high, low, close, vol, amount
```

如果要导出前复权价格，可额外传入复权因子 DataFrame：

```text
ts_code, trade_date, adj_factor
```

### Q: 导出器写入哪些文件？

A: 按市场写入 Hikyuu 数据目录下的 `sh_day.h5`、`sz_day.h5`、`bj_day.h5`，数据集路径为 `/data/SH600000`、`/data/SZ000001` 这类格式。

### Q: 如何避免退市股票导致回测幸存者偏差？

A: 研究脚本获取股票池时应使用：

```python
data_tool.get_stock_info(active_only=False)
```

是否导出退市股票取决于上游传给导出器的代码集合。

## 内存适配器

### Q: 旧文档里的 `create_kdata()` 和 `generate_ma_cross_signal()` 为什么找不到？

A: 当前方法名是：

```python
adapter.create_kdata_from_dataframe(df, "sz000001")
adapter.calculate_indicator(kdata, "MA", {"n": 5})
adapter.generate_signals(kdata, "MA_CROSS", {"fast_n": 5, "slow_n": 20})
```

组合级信号生成在 `HikyuuSignalGenerator.generate_portfolio_signals()`。

### Q: 没安装 Hikyuu 时内存适配器还能用吗？

A: 可以部分使用。适配器会进入 mock 或 pandas/numpy 回退模式，适合单元测试和轻量验证，但不能代表 Hikyuu 原生回测性能。

## 常见错误

| 错误 | 处理 |
| --- | --- |
| `Missing Hikyuu data dir` | 设置 `backtesting.hikyuu_data_dir`、`HIKYUU_DATA_DIR` 或 `--hikyuu-data-dir` |
| 找不到 `scripts/production/exporters/hikyuu_day_export.py` | 该脚本当前不存在，使用 `HikyuuH5Exporter` API |
| H5 文件为空或没有 `/data` | 检查 Hikyuu 数据源文件，生成清单脚本会跳过缺失市场 |
| DolphinDB 连接失败 | 检查 `dolphindb.host/port/username/password` 或命令行连接参数 |
| `ImportError: No module named hikyuu` | 只有原生 Hikyuu 用法需要安装该包；5 分钟 H5 导入不需要 |

## 相关文档

- [Hikyuu 集成指南](hikyuu_integration_guide.md)
- [Hikyuu 性能说明](hikyuu_performance_analysis.md)
- [DolphinDB 集成](business/dolphindb_integration.md)
- [配置指南](setup/configuration.md)
