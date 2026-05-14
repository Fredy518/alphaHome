# Hikyuu 集成指南

本文档描述 AlphaHome 当前保留的 Hikyuu 相关能力。旧版文档中提到的 `scripts/production/exporters/hikyuu_day_export.py` 和 `research/projects/hikyuu_integration/benchmark_*.py` 当前不在主线代码中；如需恢复生产级日线导出或重跑基准，请以本页列出的现有 API 为基础新增脚本。

## 当前能力

| 场景 | 当前入口 | 状态 |
| --- | --- | --- |
| Hikyuu 5 分钟 HDF5 导入 DolphinDB | `python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min` / `scripts/import_all_hikyuu_to_ddb.ps1` | 生产可用 |
| 从 Hikyuu H5 生成导入清单 | `scripts/generate_hikyuu_5min_tickers.py` | 可用 |
| AlphaHome 日线数据导出到 Hikyuu HDF5 | `alphahome.providers.tools.hikyuu_h5_exporter.HikyuuH5Exporter` | API 可用，暂无生产 CLI |
| 内存 K 线适配和指标/信号计算 | `alphahome.providers.tools.hikyuu_data_adapter` | 实验/研究用途 |

## 配置

AlphaHome 默认从 `~/.alphahome/config.json` 读取 Hikyuu 数据目录，也支持环境变量兜底：

```json
{
  "backtesting": {
    "hikyuu_data_dir": "E:/stock"
  }
}
```

```powershell
$env:HIKYUU_DATA_DIR = "E:/stock"
```

Hikyuu 自身的 Python 包没有在 `pyproject.toml` 中固定依赖。只有直接使用 Hikyuu 原生对象或内存适配器时，才需要在当前环境中额外安装兼容版本的 `hikyuu`。

## 数据目录

当前工具默认识别以下 HDF5 文件：

```text
E:/stock/
├── sh_5min.h5
├── sz_5min.h5
├── bj_5min.h5
├── sh_day.h5
├── sz_day.h5
└── bj_day.h5
```

5 分钟导入和日线导出都使用 Hikyuu 常见结构：`/data/{MKT}{CODE}`，例如 `/data/SZ000001`。

## 5 分钟数据导入 DolphinDB

先生成或更新 `ts_code` 清单：

```powershell
python scripts/generate_hikyuu_5min_tickers.py --hikyuu-dir E:/stock --output-dir scripts/tickers
```

初始化 DolphinDB 库表：

```powershell
python -m alphahome.integrations.dolphindb.cli init-kline5m
```

增量导入：

```powershell
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min --codes-file scripts/tickers/all.txt --incremental
```

PowerShell 一键脚本封装了清单、备份、重建和导入参数：

```powershell
.\scripts\import_all_hikyuu_to_ddb.ps1 -Incremental
.\scripts\import_all_hikyuu_to_ddb.ps1 -InitTable
.\scripts\import_all_hikyuu_to_ddb.ps1 -ResetDb -InitTable
```

更多 DolphinDB 参数见 [DolphinDB 集成](business/dolphindb_integration.md) 和 [tickers 说明](../scripts/tickers/README.md)。

## 日线 HDF5 导出 API

当前没有生产 CLI，但可在研究或维护脚本中直接使用 `HikyuuH5Exporter`：

```python
from alphahome.common.config_manager import get_hikyuu_data_dir
from alphahome.common.db_manager import create_sync_manager
from alphahome.providers import AlphaDataTool
from alphahome.providers.tools.hikyuu_h5_exporter import HikyuuH5Exporter

symbols = ["000001.SZ", "600519.SH"]
start_date = "2020-01-01"
end_date = "2024-12-31"

db = create_sync_manager()
data_tool = AlphaDataTool(db)

raw_data = data_tool.get_stock_data(symbols, start_date, end_date)
adj_factor = data_tool.get_adj_factor_data(symbols, start_date, end_date)

exporter = HikyuuH5Exporter(get_hikyuu_data_dir() or "E:/stock")
exporter.export_day_incremental(raw_data, adj_factor)
```

输入 DataFrame 需要包含：

```text
ts_code, trade_date, open, high, low, close, vol, amount
```

复权因子 DataFrame 需要包含：

```text
ts_code, trade_date, adj_factor
```

导出器会按市场写入 `{sh,sz,bj}_day.h5`，并按 `datetime` 去重增量追加。

## 内存适配器

`HikyuuDataAdapter` 适合小样本指标验证。若本机未安装 `hikyuu`，适配器会回退到 pandas/numpy 直算或 mock 行为，不能替代正式回测引擎。

```python
from alphahome.providers.tools.hikyuu_data_adapter import HikyuuDataAdapter

adapter = HikyuuDataAdapter()
kdata = adapter.create_kdata_from_dataframe(df, "sz000001")

ma5 = adapter.calculate_indicator(kdata, "MA", {"n": 5})
signals = adapter.generate_signals(
    kdata,
    "MA_CROSS",
    {"fast_n": 5, "slow_n": 20},
)
```

支持的指标包括 `MA`、`EMA`、`RSI`、`MACD`、`KDJ`、`BOLL`、`ATR`、`VOL`；支持的信号包括 `MA_CROSS`、`RSI_OVERBOUGHT`、`MACD_CROSS`。

## 代码映射

```python
from alphahome.providers import map_ts_code_to_hikyuu

map_ts_code_to_hikyuu("000001.SZ")  # "SZ000001"
map_ts_code_to_hikyuu("600519.SH")  # "SH600519"
map_ts_code_to_hikyuu("430047.BJ")  # "BJ430047"
```

## 排障

| 问题 | 处理 |
| --- | --- |
| `Missing Hikyuu data dir` | 设置 `backtesting.hikyuu_data_dir`、`HIKYUU_DATA_DIR` 或命令行 `--hikyuu-data-dir` |
| H5 文件没有 `/data` 组 | 先确认 Hikyuu 数据源是否完整，生成清单脚本会跳过不存在的市场文件 |
| DolphinDB 出现重复行 | 使用 `.\scripts\import_all_hikyuu_to_ddb.ps1 -ResetDb -InitTable` 重建后全量导入 |
| 日线导出字段缺失 | 确认输入列为 `ts_code/trade_date/open/high/low/close/vol/amount` |
| Hikyuu 原生对象初始化失败 | 检查本机 Hikyuu 安装和 `~/.hikyuu/hikyuu.ini`；AlphaHome 配置只负责数据目录 |

## 相关文档

- [配置指南](setup/configuration.md)
- [DolphinDB 集成](business/dolphindb_integration.md)
- [Hikyuu FAQ](hikyuu_faq.md)
- [Hikyuu 性能说明](hikyuu_performance_analysis.md)
