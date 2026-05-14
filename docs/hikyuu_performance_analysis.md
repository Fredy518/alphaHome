# Hikyuu 集成性能说明

本文档说明当前 Hikyuu 相关路径的性能边界和排查重点。旧版性能报告中的 `research/projects/hikyuu_integration/benchmark_b.py`、`benchmark_c.py` 当前不在主线代码中；其中的数字只能作为历史参考，不能视为当前版本的验收结果。

## 当前路径

| 路径 | 当前入口 | 性能关注点 |
| --- | --- | --- |
| Hikyuu 5min -> DolphinDB | `alphahome.integrations.dolphindb.hikyuu_5min_importer` | HDF5 读取、代码清单规模、DolphinDB 写入批次 |
| AlphaHome 日线 -> Hikyuu HDF5 | `HikyuuH5Exporter.export_day_incremental()` | PostgreSQL 查询、复权因子合并、HDF5 去重追加 |
| 内存适配器 | `HikyuuDataAdapter` | DataFrame 内存占用、指标数量、是否回退 pandas/numpy |

## 推荐使用方式

生产分钟线查询优先使用 DolphinDB 导入路径：

```powershell
python scripts/generate_hikyuu_5min_tickers.py --hikyuu-dir E:/stock --output-dir scripts/tickers
python -m alphahome.integrations.dolphindb.cli import-hikyuu-5min --codes-file scripts/tickers/all.txt --incremental
```

日线 HDF5 导出当前是 API 能力，不建议把研究脚本中的一次性导出当作生产调度。若需要生产化，应新增脚本并补充：

- 可配置的股票池和日期范围；
- 分批导出和失败重试；
- 导出前后的 H5 行数/日期覆盖校验；
- 运行日志和幂等重跑策略。

## 性能瓶颈

### 5 分钟导入 DolphinDB

常见瓶颈：

- HDF5 单文件读取速度；
- `codes-file` 太大导致单次导入耗时过长；
- DolphinDB 网络连接或 DFS 写入速度；
- 重复导入导致目标表膨胀。

调优建议：

- 全市场初次导入前先运行 `init-kline5m`；
- 大规模重跑前备份并使用 `-ResetDb -InitTable` 清理重复历史；
- 使用 `scripts/tickers/{market}_{digit}.txt` 手动分批；
- 导入前重新生成清单，避免清单与 H5 内容不一致。

### 日线 HDF5 导出

常见瓶颈：

- 一次性从 PostgreSQL 拉取过多股票和日期；
- 复权因子与行情数据 merge 后内存放大；
- 增量写入时需要读取已有 dataset 去重；
- HDF5 不适合多进程同时写同一个文件。

调优建议：

- 按市场、按股票批次导出；
- 避免多个进程同时写同一个 `{market}_day.h5`；
- 导出后抽样检查 `/data/{MKT}{CODE}` 的日期顺序和重复日期；
- 大规模重建时优先写临时目录，验证后替换。

### 内存适配器

`HikyuuDataAdapter` 面向小样本验证，不是大规模生产回测层。指标和信号会随股票数、历史长度、指标数量线性或更快增长。若未安装 Hikyuu，适配器会回退到 pandas/numpy 或 mock 行为，性能与 Hikyuu 原生引擎不可直接比较。

## 验收建议

### 5 分钟导入

导入后至少检查：

- 导入股票数是否接近清单数量；
- 目标表是否存在重复 `(ts_code, trade_time)`；
- 最近交易日数据是否覆盖预期市场；
- 单次增量导入的新增行数是否合理。

### 日线导出

导出后至少检查：

- `{sh,sz,bj}_day.h5` 是否存在；
- `/data` 组下目标 symbol 是否存在；
- `datetime` 是否单调递增且无重复；
- 前复权导出时价格是否与复权因子口径一致。

## 历史基准数字

旧文档曾记录过两组历史结果：

| 路径 | 历史规模 | 历史速度 | 备注 |
| --- | --- | --- | --- |
| 路径 B 原生回测 | 5000 股票 x 2 年 | 约 4.3 万 bars/sec 端到端 | benchmark 脚本当前不在主线 |
| 路径 C 内存适配器 | 50 股票 x 500 天 | 约 4.4 万 bars/sec 总体 | 含 pandas/numpy 回退场景，不能代表生产 |

这些数字只能用于理解量级。若要做当前环境验收，应重新编写基准脚本并记录：

- Git commit；
- Python、pandas、numpy、h5py、hikyuu、DolphinDB 客户端版本；
- HDF5 文件大小和股票数；
- 数据库/DolphinDB 部署位置；
- 冷启动和热缓存两种结果。

## 相关文档

- [Hikyuu 集成指南](hikyuu_integration_guide.md)
- [Hikyuu FAQ](hikyuu_faq.md)
- [DolphinDB 集成](business/dolphindb_integration.md)
- [tickers 说明](../scripts/tickers/README.md)
