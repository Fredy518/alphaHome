# 数据采集生产更新示例

当前生产数据采集入口是：

```bash
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py
```

虽然目录名保留 `tushare`，脚本实际会发现所有 `task_type="fetch"` 的注册任务，覆盖 Tushare、AkShare、Tinysoft、Excel 等数据源。

## 快速运行

```bash
# 默认参数
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py

# 控制并发和重试
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3 --max_retries 3 --retry_delay 5

# 调试
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 1 --log_level DEBUG

# 试运行
python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --dry-run
```

## 参数

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--workers` | 3 | 脚本级并发任务数 |
| `--max_retries` | 3 | 单个任务失败后的脚本级重试次数 |
| `--retry_delay` | 5 | 脚本级重试等待秒数 |
| `--log_level` | INFO | 日志级别 |
| `--dry-run` | false | 只分析任务，不实际执行 |

## 调度示例

Windows 任务计划程序：

```text
程序: python
参数: scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3
起始位置: E:\CodePrograms\alphaHome
```

Linux/macOS cron：

```bash
0 9 * * * cd /path/to/alphaHome && python scripts/production/data_updaters/tushare/data_collection_smart_update_production.py --workers 3
```

## 运行建议

- 先用 `--workers 1 --log_level DEBUG` 验证配置和依赖。
- Tushare 限流时降低 `--workers` 或任务级 `concurrent_limit`。
- Tinysoft 任务通常更依赖网络和服务端稳定性，建议保守并发。
- 执行失败时保留完整日志、命令和配置片段。
