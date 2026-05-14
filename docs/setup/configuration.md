# AlphaHome 配置指南

## 配置加载顺序

`ConfigManager` 默认读取 `~/.alphahome/config.json`。如果关键字段缺失，会从环境变量回退：

| 配置项 | JSON 路径 | 环境变量 |
| --- | --- | --- |
| PostgreSQL URL | `database.url` | `DATABASE_URL` |
| Tushare Token | `api.tushare_token` | `TUSHARE_TOKEN` |
| Tinysoft 用户 | `api.tinysoft.user` | `TINYSOFT_USER` |
| Tinysoft 密码 | `api.tinysoft.password` | `TINYSOFT_PASSWORD` |
| Tinysoft 主机 | `api.tinysoft.host` | `TINYSOFT_HOST` |
| Tinysoft 端口 | `api.tinysoft.port` | `TINYSOFT_PORT` |

历史路径会被自动迁移到 `~/.alphahome/config.json`，但新安装请直接使用新路径。

## 推荐配置模板

```json
{
  "database": {
    "url": "postgresql://username:password@localhost:5432/alphadb",
    "mv_refresh_timeout_seconds": 7200,
    "mv_row_count_timeout_seconds": 60,
    "pool_config": {
      "min_size": 5,
      "max_size": 25,
      "command_timeout": 180,
      "max_queries": 50000,
      "max_inactive_connection_lifetime": 300,
      "server_settings": {
        "application_name": "alphahome_fetcher",
        "tcp_keepalives_idle": "600",
        "tcp_keepalives_interval": "30",
        "tcp_keepalives_count": "3",
        "jit": "off"
      }
    }
  },
  "api": {
    "tushare_token": "your_tushare_token_here",
    "tinysoft": {
      "user": "",
      "password": "",
      "host": "tsl.tinysoft.com.cn",
      "port": 443,
      "service": "",
      "timeout_ms": 45000,
      "request_interval": 0.2,
      "ini_path": ""
    }
  },
  "tasks": {
    "tushare_stock_daily": {
      "save_batch_size": 5000,
      "concurrent_limit": 10,
      "max_retries": 3,
      "retry_delay": 1,
      "smart_lookback_days": 10
    }
  }
}
```

## 任务配置

所有 `FetcherTask` 子类都会读取 `tasks.<task_name>` 下的覆盖项。常用字段：

| 字段 | 说明 |
| --- | --- |
| `concurrent_limit` | 批次并发数 |
| `max_retries` | 单批次最大重试次数 |
| `retry_delay` | 重试等待秒数，实际会按 attempt 放大 |
| `save_batch_size` | 保存到数据库的 DataFrame 分批行数 |
| `smart_lookback_days` | SMART 增量时向前回看天数 |
| `rate_limit_delay` | Tushare 触发限流后的等待秒数 |
| `page_size` | Tushare 分页大小 |
| `request_interval` | AkShare/Tinysoft 请求间隔 |
| `query_timeout_ms` | Tinysoft 查询超时 |

## 配置检查

```bash
python -c "from alphahome.common.config_manager import ConfigManager; c=ConfigManager(); print(c.config_file); print(c.load_config().keys())"
python -c "from alphahome.common.db_manager import create_sync_manager; db=create_sync_manager(); print(db.test_connection())"
```

修改配置后，GUI 中可点击“加载当前设置”，脚本进程则需要重启。

## 安全建议

- 不要提交真实 `~/.alphahome/config.json`。
- Token 和密码优先放在本机用户目录或环境变量中。
- 数据库维护脚本和全量回填脚本执行前先备份。
