# AlphaHome 配置指南

本文档详细说明了 AlphaHome 系统的配置选项和最佳实践。

## 📁 **配置文件结构**

AlphaHome 使用 JSON 格式的配置文件，主要配置文件为 `config.json`。

```
alphahome/
├── config.json              # 主配置文件
├── config.example.json      # 配置模板
└── logs/                    # 日志目录
```

## 🔧 **基础配置**

### **1. 数据库配置**

```json
{
    "database": {
        "url": "postgresql://username:password@localhost:5432/tusharedb",
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
    }
}
```

**配置说明：**
- `url`: PostgreSQL连接字符串
- `min_size`: 连接池最小连接数
- `max_size`: 连接池最大连接数（推荐25）
- `command_timeout`: 命令超时时间（秒）
- `server_settings`: PostgreSQL服务器优化设置

### **2. API配置**

```json
{
    "api": {
        "tushare_token": "your_tushare_token_here",
        "rate_limit": {
            "requests_per_minute": 200,
            "burst_limit": 10
        },
        "retry_config": {
            "max_retries": 3,
            "backoff_factor": 2,
            "timeout": 30
        }
    }
}
```

**配置说明：**
- `tushare_token`: Tushare API访问令牌
- `rate_limit`: API调用频率限制
- `retry_config`: 重试机制配置

### **3. 性能监控配置**

```json
{
    "performance": {
        "enable_monitoring": true,
        "max_history_records": 100,
        "log_slow_operations": true,
        "slow_operation_threshold": 10.0,
        "auto_batch_size_optimization": false
    }
}
```

## 🎯 **任务配置**

### **任务特定配置**

```json
{
    "tasks": {
        "tushare_stock_daily": {
            "batch_size": 100,
            "retry_count": 3,
            "delay_seconds": 1,
            "concurrent_limit": 10,
            "rate_limit_delay": 45,
            "enable_validation": true
        },
        "tushare_stock_basic": {
            "update_frequency": "daily",
            "concurrent_limit": 5
        },
        "tushare_fina_indicator": {
            "batch_size": 50,
            "concurrent_limit": 3,
            "delay_seconds": 2
        }
    }
}
```

**配置参数说明：**
- `batch_size`: 批处理大小
- `retry_count`: 重试次数
- `delay_seconds`: 请求间隔（秒）
- `concurrent_limit`: 并发限制
- `rate_limit_delay`: 频率限制延迟
- `enable_validation`: 是否启用数据验证

### **批处理配置**

```json
{
    "batch_processing": {
        "default_batch_size": 100,
        "max_concurrent_batches": 5,
        "batch_timeout": 300,
        "enable_progress_tracking": true,
        "auto_retry_failed_batches": true
    }
}
```

## 📈 **回测配置**

```json
{
    "backtesting": {
        "default_cash": 100000,
        "default_commission": 0.001,
        "cache_data": true,
        "max_cache_size": 1000,
        "default_start_date": "2023-01-01",
        "default_end_date": "2023-12-31",
        "default_table": "tushare_stock_daily",
        "performance_monitoring": {
            "enable_memory_tracking": true,
            "enable_execution_timing": true,
            "log_performance_stats": true
        }
    }
}
```

## 🔍 **日志配置**

```json
{
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "file_config": {
            "enabled": true,
            "filename": "logs/alphahome.log",
            "max_size": "10MB",
            "backup_count": 5,
            "rotation": "daily"
        },
        "console_config": {
            "enabled": true,
            "level": "INFO",
            "colored": true
        }
    }
}
```

## 🛡️ **安全配置**

### **敏感信息保护**

```json
{
    "security": {
        "encrypt_config": false,
        "config_file_permissions": "600",
        "api_token_encryption": false,
        "database_ssl": {
            "enabled": false,
            "cert_file": "",
            "key_file": "",
            "ca_file": ""
        }
    }
}
```

### **环境变量支持**

```bash
# 设置环境变量
export ALPHAHOME_DB_URL="postgresql://user:pass@localhost/db"
export ALPHAHOME_TUSHARE_TOKEN="your_token_here"
export ALPHAHOME_LOG_LEVEL="DEBUG"
```

## 🔧 **高级配置**

### **缓存配置**

```json
{
    "cache": {
        "enabled": true,
        "backend": "memory",
        "memory_config": {
            "max_size": "512MB",
            "ttl": 3600
        },
        "redis_config": {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "password": ""
        }
    }
}
```

### **并发控制**

```json
{
    "concurrency": {
        "max_workers": 4,
        "thread_pool_size": 10,
        "async_timeout": 300,
        "semaphore_limit": 100
    }
}
```

## 📊 **配置验证**

### **验证配置文件**

```python
from alphahome.common.config_manager import ConfigManager

# 加载并验证配置
config = ConfigManager()
if config.validate():
    print("配置文件验证通过")
else:
    print("配置文件存在错误")
```

### **配置检查脚本**

```bash
# 运行配置检查
python -m alphahome.tools.config_checker

# 检查特定配置项
python -m alphahome.tools.config_checker --check database
```

## 🎯 **最佳实践**

### **1. 性能优化**
- 根据系统资源调整连接池大小
- 合理设置批处理大小
- 启用缓存机制

### **2. 稳定性保障**
- 配置适当的重试机制
- 设置合理的超时时间
- 启用性能监控

### **3. 安全考虑**
- 使用环境变量存储敏感信息
- 定期轮换API令牌
- 限制配置文件权限

### **4. 监控和调试**
- 启用详细日志记录
- 配置性能监控
- 设置告警机制

## 🔄 **配置更新**

### **热重载配置**

```python
# 重新加载配置（无需重启）
config_manager.reload()
```

### **配置版本管理**

```bash
# 备份当前配置
cp config.json config.backup.$(date +%Y%m%d_%H%M%S).json

# 恢复配置
cp config.backup.20250101_120000.json config.json
```

## 📞 **故障排除**

### **常见配置错误**

1. **数据库连接失败**
   - 检查URL格式
   - 验证用户名密码
   - 确认数据库服务状态

2. **API调用失败**
   - 验证Token有效性
   - 检查网络连接
   - 确认API配额

3. **性能问题**
   - 调整连接池大小
   - 优化批处理配置
   - 启用缓存机制

### **配置调试**

```bash
# 显示当前配置
python -c "
from alphahome.common.config_manager import ConfigManager
config = ConfigManager()
print(config.get_debug_info())
"
```

---

**提示**: 修改配置后建议先在测试环境验证，确保系统正常运行后再应用到生产环境。
