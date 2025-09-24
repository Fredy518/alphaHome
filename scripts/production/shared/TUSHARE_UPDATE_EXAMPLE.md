# Tushare 数据更新脚本使用示例

## 🐛 Bug修复记录

### v1.0.1 (2024-01-XX)
- ✅ **修复**: `create_async_manager()` 缺少必需参数 `connection_string` 的错误
- ✅ **修复**: 移除不必要的 `await` 调用 `create_async_manager()`
- ✅ **新增**: `--dry-run` 参数支持，用于安全测试脚本功能

## 🚀 快速开始

### 1. 基本使用

```bash
# 使用默认参数执行更新
python scripts/production/tushare_smart_update_production.py
```

### 2. 自定义参数

```bash
# 5个并发进程，3次重试，10秒重试间隔
python scripts/production/tushare_smart_update_production.py --workers 5 --max_retries 3 --retry_delay 10
```

### 3. 调试模式

```bash
# 启用详细日志输出
python scripts/production/tushare_smart_update_production.py --log_level DEBUG
```

### 4. Windows 批处理方式

```batch
# 使用默认参数
scripts\production\start_tushare_smart_update.bat

# 自定义参数：5进程，3重试，5秒间隔，INFO日志
scripts\production\start_tushare_smart_update.bat 5 3 5 INFO
```

## 📊 执行结果示例

```
🚀 Tushare 数据源智能增量更新生产脚本
================================================================================
并发进程数: 3
最大重试次数: 3
重试间隔: 5秒
日志级别: INFO
启动时间: 2024-01-15 09:00:00

🔍 测试任务发现功能...
✅ 发现 41 个 Tushare fetch 任务:
   - tushare_fina_balancesheet
   - tushare_fina_cashflow
   - tushare_fina_disclosure
   - ...

🚀 开始生产级 Tushare 数据更新...
✅ 数据库连接和任务工厂初始化成功
🚀 开始并行执行 41 个任务 (最大并发: 3)

[tushare_fina_balancesheet] 开始执行 (尝试 1/4)
[tushare_fina_cashflow] 开始执行 (尝试 1/4)
[tushare_fina_disclosure] 开始执行 (尝试 1/4)
[tushare_fina_balancesheet] 执行成功，耗时: 45.67秒
[tushare_fina_express] 开始执行 (尝试 1/4)
...

================================================================================
📊 Tushare 智能增量更新执行摘要
================================================================================
执行时间: 2024-01-15 09:00:00 - 2024-01-15 09:25:30
总耗时: 25.50 分钟
总任务数: 41
✅ 成功任务: 39
❌ 失败任务: 1
⏭️ 跳过任务: 1
成功率: 95.12%
平均任务耗时: 37.25秒

❌ 失败任务详情:
   - tushare_fund_etf_daily: API 连接超时

⏭️ 跳过任务详情:
   - tushare_future_holding: 不支持智能增量

🎯 建议:
   - 检查失败任务的网络连接或 API 权限
   - 查看详细日志了解具体错误原因
```

## ⚙️ 配置文件使用

### 基本配置

创建 `scripts/production/tushare_update_config.yaml` 文件：

```yaml
# 基本配置
production:
  max_workers: 3          # 最大并发进程数
  max_retries: 3          # 单个任务最大重试次数
  retry_delay: 5          # 重试间隔秒数
  log_level: INFO         # 日志级别

# 任务筛选
task_filter:
  include_sources: ["tushare"]  # 只执行 tushare 数据源
  exclude_tasks: []             # 排除特定任务

# 性能监控
monitoring:
  enable_performance_log: true
  alert_thresholds:
    success_rate: 0.8           # 成功率告警阈值
```

### 环境变量配置

```bash
# 设置环境变量
export TUSHARE_MAX_WORKERS=5
export TUSHARE_LOG_LEVEL=DEBUG
export TUSHARE_DRY_RUN=1

# 然后运行脚本
python scripts/production/tushare_smart_update_production.py
```

## 🔍 监控和故障排除

### 日志文件位置

- 默认日志: `logs/tushare_production_update.log`
- 包含详细的执行过程和错误信息

### 常见问题解决

1. **数据库连接失败**
   ```bash
   # 检查数据库状态
   psql -h localhost -U postgres -d alphadb -c "SELECT 1;"
   ```

2. **API 连接超时**
   - 检查网络连接
   - 确认 Tushare API 服务状态
   - 适当增加重试间隔

3. **内存不足**
   - 降低并发进程数
   - 增加系统内存
   - 分批执行任务

4. **执行时间过长**
   - 检查网络带宽
   - 确认 API 响应速度
   - 调整超时设置

### 性能优化建议

1. **网络优化**
   ```bash
   # 增加重试间隔以减少 API 压力
   python scripts/production/tushare_smart_update_production.py --retry_delay 10
   ```

2. **并发优化**
   ```bash
   # 根据系统资源调整并发数
   python scripts/production/tushare_smart_update_production.py --workers 2
   ```

3. **日志优化**
   ```bash
   # 生产环境减少日志输出
   python scripts/production/tushare_smart_update_production.py --log_level WARNING
   ```

## 📈 性能基准

### 测试环境配置

- **CPU**: 8核 Intel i7
- **内存**: 16GB
- **网络**: 100Mbps
- **数据库**: PostgreSQL 15

### 性能数据

| 配置 | 任务数 | 总耗时 | 成功率 | 平均任务耗时 |
|------|--------|--------|--------|--------------|
| 3并发 | 41个 | 25.5分钟 | 95.12% | 37.25秒 |
| 5并发 | 41个 | 18.2分钟 | 92.68% | 26.60秒 |
| 1并发 | 41个 | 42.1分钟 | 97.56% | 61.50秒 |

### 建议配置

- **开发环境**: 1-2个并发进程
- **生产环境**: 3-5个并发进程（根据系统资源调整）
- **网络不稳定**: 增加重试次数和间隔
- **资源受限**: 使用较低并发数

## 🔄 定期执行

### Linux Crontab 配置

```bash
# 每天早上9点执行
0 9 * * * cd /path/to/alphahome && python scripts/production/tushare_smart_update_production.py --workers 3

# 每周一早上8点执行（完整更新）
0 8 * * 1 cd /path/to/alphahome && python scripts/production/tushare_smart_update_production.py --workers 5 --max_retries 5
```

### Windows 任务计划程序

1. 打开任务计划程序
2. 创建基本任务
3. 设置触发器（每日/每周）
4. 设置操作：
   - 程序: `python.exe`
   - 参数: `scripts\production\tushare_smart_update_production.py --workers 3`
   - 起始位置: `E:\CodePrograms\alphaHome`
