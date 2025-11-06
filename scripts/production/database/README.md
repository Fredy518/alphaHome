# 数据库维护脚本

本目录包含用于数据库维护和管理的脚本。

## 脚本列表

### migrate_bse_code_mapping.py

**用途**：北交所代码切换映射迁移

**功能说明**：
- 从 `tushare.stock_code_mapping` 表获取北交所代码切换映射关系
- 自动识别所有需要更新的数据表（stock_ 和 fina_ 前缀的表）
- 将旧代码数据复制并映射为新代码，使用 upsert 模式保存

**使用方法**：

```bash
# 1. 查看将要迁移的表和记录数（不实际执行迁移）
python scripts/production/database/migrate_bse_code_mapping.py --dry-run

# 2. 显示详细信息
python scripts/production/database/migrate_bse_code_mapping.py --dry-run --verbose

# 3. 执行迁移（实际修改数据）
python scripts/production/database/migrate_bse_code_mapping.py

# 4. 指定特定的表进行迁移
python scripts/production/database/migrate_bse_code_mapping.py --tables stock_daily stock_dividend

# 5. 组合使用：显示详细信息并执行迁移
python scripts/production/database/migrate_bse_code_mapping.py --verbose
```

**参数说明**：
- `--dry-run`: 模拟模式，不实际修改数据，用于预览
- `--verbose`: 显示详细信息，包括映射示例和目标表列表
- `--tables`: 指定要迁移的表（表名列表）

**迁移逻辑**：
1. 从 `tushare.stock_code_mapping` 表读取映射关系（ts_code_old → ts_code_new）
2. 查找所有需要更新的表（排除 stock_basic 和 stock_code_mapping）
3. 自动检测每个表的代码列（支持 `ts_code` 和 `con_code` 列）
4. 对每个表，查找代码列在映射表中的记录
5. 复制这些记录，将代码从旧代码替换为新代码
   - 如果表有 `ts_code` 列，更新 `ts_code`
   - 如果表有 `con_code` 列，更新 `con_code`
   - 如果表同时有这两个列，同时更新两个列
6. 其他字段保持不变
7. 使用 upsert 模式保存回原表

**注意事项**：
- ⚠️ 执行实际迁移前，强烈建议先使用 `--dry-run` 参数预览
- ⚠️ 迁移会实际修改数据库，请确保在非生产环境测试
- ✅ 脚本会自动跳过没有 ts_code 或 con_code 列的表
- ✅ 脚本会同时检测和处理 `ts_code` 和 `con_code` 列
- ✅ 脚本会自动检测表的主键用于 upsert 操作
- ✅ 如果有 update_time 列，会自动更新时间戳

**典型输出**：

```
2025-10-24 13:41:24 - __main__ - INFO - ✅ 找到 242 条代码映射记录
2025-10-24 13:41:24 - __main__ - INFO - ✅ 找到 30 个目标表
2025-10-24 13:41:24 - __main__ - INFO - 目标表列表: ['stock_margin', 'fina_income', ...]
2025-10-24 13:41:24 - __main__ - INFO - 表 fina_income 找到 6812 条需要迁移的记录
...
2025-10-24 13:41:53 - __main__ - INFO - 找到记录数: 1498615
2025-10-24 13:41:53 - __main__ - INFO - 迁移记录数: 1498615
2025-10-24 13:41:53 - __main__ - INFO - 耗时: 29.43 秒
```

## 维护建议

1. **定期备份**：执行重要数据库操作前，建议先备份数据库
2. **测试环境验证**：重要脚本应在测试环境验证后再在生产环境执行
3. **监控日志**：执行脚本时注意观察日志输出，及时发现异常
4. **分批执行**：对于大批量数据迁移，可以考虑分表执行

