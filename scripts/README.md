# Scripts Directory

## 📁 目录结构

```
scripts/
├── README.md               # 本文件
├── tickers/                # Hikyuu 5分钟导入清单（ts_code列表）
├── import_all_hikyuu_to_ddb.ps1 # 一键导入 Hikyuu 5min -> DolphinDB（可选备份）
├── generate_hikyuu_5min_tickers.py # 从 Hikyuu HDF5 生成 tickers/*.txt
├── pit/                    # PIT相关脚本
│   ├── analyze_scope.py    # 分析PIT回填范围和预估耗时
│   ├── staged_backfill.py  # 分阶段执行PIT回填
│   └── full_backfill.py    # 一次性全量PIT回填
└── maintenance/            # 维护脚本
    ├── count_python_lines.py  # 统计代码行数
    └── migrate_db_name.py     # 数据库迁移工具
```

## 🎯 PIT相关脚本 (`pit/`)

### `analyze_scope.py` - 分析回填范围
**功能**: 分析PIT回填的数据范围和预估执行时间
**用途**: 在执行回填前进行评估和规划

```bash
python scripts/pit/analyze_scope.py
```

**输出内容**:
- 公告日期分布统计
- 可计算的时点数量
- 预估执行时间
- 推荐执行策略

### `staged_backfill.py` - 分阶段回填
**功能**: 分阶段执行PIT历史数据回填
**用途**: 大数据量场景下的分批处理

```bash
# 回填最近1年数据
python scripts/pit/staged_backfill.py --stage recent1y

# 回填最近2年数据
python scripts/pit/staged_backfill.py --stage recent2y

# 回填最近5年数据
python scripts/pit/staged_backfill.py --stage recent5y

# 全量回填
python scripts/pit/staged_backfill.py --stage full

# 仅分析，不执行
python scripts/pit/staged_backfill.py --analyze-only

# 清空现有数据后回填
python scripts/pit/staged_backfill.py --stage recent1y --clear
```

**阶段说明**:
- `recent1y`: 最近1年数据 (快速)
- `recent2y`: 最近2年数据 (中等)
- `recent5y`: 最近5年数据 (较慢)
- `recent10y`: 最近10年数据 (慢)
- `full`: 全部历史数据 (最慢)

### `full_backfill.py` - 一次性全量回填
**功能**: 一次性执行完整的PIT历史数据回填
**用途**: 初始化或完整重建场景

```bash
python scripts/pit/full_backfill.py
```

**特点**:
- 清空现有财务指标数据
- 重新计算所有历史数据
- 预计耗时: 2-4小时
- 提供详细的进度监控

## 🔧 维护脚本 (`maintenance/`)

### `count_python_lines.py` - 代码统计
**功能**: 统计项目中Python代码的行数

```bash
python scripts/maintenance/count_python_lines.py
```

### `migrate_db_name.py` - 数据库迁移
**功能**: 数据库名称迁移工具

```bash
python scripts/maintenance/migrate_db_name.py
```

## 📋 使用建议

### 首次使用PIT系统
1. **分析数据范围**: `python scripts/pit/analyze_scope.py`
2. **分阶段回填**: `python scripts/pit/staged_backfill.py --stage recent1y`
3. **逐步扩展**: 根据需要扩展到更长时间范围

### 日常维护
- 使用增量更新: 通过 `research.pgs_factor.core.pit_manager` 的增量模式
- 定期检查: 使用分析脚本监控数据质量

### DolphinDB（导入/备份）

```powershell
# 生成/更新 scripts/tickers/*.txt（按市场/首位数字分组）
python scripts/generate_hikyuu_5min_tickers.py --hikyuu-dir E:/stock --output-dir scripts/tickers

# 一键导入（首次建议加 -InitTable）
./scripts/import_all_hikyuu_to_ddb.ps1 -InitTable

# 增量模式：仅导入比 DolphinDB 当前 max(trade_time) 更新的数据
./scripts/import_all_hikyuu_to_ddb.ps1 -Incremental

# 重建（会删除 DolphinDB 中的 dfs://kline_5min，谨慎）
./scripts/import_all_hikyuu_to_ddb.ps1 -ResetDb -InitTable

# 导入 + DFS 目录备份（需要指定/配置 DfsRoot）
./scripts/import_all_hikyuu_to_ddb.ps1 -InitTable -Backup -DfsRoot "D:/dolphindb/server/data/dfs"
```

### 故障恢复
- 使用 `full_backfill.py` 进行完整重建
- 使用 `staged_backfill.py --clear` 清空后重新开始

## ⚠️ 注意事项

1. **执行前备份**: 重要数据请先备份
2. **资源消耗**: 全量回填会消耗大量CPU和内存
3. **时间规划**: 建议在非业务时间执行大批量操作
4. **监控进度**: 所有脚本都提供详细的进度输出

## 🔗 相关文档

- [PIT系统架构文档](../research/pgs_factor/README.md)
- [数据库设计文档](../docs/architecture/)
- [用户使用指南](../docs/user/)
