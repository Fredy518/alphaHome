# 📦 Archive 归档目录

本目录用于保存已归档的历史文件，以备审计和回滚需要。

## 📅 最后更新
2025-11-23

## 📁 归档内容

### 1. research_pit_data_backup/
- **归档时间**: 2025-09-13
- **归档原因**: PIT数据功能已重构并迁移到生产脚本模块
- **新位置**: `scripts/production/data_updaters/pit/`
- **详细说明**: 见 [README_PIT_ARCHIVE.md](README_PIT_ARCHIVE.md)
- **计划删除**: 2025-12-13（归档后3个月）

### 2. logs_backup/
- **归档时间**: 2025-11-23
- **归档原因**: 项目清理，归档旧日志文件
- **内容**:
  - PIT数据处理日志 (pit_*.log)
  - 生产因子计算日志 (production_g_factor/, production_p_factor/)
  - 测试日志 (weekly_p_factor_test_*.log)
  - 评估报告 (*.csv, *.md)
- **计划删除**: 2026-02-23（归档后3个月）

### 3. debugging_scripts_backup/
- **归档时间**: 2025-11-23
- **归档原因**: 调试脚本已完成使命，归档备用
- **内容**:
  - `demo_g_factor_null_fix.py` - G因子空值处理演示
  - `test_g_factor_null_handling.py` - G因子空值处理测试
- **计划删除**: 2026-02-23（归档后3个月）

## ⚠️ 删除前注意事项

1. 确认相关功能已在新位置稳定运行
2. 检查是否有其他代码依赖归档文件
3. 备份重要数据到外部存储（如需要）
4. 在 Git 中提交删除操作，保留变更历史

## 🔄 恢复方法

如需恢复归档文件，可以：
1. 从本目录复制回原位置
2. 使用 Git 历史记录恢复（`git checkout <commit> -- <path>`）
