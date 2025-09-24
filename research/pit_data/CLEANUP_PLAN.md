# 🧹 PIT数据目录清理计划

## 📅 清理时间表

### **阶段一：立即执行 (已完成)**
- ✅ 创建归档备份：`archive/research_pit_data_backup/`
- ✅ 添加弃用标记：`research/pit_data/DEPRECATED.md`
- ✅ 更新相关引用：测试文件、pgs_factor集成
- ✅ 更新项目文档：README.md添加重构说明

### **阶段二：观察期 (1个月 - 2025-10-13)**
- 🔄 监控是否有遗漏的引用或依赖
- 🔄 确认所有团队成员已了解新功能位置
- 🔄 验证新系统运行稳定
- 🔄 收集用户反馈和问题报告

### **阶段三：安全删除 (1个月后 - 2025-10-13)**
```bash
# 删除旧目录
rm -rf research/pit_data/

# 保留归档文件用于审计
# archive/research_pit_data_backup/ 继续保留
```

### **阶段四：最终清理 (3个月后 - 2025-12-13)**
```bash
# 删除归档文件
rm -rf archive/research_pit_data_backup/
rm -f archive/README_PIT_ARCHIVE.md
```

## ⚠️ 删除前检查清单

### **必需检查**
- [ ] 所有测试文件已更新导入路径
- [ ] pgs_factor集成已更新兼容层
- [ ] 验证脚本确认无需修改
- [ ] 项目文档已更新重构说明
- [ ] 团队成员已通知变更

### **可选检查**
- [ ] Git历史记录确认重要变更已提交
- [ ] 备份文件完整性验证
- [ ] 相关脚本的执行日志检查

## 🔄 回滚计划

如果删除后发现问题，可以从以下途径恢复：

### **方式一：从Git历史恢复**
```bash
# 查看删除前的提交
git log --oneline -n 20

# 恢复到重构前的状态
git checkout [commit-hash] -- research/pit_data/
```

### **方式二：从归档恢复**
```bash
# 从归档目录恢复
cp -r archive/research_pit_data_backup/* research/
```

## 📞 联系和支持

- **负责人**：AI Assistant
- **文档位置**：`archive/README_PIT_ARCHIVE.md`
- **新功能位置**：`scripts/production/data_updaters/pit/`

## 🎯 清理原则

1. **安全第一**：确认无依赖后再删除
2. **可回滚**：保留足够的信息用于回滚
3. **透明化**：记录清理过程和决策
4. **渐进式**：分阶段执行，降低风险

---
*清理计划由AI助手生成，建议在团队会议中讨论确认。*
