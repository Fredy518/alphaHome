# Hikyuu 集成 FAQ

## 常见问题解答

### 1. 数据相关问题

#### Q: 为什么选择 HDF5 格式而不是 CSV？

**A**: HDF5 格式具有以下优势：
- **性能**: 二进制格式，读写速度比 CSV 快 10-100 倍
- **压缩**: 内置压缩，存储空间节省 50-80%
- **结构**: 支持层次化数据结构，便于组织
- **兼容**: Hikyuu 原生支持，无需格式转换
- **扩展**: 支持大数据集，单文件可达 TB 级别

#### Q: 如何处理股票代码映射？

**A**: 使用内置的映射函数：
```python
from alphahome.providers import map_ts_code_to_hikyuu

# AlphaHome -> Hikyuu
"000001.SZ" -> "SZ000001"
"600519.SH" -> "SH600519"
"430047.BJ" -> "BJ430047"
```

#### Q: 复权因子数据从哪里获取？

**A**: 从 Tushare 数据库获取：
```python
# 数据表: tushare.stock_adjfactor
# 字段: ts_code, trade_date, adj_factor
adj_factor = data_tool.get_adj_factor_data(symbols, start_date, end_date)
```

#### Q: 为什么导出所有股票（包括退市）？

**A**: 避免未来信息泄露：
- 回测时不应该知道哪些股票会退市
- 退市股票的历史数据对回测很重要
- 使用 `active_only=False` 获取所有股票

### 2. 性能相关问题

#### Q: 路径 B 和路径 C 的性能差异？

**A**: 性能对比：
- **路径 B**: 200,000 bars/sec，内存使用可控
- **路径 C**: 335,511 bars/sec，内存使用线性增长
- **路径 B**: 适合大规模回测（>5000股票）
- **路径 C**: 适合实时计算（<1000股票）

#### Q: 如何优化回测性能？

**A**: 优化建议：
1. **数据范围**: 合理设置回测时间范围
2. **指标选择**: 避免不必要的复杂指标
3. **批量处理**: 使用批量计算减少循环
4. **内存管理**: 及时释放不需要的数据
5. **硬件升级**: 增加内存和 SSD 存储

#### Q: 内存不足怎么办？

**A**: 解决方案：
1. **使用路径 B**: 内存使用可控
2. **分批处理**: 减少同时处理的股票数量
3. **数据压缩**: 使用 HDF5 压缩
4. **硬件升级**: 增加系统内存

### 3. 配置相关问题

#### Q: 如何配置 Hikyuu 数据目录？

**A**: 在 `config.json` 中配置：
```json
{
  "backtesting": {
    "hikyuu_data_dir": "E://stock"
  }
}
```

#### Q: 数据库连接失败怎么办？

**A**: 检查步骤：
1. **配置文件**: 确认 `config.json` 中的数据库 URL
2. **网络连接**: 测试数据库连接
3. **权限检查**: 确认数据库用户权限
4. **服务状态**: 确认数据库服务运行

#### Q: Hikyuu 初始化失败？

**A**: 解决方案：
1. **配置文件**: 确认 `~/.hikyuu/hikyuu.ini` 存在
2. **数据目录**: 确认数据目录可写
3. **依赖库**: 确认 Hikyuu 安装完整
4. **权限检查**: 确认文件读写权限

### 4. 回测相关问题

#### Q: 如何创建自定义策略？

**A**: 策略模板：
```python
def my_strategy(k):
    # 计算指标
    ma5 = MA(k, 5)
    ma20 = MA(k, 20)
    
    # 生成信号
    buy_signal = Cross(ma5, ma20)  # 金叉买入
    sell_signal = Cross(ma20, ma5)  # 死叉卖出
    
    return buy_signal, sell_signal

# 使用策略
sys = System(my_strategy, MM_FixedCount(100))
sys.run(Stock("SZ000001"), Query("20200101", "20231231"))
```

#### Q: 如何处理交易成本？

**A**: 使用 Hikyuu 的交易成本模型：
```python
# 固定成本
mm = MM_FixedCount(100)

# 比例成本
mm = MM_FixedPercent(0.001)  # 0.1% 手续费

# 自定义成本
mm = MM_CustomCost(lambda x: x * 0.001)
```

#### Q: 如何计算性能指标？

**A**: 获取回测结果：
```python
# 运行回测
sys.run(Stock("SZ000001"), Query("20200101", "20231231"))

# 获取性能指标
perf = sys.get_performance()
print(f"总收益率: {perf.total_return}")
print(f"年化收益率: {perf.annual_return}")
print(f"最大回撤: {perf.max_drawdown}")
print(f"夏普比率: {perf.sharpe_ratio}")
```

### 5. 开发相关问题

#### Q: 如何添加新的技术指标？

**A**: 在适配器中添加：
```python
def calculate_custom_indicator(self, kdata, param1, param2):
    """计算自定义指标"""
    # 实现指标计算逻辑
    values = []
    for i in range(len(kdata)):
        # 计算指标值
        value = self._calculate_value(kdata, i, param1, param2)
        values.append(value)
    
    return values
```

#### Q: 如何扩展信号策略？

**A**: 添加新的信号生成方法：
```python
def generate_custom_signal(self, kdata, **params):
    """生成自定义信号"""
    # 计算基础指标
    indicator = self.calculate_indicator(kdata, "CUSTOM", **params)
    
    # 生成买卖信号
    buy_signals = []
    sell_signals = []
    
    for i in range(len(indicator)):
        if self._should_buy(indicator, i):
            buy_signals.append(1)
            sell_signals.append(0)
        elif self._should_sell(indicator, i):
            buy_signals.append(0)
            sell_signals.append(1)
        else:
            buy_signals.append(0)
            sell_signals.append(0)
    
    return buy_signals, sell_signals
```

#### Q: 如何调试回测问题？

**A**: 调试步骤：
1. **数据检查**: 确认数据完整性和正确性
2. **指标验证**: 手动验证指标计算结果
3. **信号分析**: 检查买卖信号的合理性
4. **性能分析**: 使用性能分析工具
5. **日志记录**: 添加详细的日志输出

### 6. 故障排除

#### Q: HDF5 文件损坏怎么办？

**A**: 解决方案：
1. **重新导出**: 删除损坏文件，重新导出数据
2. **数据验证**: 检查数据源完整性
3. **备份恢复**: 从备份恢复数据
4. **工具修复**: 使用 HDF5 修复工具

#### Q: 指标计算错误？

**A**: 检查步骤：
1. **数据质量**: 确认输入数据正确
2. **参数设置**: 检查指标参数
3. **边界条件**: 处理数据边界情况
4. **数值精度**: 注意浮点数精度问题

#### Q: 内存泄漏问题？

**A**: 预防措施：
1. **及时释放**: 使用 `del` 释放大对象
2. **循环引用**: 避免循环引用
3. **缓存清理**: 定期清理缓存
4. **内存监控**: 使用内存监控工具

### 7. 最佳实践

#### Q: 数据同步的最佳实践？

**A**: 建议：
1. **增量更新**: 只同步新增和变更数据
2. **错误处理**: 完善的错误处理和重试机制
3. **数据验证**: 同步后验证数据完整性
4. **备份策略**: 定期备份重要数据

#### Q: 回测的最佳实践？

**A**: 建议：
1. **数据准备**: 确保数据质量和完整性
2. **策略设计**: 避免未来信息泄露
3. **参数优化**: 使用合理的参数范围
4. **结果分析**: 深入分析回测结果

#### Q: 性能优化的最佳实践？

**A**: 建议：
1. **选择合适的路径**: 根据场景选择路径 B 或 C
2. **批量处理**: 减少循环，使用向量化计算
3. **内存管理**: 合理使用内存，及时释放
4. **硬件优化**: 使用 SSD 和充足内存

### 8. 版本兼容性

#### Q: Hikyuu 版本兼容性？

**A**: 支持版本：
- **推荐**: Hikyuu 2.6.8.5
- **最低**: Hikyuu 2.6.0
- **测试**: Windows 10/11, Linux Ubuntu 20.04+

#### Q: Python 版本要求？

**A**: 版本要求：
- **推荐**: Python 3.8+
- **最低**: Python 3.7
- **测试**: Python 3.8, 3.9, 3.10, 3.11

#### Q: 依赖库版本？

**A**: 依赖版本：
```txt
hikyuu==2.6.8.5
pandas>=1.3.0
numpy>=1.20.0
h5py>=3.0.0
```

### 9. 技术支持

#### Q: 如何获取技术支持？

**A**: 支持渠道：
1. **文档**: 查看完整文档和 FAQ
2. **示例**: 参考示例代码
3. **社区**: 参与社区讨论
4. **问题反馈**: 提交 Issue 和 Bug 报告

#### Q: 如何贡献代码？

**A**: 贡献流程：
1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 创建 Pull Request
5. 代码审查
6. 合并代码

#### Q: 如何报告问题？

**A**: 报告要求：
1. **环境信息**: 操作系统、Python 版本、依赖版本
2. **问题描述**: 详细描述问题和复现步骤
3. **错误信息**: 完整的错误堆栈信息
4. **相关代码**: 最小复现代码
5. **期望结果**: 期望的正确行为

## 总结

本 FAQ 涵盖了 Hikyuu 集成中的常见问题和解决方案。如果您遇到其他问题，请参考完整文档或提交 Issue。

记住：选择合适的集成路径、合理配置系统、遵循最佳实践，可以大大提高集成的成功率和性能。
