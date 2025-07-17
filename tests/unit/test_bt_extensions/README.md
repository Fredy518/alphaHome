# 测试 `bt_extensions` 模块

此目录用于存放 `bt_extensions` 模块的单元测试和集成测试。

## 测试优先级

1.  **`CacheManager`**:
    -   测试内存缓存的 LRU (最近最少使用) 替换策略是否正确。
    -   测试磁盘缓存的写入、读取和过期清理功能。
    -   测试内存与磁盘混合缓存的命中逻辑。

2.  **`EnhancedAnalyzer`**:
    -   使用固定的、已知的结果集来验证各个性能指标（如夏普比率、最大回撤、Calmar比率等）的计算准确性。

3.  **`BatchDataLoader` / `PostgreSQLDataFeed` 集成**:
    -   测试在 `ParallelBacktestRunner` 的子进程中，数据能否被正确地批量加载并传递给 `PostgreSQLDataFeed`。
    -   测试 `preloaded_data` 机制是否正常工作。

4.  **`ParallelBacktestRunner`**:
    -   集成测试，确保多进程能够正确启动、执行和返回结果。
    -   重点测试边界情况，如股票列表为空、部分股票回测失败等。 