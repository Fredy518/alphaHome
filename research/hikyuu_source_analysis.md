# Hikyuu 源码分析报告

## 基于源码的数据结构分析

### 1. HDF5 数据结构定义

#### C++ 结构体定义（H5Record.h）
```cpp
struct H5Record {
    uint64_t datetime;      // 日期时间
    uint32_t openPrice;    // 开盘价
    uint32_t highPrice;    // 最高价
    uint32_t lowPrice;     // 最低价
    uint32_t closePrice;   // 收盘价
    uint64_t transAmount;  // 成交金额
    uint64_t transCount;   // 成交笔数
};
```

#### Python 结构定义（common_h5.py）
```python
class H5Record(tb.IsDescription):
    """HDF5基础K线数据格式（日线、分钟线、5分钟线"""
    datetime = tb.UInt64Col()      # 日期时间
    openPrice = tb.UInt32Col()     # 开盘价
    highPrice = tb.UInt32Col()     # 最高价
    lowPrice = tb.UInt32Col()      # 最低价
    closePrice = tb.UInt32Col()    # 收盘价
    transAmount = tb.UInt64Col()   # 成交金额
    transCount = tb.UInt64Col()    # 成交笔数
```

### 2. 价格缩放因子

#### C++ 代码中的缩放处理（H5KDataDriver.cpp:617-622）
```cpp
record.openPrice = price_t(pBuf[i].openPrice) * 0.001;
record.highPrice = price_t(pBuf[i].highPrice) * 0.001;
record.lowPrice = price_t(pBuf[i].lowPrice) * 0.001;
record.closePrice = price_t(pBuf[i].closePrice) * 0.001;
record.transAmount = price_t(pBuf[i].transAmount) * 0.1;
record.transCount = price_t(pBuf[i].transCount);
```

**关键发现**：
- **价格字段缩放因子**: `0.001` (即除以 1000)
- **成交金额缩放因子**: `0.1` (即除以 10)
- **成交笔数**: 无缩放

### 3. HDF5 文件组织结构

#### 文件命名规则
```
{market}_{ktype}.h5
例如: sh_day.h5, sz_day.h5, bj_day.h5
```

#### 内部组结构
```
/data/{MARKET}{CODE}     # 日线数据
/week/{MARKET}{CODE}     # 周线索引
/month/{MARKET}{CODE}    # 月线索引
/quarter/{MARKET}{CODE}  # 季线索引
/halfyear/{MARKET}{CODE} # 半年线索引
/year/{MARKET}{CODE}     # 年线索引
```

#### 数据集命名规则
```
{MARKET}{CODE}
例如: SZ000001, SH000001
```

### 4. 权息数据结构

#### StockWeight 类定义
```cpp
class StockWeight {
public:
    Datetime datetime;         // 权息日期
    price_t countAsGift;      // 每10股送X股
    price_t countForSell;     // 每10股配X股
    price_t priceForSell;     // 配股价
    price_t bonus;            // 每10股红利
    price_t increasement;     // 每10股转增X股
    price_t totalCount;       // 总股本（万股）
    price_t freeCount;        // 流通股（万股）
    price_t suogu;           // 扩缩股比例
};
```

#### 数据库表结构（StockWeightTable.h）
```cpp
class StockWeightTable {
    uint64_t stockid;         // 股票ID
    uint64_t date;            // 日期
    double countAsGift;        // 每10股送X股
    double countForSell;       // 每10股配X股
    double priceForSell;       // 配股价
    double bonus;              // 每10股红利
    double countOfIncreasement; // 每10股转增X股
    double totalCount;         // 总股本
    double freeCount;          // 流通股
    double suogu;              // 扩缩股比例
};
```

### 5. 数据驱动配置

#### H5KDataDriver 初始化
```cpp
// 日线数据文件映射
if (ktype == KQuery::getKTypeName(KQuery::DAY)) {
    H5FilePtr h5file(new H5::H5File(filename, H5F_ACC_RDONLY), Hdf5FileCloser());
    m_h5file_map[market + "_DAY"] = h5file;
    m_h5file_map[market + "_WEEK"] = h5file;
    m_h5file_map[market + "_MONTH"] = h5file;
    m_h5file_map[market + "_QUARTER"] = h5file;
    m_h5file_map[market + "_HALFYEAR"] = h5file;
    m_h5file_map[market + "_YEAR"] = h5file;
}
```

### 6. 复权处理机制

#### 关键发现
1. **不复权因子存储在 HDF5 中**: 复权因子不存储在 HDF5 文件中
2. **权息信息存储在数据库中**: 通过 `StockWeight` 表存储
3. **动态复权计算**: 在读取 K 线数据时根据权息信息动态计算复权价格

#### 复权计算逻辑
```cpp
// 在 KData 读取时，根据 recover_type 参数决定是否复权
// recover_type 可以是：
// - Query.NONE: 不复权
// - Query.FORWARD: 前复权
// - Query.BACKWARD: 后复权
```

### 7. 数据导出策略

#### 基于源码分析的最佳实践

1. **导出前复权数据**
   - 在导出时计算前复权价格
   - 避免回测时的未来信息泄露
   - 确保价格连续性

2. **数据格式转换**
   ```python
   # 价格字段需要乘以 1000 转为整数存储
   price_int = int(price * 1000)
   
   # 成交金额需要乘以 10 转为整数存储
   amount_int = int(amount * 10)
   
   # 日期格式: YYYY-MM-DD -> yyyymmdd0000
   datetime_int = int(date.strftime('%Y%m%d') + '0000')
   ```

3. **HDF5 文件结构**
   ```python
   # 创建 HDF5 文件
   h5file = tb.open_file(filename, "a", filters=tb.Filters(complevel=9, complib='zlib', shuffle=True))
   
   # 创建数据组
   group = h5file.create_group("/", "data")
   
   # 创建数据集
   table = h5file.create_table(group, tablename, H5Record)
   ```

### 8. 实现建议

#### 更新 HikyuuH5Exporter
```python
class HikyuuH5Exporter:
    """基于源码分析的 Hikyuu HDF5 导出器"""
    
    # 价格缩放因子（基于源码分析）
    PRICE_SCALE = 1000      # 价格 * 1000 转为整数
    AMOUNT_SCALE = 10       # 成交金额 * 10 转为整数
    
    # HDF5 数据类型定义
    DTYPE = np.dtype([
        ('datetime', '<u8'),      # uint64
        ('openPrice', '<u4'),     # uint32
        ('highPrice', '<u4'),     # uint32
        ('lowPrice', '<u4'),      # uint32
        ('closePrice', '<u4'),    # uint32
        ('transAmount', '<u8'),    # uint64
        ('transCount', '<u8')      # uint64
    ])
    
    def export_day_incremental(self, df: pd.DataFrame) -> None:
        """增量导出日频数据"""
        # 1. 计算前复权价格
        adj_df = self.calculate_forward_adj(df)
        
        # 2. 转换为 Hikyuu 格式
        h5_data = self.convert_to_h5_format(adj_df)
        
        # 3. 写入 HDF5 文件
        self.write_to_h5(h5_data)
    
    def calculate_forward_adj(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算前复权价格"""
        # 实现前复权逻辑
        pass
    
    def convert_to_h5_format(self, df: pd.DataFrame) -> np.ndarray:
        """转换为 HDF5 格式"""
        records = []
        for _, row in df.iterrows():
            record = (
                int(row['trade_date'].strftime('%Y%m%d') + '0000'),  # datetime
                int(row['open'] * self.PRICE_SCALE),                 # openPrice
                int(row['high'] * self.PRICE_SCALE),                 # highPrice
                int(row['low'] * self.PRICE_SCALE),                  # lowPrice
                int(row['close'] * self.PRICE_SCALE),                # closePrice
                int(row['amount'] * self.AMOUNT_SCALE),              # transAmount
                int(row['vol'])                                      # transCount
            )
            records.append(record)
        
        return np.array(records, dtype=self.DTYPE)
```

### 9. 总结

#### 关键发现
1. **价格缩放因子**: 价格字段使用 `1000` 倍缩放，成交金额使用 `10` 倍缩放
2. **数据结构**: 严格按照 `H5Record` 结构定义，字段顺序不能改变
3. **复权处理**: 不复权因子存储在 HDF5 中，权息信息存储在数据库中
4. **文件组织**: 按市场分组，每个股票一个数据集
5. **索引支持**: 提供周、月、季、半年、年等时间维度的索引

#### 实现要点
1. **导出前复权数据**: 避免回测时的未来信息泄露
2. **正确的缩放因子**: 使用源码中确定的缩放因子
3. **严格的数据格式**: 确保字段顺序和数据类型正确
4. **增量更新**: 支持增量数据更新，避免重复导出

这个分析为 AlphaHome 与 Hikyuu 的集成提供了准确的技术基础。