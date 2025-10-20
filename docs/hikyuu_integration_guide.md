# Hikyuu 集成指南

## 概述

本文档介绍如何将 AlphaHome 与 Hikyuu 量化框架集成，实现高效的数据导出、回测和策略分析。

## 架构设计

### 集成路径

我们提供了三种集成路径，适用于不同的使用场景：

#### 路径 A: HDF5 数据管道
- **用途**: 将 AlphaHome 数据导出为 Hikyuu 兼容的 HDF5 文件。
- **特点**: 数据准备与回测分离，支持自动化、增量更新。
- **适用场景**: 搭建自动化的数据同步流程，为原生回测提供数据源。

#### 路径 B: 原生回测
- **用途**: 使用 Hikyuu 原生引擎，读取 HDF5 文件进行回测。
- **特点**: 充分利用 Hikyuu 的 C++ 核心性能，支持大规模、高复杂度回测。
- **适用场景**: 大规模策略回测、参数优化（依赖路径 A 提供的数据）。

#### 路径 C: 内存适配器
- **用途**: 直接在内存中适配数据，计算指标和信号
- **特点**: 低延迟，适合实时计算
- **适用场景**: 实时交易、小规模策略验证

## 安装配置

### 1. 环境要求

```bash
# Python 依赖
pip install hikyuu==2.6.8.5
pip install pandas numpy h5py

# 系统要求
- Windows 10/11 或 Linux
- 至少 8GB 内存（路径 C 需要更多内存）
- 足够的磁盘空间存储 HDF5 数据
```

### 2. 配置文件

在 `config.json` 中添加 Hikyuu 配置：

```json
{
  "backtesting": {
    "hikyuu_data_dir": "E://stock"
  }
}
```

### 3. 数据目录结构

```
E://stock/
├── sh_day.h5          # 上海市场日线数据
├── sz_day.h5          # 深圳市场日线数据
├── bj_day.h5          # 北京市场日线数据
└── weight/            # 权息信息（可选）
    ├── sh_weight.h5
    ├── sz_weight.h5
    └── bj_weight.h5
```

## 使用方法

### 路径 A: HDF5 数据导出

#### 1. 自动化脚本 (推荐)

运行生产脚本来进行自动化、增量的 HDF5 数据导出。

```python
# scripts/production/exporters/hikyuu_day_export.py
python scripts/production/exporters/hikyuu_day_export.py --all-listed --start 20200101 --end 20231231
```

#### 2. 编程式导出 (可选)

您也可以在自己的代码中调用导出器，手动执行导出。

```python
from alphahome.providers.tools.hikyuu_h5_exporter import HikyuuH5Exporter
from alphahome.providers.data_access import AlphaDataTool

# 初始化
data_tool = AlphaDataTool(db_manager)
exporter = HikyuuH5Exporter("E://stock")

# 获取数据
symbols = ["000001.SZ", "600519.SH"]
start_date = "20200101"
end_date = "20231231"

raw_data = data_tool.get_stock_data(symbols, start_date, end_date)
adj_factor = data_tool.get_adj_factor_data(symbols, start_date, end_date)

# 导出数据
exporter.export_day_incremental(raw_data, adj_factor)
```

### 路径 B: 原生回测

此路径假定 HDF5 数据已通过 **路径 A** 准备就绪。

#### 1. Hikyuu 回测

```python
import hikyuu as hk
from hikyuu import *

# 初始化 Hikyuu
hk.hikyuu_init("~/.hikyuu/hikyuu.ini")

# 创建股票对象
stk = Stock("SZ000001")

# 获取K线数据
query_start = Datetime("20200101")
query_end = Datetime("20231231")
k = stk.get_kdata(Query(query_start, query_end))

# 计算指标
ma5 = MA(k, 5)
ma20 = MA(k, 20)

# 创建策略
def my_strategy(k):
    ma5 = MA(k, 5)
    ma20 = MA(k, 20)
    
    # 金叉买入
    c1 = Cross(ma5, ma20)
    # 死叉卖出
    c2 = Cross(ma20, ma5)
    
    return c1, c2

# 回测
sys = System(my_strategy, MM_FixedCount(100))
sys.run(Stock("SZ000001"), Query(query_start, query_end))
```

#### 2. 性能基准测试

```python
# research/projects/hikyuu_integration/benchmark_b.py
python research/projects/hikyuu_integration/benchmark_b.py
```

### 路径 C: 内存适配器

#### 1. 数据适配

```python
from alphahome.providers.tools.hikyuu_data_adapter import HikyuuDataAdapter

# 初始化适配器
adapter = HikyuuDataAdapter()

# 获取数据
df = data_tool.get_stock_data(["000001.SZ"], "20200101", "20231231")

# 转换为 KData
kdata = adapter.create_kdata(df)

# 计算指标
ma5 = adapter.calculate_indicator(kdata, "MA", 5)
ma20 = adapter.calculate_indicator(kdata, "MA", 20)
```

#### 2. 信号生成

```python
from alphahome.providers.tools.hikyuu_data_adapter import HikyuuSignalGenerator

# 初始化信号生成器
signal_gen = HikyuuSignalGenerator()

# 生成移动平均交叉信号
signals = signal_gen.generate_ma_cross_signal(kdata, fast_n=5, slow_n=20)

# 生成 RSI 信号
rsi_signals = signal_gen.generate_rsi_signal(kdata, n=14, oversold=30, overbought=70)
```

#### 3. 性能基准测试

```python
# research/projects/hikyuu_integration/benchmark_c.py
python research/projects/hikyuu_integration/benchmark_c.py
```

## 性能对比

### 基准测试结果

| 路径 | 数据规模 | 处理速度 | 内存使用 | 适用场景 |
|------|----------|----------|----------|----------|
| 路径 B | 5000股票×2年 | 200,000 bars/sec | 固定预加载 | 大规模回测 |
| 路径 C | 50股票×500天 | 335,511 bars/sec | 线性增长 | 实时计算 |

### 性能特征

#### 路径 B (原生回测)
- **优势**: 支持大规模回测，内存使用可控，性能高。
- **劣势**: 依赖路径 A 预先准备的数据。
- **适用**: 大规模策略回测、参数优化

#### 路径 C (内存适配器)
- **优势**: 低延迟，支持实时计算
- **劣势**: 内存使用线性增长，不适合大规模回测
- **适用**: 实时交易、小规模策略验证

## 数据格式

### HDF5 数据结构

```python
# 日线数据结构
{
    'closePrice': uint32,    # 收盘价 * 1000
    'datetime': uint64,      # 日期时间 (yyyymmddHHMM)
    'highPrice': uint32,     # 最高价 * 1000
    'lowPrice': uint32,      # 最低价 * 1000
    'openPrice': uint32,     # 开盘价 * 1000
    'transAmount': uint64,   # 成交金额 * 10
    'transCount': uint64     # 成交笔数
}
```

### 股票代码映射

```python
# AlphaHome -> Hikyuu
"000001.SZ" -> "SZ000001"
"600519.SH" -> "SH600519"
"430047.BJ" -> "BJ430047"
```

## 复权处理

### 前复权计算

```python
# 自动前复权
adj_factor = data_tool.get_adj_factor_data(symbols, start_date, end_date)
exporter.export_day_incremental(raw_data, adj_factor)
```

### 复权因子数据源

- **数据表**: `tushare.stock_adjfactor`
- **字段**: `ts_code`, `trade_date`, `adj_factor`
- **计算方式**: 前复权 (Forward Adjustment)

## 常见问题

### Q1: 为什么选择 HDF5 格式？

**A**: HDF5 格式具有以下优势：
- 高效的数据压缩和存储
- 支持快速随机访问
- 跨平台兼容性好
- Hikyuu 原生支持

### Q2: 如何处理退市股票？

**A**: 默认导出所有股票（包括退市），避免未来信息泄露：
```python
# 获取所有股票（包括退市）
stock_info = data_tool.get_stock_info(active_only=False)
```

### Q3: 内存不足怎么办？

**A**: 推荐使用路径 B 进行大规模回测：
- 路径 B 内存使用可控
- 支持分批处理
- 可分布式扩展

### Q4: 如何优化回测性能？

**A**: 性能优化建议：
- 使用路径 B 进行大规模回测
- 合理设置数据范围
- 避免不必要的指标计算
- 使用 Hikyuu 原生指标

### Q5: 数据同步频率？

**A**: 建议同步频率：
- 日线数据：每日收盘后
- 分钟数据：实时同步（如需要）
- 复权因子：每月更新

## 故障排除

### 1. 数据库连接问题

```python
# 检查数据库配置
from alphahome.common.config_manager import get_database_url
print(get_database_url())
```

### 2. HDF5 文件损坏

```python
# 重新导出数据
python scripts/production/exporters/hikyuu_day_export.py --all-listed --start 20200101 --end 20231231
```

### 3. 内存不足

```python
# 使用路径 B 替代路径 C
# 减少同时处理的股票数量
# 增加系统内存
```

### 4. 指标计算错误

```python
# 检查数据质量
# 验证复权因子
# 确认数据范围
```

## 开发指南

### 1. 添加新指标

```python
# 在 hikyuu_data_adapter.py 中添加
def calculate_custom_indicator(self, kdata, param1, param2):
    # 实现自定义指标计算
    pass
```

### 2. 扩展信号策略

```python
# 在 hikyuu_data_adapter.py 中添加
def generate_custom_signal(self, kdata, **params):
    # 实现自定义信号生成
    pass
```

### 3. 性能优化

```python
# 使用向量化计算
# 避免循环计算
# 合理使用缓存
```

## 更新日志

### v1.0.0 (2025-01-01)
- 初始版本发布
- 支持三种集成路径
- 完整的基准测试
- 详细的文档说明

## 贡献指南

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 创建 Pull Request

## 许可证

本项目采用 MIT 许可证。
