# 生产环境脚本

本目录包含用于生产环境运行的脚本，与分析和调试脚本分离。

## 📁 目录结构

```
scripts/production/
├── README.md                                           # 本文件
├── start_parallel_g_factor_calculation.py             # G因子年度并行计算启动器 (Python版本)
├── start_parallel_g_factor_calculation.bat            # G因子年度并行计算启动器 (批处理版本)
├── start_parallel_g_factor_calculation_quarterly.py   # G因子季度并行计算启动器 (Python版本)
├── start_parallel_g_factor_calculation_quarterly.bat  # G因子季度并行计算启动器 (批处理版本)
├── g_factor_parallel_by_year.py                       # G因子年度并行计算脚本
└── g_factor_parallel_by_quarter.py                    # G因子季度并行计算脚本
```

## 🚀 使用方法

### 年度并行计算

#### Python启动器（推荐）

```bash
# 基本用法
python scripts/production/start_parallel_g_factor_calculation.py --start_year 2020 --end_year 2024 --workers 5

# 参数说明
--start_year    开始年份 (默认: 2020)
--end_year      结束年份 (默认: 2024)  
--workers       工作进程数 (默认: 10，会自动调整为不超过年份数)
--delay         进程启动间隔秒数 (默认: 2)
```

#### 批处理启动器

```bash
# Windows系统
scripts\production\start_parallel_g_factor_calculation.bat
```

### 季度并行计算（新增）

#### Python启动器（推荐）

```bash
# 基本用法
python scripts/production/start_parallel_g_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16

# 参数说明
--start_year    开始年份 (默认: 2020)
--end_year      结束年份 (默认: 2024)  
--workers       工作进程数 (默认: 16，会自动调整为不超过季度数)
--delay         进程启动间隔秒数 (默认: 2)
```

#### 批处理启动器

```bash
# Windows系统
scripts\production\start_parallel_g_factor_calculation_quarterly.bat 2020 2024 16
```

## 📊 功能说明

### G因子并行计算系统

#### 年度并行计算
- **并行策略**: "土法"并行 - 多终端年度并行计算
- **数据一致性**: 100%保证，使用原始计算逻辑
- **性能提升**: 理论加速比 = 工作进程数
- **监控方式**: 每个终端窗口显示一个工作进程的进度

#### 季度并行计算（新增）
- **并行策略**: "土法"并行 - 多终端季度并行计算
- **粒度更细**: 按季度分割，适合大规模计算
- **负载均衡**: 智能季度分配算法，平衡各进程计算量
- **灵活配置**: 支持自定义工作进程数和季度范围

### 脚本说明

#### 年度并行脚本
1. **start_parallel_g_factor_calculation.py**
   - 智能启动器，自动调整工作进程数
   - 支持跨平台（Windows/Linux/Mac）
   - 提供详细的性能预期和监控说明

2. **start_parallel_g_factor_calculation.bat**
   - Windows批处理版本
   - 简化配置，避免中文字符问题
   - 适合快速启动

3. **g_factor_parallel_by_year.py**
   - 核心计算脚本
   - 基于原始G因子计算逻辑
   - 支持年度范围计算

#### 季度并行脚本（新增）
4. **start_parallel_g_factor_calculation_quarterly.py**
   - 季度并行启动器，支持按季度分配计算任务
   - 智能季度分配算法，按时间顺序轮询分配
   - 自动调整工作进程数，避免资源浪费

5. **start_parallel_g_factor_calculation_quarterly.bat**
   - Windows批处理版本
   - 支持命令行参数传递
   - 环境检查和错误处理

6. **g_factor_parallel_by_quarter.py**
   - 季度计算脚本，支持单个或多个季度计算
   - 基于原始G因子计算逻辑
   - 支持季度日期范围自动计算

## ⚠️ 注意事项

1. **数据库连接**: 监控数据库连接数，避免连接池耗尽
2. **磁盘空间**: 定期检查磁盘空间，确保有足够存储空间
3. **进程管理**: 可以随时关闭单个终端窗口来停止对应进程
4. **路径依赖**: 所有脚本需要在项目根目录下运行
5. **季度并行**: 季度并行比年度并行粒度更细，适合大规模计算
6. **资源分配**: 建议根据系统资源合理设置工作进程数

## 🔧 维护说明

- 生产环境脚本与分析和调试脚本分离
- 路径引用已更新为相对路径
- 支持智能工作进程数调整
- 避免中文字符导致的Windows路径问题
- 新增季度并行计算支持，提供更细粒度的并行控制

## 📈 性能对比

| 并行方式 | 粒度 | 适用场景 | 理论加速比 | 资源消耗 |
|---------|------|----------|------------|----------|
| 年度并行 | 年 | 中等规模计算 | 工作进程数 | 中等 |
| 季度并行 | 季度 | 大规模计算 | 工作进程数 | 较高 |

**建议**：
- 小规模计算（<5年）：使用年度并行
- 大规模计算（≥5年）：使用季度并行
- 根据系统资源调整工作进程数
