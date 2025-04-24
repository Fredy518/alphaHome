# 自动金融数据库系统

自动金融数据库系统是一个高效的工具集，用于自动化处理金融数据获取、数据清洗、存储和验证。支持多种金融数据源，灵活的更新策略，以及全面的数据质量检查。

## 特性

- **异步数据获取**：使用Python异步编程提高数据获取效率
- **声明式任务定义**：通过简单的配置即可定义新的数据任务
- **自动数据处理**：支持自动数据清洗、转换和验证
- **灵活更新模式**：支持全量更新、增量更新和指定日期更新
- **数据质量检查**：自动检测数据缺失、异常和一致性问题
- **多数据源支持**：目前支持Tushare，可扩展支持更多数据源
- **可视化报告**：生成数据质量和覆盖度的可视化报告

## 环境配置

### 系统要求

- Python 3.8+
- PostgreSQL 12+
- 必要的Python库（详见`requirements.txt`）

### 配置

1. 克隆项目
```bash
git clone https://github.com/yourusername/autoDatabase.git
cd autoDatabase
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 设置环境变量

创建`.env`文件，包含以下配置：
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=auto_finance
DB_USER=your_username
DB_PASSWORD=your_password

# Tushare API
TUSHARE_TOKEN=your_tushare_token
```

4. 初始化数据库
```bash
python scripts/init_database.py
```

## 使用方法

系统提供多种命令行工具用于数据管理和质量控制。

### 命令行工具

#### 1. 股票数据更新

更新股票日线数据：
```bash
python scripts/tushare_stock_daily_updater.py

# 可用选项：
# --auto          自动确定更新时间范围（默认）
# --start_date    指定开始日期，格式：YYYYMMDD
# --end_date      指定结束日期，格式：YYYYMMDD
# --full          执行全量更新（从1990年开始）
```

更新股票基本面数据：
```bash
python scripts/tushare_stock_dailybasic_updater.py

# 选项与股票日线数据更新相同
```

#### 2. 基金数据更新

更新基金净值数据：
```bash
python scripts/tushare_fund_nav_updater.py

# 可用选项：
# --auto          自动确定更新时间范围（默认）
# --start_date    指定开始日期，格式：YYYYMMDD
# --end_date      指定结束日期，格式：YYYYMMDD
# --full          执行全量更新
```

#### 3. 指数数据更新

更新指数日线数据：
```bash
python scripts/tushare_index_daily_updater.py

# 可用选项：
# --auto          自动确定更新时间范围（默认）
# --start_date    指定开始日期，格式：YYYYMMDD
# --end_date      指定结束日期，格式：YYYYMMDD
# --index_code    指定指数代码（可选）
# --full          执行全量更新
```

#### 4. 数据库质量检查工具

运行全面的数据库表质量检查：
```bash
python scripts/check_db_quality.py

# 可用选项：
# -t, --tables       指定要检查的表名列表，用空格分隔多个表名
# -s, --start_date   指定开始日期，格式：YYYYMMDD
# -e, --end_date     指定结束日期，格式：YYYYMMDD
# -o, --output_dir   指定输出目录路径
# -v, --verbose      显示详细日志
# -h, --help         显示帮助信息
```

功能特点：
- 自动检测数据库中的所有表并进行质量分析
- 检查各表的日期覆盖情况并生成直观的日期覆盖率图表
- 分析数据完整性，检测空值和异常值
- 生成全面的HTML报告，包含图表和详细的问题描述
- 支持导出JSON格式的详细结果和摘要信息

使用示例：

检查特定表：
```bash
python scripts/check_db_quality.py -t stock_daily fund_nav
```

检查特定日期范围：
```bash
python scripts/check_db_quality.py -s 20230101 -e 20231231
```

指定输出目录：
```bash
python scripts/check_db_quality.py -o logs/custom_quality_check
```

### 批量处理脚本

#### 每日数据更新

执行所有数据类型的日常更新：
```bash
python scripts/batch/daily_update.py
```

#### 周期性全面更新

执行全面的数据更新和质量检查：
```bash
python scripts/batch/weekly_full_update.py
```

## 项目结构

```
autoDatabase/
├── data_module/           # 核心数据处理模块
│   ├── sources/           # 数据源适配器
│   │   └── tushare/       # Tushare数据源
│   ├── tasks/             # 任务定义
│   │   ├── stock/         # 股票相关任务
│   │   ├── fund/          # 基金相关任务
│   │   └── index/         # 指数相关任务
│   └── tools/             # 工具函数
├── scripts/               # 命令行脚本
│   ├── batch/             # 批处理脚本
│   └── tasks/             # 任务执行脚本
├── logs/                  # 日志文件
│   └── db_quality_*/      # 数据质量检查结果和图表
├── docs/                  # 文档
├── examples/              # 示例代码
└── .env                   # 环境变量配置
```

## 表名对应关系

系统中的表名与实际数据内容的对应关系如下：

| 表名 | 数据内容 | 来源 |
|------|----------|------|
| tushare_stock_daily | 股票日线行情数据 | Tushare |
| tushare_stock_dailybasic | 股票每日基本面指标 | Tushare |
| tushare_index_daily | 指数日线行情数据 | Tushare |
| tushare_fund_nav | 基金净值数据 | Tushare |
| tushare_fund_daily | 场内基金日线数据 | Tushare |
| tushare_stock_income | 股票利润表 | Tushare |
| tushare_stock_balancesheet | 股票资产负债表 | Tushare |
| tushare_stock_cashflow | 股票现金流量表 | Tushare |

每个任务的`table_name`参数对应的就是数据库表名。

## 扩展和自定义

查看[开发者指南](docs/developer_guide.md)了解如何扩展系统功能，包括：

- 添加新的数据源
- 创建自定义任务
- 修改数据处理逻辑
- 开发新的分析工具

## 数据质量管理

### 数据质量工具

项目提供了全面的数据质量管理工具，用于检查和维护数据的完整性与准确性：

1. **数据库表质量全面检查工具** (`scripts/check_db_quality.py`)：
   - 全表自动探测和分析
   - 空值分析和统计
   - 日期覆盖率分析
   - 按月份的数据覆盖分析
   - 自动问题分类和标记
   - 图表可视化报告
   - 灵活的配置选项
   
   使用示例：
   ```bash
   # 检查整个数据库的表质量
   python scripts/check_db_quality.py
   
   # 检查特定表的数据质量
   python scripts/check_db_quality.py -t tushare_stock_daily tushare_fund_daily
   
   # 检查特定日期范围的数据质量
   python scripts/check_db_quality.py -s 20230101 -e 20231231
   
   # 指定输出目录
   python scripts/check_db_quality.py -o logs/custom_quality_check
   
   # 启用详细日志输出
   python scripts/check_db_quality.py -v
   ```

2. **股票日线数据质量检查工具** (`scripts/tools/check_stock_daily_quality.py`)：
   - 专门针对股票日线数据的质量检查
   - 分析数据间隔和缺失情况
   - 生成详细的问题报告

   使用示例：
   ```bash
   python scripts/tools/check_stock_daily_quality.py --start-date 20230101 --end-date 20230331
   ```

### 质量报告查看

质量检查完成后，会在指定目录（默认为 `logs/db_quality_时间戳/`）生成以下报告：

- `report.html`: 包含所有质量指标的综合HTML报告
- `charts/`: 包含数据覆盖率和完整性的可视化图表
- `results.json`: 包含所有质量检查的详细结果数据

通过Web浏览器打开HTML报告可查看所有质量分析结果和建议的修复措施。

## 常见问题

参考[用户指南](docs/user_guide.md)的常见问题解答部分，或提交Issue获取帮助。