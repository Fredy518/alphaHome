# 数据库驱动研究项目模板

这是 AlphaHome 的高级研究项目模板，展示了如何使用 `ResearchContext` 和 `ResearchPipeline` 构建数据库驱动的量化研究流程。

## 📁 文件结构

```
database_research/
├── config.yml                         # 项目配置文件
├── main.py                           # 流水线执行入口
├── README.md                         # 本文件
├── notebooks/
│   └── 01_interactive_analysis.ipynb # 交互式分析笔记本
├── src/
│   ├── __init__.py
│   ├── factors.py                    # 因子计算逻辑
│   └── steps.py                      # 流水线步骤定义
└── output/                           # 输出目录（自动创建）
```

## 🚀 快速开始

### 1. 配置数据库连接

编辑 `config.yml` 文件，修改数据库连接参数：

```yaml
db_manager:
  user: 'your_actual_username'
  password: 'your_actual_password'
  # ... 其他参数
```

### 2. 运行研究流水线

```bash
# 在项目目录下执行
python main.py
```

这将自动：
- 加载指定股票的历史数据
- 计算各种技术因子
- 分析结果并生成报告
- 保存输出到 `output/` 目录

### 3. 交互式探索（可选）

使用 Jupyter Notebook 进行更灵活的数据探索：

```bash
jupyter notebook notebooks/01_interactive_analysis.ipynb
```

## 📋 核心功能

### ResearchContext - 统一接口

- **自动配置加载**: 读取 `config.yml` 并初始化连接
- **数据库访问**: 提供 `db_manager` 实例用于查询
- **批处理支持**: 提供 `planner` 实例用于大规模计算
- **便捷方法**: 内置常用查询方法（获取股票列表、交易日等）

### ResearchPipeline - 流程自动化

- **步骤化执行**: 将研究过程分解为独立步骤
- **数据流管理**: 自动在步骤间传递数据
- **错误处理**: 完善的异常捕获和日志记录
- **执行报告**: 自动生成执行摘要和时间统计

### 内置步骤

1. **LoadStockDataStep**: 从数据库加载股票日线数据
2. **CalculateFactorsStep**: 计算技术因子
   - 移动平均线（MA, EMA）
   - 成交量特征
   - 价格形态特征
   - 技术指标（RSI, MACD, 布林带等）
3. **AnalyzeResultsStep**: 分析计算结果
   - 基础统计
   - 因子相关性
   - 收益率分析
   - 按股票分组统计
4. **SaveResultsStep**: 保存结果
   - CSV文件导出
   - 数据库存储（可选）
   - JSON格式分析报告

## 🛠️ 自定义扩展

### 添加新的因子

在 `src/factors.py` 中添加您的因子计算函数：

```python
def calculate_my_factor(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算自定义因子
    """
    result = df[['ts_code', 'trade_date']].copy()
    
    # 您的因子计算逻辑
    result['my_factor'] = ...
    
    return result
```

### 添加新的流水线步骤

在 `src/steps.py` 中创建新的步骤类：

```python
class MyCustomStep(Step):
    """自定义步骤"""
    
    def run(self, **kwargs) -> Dict[str, Any]:
        # 您的处理逻辑
        return {'output_key': output_data}
```

然后在 `main.py` 中添加到流水线：

```python
steps = [
    LoadStockDataStep(context),
    MyCustomStep(context),  # 新步骤
    # ...
]
```

## 📊 输出说明

运行流水线后，`output/` 目录将包含：

- `factor_data_YYYYMMDD_HHMMSS.csv`: 完整的因子数据
- `analysis_summary_YYYYMMDD_HHMMSS.json`: 分析结果摘要
- `stocks/`: 按股票分别保存的数据文件
- `pipeline_results_YYYYMMDD_HHMMSS.json`: 流水线执行报告

## 使用方法

1. **环境准备**
   - 确保已安装 alphahome 项目的所有依赖
   - 配置好数据库连接（参考项目根目录的 config.example.json）

2. **启动笔记本**
   ```bash
   cd research/templates/database_research/notebooks
   jupyter notebook 01_interactive_analysis.ipynb
   ```

3. **按顺序执行单元格**
   - 环境设置和库导入
   - 数据库连接配置
   - 数据库结构探索
   - 数据查询和分析
   - 数据质量检查
   - 自定义查询
   - 数据导出

## 功能特性

### 🔗 数据库连接
- 自动加载配置文件
- 异步数据库连接管理
- 支持 Jupyter 环境的事件循环处理

### 📊 数据探索
- 自动发现数据库中的所有表
- 表结构和列信息查看
- 数据行数统计
- 示例数据预览

### 🔍 数据质量检查
- 空值统计和百分比
- 数据类型分析
- 自动选择最大的表进行详细分析

### 📝 自定义查询
- 灵活的 SQL 查询接口
- 错误处理和结果展示
- 查询结果的 DataFrame 转换

### 💾 数据导出
- CSV 格式导出
- 自动创建导出目录
- 时间戳文件命名

## 扩展建议

你可以基于这个模板添加以下功能：

- **数据可视化**: 添加 matplotlib/seaborn 图表
- **统计分析**: 描述性统计、相关性分析
- **时间序列分析**: 针对时间序列数据的专门分析
- **机器学习**: 数据预处理和模型训练
- **自动化报告**: 生成 HTML/PDF 报告

## 注意事项

1. **性能考虑**
   - 对大表使用 LIMIT 子句
   - 避免全表扫描查询
   - 合理使用索引

2. **安全考虑**
   - 不要在笔记本中硬编码敏感信息
   - 使用配置文件管理数据库连接
   - 定期清理临时数据

3. **最佳实践**
   - 定期保存重要的分析结果
   - 为复杂查询添加注释
   - 将常用逻辑封装成函数

## 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查配置文件是否正确
   - 确认数据库服务是否运行
   - 验证网络连接

2. **异步代码执行问题**
   - 确保安装了 nest_asyncio
   - 重启 Jupyter 内核

3. **中文字体显示问题**
   - 安装 SimHei 或 Microsoft YaHei 字体
   - 检查 matplotlib 配置

## 贡献

欢迎提交改进建议和功能扩展！