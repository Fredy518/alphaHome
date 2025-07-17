# 🚀 Database Research 快速入门指南

## 1. 三步快速开始

### 第1步：配置数据库连接
编辑 `config.yml`，只需修改以下几行：
```yaml
db_manager:
  user: 'your_username'      # 改成您的数据库用户名
  password: 'your_password'   # 改成您的数据库密码
```

### 第2步：运行研究流水线
```bash
python main.py
```

### 第3步：查看结果
打开 `output/` 目录，查看生成的：
- 因子数据 CSV 文件
- 分析报告 JSON 文件
- 流水线执行日志

## 2. 常用自定义示例

### 修改股票列表
在 `main.py` 中找到 `research_params`，修改股票列表：
```python
'stock_list': [
    '您的股票代码1.SZ',
    '您的股票代码2.SH',
    # 添加更多...
]
```

### 修改时间范围
```python
'start_date': '2023-01-01',  # 修改开始日期
'end_date': '2023-12-31',    # 修改结束日期
```

### 添加新因子
在 `src/factors.py` 的 `calculate_custom_factors` 函数中添加：
```python
# 示例：计算20日收益率
result['return_20'] = df['close'].pct_change(20) * 100

# 示例：计算成交额移动平均
result['amount_ma_10'] = df['amount'].rolling(10).mean()
```

## 3. 快速调试技巧

### 只运行部分步骤
注释掉不需要的步骤：
```python
steps = [
    LoadStockDataStep(context),      # 必需
    CalculateFactorsStep(context),   # 可选
    # AnalyzeResultsStep(context),   # 注释掉跳过
    # SaveResultsStep(context)       # 注释掉跳过
]
```

### 测试单只股票
```python
'stock_list': ['000001.SZ'],  # 只测试一只股票
'start_date': '2024-01-01',
'end_date': '2024-01-07',     # 只测试一周数据
```

### 查看中间结果
在任意步骤的 `run` 方法中添加打印：
```python
def run(self, **kwargs):
    # 打印输入数据
    print("输入数据keys:", kwargs.keys())
    
    # 您的处理逻辑...
    
    # 打印输出数据
    print("输出数据shape:", result.shape)
    return {'output': result}
```

## 4. 常见问题快速解决

### 问题1：ImportError
**解决**：确保在项目根目录运行，或检查 `sys.path` 设置

### 问题2：数据库连接失败
**解决**：
1. 检查数据库服务是否启动
2. 验证用户名密码是否正确
3. 确认防火墙/网络设置

### 问题3：查询无数据
**解决**：
1. 检查表名是否正确（如 `stock_daily`）
2. 验证股票代码格式（如 `000001.SZ`）
3. 确认日期范围内有数据

## 5. 进阶技巧

### 并行处理多只股票
使用 `BatchPlanner`：
```python
# 在 context 中访问 planner
planner = context.planner
batches = planner.create_stock_batches(stock_list, batch_size=10)
```

### 缓存中间结果
```python
# 保存中间结果
factor_data.to_pickle('cache/factors.pkl')

# 下次直接加载
factor_data = pd.read_pickle('cache/factors.pkl')
```

### 自定义日志级别
```python
import logging
logging.getLogger('src.steps').setLevel(logging.DEBUG)
```

## 6. 有用的代码片段

### 获取所有A股列表
```python
# 使用providers数据提供层获取股票列表（推荐）
all_stocks = context.get_stock_list()
# 或只获取主板
main_board = context.get_stock_list(market='主板')

# 直接使用data_tool
stock_info = context.data_tool.get_stock_info(list_status='L')
all_stocks = stock_info['ts_code'].tolist()
```

### 获取股票行情数据
```python
# 使用providers获取股票数据（替代直接SQL查询）
stock_data = context.get_stock_data(
    symbols=['000001.SZ', '000002.SZ'],
    start_date='2024-01-01',
    end_date='2024-01-31',
    adjust=True  # 使用复权价格
)

# 获取指数权重数据
index_weights = context.get_index_weights(
    index_code='000300.SH',
    start_date='2024-01-01',
    end_date='2024-01-31',
    monthly=True  # 只获取月末数据
)

# 获取行业分类数据
industry_data = context.get_industry_data(
    symbols=['000001.SZ', '000002.SZ'],
    level='sw_l1'  # 申万一级行业
)
```

### 批量计算因子并保存
```python
# 按月份批量处理
for month in pd.date_range('2023-01', '2023-12', freq='M'):
    start = month.strftime('%Y-%m-01')
    end = month.strftime('%Y-%m-%d')
    
    params = {
        'stock_list': stock_list,
        'start_date': start,
        'end_date': end,
        'output_dir': f'output/{month.strftime("%Y%m")}'
    }
    
    pipeline.run(params)
```

## 7. 下一步

1. 查看 `example_usage.py` 了解更多用法
2. 阅读完整的 `README.md` 了解架构设计
3. 探索 `src/factors.py` 学习因子计算方法
4. 使用 Jupyter Notebook 进行交互式分析

---

💡 **提示**：遇到问题时，先查看日志输出，通常能找到问题原因。

📧 **需要帮助**？查看项目文档或提交 Issue。
