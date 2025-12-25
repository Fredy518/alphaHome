# Excel 数据源使用指南

## 概述

Excel 数据源适配器用于从本地 Excel 文件读取数据并保存到数据库，适用于补充难以从常规数据源获取的数据。

## 架构设计

### 核心组件

1. **ExcelTask** (`alphahome/fetchers/sources/excel/excel_task.py`)
   - Excel 数据源的抽象基类
   - 提供文件读取、数据处理的通用框架
   - 继承自 `FetcherTask`

2. **具体任务实现** (如 `ExcelFundAnalysisOutlookTask`)
   - 定义具体的 Excel 文件路径和配置
   - 定义数据表结构
   - 实现数据处理逻辑

### 特点

- ✅ 无需 API 调用，直接读取本地文件
- ✅ 支持单一批次模式（通常 Excel 文件作为整体处理）
- ✅ 自动处理数据清洗和转换
- ✅ 支持列名映射
- ✅ 支持日期解析
- ✅ 完整的数据验证框架

## 快速开始

### 1. 创建新的 Excel 数据采集任务

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from alphahome.fetchers.sources.excel import ExcelTask
from alphahome.common.task_system.task_decorator import task_register

@task_register()
class ExcelYourDataTask(ExcelTask):
    """从 Excel 读取您的数据"""
    
    # 任务标识
    name = "excel_your_data"
    description = "您的数据描述"
    
    # 数据表配置
    table_name = "your_table_name"
    schema_name = "excel"  # 或其他 schema
    primary_keys = ["key1", "key2"]
    date_column = "date_field"
    data_source = "excel"
    domain = "your_domain"  # stock/fund/macro 等
    
    # Excel 文件配置
    excel_file_path = r"E:\path\to\your\file.xlsx"
    sheet_name = 0  # sheet 索引或名称
    header_row = 0  # 表头所在行
    
    # 日期列配置（需要解析为日期的列）
    date_columns = ["date1", "date2"]
    
    # 列名映射（可选，Excel列名 -> 数据库列名）
    column_mapping = {
        "Excel列名": "数据库列名",
    }
    
    # 数据表结构定义
    schema_def = {
        "columns": [
            {"name": "column1", "type": "VARCHAR(20)", "nullable": False, "comment": "说明"},
            {"name": "column2", "type": "INTEGER", "nullable": True, "comment": "说明"},
            {"name": "column3", "type": "TEXT", "nullable": True, "comment": "说明"},
        ],
        "indexes": [
            {"name": "idx_column1", "columns": ["column1"]},
        ],
        "comment": "表说明"
    }
    
    # 数据验证规则（可选）
    validation_rules = [
        {
            "name": "验证规则说明",
            "condition": lambda df: df["column1"].notna(),
        },
    ]
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """自定义数据处理逻辑"""
        # 调用父类的基础处理
        data = super().process_data(data)
        
        # 您的自定义处理
        # ...
        
        return data
```

### 2. 注册任务到模块

在对应的 `__init__.py` 中添加导入：

```python
from .excel_your_data import ExcelYourDataTask

__all__ = [
    # ... 其他任务
    "ExcelYourDataTask",
]
```

### 3. 使用任务

```python
from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import ConfigManager
from alphahome.fetchers.tasks.your_module import ExcelYourDataTask

# 创建数据库连接
config_manager = ConfigManager()
db_url = config_manager.get_database_url()
db_manager = DBManager(connection_string=db_url, mode="async")
await db_manager.connect()

# 创建并执行任务
task = ExcelYourDataTask(db_connection=db_manager, update_type="full")
result = await task.execute()

await db_manager.close()
```

## 配置项说明

### 必需配置

| 配置项 | 类型 | 说明 |
|-------|------|------|
| `name` | str | 任务唯一标识 |
| `table_name` | str | 数据库表名 |
| `excel_file_path` | str | Excel 文件完整路径 |

### 可选配置

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|--------|------|
| `schema_name` | str | None | 数据库 schema 名称 |
| `sheet_name` | str/int | 0 | Sheet 名称或索引 |
| `header_row` | int | 0 | 表头所在行号（0-based） |
| `skip_rows` | List[int] | None | 要跳过的行 |
| `use_cols` | List[str] | None | 要读取的列 |
| `column_mapping` | Dict | None | 列名映射字典 |
| `date_columns` | List[str] | None | 日期列列表 |
| `primary_keys` | List[str] | [] | 主键列表 |
| `date_column` | str | None | 日期字段 |
| `schema_def` | Dict | None | 表结构定义 |

## 数据表结构定义

`schema_def` 定义数据表的结构，系统会自动创建表：

```python
schema_def = {
    "columns": [
        {
            "name": "列名",
            "type": "数据类型",  # VARCHAR(n), INTEGER, BIGINT, FLOAT, TEXT, DATE 等
            "nullable": True/False,
            "comment": "列说明"
        },
    ],
    "indexes": [
        {
            "name": "索引名",
            "columns": ["列1", "列2"]  # 单列或多列索引
        },
    ],
    "comment": "表说明"
}
```

### 常用数据类型

- `VARCHAR(n)` - 变长字符串，如 `VARCHAR(20)`
- `TEXT` - 长文本
- `INTEGER` - 整数
- `BIGINT` - 大整数
- `FLOAT` - 浮点数
- `DECIMAL(p,s)` - 精确小数，如 `DECIMAL(10,2)`
- `DATE` - 日期
- `TIMESTAMP` - 时间戳

## 数据处理流程

1. **读取 Excel** - `fetch_batch()` 方法使用 pandas 读取文件
2. **基础清洗** - `process_data()` 父类方法：
   - 去除空行
   - 去除字符串前后空格
3. **自定义处理** - 子类可重写 `process_data()` 进行：
   - 日期格式转换
   - 数据类型转换
   - 业务逻辑处理
4. **数据验证** - 根据 `validation_rules` 验证数据
5. **保存数据库** - 根据 `primary_keys` 执行 upsert 操作

## 最佳实践

### 1. 文件路径

使用原始字符串（r"..."）避免转义问题：

```python
excel_file_path = r"E:\stock\Excel\your_file.xlsx"
```

### 2. 日期处理

定义 `date_columns` 自动解析日期，然后在 `process_data` 中格式化：

```python
date_columns = ["trade_date", "ann_date"]

def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
    data = super().process_data(data)
    
    # 转换为 YYYYMMDD 格式
    for col in self.date_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], errors='coerce').dt.strftime('%Y%m%d')
    
    return data
```

### 3. 文本处理

对于长文本字段，清理多余空白字符：

```python
text_columns = ['description', 'comment']
for col in text_columns:
    if col in data.columns:
        data[col] = data[col].apply(
            lambda x: ' '.join(str(x).split()) if pd.notna(x) else None
        )
```

### 4. 数据验证

定义详细的验证规则，确保数据质量：

```python
validation_rules = [
    {
        "name": "代码不能为空",
        "condition": lambda df: df["code"].notna(),
    },
    {
        "name": "日期格式正确",
        "condition": lambda df: df["date"].str.match(r'^\d{8}$').fillna(False),
    },
    {
        "name": "数值在合理范围",
        "condition": lambda df: (df["value"] >= 0) & (df["value"] <= 100),
    },
]
```

### 5. 主键设计

选择合适的主键，确保数据唯一性：

```python
# 单列主键
primary_keys = ["ts_code"]

# 复合主键
primary_keys = ["ts_code", "trade_date"]

# 包含报告类型的复合主键
primary_keys = ["ts_code", "end_date", "report_type"]
```

## 示例：基金分析展望数据

完整的实现示例请参考：
`alphahome/fetchers/tasks/fund/excel_fund_analysis_outlook.py`

该任务展示了：
- 如何处理大文本字段（~23万行）
- 如何清洗文本数据
- 如何处理特殊值（如将 "0" 转为 NULL）
- 如何定义复合主键
- 如何定义完整的表结构

## 常见问题

### Q: 如何处理多个 Sheet？

重写 `get_batch_list()` 方法，为每个 sheet 生成一个批次：

```python
async def get_batch_list(self, **kwargs) -> List[Dict]:
    sheets = ["Sheet1", "Sheet2", "Sheet3"]
    return [
        {"file_path": str(self.excel_file_path), "sheet_name": sheet}
        for sheet in sheets
    ]
```

### Q: 如何跳过某些行？

使用 `skip_rows` 配置：

```python
skip_rows = [0, 1, 2]  # 跳过前3行
```

### Q: 如何只读取特定列？

使用 `use_cols` 配置：

```python
use_cols = ["A", "B", "C"]  # Excel 列名
# 或
use_cols = ["column1", "column2"]  # 表头名称
```

### Q: Excel 文件编码问题？

pandas 默认处理大多数编码。如有问题，可在 `fetch_batch` 中自定义：

```python
async def fetch_batch(self, params, stop_event=None):
    # 自定义读取参数
    data = pd.read_excel(
        self.excel_file_path,
        sheet_name=self.sheet_name,
        encoding='utf-8'  # 或其他编码
    )
    return self.process_data(data)
```

## 扩展性

### 添加新的数据源

如需添加其他类型的数据源（如 CSV, JSON 等），可参考 `ExcelTask` 的实现方式：

1. 创建新的基类继承自 `FetcherTask`
2. 实现 `fetch_batch()` 方法读取文件
3. 提供通用的数据处理框架
4. 创建具体任务继承新基类

示例：

```python
class CsvTask(FetcherTask, abc.ABC):
    """CSV 文件数据源基类"""
    
    csv_file_path: str
    delimiter: str = ","
    
    async def fetch_batch(self, params, stop_event=None):
        data = pd.read_csv(
            self.csv_file_path,
            delimiter=self.delimiter
        )
        return self.process_data(data)
```

## 总结

Excel 数据源适配器提供了一个简单但强大的框架，用于将本地 Excel 数据集成到系统中。通过定义清晰的任务类和表结构，您可以快速添加新的数据源，补充常规 API 无法获取的数据。
