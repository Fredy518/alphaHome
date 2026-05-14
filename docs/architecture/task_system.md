# AlphaHome 任务系统

## 核心原则

任务系统采用模板方法模式：所有任务只有一个执行入口 `execute()`，数据源差异由子类钩子实现。

```text
BaseTask.execute()
  -> _pre_execute()
  -> _fetch_data()
  -> process_data()
  -> _validate_data()
  -> _save_data()
  -> _post_execute()
```

## 分层

### `BaseTask`

位置：`alphahome/common/task_system/base_task.py`

职责：

- 定义任务生命周期。
- 应用基础 transformations。
- 执行 validations，支持 `report` / `filter` 模式。
- 自动建表、主键去重、主键空值过滤、分批保存。
- 使用 UPSERT 或 COPY/INSERT 保存数据。
- 为数据源表创建/更新 `rawdata` 视图。

关键属性：

| 属性 | 说明 |
| --- | --- |
| `name` | 任务唯一名称 |
| `table_name` | 目标表名 |
| `data_source` | schema / 数据源名 |
| `domain` | 业务域 |
| `primary_keys` | upsert 和去重主键 |
| `date_column` | 智能增量日期列 |
| `schema_def` | 自动建表定义 |
| `validations` | 验证规则列表，支持 `(callable, name)` |
| `default_save_batch_size` | 保存批次大小 |

### `FetcherTask`

位置：`alphahome/fetchers/base/fetcher_task.py`

职责：

- 根据 `UpdateTypes.SMART` / `MANUAL` / `FULL` 计算日期范围。
- 调用 `get_batch_list()` 生成批次。
- 用 semaphore 并发执行 `prepare_params()` 和 `fetch_batch()`。
- 失败批次会重试；最终仍失败则中止保存，避免写入半批脏数据。
- 聚合批次返回的 DataFrame。

子类必须实现：

```python
async def get_batch_list(self, **kwargs): ...
async def prepare_params(self, batch): ...
async def fetch_batch(self, params, stop_event=None): ...
```

常用配置：

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `concurrent_limit` | 5 | 批次并发数 |
| `max_retries` | 3 | 单批次重试次数 |
| `retry_delay` | 2 | 重试等待基数 |
| `smart_lookback_days` | 10 | SMART 模式回看天数 |
| `save_batch_size` | 10000 | 入库分批行数 |

### 数据源基类

| 类 | 位置 | 说明 |
| --- | --- | --- |
| `TushareTask` | `fetchers/sources/tushare/tushare_task.py` | 统一 token、分页、限流、字段和 Tushare 转换 |
| `AkShareTask` | `fetchers/sources/akshare/akshare_task.py` | 统一 AkShare 调用、中文列名映射、宽表转换 |
| `TinySoftTask` | `fetchers/sources/tinysoft/tinysoft_task.py` | 统一 pyTSL 参数、超时、周期、服务节点 |
| `ExcelTask` | `fetchers/sources/excel/excel_task.py` | 统一 Excel 文件读取与转换 |

数据源基类一般实现 `prepare_params()` 和 `fetch_batch()`，具体任务只需要定义元数据、schema 和 `get_batch_list()`。

## 任务注册与创建

具体任务使用 `@task_register()` 注册：

```python
from alphahome.common.task_system.task_decorator import task_register

@task_register()
class TushareStockDailyTask(TushareTask):
    name = "tushare_stock_daily"
    ...
```

`UnifiedTaskFactory` 负责：

- 初始化数据库连接。
- 读取 `~/.alphahome/config.json`。
- 注入 `TUSHARE_TOKEN`、Tinysoft 配置和任务级配置。
- 按类型列出和创建任务实例。

常用接口：

```python
await UnifiedTaskFactory.initialize()
task = await UnifiedTaskFactory.create_task_instance(
    "tushare_stock_daily",
    update_type=UpdateTypes.MANUAL,
    start_date="20260501",
    end_date="20260512",
)
result = await task.execute()
await UnifiedTaskFactory.shutdown()
```

## 验证与保存

`validations` 支持两种写法：

```python
validations = [
    (lambda df: df["ts_code"].notna(), "ts_code 非空"),
    (lambda df: df["close"] > 0, "收盘价为正"),
]
```

默认是 `report` 模式：记录验证失败但不过滤数据，最终结果可能为 `partial_success`。如需过滤，可在任务上设置 `validation_mode = "filter"`。

保存前会执行：

1. 目标表存在性和 schema 兼容检查。
2. 基于 `primary_keys` 去重。
3. 主键空值过滤。
4. `inf` 转 `NaN`，`NaN` 入库为 `NULL`。
5. 分批 UPSERT 或 COPY/INSERT。

## 新增任务建议

- 新任务优先继承现有数据源基类，不直接继承 `BaseTask`。
- `schema_def`、`primary_keys`、`date_column` 必须先设计清楚。
- `get_batch_list()` 只负责分批，不做 API 调用。
- 数据源 API 调用放在 `fetch_batch()` 或父类实现中。
- 风险较高的任务先用 MANUAL 短日期范围验证，再启用 SMART。
