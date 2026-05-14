# 新任务开发指南

本文档说明如何在当前任务系统中新增数据采集任务。

## 开发前确认

先回答四个问题：

1. 数据源是什么：Tushare、AkShare、Tinysoft、Excel，还是其他？
2. 目标 schema/table 是什么？
3. 主键和日期列是什么？
4. 批处理策略是什么：按交易日、自然日、代码、季度、单批次，还是多维组合？

## 推荐继承关系

| 数据源 | 基类 |
| --- | --- |
| Tushare | `alphahome.fetchers.sources.tushare.tushare_task.TushareTask` |
| AkShare | `alphahome.fetchers.sources.akshare.akshare_task.AkShareTask` |
| Tinysoft | `alphahome.fetchers.sources.tinysoft.tinysoft_task.TinySoftTask` |
| Excel | `alphahome.fetchers.sources.excel.excel_task.ExcelTask` |

不要为普通采集任务直接继承 `BaseTask`。

## 最小 Tushare 模板

```python
from typing import Any, Dict, List

from alphahome.common.task_system.task_decorator import task_register
from alphahome.fetchers.sources.tushare.batch_utils import generate_trade_day_batches
from alphahome.fetchers.sources.tushare.tushare_task import TushareTask


@task_register()
class TushareMyDailyTask(TushareTask):
    name = "tushare_my_daily"
    description = "示例日频任务"
    task_type = "fetch"
    domain = "stock"

    data_source = "tushare"
    table_name = "my_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20200101"

    api_name = "daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]

    schema_def = {
        "ts_code": "VARCHAR(12) NOT NULL",
        "trade_date": "DATE NOT NULL",
        "open": "NUMERIC(18,4)",
        "high": "NUMERIC(18,4)",
        "low": "NUMERIC(18,4)",
        "close": "NUMERIC(18,4)",
        "vol": "NUMERIC(20,4)",
        "amount": "NUMERIC(20,4)",
        "update_time": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 非空"),
        (lambda df: df["trade_date"].notna(), "trade_date 非空"),
        (lambda df: df["close"] > 0, "收盘价为正"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        return await generate_trade_day_batches(
            start_date=kwargs["start_date"],
            end_date=kwargs["end_date"],
            batch_size=1,
        )
```

## 批处理模式

### 按交易日

```python
from alphahome.fetchers.sources.tushare.batch_utils import generate_trade_day_batches

async def get_batch_list(self, **kwargs):
    return await generate_trade_day_batches(
        kwargs["start_date"],
        kwargs["end_date"],
        batch_size=1,
    )
```

适合日线行情、每日指标、涨跌停、资金流等接口。

### 按自然日或月份

使用 `alphahome.common.planning`：

```python
from alphahome.common.planning import BatchPlanner, Source, Partition, Map

async def get_batch_list(self, **kwargs):
    planner = BatchPlanner(
        source=Source.from_callable(lambda: self._build_dates(kwargs["start_date"], kwargs["end_date"])),
        partition_strategy=Partition.by_month(),
        map_strategy=Map.to_date_range("start_date", "end_date"),
    )
    return await planner.generate()
```

### 按代码

```python
async def get_batch_list(self, **kwargs):
    codes = await self.db.get_distinct_values("tushare.stock_basic", "ts_code")
    return [{"ts_code": code} for code in codes]
```

如果还需要日期范围，把 `start_date` / `end_date` 合并进每个批次。

### 多维分批

使用 `ExtendedBatchPlanner`，把资产集合先按市场、状态、行业等维度切分，再映射成 API 参数。适合接口限制复杂、单批请求上限不稳定的任务。

## AkShare 任务要点

AkShare 任务通常需要处理中文列名和宽表：

```python
@task_register()
class AkShareMacroBondRateTask(AkShareNoDateSingleBatchTask):
    name = "akshare_macro_bond_rate"
    domain = "macro"
    table_name = "macro_bond_rate"
    api_name = "bond_zh_us_rate"
    date_column = "trade_date"
    primary_keys = ["trade_date"]
    column_mapping = {"日期": "trade_date", "中国国债收益率10年": "cn_10y"}
```

若接口不接受日期参数，优先继承 `AkShareNoDateSingleBatchTask`，它会在 `process_data()` 阶段按有效日期窗口过滤。

## Tinysoft 任务要点

Tinysoft 批次必须提供：

- `stock`
- `begin_time`
- `end_time`

可选字段：

- `cycle`
- `fields`
- `service`
- `timeout_ms`

配置项写入 `tasks.<task_name>`，例如 `symbol_batch_size`、`query_timeout_ms`、`request_interval`。

## schema 与 rawdata 视图

`BaseTask` 会根据 `data_source` 和 `table_name` 写入源 schema，并尝试创建 `rawdata.<table_name>` 视图。

规则：

- Tushare 表优先：`tushare.<table>` 会覆盖同名 rawdata 视图。
- 非 Tushare 表只在没有 Tushare 同名表、且 rawdata 视图不存在时创建。

因此新任务命名要避免和已有核心表无意义冲突。

## 本地验证

```python
import asyncio
import alphahome.fetchers  # 触发任务注册
from alphahome.common.constants import UpdateTypes
from alphahome.common.task_system import UnifiedTaskFactory

async def main():
    await UnifiedTaskFactory.initialize()
    try:
        task = await UnifiedTaskFactory.create_task_instance(
            "tushare_my_daily",
            update_type=UpdateTypes.MANUAL,
            start_date="20260501",
            end_date="20260508",
        )
        print(await task.execute())
    finally:
        await UnifiedTaskFactory.shutdown()

asyncio.run(main())
```

检查点：

- 任务能被注册。
- MANUAL 短日期范围可执行。
- 目标表自动创建，主键正确。
- 重跑同一日期不会产生重复数据。
- 日志中没有验证失败或批次失败。

## 提交前清单

- [ ] 任务类使用 `@task_register()`。
- [ ] `name`、`domain`、`data_source`、`table_name` 明确。
- [ ] `primary_keys` 覆盖唯一性。
- [ ] `date_column` 支持 SMART 增量。
- [ ] `schema_def` 与实际字段一致。
- [ ] `get_batch_list()` 不执行 API 请求。
- [ ] 至少有一个短日期范围验证记录。
- [ ] 文档或任务说明同步更新。
