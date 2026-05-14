# AlphaHome Providers

`alphahome.providers` 提供研究侧简化数据访问入口，核心类是 `AlphaDataTool`。

## 快速开始

```python
from research.tools.context import ResearchContext

with ResearchContext() as context:
    data_tool = context.data_tool
    df = data_tool.get_stock_data(
        ["000001.SZ", "600000.SH"],
        "2024-01-01",
        "2024-12-31",
    )
```

也可以直接传入数据库管理器：

```python
from alphahome.common.db_manager import create_sync_manager
from alphahome.providers import AlphaDataTool

db = create_sync_manager()
data_tool = AlphaDataTool(db)
```

## API

### `get_stock_data`

```python
get_stock_data(symbols, start_date, end_date, fields=None, adjust=True)
```

返回 `tushare.stock_daily` 中的行情数据，输出列包括：

`ts_code`、`trade_date`、`open`、`high`、`low`、`close`、`pre_close`、`change`、`pct_chg`、`vol`、`amount`。

### `get_adj_factor_data`

```python
get_adj_factor_data(symbols=None, start_date=None, end_date=None)
```

返回 `tushare.stock_adjfactor` 的复权因子。

### `get_index_weights`

```python
get_index_weights(index_code, start_date, end_date, symbols=None, monthly=False)
```

返回指数成分权重。`monthly=True` 时只取月末附近记录。

### `get_stock_info`

```python
get_stock_info(symbols=None, fields=None, active_only=False)
```

返回 `tushare.stock_basic` 的基础信息。`active_only=True` 时筛选 `list_status='L'`。

### `get_trade_dates`

```python
get_trade_dates(start_date, end_date, market="SSE")
```

返回交易日历。参数名是 `market`，不是 `exchange`。

### `get_industry_data`

```python
get_industry_data(symbols=None, industry_type="SW2021", active_only=False)
```

当前实现基于 `tushare.stock_basic.industry` 返回简化行业字段。

### `custom_query`

```python
custom_query(query, params=None, as_dict=False)
```

执行自定义 SQL。默认返回 `DataFrame`，`as_dict=True` 时返回字典列表。

### `get_raw_db_manager`

```python
get_raw_db_manager()
```

返回底层 DBManager。

## 注意事项

- `AlphaDataTool` 是轻量研究接口，不替代生产数据采集任务。
- 当前方法直接查询数据库，要求对应表已经由 fetchers 或生产脚本写入。
- 参数会被传入 `fetch_sync`，不要拼接用户输入到 SQL 字符串中。
- 更复杂的因子、PIT 或跨表逻辑建议写成 research pipeline 或 features recipe。
