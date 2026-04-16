# 宏观 PIT 发布日历任务

## 任务目标

`macro_release_calendar` 用于生成宏观数据的真实历史发布日期日历，并写入 `akshare.macro_release_calendar`。这张表的目标不是替代宏观原始值表，而是给 `PMI / 社融 / 货币` 这三类月频宏观指标提供可回测的 PIT 发布时间锚点，供 release-lag、T+1 生效和 shadow-live 审计直接复用。

当前覆盖的指标：

- `pmi`
- `money`
- `credit`

## 数据源优先级

### PMI

1. 国家统计局历史发布日程表页面（2013-2021）
2. 国家统计局资讯发布列表页中的正式文章（2022+）
3. 2012 年边界月份人工种子值（`2012-10`、`2012-11`）

### 货币 / 社融

1. 人民银行站内搜索结果
2. 人民银行正式文章页的发布时间或 `PubDate`
3. 联合发布代理兜底
   `money` 未命中时，允许用同月社融公告做代理发布时间
   `credit` 未命中时，允许用同月金融统计数据公告做代理发布时间

## 落表位置

目标实体表：`akshare.macro_release_calendar`

统一访问视图：

- `rawdata.macro_release_calendar`

说明：

- 按 AlphaHome 现有约定，`rawdata` 主要作为统一访问视图层
- 本任务的真实入库位置是 `akshare` schema
- 在没有同名 `tushare` 表冲突时，系统会自动创建 `rawdata.macro_release_calendar -> akshare.macro_release_calendar` 的映射视图

核心字段：

- `indicator_code`
- `period_end_date`
- `release_date`
- `release_time`
- `source_name`
- `source_title`
- `source_url`
- `query_text`
- `match_method`
- `search_rank`

主键：

- `(indicator_code, period_end_date)`

约束原则：

- 任一月份未解析成功时，任务整批失败，不静默入库半成品
- 原始源表必须已存在：
  - `tushare.macro_pmi`
  - `tushare.macro_cn_m`
  - `tushare.macro_sf_month`

## 运行方式

### GUI

```bash
python run.py
```

在图形界面选择 `macro_release_calendar` 任务执行即可。

### Python 调用

```python
import asyncio

from alphahome.common.task_system.task_factory import UnifiedTaskFactory


async def main():
    await UnifiedTaskFactory.initialize()
    task = UnifiedTaskFactory.create_task(
        task_name="macro_release_calendar",
        update_type="manual",
        start_date="20121130",
        end_date="20261231",
        task_config={"request_sleep": 0.15},
    )
    result = await task.execute()
    print(result)


asyncio.run(main())
```

## 与宏观 PIT 修复的关系

这项任务服务于 S1 宏观 PIT 修复，不再依赖 `update_time` 直接充当真实发布时间。标准用法是：

1. 先运行 `tushare_macro_pmi`、`tushare_macro_cnm`、`tushare_macro_sf`
2. 再运行 `macro_release_calendar`
3. 将 `rawdata.macro_release_calendar` 视图或 `akshare.macro_release_calendar` 实体表与宏观原始表按 `period_end_date` 对齐
4. 信号生效日统一取 `release_date` 对应交易日的下一个交易日

## 边界说明

- 本任务只负责发布日期，不负责交易日映射
- `release_time` 为可选字段，缺失时只使用 `release_date`
- 任务使用官方网页解析，页面结构变化会影响命中率，因此需要配合单元测试和定期抽查
