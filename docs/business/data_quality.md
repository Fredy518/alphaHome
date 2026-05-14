# 数据质量与验证机制

## 当前机制

AlphaHome 的数据质量控制主要在三个位置：

| 层 | 位置 | 作用 |
| --- | --- | --- |
| 数据转换 | 各数据源 transformer | 列名映射、日期/数值类型转换、源数据特殊格式整理 |
| 任务验证 | `BaseTask._validate_data()` | 执行任务 `validations`，返回验证详情 |
| 保存前保护 | `BaseTask._save_data()` | 主键去重、主键空值过滤、`inf`/`NaN` 处理、分批保存 |

`_validate_data()` 当前返回：

```python
(validation_passed, result_data, validation_details)
```

默认验证模式是 `report`：记录失败但不过滤数据，最终任务结果可能是 `partial_success`。任务可设置：

```python
validation_mode = "filter"
```

以过滤不通过验证的行。

## validations 写法

推荐使用 `(callable, name)`：

```python
validations = [
    (lambda df: df["ts_code"].notna(), "ts_code 非空"),
    (lambda df: df["trade_date"].notna(), "trade_date 非空"),
    (lambda df: df["close"] > 0, "收盘价为正"),
    (lambda df: df["high"] >= df["low"], "最高价不低于最低价"),
]
```

验证器可返回：

- `bool`：整批通过或失败。
- `pd.Series[bool]`：逐行验证，日志会记录失败行数。

## 保存前保护

保存前会自动执行：

1. 自动建表或 schema 兼容检查。
2. 基于 `primary_keys` 去重，保留最后一条。
3. 过滤主键字段为空、空字符串或全空白的记录。
4. 将 `inf` / `-inf` 转成 `NaN`，入库时写成 `NULL`。
5. 按 `save_batch_size` 分批保存。
6. 有主键时使用 UPSERT，无主键时使用 COPY/INSERT。

## rawdata 视图

任务保存后会尝试维护 `rawdata` 视图：

- `data_source="tushare"` 的任务会创建或替换 `rawdata.<table_name>`。
- 非 Tushare 数据源只有在没有 Tushare 同名表且 rawdata 视图不存在时创建。
- 视图创建失败会记录 warning，不中断数据采集。

## 常见质量问题

| 问题 | 排查方向 |
| --- | --- |
| `partial_success` | 查看任务返回的 `validation_details` 和日志中的验证失败项 |
| 主键冲突或重复 | 检查 `primary_keys` 是否覆盖真实唯一性 |
| 数据缺日期 | 检查 `date_column`、SMART 回看窗口和交易日历 |
| 数值全为空 | 检查 source transformer 的列名映射和单位转换 |
| rawdata 视图不符合预期 | 检查是否存在 Tushare 同名表覆盖 |

## 建议

- 新任务必须定义 `primary_keys`、`date_column` 和基本 `validations`。
- 高风险任务先用 MANUAL 小范围验证，再跑 SMART 或 FULL。
- 对会影响因子和 PIT 的表，新增或修改字段后同步更新 feature/PIT 校验。
- 全量修复脚本执行前先备份目标 schema。
