# 蛋卷基金交易规则任务 GUI 降级设计

## 背景

`akshare_fund_individual_detail_info_xq` 当前以 `tushare.fund_basic` 的全部在市基金作为批次全集，但蛋卷接口只对其在售基金返回交易规则。2026-07-17 的运行在处理 3,100/6,929 个待补批次时仍为 0 行写入，主要响应是“该基金暂不销售”，不是可通过重试解决的网络故障。

AlphaDB 当前已有蛋卷历史快照；其基金覆盖完全包含在东方财富 `fund_fee_em` 的覆盖内。蛋卷数据仍可用于历史追溯或按需交叉核验，但不应继续参加 GUI 的常规全选采集。

## 目标

- 将蛋卷任务从 GUI“数据采集”任务列表中隐藏，使 GUI“全选”不再运行它。
- 保留任务在 `UnifiedTaskFactory` 中的注册，继续支持 CLI、脚本和代码显式调用。
- 保留 `akshare.fund_individual_detail_info_xq` 与 `rawdata.fund_individual_detail_info_xq` 的既有数据和视图，不执行删除或迁移。
- 保留缺少 `data` 或 `declare_rate_table` 时按业务无数据跳过的容错行为。

## 非目标

- 不删除蛋卷任务实现、历史表或 `rawdata` 视图。
- 不修改东方财富 `fund_fee_em`、`fund_overview_em` 或 `fund_purchase_limit` 任务。
- 不在本次变更中建设通用 AkShare 健康指标、熔断器或新的调度策略。
- 不改变 CLI、脚本或直接通过任务工厂创建蛋卷任务的行为。

## 方案

在 `AkShareFundIndividualDetailInfoXqTask` 上增加类级元数据：

```python
hide_from_gui_collection = True
```

GUI 的 `task_registry_service._is_hidden_from_collection_gui()` 已读取该元数据。任务发现时会跳过该任务，因此它不会进入 GUI 缓存，也不会被“全选”选中。任务装饰器和工厂注册不变，所以显式调用路径不受影响。

这一方案复用现有 GUI 边界，不在通用任务工厂中引入“禁用”语义，避免误伤非 GUI 调度器。

## 数据流与边界

1. 应用启动后，任务模块仍通过 `@task_register()` 注册蛋卷任务。
2. GUI 请求采集任务列表时，注册服务读取任务类元数据。
3. `hide_from_gui_collection=True` 使蛋卷任务不进入 GUI 列表及选择缓存。
4. CLI、脚本或代码显式使用任务名时，工厂仍能创建和执行该任务。
5. 已有数据库表、快照和统一视图保持原样。

## 错误处理

蛋卷接口对不在售基金可能返回缺少 `data` 的业务响应；部分在售基金也可能缺少具体费率表。任务继续将已识别的 `data` 和 `declare_rate_table` 缺失转换为无数据返回，而不是重试或使整个任务失败。其他未识别异常仍向上抛出，避免掩盖真实接口漂移。

## 测试设计

- 在蛋卷任务单元测试中断言 `hide_from_gui_collection is True`，锁定降级元数据。
- 复用任务注册服务现有测试，确认带隐藏元数据的采集任务不会进入 GUI 列表，而普通任务仍可见。
- 保留并运行 `data`、`declare_rate_table` 两种缺失字段的参数化测试，确认业务无数据容错不回退。
- 运行完整 `tests/unit/test_akshare_fund_fee_tasks.py` 和 `tests/unit/test_task_registry_service.py`。

## 验收标准

- 蛋卷任务不出现在 GUI 数据采集列表中，因而不会被 GUI“全选”执行。
- 蛋卷任务类仍由 `UnifiedTaskFactory` 注册，可被显式创建。
- 两类已知缺字段响应均返回无数据，不计为未处理异常。
- 不产生数据库 DDL/DML，不改变历史快照。
- 两个相关单元测试文件全部通过。
