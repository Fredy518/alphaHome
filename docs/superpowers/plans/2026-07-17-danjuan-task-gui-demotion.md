# 蛋卷基金交易规则任务 GUI 降级实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将蛋卷基金交易规则任务移出 GUI 数据采集列表和“全选”范围，同时保留任务注册、显式调用、历史表及已知业务无数据容错。

**Architecture:** 在蛋卷任务类上声明现有 GUI 注册服务已经支持的 `hide_from_gui_collection` 元数据，不修改通用任务工厂或数据库。用任务单测锁定元数据，并用现有 GUI 注册服务测试及缺字段参数化测试验证边界行为。

**Tech Stack:** Python 3.12、pytest、pytest-asyncio、AlphaHome UnifiedTaskFactory 与 GUI task registry service。

## Global Constraints

- 不删除或迁移 `akshare.fund_individual_detail_info_xq` 与 `rawdata.fund_individual_detail_info_xq`。
- 不改变 CLI、脚本和代码显式创建任务的路径。
- 不修改东方财富基金任务或通用 AkShare 调度架构。
- 不覆盖工作区中与本任务无关的未提交改动。
- 生产代码必须经过先失败、后通过的 TDD 循环。

---

## 文件结构

- `alphahome/fetchers/tasks/fund/akshare_fund_individual_detail_info_xq.py`：声明 GUI 隐藏元数据，并保留现有缺字段容错。
- `tests/unit/test_akshare_fund_fee_tasks.py`：锁定蛋卷任务 GUI 降级元数据和业务无数据容错。
- `tests/unit/test_task_registry_service.py`：复用现有通用 GUI 隐藏行为测试，不新增生产代码耦合。

### Task 1: 以测试锁定蛋卷任务 GUI 降级元数据

**Files:**
- Modify: `tests/unit/test_akshare_fund_fee_tasks.py:368`
- Modify: `alphahome/fetchers/tasks/fund/akshare_fund_individual_detail_info_xq.py:28`

**Interfaces:**
- Consumes: `task_registry_service._is_hidden_from_collection_gui()` 已支持的类属性 `hide_from_gui_collection`。
- Produces: `AkShareFundIndividualDetailInfoXqTask.hide_from_gui_collection: bool = True`。

- [ ] **Step 1: 写入失败测试**

在蛋卷任务批次测试之前加入：

```python
def test_fund_detail_xq_is_hidden_from_collection_gui():
    assert AkShareFundIndividualDetailInfoXqTask.hide_from_gui_collection is True
```

- [ ] **Step 2: 运行测试并确认按预期失败**

Run:

```powershell
python -m pytest tests/unit/test_akshare_fund_fee_tasks.py::test_fund_detail_xq_is_hidden_from_collection_gui -q
```

Expected: FAIL，错误为 `AkShareFundIndividualDetailInfoXqTask` 缺少 `hide_from_gui_collection` 属性。

- [ ] **Step 3: 写入最小生产实现**

在蛋卷任务的基础元数据区加入：

```python
hide_from_gui_collection = True
```

- [ ] **Step 4: 运行目标测试并确认通过**

Run:

```powershell
python -m pytest tests/unit/test_akshare_fund_fee_tasks.py::test_fund_detail_xq_is_hidden_from_collection_gui -q
```

Expected: `1 passed`。

### Task 2: 验证显式调用边界与业务无数据容错

**Files:**
- Verify: `alphahome/fetchers/tasks/fund/akshare_fund_individual_detail_info_xq.py`
- Verify: `tests/unit/test_akshare_fund_fee_tasks.py`
- Verify: `tests/unit/test_task_registry_service.py`

**Interfaces:**
- Consumes: `@task_register()`、`missing_payload_fields = ("data", "declare_rate_table")`、`fetch_batch(...) -> Optional[pd.DataFrame]`。
- Produces: GUI 隐藏但仍注册的任务；已知缺字段返回 `None`，其他异常继续抛出。

- [ ] **Step 1: 运行蛋卷与基金费率任务完整单测**

Run:

```powershell
python -m pytest tests/unit/test_akshare_fund_fee_tasks.py -q
```

Expected: 全部通过；参数化用例同时覆盖 `data` 与 `declare_rate_table`。

- [ ] **Step 2: 运行 GUI 注册服务完整单测**

Run:

```powershell
python -m pytest tests/unit/test_task_registry_service.py -q
```

Expected: 全部通过；隐藏任务不会进入 GUI 缓存，普通任务仍进入缓存。

- [ ] **Step 3: 验证任务仍注册且数据库边界未变化**

Run:

```powershell
python -c "from alphahome.fetchers.tasks.fund.akshare_fund_individual_detail_info_xq import AkShareFundIndividualDetailInfoXqTask; from alphahome.common.task_system.task_factory import UnifiedTaskFactory; print(AkShareFundIndividualDetailInfoXqTask.name in UnifiedTaskFactory._task_registry); print(AkShareFundIndividualDetailInfoXqTask.table_name)"
```

Expected:

```text
True
fund_individual_detail_info_xq
```

### Task 3: 范围审查、验证与提交

**Files:**
- Commit: `alphahome/fetchers/tasks/fund/akshare_fund_individual_detail_info_xq.py`
- Commit: `tests/unit/test_akshare_fund_fee_tasks.py`

**Interfaces:**
- Consumes: Task 1 的类元数据与 Task 2 的验证结果。
- Produces: 一个仅包含蛋卷任务降级及其回归测试的实现提交。

- [ ] **Step 1: 检查差异范围和空白错误**

Run:

```powershell
git diff --check -- alphahome/fetchers/tasks/fund/akshare_fund_individual_detail_info_xq.py tests/unit/test_akshare_fund_fee_tasks.py
git diff -- alphahome/fetchers/tasks/fund/akshare_fund_individual_detail_info_xq.py tests/unit/test_akshare_fund_fee_tasks.py
```

Expected: `git diff --check` 退出码为 0；差异只包含缺字段业务无数据容错、GUI 隐藏元数据及对应测试。

- [ ] **Step 2: 重新运行最终验证**

Run:

```powershell
python -m pytest tests/unit/test_akshare_fund_fee_tasks.py tests/unit/test_task_registry_service.py -q
```

Expected: 两个测试文件全部通过且无失败。

- [ ] **Step 3: 只暂存相关实现文件**

Run:

```powershell
git add -- alphahome/fetchers/tasks/fund/akshare_fund_individual_detail_info_xq.py tests/unit/test_akshare_fund_fee_tasks.py
git diff --cached --check
git diff --cached --name-only
```

Expected: 暂存区只包含上述两个文件，且空白检查退出码为 0。

- [ ] **Step 4: 提交实现**

Run:

```powershell
git commit -m "fix(fund): demote danjuan fee task from GUI"
```

Expected: 提交成功；与本任务无关的工作区改动仍保持未暂存。
