# Tinysoft API 资源生命周期修复实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 确保每次 `TinySoftTask.execute()` 结束后关闭 OPI session 或注销 pyTSL 会话，消除 `Unclosed client session` 与 connector 泄漏。

**Architecture:** 在 `TinySoftTask` 中包装继承自 `FetcherTask` 的执行入口，并在 `finally` 调用聚焦的 API 清理助手。清理助手优先使用 `close()`，否则使用 `logout()`；清理异常只记录警告，保持任务原始结果、异常和取消语义。

**Tech Stack:** Python 3.12、asyncio、pytest、pytest-asyncio、unittest.mock。

## Global Constraints

- 不修改 GUI 任务执行器或通用 `BaseTask` 生命周期。
- 不改变 Tinysoft 查询、重试、保存和数据库结构。
- 成功、失败和取消路径都必须清理。
- 清理异常不能覆盖任务成功结果或原始异常。
- 不纳入当前主工作区的 KPL 和临时文件改动。

---

### Task 1: 用失败测试定义 Tinysoft API 生命周期

**Files:**
- Create: `tests/unit/test_tinysoft_task_lifecycle.py`

**Interfaces:**
- Consumes: `TinySoftTask.execute(stop_event=None, **kwargs)` 与后端可选的 `close()` / `logout()`。
- Produces: 成功、异常、取消、logout 回退和清理失败五个回归测试。

- [x] **Step 1: 创建聚焦测试任务和五个生命周期测试**

测试使用一个实现 `get_batch_list()` 的最小 `TinySoftTask` 子类，并 monkeypatch `FetcherTask.execute()` 作为父执行边界。断言：

```python
@pytest.mark.asyncio
async def test_execute_closes_api_after_success(monkeypatch):
    result = {"status": "success", "rows": 1}
    parent_execute = AsyncMock(return_value=result)
    monkeypatch.setattr(FetcherTask, "execute", parent_execute)
    api = _CloseAPI()
    task = _LifecycleTinySoftTask(db_connection=object(), api=api)

    actual = await task.execute()

    assert actual is result
    assert api.close_calls == 1
```

其余测试分别让父执行器抛出 `RuntimeError`、抛出 `asyncio.CancelledError`，使用仅带 `logout()` 的 API，以及让 `close()` 抛出异常并检查 WARNING。

- [x] **Step 2: 运行新测试并确认按预期失败**

Run:

```powershell
python -m pytest tests/unit/test_tinysoft_task_lifecycle.py -q
```

Expected: 清理调用和清理警告断言失败，因为 `TinySoftTask` 尚未包装执行入口。

### Task 2: 实现最小资源清理包装

**Files:**
- Modify: `alphahome/fetchers/sources/tinysoft/tinysoft_task.py`
- Test: `tests/unit/test_tinysoft_task_lifecycle.py`

**Interfaces:**
- Consumes: 父类 `execute(stop_event=None, **kwargs)`；API 的可选 `close()` / `logout()`。
- Produces: `TinySoftTask._close_api_resource() -> None` 和覆盖后的 `TinySoftTask.execute(...)`。

- [x] **Step 1: 加入最小执行包装与清理助手**

```python
async def _close_api_resource(self) -> None:
    cleanup = getattr(self.api, "close", None)
    if not callable(cleanup):
        cleanup = getattr(self.api, "logout", None)
    if not callable(cleanup):
        return
    try:
        result = cleanup()
        if inspect.isawaitable(result):
            await result
    except Exception as exc:
        self.logger.warning("关闭 Tinysoft API 资源失败: %s", exc)

async def execute(self, stop_event=None, **kwargs):
    try:
        return await super().execute(stop_event=stop_event, **kwargs)
    finally:
        await self._close_api_resource()
```

- [x] **Step 2: 运行新测试并确认全部通过**

Run:

```powershell
python -m pytest tests/unit/test_tinysoft_task_lifecycle.py -q
```

Expected: `5 passed`。

- [x] **Step 3: 运行既有 Tinysoft OPI 与 GUI 执行器测试**

Run:

```powershell
python -m pytest tests/unit/test_tinysoft_opi_api.py tests/unit/test_task_execution_service.py -q
```

Expected: 全部通过。

### Task 3: 全量验证、范围审查与提交

**Files:**
- Modify: `alphahome/fetchers/sources/tinysoft/tinysoft_task.py`
- Create: `tests/unit/test_tinysoft_task_lifecycle.py`
- Create: `docs/superpowers/plans/2026-07-20-tinysoft-api-lifecycle.md`

**Interfaces:**
- Consumes: Task 1 与 Task 2 的实现和验证结果。
- Produces: 可合并的 Tinysoft 资源生命周期修复提交。

- [x] **Step 1: 运行全量测试**

Run:

```powershell
python -m pytest -q
```

Expected: 全量测试无失败。

- [x] **Step 2: 检查差异范围与空白错误**

Run:

```powershell
git diff --check
git status --short
git diff -- alphahome/fetchers/sources/tinysoft/tinysoft_task.py tests/unit/test_tinysoft_task_lifecycle.py docs/superpowers/plans/2026-07-20-tinysoft-api-lifecycle.md
```

Expected: 仅包含本计划列出的生命周期文件；`git diff --check` 退出码为 0。

- [x] **Step 3: 限定暂存并提交**

Run:

```powershell
git add -- alphahome/fetchers/sources/tinysoft/tinysoft_task.py tests/unit/test_tinysoft_task_lifecycle.py docs/superpowers/plans/2026-07-20-tinysoft-api-lifecycle.md
git diff --cached --check
git commit -m "fix(tinysoft): close API resources after tasks"
```

Expected: 提交成功，暂存区只包含上述三个文件。
