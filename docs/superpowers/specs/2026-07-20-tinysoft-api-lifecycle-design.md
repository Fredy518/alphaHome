# Tinysoft API 资源生命周期修复设计

## 背景

GUI 顺序执行 Tinysoft OPI 任务时，任务数据能够成功保存，但任务结束后出现 `Unclosed client session` 和 `Unclosed connector`。`TinySoftOPIAPI` 会复用持久化 `aiohttp.ClientSession` 并提供异步 `close()`；pyTSL 后端提供异步 `logout()`。当前 `TinySoftTask` 没有统一释放后端资源，一次性任务实例失去引用时由 aiohttp 报告泄漏。

## 目标

- Tinysoft 任务在成功、异常和取消后都释放自身 API 资源。
- OPI 后端调用 `close()`；pyTSL 后端调用 `logout()`。
- 修复覆盖 GUI、CLI、生产脚本等所有通过 `TinySoftTask.execute()` 执行的入口。
- 清理失败只记录警告，不覆盖原任务结果或原始异常。

## 非目标

- 不修改通用 GUI 任务执行器。
- 不给全部 `BaseTask` 引入新的资源协议。
- 不改变 Tinysoft 查询、重试、保存或数据校验逻辑。
- 不改变数据库表结构或既有数据。

## 方案比较

1. **在 `TinySoftTask.execute()` 中使用 `try/finally`（采用）**：资源所有权和释放责任位于同一基类，所有执行入口都受保护，范围最小。
2. **只在 GUI 每任务 `finally` 中关闭 API（不采用）**：能够修复当前日志，但 CLI 和生产脚本仍可能泄漏。
3. **在 `BaseTask` 建立通用资源生命周期协议（不采用）**：扩展性较强，但会影响所有任务类型，超出本次故障范围。

## 组件与数据流

`TinySoftTask.execute()` 包装现有 `FetcherTask.execute()`：

1. 调用父类执行完整的拉取、处理、校验与保存流程。
2. 无论父类返回、抛出异常还是收到取消，都进入 `finally`。
3. 若 API 提供 `close()`，调用并等待它完成。
4. 否则若 API 提供 `logout()`，调用并等待它完成。
5. 释放完成后保持父类原始返回值或异常语义。

关闭后的 `TinySoftOPIAPI` 会把 session 置为 `None`，同一任务实例再次执行时可按现有 `_get_session()` 逻辑重新创建连接。pyTSL `logout()` 会注销现有会话；同一任务实例再次执行时由现有 `login()` 重新登录。

## 错误处理

- 查询或保存失败时，仍抛出原始任务异常；关闭动作在 `finally` 中执行。
- 取消时，仍传播 `asyncio.CancelledError`；关闭动作在传播前执行。
- `close()` 或 `logout()` 自身失败时记录 WARNING，不把已成功任务改成失败，也不掩盖原始异常。
- 若注入的测试或兼容 API 同时没有 `close()` 和 `logout()`，清理为无操作。

## 测试设计

新增聚焦的 `tests/unit/test_tinysoft_task_lifecycle.py`：

- 父类执行成功时调用 OPI 风格 `close()`，并保持返回结果。
- 父类执行失败时仍调用 `close()`，并重新抛出原异常。
- 父类执行取消时仍调用 `close()`，并传播取消。
- API 没有 `close()` 但有 `logout()` 时调用 `logout()`。
- 清理动作失败时记录警告，并保持任务成功结果。

测试通过 monkeypatch 隔离 `FetcherTask.execute()`，只验证 `TinySoftTask` 的资源所有权边界，不依赖数据库或真实 Tinysoft 网络。

## 验收标准

- 新生命周期测试先失败、加入实现后全部通过。
- 现有 `test_tinysoft_opi_api.py`、`test_task_execution_service.py` 和全量测试通过。
- 不再存在任务正常结束但 API `close()`/`logout()` 未调用的路径。
- 提交只包含 Tinysoft 基类、生命周期测试和本设计/实施文档。
