# 上下文
文件名：gui_progress_fix_task.md
创建于：2025-04-30 10:46
创建者：Roo
Yolo模式：False

# 任务描述
GUI中的进度百分比是在哪里定义的？目前总是显示为0%。我提供一个解决思路：批次只要成功保存，就应该用n计数，然后n = n+1。并用n/总批次来计算百分比

# 项目概述
用户观察到 alphaHome GUI 中的任务进度条始终显示 0%，并建议修改进度计算逻辑，使其基于成功保存的批次数与总批次数的比例。

⚠️ 警告：切勿修改此部分 ⚠️
[RIPER-5 Protocol Summary Placeholder]
⚠️ 警告：切勿修改此部分 ⚠️

# 分析
1.  **进度显示**: GUI 进度条更新由 `alphahome/gui/controller.py` 中的 `_update_gui_progress` 回调函数处理。
2.  **回调传递**: 此回调函数作为 `progress_callback` 参数传递给任务执行逻辑。
3.  **计算位置**: 实际的进度百分比计算发生在 `alphahome/data_module/sources/tushare/tushare_task.py` 的 `TushareTask.execute` 方法内部（并发模式在 `process_batch` 内，串行模式在主循环内）。
4.  **当前逻辑**: 当前进度计算基于已 *处理* (或尝试处理) 的批次数，而非 *成功保存* 的批次数。`_process_single_batch` 方法返回处理的行数，成功时 > 0，失败时 == 0。`execute` 方法目前使用批次 *索引* 或 `tqdm` 计数器来计算百分比，没有直接利用 `_process_single_batch` 的返回值来判断成功与否进行计数。
5.  **根本原因**: 进度更新逻辑没有将批次处理的成功/失败状态纳入计算，导致即使批次失败，进度百分比也会增加。

# 提议的解决方案
采用方法 1：在 `TushareTask.execute` 方法中维护一个成功批次的计数器。
1.  在 `execute` 方法开始时初始化 `successful_batches_count = 0`。
2.  **并发模式**: 在 `asyncio.gather` 返回结果后，遍历 `batch_results`。对于每个表示成功的结果（非 Exception 且非 None），增加 `successful_batches_count`。在调用回调函数 `cb` 时，使用 `percentage = int((successful_batches_count / total_count) * 100)` 计算百分比。
3.  **串行模式**: 在 `for` 循环中，检查 `_process_single_batch` 的返回值 `rows`。如果 `rows > 0`，增加 `successful_batches_count`。在调用回调函数 `progress_callback` 时，使用 `percentage = int((successful_batches_count / total_count) * 100)` 计算百分比。

# 当前执行步骤："[步骤编号和名称]"
- "5. 完成最终审查"

# 任务进度
[2025-04-30 10:51]
- 修改：alphahome/data_module/sources/tushare/tushare_task.py
- 更改：在 TushareTask.execute 方法中添加了 successful_batches_count 计数器和相应的锁（并发模式），并修改了并发和串行模式下的进度回调逻辑，使其基于成功批次数计算百分比。
- 原因：实现用户请求的基于成功保存批次的进度更新逻辑。
- 阻碍：无
- 状态：成功

# 最终审查
代码修改已根据计划成功实施。
- 在 `TushareTask.execute` 方法中添加了 `successful_batches_count` 和 `asyncio.Lock`。
- 并发和串行模式下的进度百分比计算逻辑均已更新，现在基于成功完成的批次数。
- 实施与计划完全匹配，未发现偏差。