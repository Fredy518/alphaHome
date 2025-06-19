"""
任务执行UI事件处理器

本模块为"任务运行与状态"标签页提供完整的UI更新和事件处理功能。
负责任务执行参数收集、状态显示更新、日志管理和用户交互处理。

## 主要功能

### 任务执行控制
- 执行模式切换（智能增量、手动增量、全量导入）
- 任务参数收集和验证
- 执行状态监控

### 状态显示管理
- 任务状态树形列表更新
- 历史任务与当前会话任务切换
- 状态颜色标记和视觉反馈

### 日志系统
- 彩色日志条目添加
- 自动滚动到最新日志
- 日志清除功能

### 数据集成
- 从数据采集和数据处理模块获取选中任务
- 参数验证和错误处理
- 与控制器的异步通信

## 使用方式

```python
# 添加日志条目
add_log_entry(widgets, "执行开始", "info")

# 更新任务状态
update_task_run_status(widgets, status_list)

# 获取执行参数
params = get_execution_params(widgets)
```

## 状态管理
- success: 成功完成（绿色背景）
- error: 执行失败（浅红色背景）
- running: 正在运行（浅蓝色背景）
- cancelled: 已取消（灰色文字）
- partial_success: 部分成功（浅黄色背景）
"""

import tkinter as tk
from typing import Dict, Any, List, Optional

from ..utils.common import validate_date_string
from ...common.logging_utils import get_logger
from .. import controller
# 添加导入以获取选中的任务
from . import data_collection_handler as data_collection, data_processing_handler as data_processing

logger = get_logger(__name__)


def add_log_entry(widgets: Dict[str, tk.Widget], message: str, level: str = "info"):
    """
    向日志视图Text小部件中添加一条带颜色标记的日志条目
    
    根据日志级别自动应用不同的颜色标记，并自动滚动到最新日志。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
        message (str): 日志消息内容
        level (str): 日志级别，可选值：info, warning, error, success
    """
    log_view = widgets.get("log_view")
    if not log_view:
        return

    # 定义颜色标签
    log_view.tag_config("info", foreground="black")
    log_view.tag_config("warning", foreground="orange")
    log_view.tag_config("error", foreground="red")
    log_view.tag_config("success", foreground="green")

    log_view.config(state=tk.NORMAL)
    log_view.insert(tk.END, f"{message}\n", level.lower())
    log_view.config(state=tk.DISABLED)
    log_view.see(tk.END)  # 自动滚动到底部


def update_task_status_treeview(widgets: Dict[str, tk.Widget], status_list: List[Dict[str, Any]]):
    """
    用控制器发送的最新状态列表更新任务状态Treeview
    
    清除现有状态，插入新的任务状态，并根据状态类型应用相应的颜色标记。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
        status_list (List[Dict[str, Any]]): 任务状态数据列表，每个状态包含：
            - task_name: 任务名称
            - status: 状态值
            - status_display: 显示用状态文本
            - update_time: 更新时间
            - details: 详细信息
    """
    tree = widgets.get("task_status_tree")
    if not tree:
        return

    # 定义状态颜色标签
    tree.tag_configure("success", background="lightgreen")
    tree.tag_configure("error", background="#ffcccb") # light red
    tree.tag_configure("running", background="lightblue")
    tree.tag_configure("cancelled", foreground="gray")
    tree.tag_configure("partial_success", background="lightyellow")

    # Clear existing items
    for item in tree.get_children():
        tree.delete(item)

    # Insert new items
    if not status_list:
        tree.insert("", tk.END, values=("", "没有可用的任务状态信息。", "", ""), tags=("empty",))
        return

    for status in status_list:
        status_val = status.get("status", "pending")
        values = (
            status.get("task_name", "N/A"),
            status.get("status_display", "未知"),
            status.get("update_time", "N/A"),
            status.get("details", "")
        )
        tree.insert("", tk.END, values=values, tags=(status_val,))


def handle_exec_mode_change(widgets: Dict[str, tk.Widget]):
    """
    处理执行模式变化事件
    
    根据选择的执行模式显示或隐藏日期选择框架。
    只有在"手动增量"模式下才显示日期选择。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
    """
    exec_mode = widgets.get("exec_mode", tk.StringVar()).get()
    date_frame = widgets.get("date_frame")
    if date_frame:
        if exec_mode == "手动增量":
            date_frame.pack(side=tk.TOP, fill=tk.X, pady=(10, 0))
        else:
            date_frame.pack_forget()


def handle_stop_tasks(widgets: Dict[str, tk.Widget]):
    """
    处理停止任务按钮点击事件
    
    向控制器发送停止任务请求，并在日志中记录操作。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
    """
    logger.info("Stop tasks button clicked")
    # Add log entry to show that stop was requested
    add_log_entry(widgets, "停止任务请求已发送", "warning")
    # 调用控制器停止任务
    import asyncio
    asyncio.create_task(controller.handle_request("STOP_TASKS"))


def update_task_run_status(widgets: Dict[str, tk.Widget], status_list: List[Dict[str, Any]]):
    """
    更新任务运行状态（兼容性别名）
    
    为update_task_status_treeview提供别名，保持向后兼容。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
        status_list (List[Dict[str, Any]]): 任务状态数据列表
    """
    update_task_status_treeview(widgets, status_list)


def handle_clear_task_run(widgets: Dict[str, tk.Widget]):
    """
    清除任务运行日志和状态信息
    
    清空日志视图和状态树形列表，重置显示状态。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
    """
    logger.info("Clearing task run information")
    
    # Clear the log view
    log_view = widgets.get("log_view")
    if log_view:
        log_view.config(state=tk.NORMAL)
        log_view.delete("1.0", tk.END)
        log_view.config(state=tk.DISABLED)
    
    # Clear the status tree
    tree = widgets.get("task_status_tree")
    if tree:
        for item in tree.get_children():
            tree.delete(item)
        tree.insert("", tk.END, values=("", "任务状态已清除", "", ""), tags=("empty",))
    
    add_log_entry(widgets, "任务运行信息已清除", "info")


def handle_execute_tasks(widgets: Dict[str, tk.Widget]):
    """
    处理任务执行请求
    
    任务执行的占位符处理器。实际的执行由main_window.py直接调用控制器完成。
    主要用于记录执行请求到日志。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
    """
    add_log_entry(widgets, "任务执行请求已发送", "info")


def get_execution_params(widgets: Dict[str, tk.Widget]) -> Optional[Dict[str, Any]]:
    """
    从UI收集任务执行所需的参数
    
    从各个模块收集选中的任务，验证执行参数，返回完整的执行配置。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
        
    Returns:
        Optional[Dict[str, Any]]: 执行参数字典，包含：
            - tasks_to_run: 要执行的任务列表
            - start_date: 开始日期（手动增量模式）
            - end_date: 结束日期（手动增量模式）
            - exec_mode: 执行模式
        如果参数验证失败返回None
    """
    # 1. 收集选中的数据采集任务
    selected_collection_tasks = data_collection.get_selected_collection_tasks()
    
    # 2. 收集选中的数据处理任务  
    selected_processing_tasks = data_processing.get_selected_processing_tasks()
    
    # 3. 合并所有选中的任务
    all_selected_tasks = selected_collection_tasks + selected_processing_tasks
    
    # 获取执行模式和参数
    exec_mode = widgets["exec_mode"].get()
    start_date, end_date = None, None

    if exec_mode == "手动增量":
        start_date = widgets["start_date_entry"].get()
        end_date = widgets["end_date_entry"].get()
        if not (validate_date_string(start_date) and validate_date_string(end_date)):
            # 在 main_window 中已经处理了messagebox
            add_log_entry(widgets, "日期格式错误，请输入有效的 YYYY-MM-DD 格式日期。", "error")
            return None
    
    # 记录选中的任务数量
    if all_selected_tasks:
        add_log_entry(widgets, f"准备执行 {len(all_selected_tasks)} 个选中的任务", "info")
    else:
        add_log_entry(widgets, "未选择任何任务，请先选择要执行的任务", "warning")
    
    return {
        "tasks_to_run": all_selected_tasks,
        "start_date": start_date,
        "end_date": end_date,
        "exec_mode": exec_mode,
    }

def handle_toggle_history_mode(widgets: Dict[str, tk.Widget]):
    """
    处理历史任务显示模式切换
    
    在当前会话任务和历史任务之间切换显示模式，
    更新相关UI元素的文本和状态。
    
    Args:
        widgets (Dict[str, tk.Widget]): UI组件字典
    """
    from ..services import task_execution_service
    
    # 切换模式
    is_history_mode = task_execution_service.toggle_history_mode()
    
    # 更新按钮文本和状态标签
    button = widgets.get("history_toggle_button")
    label = widgets.get("status_mode_label")
    
    if is_history_mode:
        if button:
            button.config(text="显示当前会话")
        if label:
            label.config(text="历史任务", foreground="orange")
    else:
        if button:
            button.config(text="显示历史任务")
        if label:
            label.config(text="当前会话任务", foreground="blue")
    
    # 刷新任务状态显示
    controller.request_all_task_status()
    
    logger.info(f"任务状态显示模式已切换为: {'历史模式' if is_history_mode else '当前会话模式'}")