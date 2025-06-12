"""
任务运行UI事件处理器

为"任务运行与状态"标签页提供UI更新和数据提取的辅助函数。
"""
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Dict, Any, List, Optional

from ..utils.common import validate_date_string
from ...common.logging_utils import get_logger
from .. import controller
# 添加导入以获取选中的任务
from . import data_collection, data_processing

logger = get_logger(__name__)


def add_log_entry(widgets: Dict[str, tk.Widget], message: str, level: str = "info"):
    """
    向日志视图Text小部件中添加一条带颜色标记的日志条目。
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
    用控制器发送的最新状态列表更新任务状态Treeview。
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
    Shows or hides the date selection frame based on the selected execution mode.
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
    Handles the stop tasks button click.
    """
    logger.info("Stop tasks button clicked")
    # Add log entry to show that stop was requested
    add_log_entry(widgets, "停止任务请求已发送", "warning")
    # Here you could call controller.stop_tasks() or similar
    # For now, this is a placeholder


def update_task_run_status(widgets: Dict[str, tk.Widget], status_list: List[Dict[str, Any]]):
    """
    Alias for update_task_status_treeview to maintain compatibility.
    """
    update_task_status_treeview(widgets, status_list)


def handle_clear_task_run(widgets: Dict[str, tk.Widget]):
    """
    Clears the task run log and status information.
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
    Placeholder for task execution handler.
    The actual execution is handled by main_window.py calling controller directly.
    """
    add_log_entry(widgets, "任务执行请求已发送", "info")


def get_execution_params(widgets: Dict[str, tk.Widget]) -> Optional[Dict[str, Any]]:
    """
    从UI收集任务执行所需的参数。
    此函数现在收集选中的任务并返回给控制器。
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
    处理历史任务显示模式切换。
    """
    from ..controller_logic import task_execution
    
    # 切换模式
    is_history_mode = task_execution.toggle_history_mode()
    
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