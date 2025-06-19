"""
数据处理任务UI事件处理器

负责处理"数据处理"标签页上的用户交互事件。
"""
import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List

from ...common.logging_utils import get_logger
from .. import controller

logger = get_logger(__name__)


_full_processing_task_list: List[Dict[str, Any]] = []


def update_processing_task_list_ui(
    root: tk.Tk, ui_elements: Dict[str, tk.Widget], task_list: List[Dict[str, Any]]
):
    """
    Callback function to update the data processing task list in the UI.
    """
    global _full_processing_task_list
    _full_processing_task_list = task_list
    print(f"UI更新回调：接收到 {len(task_list)} 个数据处理任务。")
    _update_processing_task_display(ui_elements)


def handle_processing_refresh_complete(
    ui_elements: Dict[str, tk.Widget], data: Dict[str, Any]
):
    """Handles completion of the processing task list refresh."""
    print("UI事件：处理任务刷新完成。")
    refresh_button = ui_elements.get("processing_refresh_button")
    if refresh_button:
        refresh_button.config(state=tk.NORMAL)
    # Potentially update a status bar or log here
    logger.info("Processing task list refresh complete.")


def handle_refresh_processing_tasks(widgets: Dict[str, tk.Widget]):
    """Handles the 'Refresh' button click for processing tasks."""
    logger.info("Requesting refresh of data processing task list...")
    refresh_button = widgets.get("processing_refresh_button")
    if refresh_button:
        refresh_button.config(state=tk.DISABLED)
    controller.request_processing_tasks()


def handle_select_all_processing(widgets: Dict[str, tk.Widget]):
    """Handles 'Select All' for processing tasks."""
    logger.info("Requesting to select all processing tasks.")
    task_names = [task["name"] for task in _full_processing_task_list]
    controller.request_select_specific_processing(task_names)


def handle_deselect_all_processing(widgets: Dict[str, tk.Widget]):
    """Handles 'Deselect All' for processing tasks."""
    logger.info("Requesting to deselect all processing tasks.")
    task_names = [task["name"] for task in _full_processing_task_list]
    controller.request_deselect_specific_processing(task_names, select=False)


def handle_processing_task_tree_click(event: tk.Event, widgets: Dict[str, tk.Widget]):
    """
    处理数据处理任务树的点击事件以切换选择
    
    Args:
        event: 鼠标点击事件
        widgets (Dict[str, tk.Widget]): UI组件字典
    """
    tree = widgets.get("processing_task_tree")
    if not tree or not isinstance(tree, ttk.Treeview):
        logger.error("Processing task tree widget not found or is wrong type.")
        return
        
    region = tree.identify("region", event.x, event.y)
    if region == "cell":
        item_id = tree.identify_row(event.y)
        if item_id:
            try:
                task_name = tree.item(item_id, "values")[1]
                logger.debug(f"Toggling selection for processing task: {task_name}")
                controller.toggle_processing_task_selection(task_name)
            except IndexError:
                logger.error(
                    "Failed to get task name on tree click - column index may be wrong."
                )


def _update_processing_task_display(ui_elements: Dict[str, tk.Widget]):
    """Updates the processing task list Treeview display."""
    tree = ui_elements.get("processing_task_tree")
    if not tree or not isinstance(tree, ttk.Treeview):
        logger.error("Processing task tree widget not found or is wrong type.")
        return

    tree.delete(*tree.get_children())

    for task in _full_processing_task_list:
        selected_char = "✓" if task.get("selected") else ""
        tags = ("selected",) if task.get("selected") else ()
        tree.insert(
            "",
            "end",
            values=(
                selected_char,
                task.get("name", "N/A"),
                task.get("description", "N/A"),
                task.get("type", "N/A"),
            ),
            tags=tags,
        )


def _update_status(widgets: Dict[str, tk.Widget], message: str):
    """更新状态栏。"""
    statusbar = widgets.get("statusbar")
    if statusbar:
        statusbar.config(text=message)


def get_selected_processing_tasks() -> List[Dict[str, Any]]:
    """
    获取当前选中的数据处理任务列表。
    
    Returns:
        List[Dict]: 选中的任务列表，每个任务字典包含task_name等字段  
    """
    selected_tasks = []
    for task in _full_processing_task_list:
        if task.get("selected", False):
            # 将name字段映射为task_name以符合执行器的期望格式
            task_info = {
                "task_name": task.get("name"),
                "type": task.get("type"),
                "description": task.get("description")
            }
            selected_tasks.append(task_info)
    return selected_tasks 