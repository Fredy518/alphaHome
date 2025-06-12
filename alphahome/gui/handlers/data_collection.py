"""
数据采集UI事件处理器

负责处理"数据采集"标签页上的所有用户交互事件。
"""
import tkinter as tk
from tkinter import messagebox, ttk
import operator
from typing import Dict, Any, List

from .. import controller
from ..controller_logic import data_collection

_ALL_TYPES_OPTION = "所有类型"

# --- 缓存 ---
_current_sort_col = "type"
_current_sort_reverse = False
_full_task_list_data: List[Dict[str, Any]] = []

# 中文表头映射
_COLUMN_HEADERS = {
    "selected": "选择",
    "type": "类型", 
    "name": "名称",
    "description": "描述",
    "latest_update_time": "更新时间"
}

def handle_refresh_collection_tasks(widgets: Dict[str, tk.Widget]):
    """处理刷新数据采集任务列表的请求。"""
    button = widgets.get("collection_refresh_button")
    if button:
        button.config(state=tk.DISABLED)
    _update_status(widgets, "正在刷新数据采集任务列表...")
    controller.request_collection_tasks()

def handle_select_all_collection(widgets: Dict[str, tk.Widget]):
    """处理全选数据采集任务的请求。"""
    tree = widgets.get("collection_task_tree")
    if not tree: return
    for item_id in tree.get_children():
        task_name = tree.item(item_id, "values")[2]
        _set_task_selection_state(task_name, True)
    _update_collection_task_display(widgets)


def handle_deselect_all_collection(widgets: Dict[str, tk.Widget]):
    """处理取消全选数据采集任务的请求。"""
    tree = widgets.get("collection_task_tree")
    if not tree: return
    for item_id in tree.get_children():
        task_name = tree.item(item_id, "values")[2]
        _set_task_selection_state(task_name, False)
    _update_collection_task_display(widgets)

def handle_collection_task_tree_click(event, tree):
    """处理数据采集任务树的点击事件以切换选择。"""
    if tree.identify_region(event.x, event.y) != "cell": return
    item_id = tree.identify_row(event.y)
    if not item_id: return

    if tree.identify_column(event.x) == "#1":  # "选择"列
        task_name = tree.item(item_id, "values")[2]
        task = next((t for t in _full_task_list_data if t["name"] == task_name), None)
        if task:
            task["selected"] = not task.get("selected", False)
            _update_collection_task_display({"collection_task_tree": tree}) # 仅更新树

def handle_collection_sort_column(widgets: Dict[str, tk.Widget], col: str):
    """处理数据采集任务列表的排序。"""
    global _current_sort_col, _current_sort_reverse
    if _current_sort_col == col:
        _current_sort_reverse = not _current_sort_reverse
    else:
        _current_sort_col = col
        _current_sort_reverse = False
    
    tree = widgets.get("collection_task_tree")
    if tree:
        # 重置所有列标题为中文，不使用英文
        for c in tree["columns"]:
            tree.heading(c, text=_COLUMN_HEADERS.get(c, c))
        
        # 为当前排序列添加箭头指示
        arrow = " ▲" if not _current_sort_reverse else " ▼"
        current_header = _COLUMN_HEADERS.get(col, col)
        tree.heading(col, text=current_header + arrow)

    _update_collection_task_display(widgets)

def handle_collection_type_filter_change(widgets: Dict[str, tk.Widget]):
    """处理数据采集任务列表的类型过滤。"""
    _update_collection_task_display(widgets)

def handle_collection_name_filter_change(widgets: Dict[str, tk.Widget]):
    """处理数据采集任务列表的名称过滤。"""
    _update_collection_task_display(widgets)

def update_collection_task_list_ui(widgets: Dict[str, tk.Widget], task_list: List[Dict]):
    """从控制器接收到更新后，刷新UI。"""
    global _full_task_list_data
    _full_task_list_data = task_list
    
    # 更新类型过滤器
    combo = widgets.get("collection_task_type_combo")
    if combo:
        types = sorted(list(set(t["type"] for t in task_list)))
        current_selection = combo.get()
        combo["values"] = [_ALL_TYPES_OPTION] + types
        if current_selection not in combo["values"]:
            combo.set(_ALL_TYPES_OPTION)

    _update_collection_task_display(widgets)
    button = widgets.get("collection_refresh_button")
    if button:
        button.config(state=tk.NORMAL)
    _update_status(widgets, f"数据采集任务列表已更新 ({len(task_list)}个任务)。")


def _update_collection_task_display(widgets: Dict[str, tk.Widget]):
    """根据当前的过滤和排序更新Treeview。"""
    tree = widgets.get("collection_task_tree")
    type_combo = widgets.get("collection_task_type_combo")
    name_filter_entry = widgets.get("collection_filter_entry")
    if not tree: return

    for item in tree.get_children():
        tree.delete(item)

    filtered_list = _full_task_list_data
    
    # 类型过滤
    if type_combo and type_combo.get() != _ALL_TYPES_OPTION:
        selected_type = type_combo.get()
        filtered_list = [t for t in filtered_list if t["type"] == selected_type]
    
    # 名称过滤
    if name_filter_entry:
        name_filter = name_filter_entry.get().strip().lower()
        if name_filter:
            filtered_list = [t for t in filtered_list if name_filter in t.get("name", "").lower()]

    # 排序
    if _current_sort_col:
        filtered_list.sort(key=lambda x: (x.get(_current_sort_col) is None, x.get(_current_sort_col, "")), reverse=_current_sort_reverse)

    # 填充UI
    for task in filtered_list:
        values = (
            "✓" if task.get("selected") else "",
            task.get("type", ""),
            task.get("name", ""),
            task.get("description", ""),
            task.get("latest_update_time", "")
        )
        tree.insert("", "end", values=values)

def _set_task_selection_state(task_name: str, selected: bool):
    """在内部缓存中设置任务的选择状态。"""
    task = next((t for t in _full_task_list_data if t["name"] == task_name), None)
    if task:
        task["selected"] = selected

def _update_status(widgets: Dict[str, tk.Widget], message: str):
    """更新状态栏。"""
    statusbar = widgets.get("statusbar")
    if statusbar:
        statusbar.config(text=message)

def get_selected_collection_tasks() -> List[Dict[str, Any]]:
    """
    获取当前选中的数据采集任务列表。
    
    Returns:
        List[Dict]: 选中的任务列表，每个任务字典包含task_name等字段
    """
    selected_tasks = []
    for task in _full_task_list_data:
        if task.get("selected", False):
            # 将name字段映射为task_name以符合执行器的期望格式
            task_info = {
                "task_name": task.get("name"),
                "type": task.get("type"),
                "description": task.get("description"),
                "latest_update_time": task.get("latest_update_time")
            }
            selected_tasks.append(task_info)
    return selected_tasks 