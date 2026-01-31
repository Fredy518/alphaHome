"""
特征更新任务UI事件处理器

负责处理"特征更新"标签页上的用户交互事件。
管理 features 模块中物化视图的刷新和状态。
"""
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Any, Dict, List, Optional

from ...common.logging_utils import get_logger
from .. import controller

logger = get_logger(__name__)


# 完整的特征列表缓存
_full_feature_list: List[Dict[str, Any]] = []
# 当前过滤后的列表
_filtered_feature_list: List[Dict[str, Any]] = []
# 当前选择的分类
_current_category: str = "全部"


def update_feature_list_ui(
    root: tk.Tk, ui_elements: Dict[str, tk.Widget], feature_list: List[Dict[str, Any]]
):
    """
    Callback function to update the feature list in the UI.
    """
    global _full_feature_list
    _full_feature_list = feature_list
    logger.info(f"UI更新回调：接收到 {len(feature_list)} 个特征配方。")
    _apply_category_filter(ui_elements)
    _update_stats_label(ui_elements)


def handle_feature_refresh_complete(
    ui_elements: Dict[str, tk.Widget], data: Dict[str, Any]
):
    """Handles completion of the feature list refresh."""
    logger.info("UI事件：特征列表刷新完成。")
    refresh_button = ui_elements.get("feature_refresh_button")
    if refresh_button:
        refresh_button.config(state=tk.NORMAL)
    
    # 更新状态标签
    status_label = ui_elements.get("feature_status_label")
    if status_label:
        if data.get("success"):
            status_label.config(text="特征列表加载完成")
        else:
            status_label.config(text="特征列表加载失败")


def handle_refresh_features(widgets: Dict[str, tk.Widget]):
    """Handles the 'Refresh' button click for features."""
    logger.info("Requesting refresh of feature list...")
    refresh_button = widgets.get("feature_refresh_button")
    if refresh_button:
        refresh_button.config(state=tk.DISABLED)
    
    status_label = widgets.get("feature_status_label")
    if status_label:
        status_label.config(text="正在加载特征列表...")
    
    controller.request_feature_list()


def handle_select_all_features(widgets: Dict[str, tk.Widget]):
    """Handles 'Select All' for features."""
    logger.info("Selecting all features.")
    global _full_feature_list
    for feature in _full_feature_list:
        feature["selected"] = True
    _apply_category_filter(widgets)


def handle_deselect_all_features(widgets: Dict[str, tk.Widget]):
    """Handles 'Deselect All' for features."""
    logger.info("Deselecting all features.")
    global _full_feature_list
    for feature in _full_feature_list:
        feature["selected"] = False
    _apply_category_filter(widgets)


def handle_feature_tree_click(event: tk.Event, widgets: Dict[str, tk.Widget]):
    """
    处理特征树的点击事件以切换选择
    
    Args:
        event: 鼠标点击事件
        widgets (Dict[str, tk.Widget]): UI组件字典
    """
    tree = widgets.get("feature_tree")
    if not tree or not isinstance(tree, ttk.Treeview):
        logger.error("Feature tree widget not found or is wrong type.")
        return
        
    region = tree.identify("region", event.x, event.y)
    if region == "cell":
        item_id = tree.identify_row(event.y)
        if item_id:
            try:
                feature_name = tree.item(item_id, "values")[1]
                _toggle_feature_selection(feature_name, widgets)
            except IndexError:
                logger.error(
                    "Failed to get feature name on tree click - column index may be wrong."
                )


def handle_category_filter_change(widgets: Dict[str, tk.Widget]):
    """处理分类筛选下拉框变化"""
    global _current_category
    category_var = widgets.get("feature_category_var")
    if category_var:
        _current_category = category_var.get()
        logger.debug(f"分类筛选变更为: {_current_category}")
        _apply_category_filter(widgets)


def handle_refresh_selected_features(widgets: Dict[str, tk.Widget]):
    """处理刷新选中视图按钮点击"""
    selected = get_selected_features()
    if not selected:
        messagebox.showwarning("提示", "请先选择要刷新的特征视图。")
        return
    
    logger.info(f"Requesting refresh of {len(selected)} selected features...")
    
    status_label = widgets.get("feature_status_label")
    if status_label:
        status_label.config(text=f"正在刷新 {len(selected)} 个特征视图...")
    
    controller.request_refresh_features(selected)


def handle_create_missing_features(widgets: Dict[str, tk.Widget]):
    """处理创建缺失视图按钮点击"""
    # 获取所有未创建的视图
    missing = [f for f in _full_feature_list if f.get("status") == "未创建"]
    
    if not missing:
        messagebox.showinfo("提示", "所有特征视图均已创建，无需操作。")
        return
    
    if not messagebox.askyesno(
        "确认", 
        f"即将创建 {len(missing)} 个缺失的特征视图，是否继续？"
    ):
        return
    
    logger.info(f"Requesting creation of {len(missing)} missing features...")
    
    status_label = widgets.get("feature_status_label")
    if status_label:
        status_label.config(text=f"正在创建 {len(missing)} 个特征视图...")
    
    controller.request_create_features([f["name"] for f in missing])


def handle_feature_operation_complete(
    ui_elements: Dict[str, tk.Widget], data: Dict[str, Any]
):
    """处理特征操作（刷新/创建）完成的回调"""
    operation = data.get("operation", "操作")
    success_count = data.get("success_count", 0)
    fail_count = data.get("fail_count", 0)
    
    status_label = ui_elements.get("feature_status_label")
    if status_label:
        if fail_count == 0:
            status_label.config(text=f"{operation}完成: 成功 {success_count} 个")
        else:
            status_label.config(text=f"{operation}完成: 成功 {success_count}, 失败 {fail_count}")
    
    # 刷新列表以更新状态
    controller.request_feature_list()


def _toggle_feature_selection(feature_name: str, widgets: Dict[str, tk.Widget]):
    """切换特征的选择状态"""
    global _full_feature_list
    for feature in _full_feature_list:
        if feature.get("name") == feature_name:
            feature["selected"] = not feature.get("selected", False)
            logger.debug(f"Toggled selection for feature: {feature_name} -> {feature['selected']}")
            break
    _apply_category_filter(widgets)


def _apply_category_filter(ui_elements: Dict[str, tk.Widget]):
    """应用分类过滤并更新显示"""
    global _filtered_feature_list, _current_category
    
    if _current_category == "全部":
        _filtered_feature_list = _full_feature_list
    else:
        _filtered_feature_list = [
            f for f in _full_feature_list 
            if f.get("category", "").lower() == _current_category.lower()
        ]
    
    _update_feature_display(ui_elements)


def _update_feature_display(ui_elements: Dict[str, tk.Widget]):
    """Updates the feature list Treeview display."""
    tree = ui_elements.get("feature_tree")
    if not tree or not isinstance(tree, ttk.Treeview):
        logger.error("Feature tree widget not found or is wrong type.")
        return

    tree.delete(*tree.get_children())

    for feature in _filtered_feature_list:
        selected_char = "✓" if feature.get("selected") else ""
        status = feature.get("status", "未知")
        
        # 根据状态设置标签
        tags = ()
        if feature.get("selected"):
            tags = ("selected",)
        if status == "未创建":
            tags = tags + ("missing",)
        elif status == "错误":
            tags = tags + ("error",)
        
        tree.insert(
            "",
            "end",
            values=(
                selected_char,
                feature.get("name", "N/A"),
                feature.get("description", "N/A"),
                feature.get("category", "N/A"),
                status,
                feature.get("row_count", "N/A"),
                feature.get("last_refresh", "N/A"),
            ),
            tags=tags,
        )
    
    # 配置标签样式
    tree.tag_configure("missing", foreground="gray")
    tree.tag_configure("error", foreground="red")
    tree.tag_configure("selected", background="#e8f4fd")


def _update_stats_label(ui_elements: Dict[str, tk.Widget]):
    """更新统计标签"""
    stats_label = ui_elements.get("feature_stats_label")
    if not stats_label:
        return
    
    total = len(_full_feature_list)
    created = sum(1 for f in _full_feature_list if f.get("status") == "已创建")
    missing = sum(1 for f in _full_feature_list if f.get("status") == "未创建")
    
    stats_label.config(text=f"已注册: {total} | 已创建: {created} | 待创建: {missing}")


def get_selected_features() -> List[str]:
    """
    获取当前选中的特征名称列表。
    
    Returns:
        List[str]: 选中的特征名称列表
    """
    return [f["name"] for f in _full_feature_list if f.get("selected", False)]
