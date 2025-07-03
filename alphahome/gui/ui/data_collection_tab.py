"""
数据采集标签页 UI

负责创建"数据采集"标签页的全部Tkinter控件。
"""
import tkinter as tk
from tkinter import font as tkFont
from tkinter import ttk
from typing import Dict
from ..utils.layout_manager import create_data_collection_column_manager
from ..utils.dpi_aware_ui import get_ui_factory
from ..handlers import data_collection_handler

_ALL_TYPES_OPTION = "所有类型"


def create_data_collection_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"数据采集"标签页的Tkinter布局。"""
    widgets = {}
    
    # 获取DPI感知的UI工厂
    ui_factory = get_ui_factory()

    # --- 顶部按钮和过滤框架 ---
    top_frame = ttk.Frame(parent)
    top_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))
    top_frame.grid_columnconfigure(1, weight=1)

    # --- 左侧按钮容器 ---
    button_frame = ttk.Frame(top_frame)
    button_frame.grid(row=0, column=0, sticky="w")

    refresh_button = ui_factory.create_button(button_frame, text="刷新列表")
    refresh_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_refresh_button"] = refresh_button

    select_all_button = ui_factory.create_button(button_frame, text="全选")
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_select_all_button"] = select_all_button

    deselect_all_button = ui_factory.create_button(button_frame, text="取消全选")
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_deselect_all_button"] = deselect_all_button

    # --- 右侧过滤 (同步创建，延迟布局) ---
    filter_frame = ttk.Frame(top_frame)
    widgets["filter_frame"] = filter_frame
    
    # 创建控件并添加到字典
    name_filter_label = ui_factory.create_label(filter_frame, text="名称过滤:")
    name_filter_entry = ui_factory.create_entry(filter_frame, width=18)
    widgets["collection_filter_entry"] = name_filter_entry

    data_source_label = ui_factory.create_label(filter_frame, text="数据源过滤:")
    data_source_filter_combo = ui_factory.create_combobox(
        filter_frame, values=[_ALL_TYPES_OPTION], state="readonly", width=12
    )
    data_source_filter_combo.set(_ALL_TYPES_OPTION)
    widgets["collection_data_source_combo"] = data_source_filter_combo

    type_label = ui_factory.create_label(filter_frame, text="类型过滤:")
    type_filter_combo = ui_factory.create_combobox(
        filter_frame, values=[_ALL_TYPES_OPTION], state="readonly", width=12
    )
    type_filter_combo.set(_ALL_TYPES_OPTION)
    widgets["collection_task_type_combo"] = type_filter_combo

    def _layout_filters():
        """延迟布局过滤器以确保父容器尺寸已确定"""
        filter_frame.grid(row=0, column=1, sticky="e", padx=(10, 0))
        
        filter_frame.grid_columnconfigure(1, weight=1)
        filter_frame.grid_columnconfigure(3, weight=1)
        filter_frame.grid_columnconfigure(5, weight=1)

        name_filter_label.grid(row=0, column=0, sticky="w", padx=(0, 2))
        name_filter_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))
        
        data_source_label.grid(row=0, column=2, sticky="w", padx=(0, 2))
        data_source_filter_combo.grid(row=0, column=3, sticky="ew", padx=(0, 8))

        type_label.grid(row=0, column=4, sticky="w", padx=(0, 2))
        type_filter_combo.grid(row=0, column=5, sticky="ew")

    # --- Treeview (表格) 框架 ---
    tree_frame = ttk.Frame(parent)
    tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    
    columns = ("selected", "data_source", "type", "name", "description", "latest_update_time")
    tree = ui_factory.create_treeview(
        tree_frame, columns=columns, show="headings"
    )

    tree.heading("selected", text="选择")
    tree.heading("data_source", text="数据源")
    tree.heading("type", text="类型")
    tree.heading("name", text="名称")
    tree.heading("description", text="描述")
    tree.heading("latest_update_time", text="更新时间")

    # 初始化动态列宽管理器
    column_manager = create_data_collection_column_manager(tree)
    tree._column_manager = column_manager # type: ignore

    vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")

    tree_frame.grid_rowconfigure(0, weight=1)
    tree_frame.grid_columnconfigure(0, weight=1)

    widgets["collection_task_tree"] = tree
    tree.insert("", tk.END, values=("", "", "", "正在加载, 请稍候...", "", ""), tags=("loading",))

    # 为排序状态添加属性
    tree._last_sort_col = "type" # type: ignore
    tree._last_sort_reverse = False # type: ignore
    
    # 启用动态列宽管理器
    column_manager.bind_resize_event()
    tree.after_idle(column_manager.configure_columns)

    # 延迟布局过滤器
    parent.after(50, _layout_filters)

    return widgets 