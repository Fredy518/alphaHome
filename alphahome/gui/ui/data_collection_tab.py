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

_ALL_TYPES_OPTION = "所有类型"


def create_data_collection_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"数据采集"标签页的Tkinter布局。"""
    widgets = {}
    
    # 获取DPI感知的UI工厂
    ui_factory = get_ui_factory()

    # --- 顶部按钮和过滤框架 ---
    top_frame = ttk.Frame(parent)
    top_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))

    # --- 左侧按钮 ---
    refresh_button = ui_factory.create_button(
        top_frame,
        text="刷新列表",
    )
    refresh_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_refresh_button"] = refresh_button

    select_all_button = ui_factory.create_button(
        top_frame,
        text="全选",
    )
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_select_all_button"] = select_all_button

    deselect_all_button = ui_factory.create_button(
        top_frame,
        text="取消全选",
    )
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_deselect_all_button"] = deselect_all_button

    # --- 右侧过滤 (使用Grid布局支持小屏幕) ---
    filter_frame = ttk.Frame(top_frame)
    filter_frame.pack(side=tk.RIGHT, padx=(10, 0))
    
    # 配置网格权重
    filter_frame.grid_columnconfigure(1, weight=1)
    filter_frame.grid_columnconfigure(3, weight=1)
    filter_frame.grid_columnconfigure(5, weight=1)

    # 第一行：名称过滤和数据源过滤
    ui_factory.create_label(filter_frame, text="名称过滤:").grid(row=0, column=0, sticky="w", padx=(0, 2))
    name_filter_entry = ui_factory.create_entry(filter_frame, width=18)  # 增加宽度
    name_filter_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))
    widgets["collection_filter_entry"] = name_filter_entry

    ui_factory.create_label(filter_frame, text="数据源过滤:").grid(row=0, column=2, sticky="w", padx=(0, 2))
    data_source_filter_combo = ui_factory.create_combobox(
        filter_frame, values=[_ALL_TYPES_OPTION], state="readonly", width=12  # 增加宽度
    )
    data_source_filter_combo.set(_ALL_TYPES_OPTION)
    data_source_filter_combo.grid(row=0, column=3, sticky="ew", padx=(0, 8))
    widgets["collection_data_source_combo"] = data_source_filter_combo

    # 第二行：类型过滤（在小屏幕上可能换行显示）
    ui_factory.create_label(filter_frame, text="类型过滤:").grid(row=0, column=4, sticky="w", padx=(0, 2))
    type_filter_combo = ui_factory.create_combobox(
        filter_frame, values=[_ALL_TYPES_OPTION], state="readonly", width=12  # 增加宽度
    )
    type_filter_combo.set(_ALL_TYPES_OPTION)
    type_filter_combo.grid(row=0, column=5, sticky="ew")
    widgets["collection_task_type_combo"] = type_filter_combo

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
    tree._column_manager = column_manager  # 保存引用以便后续使用
    
    # 注释掉静态列宽配置，完全由动态管理器控制
    # tree.column("selected", width=60, anchor=tk.CENTER, stretch=False)
    # tree.column("data_source", width=80, anchor=tk.CENTER, stretch=False)
    # tree.column("type", width=100, stretch=False)
    # tree.column("name", width=220, stretch=True)
    # tree.column("description", width=350, stretch=True)
    # tree.column("latest_update_time", width=160, anchor=tk.CENTER, stretch=False)

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
    tree._last_sort_col = "type"
    tree._last_sort_reverse = False
    
    # 启用动态列宽管理器
    column_manager.bind_resize_event()
    # 延迟配置列宽以确保父容器已完全初始化
    tree.after_idle(column_manager.configure_columns)

    return widgets 