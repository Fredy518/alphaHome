"""
数据采集标签页 UI

负责创建"数据采集"标签页的全部Tkinter控件。
"""
import tkinter as tk
from tkinter import font as tkFont
from tkinter import ttk
from typing import Dict

_ALL_TYPES_OPTION = "所有类型"


def create_data_collection_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"数据采集"标签页的Tkinter布局。"""
    widgets = {}

    # --- 顶部按钮和过滤框架 ---
    top_frame = ttk.Frame(parent)
    top_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))

    # --- 左侧按钮 ---
    refresh_button = ttk.Button(
        top_frame,
        text="刷新列表",
    )
    refresh_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_refresh_button"] = refresh_button

    select_all_button = ttk.Button(
        top_frame,
        text="全选",
    )
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_select_all_button"] = select_all_button

    deselect_all_button = ttk.Button(
        top_frame,
        text="取消全选",
    )
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["collection_deselect_all_button"] = deselect_all_button

    # --- 右侧过滤 ---
    filter_frame = ttk.Frame(top_frame)
    filter_frame.pack(side=tk.RIGHT, padx=(10, 0))

    ttk.Label(filter_frame, text="名称过滤:").pack(side=tk.LEFT, padx=(0, 5))
    name_filter_entry = ttk.Entry(filter_frame, width=20)
    name_filter_entry.pack(side=tk.LEFT, padx=(0, 10))
    widgets["collection_filter_entry"] = name_filter_entry

    ttk.Label(filter_frame, text="数据源过滤:").pack(side=tk.LEFT, padx=(0, 5))
    data_source_filter_combo = ttk.Combobox(
        filter_frame, values=[_ALL_TYPES_OPTION], state="readonly", width=10
    )
    data_source_filter_combo.set(_ALL_TYPES_OPTION)
    data_source_filter_combo.pack(side=tk.LEFT, padx=(0, 10))
    widgets["collection_data_source_combo"] = data_source_filter_combo

    ttk.Label(filter_frame, text="类型过滤:").pack(side=tk.LEFT, padx=(0, 5))
    type_filter_combo = ttk.Combobox(
        filter_frame, values=[_ALL_TYPES_OPTION], state="readonly", width=12
    )
    type_filter_combo.set(_ALL_TYPES_OPTION)
    type_filter_combo.pack(side=tk.LEFT)
    widgets["collection_task_type_combo"] = type_filter_combo

    # --- Treeview (表格) 框架 ---
    tree_frame = ttk.Frame(parent)
    tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    tree_font = tkFont.Font(family="Microsoft YaHei UI", size=10)
    style = ttk.Style()
    default_rowheight = int(tree_font.metrics("linespace") * 1.4)
    style.configure("Collection.Treeview", font=tree_font, rowheight=default_rowheight)
    style.configure("Collection.Treeview.Heading", font=tree_font)

    columns = ("selected", "data_source", "type", "name", "description", "latest_update_time")
    tree = ttk.Treeview(
        tree_frame, columns=columns, show="headings", style="Collection.Treeview"
    )

    tree.heading("selected", text="选择")
    tree.heading("data_source", text="数据源")
    tree.heading("type", text="类型")
    tree.heading("name", text="名称")
    tree.heading("description", text="描述")
    tree.heading("latest_update_time", text="更新时间")

    tree.column("selected", width=50, anchor=tk.CENTER, stretch=False)
    tree.column("data_source", width=80, anchor=tk.CENTER, stretch=False)
    tree.column("type", width=100, stretch=False)
    tree.column("name", width=220, stretch=False)
    tree.column("description", width=350, stretch=True)
    tree.column("latest_update_time", width=160, anchor=tk.CENTER, stretch=False)

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

    return widgets 