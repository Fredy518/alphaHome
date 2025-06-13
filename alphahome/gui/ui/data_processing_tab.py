"""
数据处理标签页 UI

负责创建"数据处理"标签页的全部Tkinter控件。
"""
import tkinter as tk
from tkinter import font as tkFont
from tkinter import ttk
from typing import Dict
from ..utils.dpi_aware_ui import get_ui_factory


def create_data_processing_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"数据处理"标签页的Tkinter布局。"""
    widgets = {}
    
    # 获取DPI感知UI工厂
    ui_factory = get_ui_factory()

    # --- 顶部按钮框架 ---
    top_frame = ui_factory.create_frame(parent)
    top_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))

    refresh_button = ui_factory.create_button(
        top_frame,
        text="刷新列表",
    )
    refresh_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["processing_refresh_button"] = refresh_button

    select_all_button = ui_factory.create_button(
        top_frame,
        text="全选",
    )
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["processing_select_all_button"] = select_all_button

    deselect_all_button = ui_factory.create_button(
        top_frame,
        text="取消全选",
    )
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["processing_deselect_all_button"] = deselect_all_button

    # --- Treeview (表格) 框架 ---
    tree_frame = ui_factory.create_frame(parent)
    tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    # 使用DPI感知的Treeview，确保表头样式与其他页面一致
    columns = ("selected", "name", "description", "type")
    tree = ui_factory.create_treeview(
        tree_frame, columns=columns, show="headings"
    )

    tree.heading("selected", text="选择")
    tree.heading("name", text="名称")
    tree.heading("description", text="描述")
    tree.heading("type", text="类型")

    # 增大选择列和名称列的宽度，提供更好的显示效果
    tree.column("selected", width=80, anchor=tk.CENTER, stretch=False)
    tree.column("name", width=350, stretch=False)
    tree.column("description", width=400, stretch=True)
    tree.column("type", width=150, stretch=False)

    vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")

    tree_frame.grid_rowconfigure(0, weight=1)
    tree_frame.grid_columnconfigure(0, weight=1)

    widgets["processing_task_tree"] = tree
    tree.insert(
        "", tk.END, values=("", "正在加载, 请稍候...", "", ""), tags=("loading",)
    )

    return widgets