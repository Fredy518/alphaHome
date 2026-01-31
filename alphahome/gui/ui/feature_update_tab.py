"""
特征更新标签页 UI

负责创建"特征更新"标签页的全部Tkinter控件。
用于管理和刷新 features 模块中的物化视图。
"""
import tkinter as tk
from tkinter import font as tkFont
from tkinter import ttk
from typing import Dict
from ..utils.dpi_aware_ui import get_ui_factory


def create_feature_update_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"特征更新"标签页的Tkinter布局。"""
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
    widgets["feature_refresh_button"] = refresh_button

    select_all_button = ui_factory.create_button(
        top_frame,
        text="全选",
    )
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["feature_select_all_button"] = select_all_button

    deselect_all_button = ui_factory.create_button(
        top_frame,
        text="取消全选",
    )
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["feature_deselect_all_button"] = deselect_all_button

    # --- 刷新操作按钮 ---
    refresh_selected_button = ui_factory.create_button(
        top_frame,
        text="刷新选中视图",
    )
    refresh_selected_button.pack(side=tk.LEFT, padx=(20, 5))
    widgets["feature_refresh_selected_button"] = refresh_selected_button

    create_missing_button = ui_factory.create_button(
        top_frame,
        text="创建缺失视图",
    )
    create_missing_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets["feature_create_missing_button"] = create_missing_button

    # --- 分类过滤下拉框 ---
    filter_label = ui_factory.create_label(top_frame, text="分类筛选:")
    filter_label.pack(side=tk.LEFT, padx=(20, 5))

    category_var = tk.StringVar(value="全部")
    category_combobox = ttk.Combobox(
        top_frame,
        textvariable=category_var,
        values=["全部", "market", "stock", "index", "industry", "fund", "macro", "derivatives"],
        state="readonly",
        width=15
    )
    category_combobox.pack(side=tk.LEFT, padx=(0, 5))
    widgets["feature_category_var"] = category_var
    widgets["feature_category_combobox"] = category_combobox

    # --- Treeview (表格) 框架 ---
    tree_frame = ui_factory.create_frame(parent)
    tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    # 使用DPI感知的Treeview，确保表头样式与其他页面一致
    columns = ("selected", "name", "description", "category", "status", "row_count", "last_refresh")
    tree = ui_factory.create_treeview(
        tree_frame, columns=columns, show="headings"
    )

    tree.heading("selected", text="选择")
    tree.heading("name", text="特征名称")
    tree.heading("description", text="描述")
    tree.heading("category", text="分类")
    tree.heading("status", text="状态")
    tree.heading("row_count", text="行数")
    tree.heading("last_refresh", text="最后刷新")

    # 设置列宽
    tree.column("selected", width=60, anchor=tk.CENTER, stretch=False)
    tree.column("name", width=200, stretch=False)
    tree.column("description", width=300, stretch=True)
    tree.column("category", width=100, anchor=tk.CENTER, stretch=False)
    tree.column("status", width=80, anchor=tk.CENTER, stretch=False)
    tree.column("row_count", width=80, anchor=tk.E, stretch=False)
    tree.column("last_refresh", width=150, anchor=tk.CENTER, stretch=False)

    vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")

    tree_frame.grid_rowconfigure(0, weight=1)
    tree_frame.grid_columnconfigure(0, weight=1)

    widgets["feature_tree"] = tree
    tree.insert(
        "", tk.END, values=("", "正在加载, 请稍候...", "", "", "", "", ""), tags=("loading",)
    )

    # --- 底部状态/信息框架 ---
    bottom_frame = ui_factory.create_frame(parent)
    bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))

    status_label = ui_factory.create_label(bottom_frame, text="就绪")
    status_label.pack(side=tk.LEFT, padx=(0, 10))
    widgets["feature_status_label"] = status_label

    # 统计信息
    stats_label = ui_factory.create_label(bottom_frame, text="已注册: 0 | 已创建: 0 | 待创建: 0")
    stats_label.pack(side=tk.RIGHT, padx=(10, 0))
    widgets["feature_stats_label"] = stats_label

    return widgets
