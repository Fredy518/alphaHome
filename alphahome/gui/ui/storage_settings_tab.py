"""
存储设置标签页 UI

负责创建"存储设置"标签页的全部Tkinter控件。
"""
import tkinter as tk
from tkinter import ttk
from typing import Dict


def create_storage_settings_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"存储设置"标签页的Tkinter布局。"""
    widgets = {}

    # --- PostgreSQL 框架 ---
    db_frame = ttk.LabelFrame(parent, text="PostgreSQL 设置", padding="10")
    db_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

    db_labels = ["主机:", "端口:", "数据库名:", "用户名:", "密码:"]
    db_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
    for i, (label_text, key) in enumerate(zip(db_labels, db_keys)):
        lbl = ttk.Label(db_frame, text=label_text, width=12, anchor=tk.W)
        lbl.grid(row=i, column=0, padx=5, pady=2, sticky=tk.W)
        entry = ttk.Entry(
            db_frame,
            width=40,
            show="*" if key == "db_password" else None,
            state="normal",
        )
        entry.grid(row=i, column=1, padx=5, pady=2, sticky=tk.EW)
        widgets[key] = entry

    db_frame.grid_columnconfigure(1, weight=1)

    # --- Tushare 框架 ---
    ts_frame = ttk.LabelFrame(parent, text="Tushare 设置", padding="10")
    ts_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

    ts_lbl = ttk.Label(ts_frame, text="Tushare Token:", width=15, anchor=tk.W)
    ts_lbl.grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)
    ts_entry = ttk.Entry(ts_frame, width=50)
    ts_entry.grid(row=0, column=1, padx=5, pady=2, sticky=tk.EW)
    widgets["tushare_token"] = ts_entry

    ts_frame.grid_columnconfigure(1, weight=1)

    # --- 底部按钮框架 ---
    button_frame = ttk.Frame(parent, padding=(0, 10))
    button_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

    load_button = ttk.Button(
        button_frame, text="加载当前设置"
    )
    load_button.pack(side=tk.LEFT, padx=(0, 10))
    widgets["load_settings_button"] = load_button

    save_button = ttk.Button(
        button_frame, text="保存设置"
    )
    save_button.pack(side=tk.LEFT)
    widgets["save_settings_button"] = save_button

    test_db_button = ttk.Button(
        button_frame,
        text="测试数据库连接",
    )
    test_db_button.pack(side=tk.LEFT, padx=(10, 0))
    widgets["test_db_button"] = test_db_button

    # --- 状态/信息标签 ---
    info_label = ttk.Label(
        parent,
        text="注意: 数据库设置当前为只读，用于展示从配置文件加载的信息。\n保存操作将仅更新Tushare Token。",
        justify=tk.LEFT,
        wraplength=400,
    )
    info_label.pack(side=tk.TOP, fill=tk.X, pady=(10, 0))
    widgets["settings_info_label"] = info_label

    return widgets 