"""
存储设置标签页 UI

负责创建"存储设置"标签页的全部Tkinter控件。
"""
import tkinter as tk
from tkinter import ttk
from typing import Dict
from ..utils.dpi_manager import get_dpi_manager, DisplayMode
from ..utils.dpi_aware_ui import get_ui_factory


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
            show="*" if key == "db_password" else "", # type: ignore
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

    # --- 显示设置框架 ---
    display_frame = ttk.LabelFrame(parent, text="显示设置", padding="10")
    display_frame.pack(side=tk.TOP, fill=tk.X, pady=5)
    
    # 获取DPI管理器和UI工厂
    dpi_manager = get_dpi_manager()
    ui_factory = get_ui_factory()
    
    # 当前显示信息
    info_text = f"当前分辨率: {dpi_manager.dpi_info.logical_resolution[0]}x{dpi_manager.dpi_info.logical_resolution[1]}\n"
    info_text += f"DPI缩放: {dpi_manager.dpi_info.scale_factor:.0%}\n"
    info_text += f"高DPI环境: {'是' if dpi_manager.dpi_info.is_high_dpi else '否'}"
    
    display_info_label = ui_factory.create_label(display_frame, text=info_text, justify=tk.LEFT)
    display_info_label.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))
    widgets["display_info_label"] = display_info_label
    
    # 显示模式选择
    mode_label = ui_factory.create_label(display_frame, text="显示模式:", width=12, anchor=tk.W)
    mode_label.grid(row=1, column=0, padx=5, pady=2, sticky=tk.W)
    
    mode_values = [
        ("自动检测", DisplayMode.AUTO.value),
        ("标准模式", DisplayMode.STANDARD.value),
        ("高DPI模式", DisplayMode.HIGH_DPI.value),
        ("4K优化模式", DisplayMode.UHD_4K.value)
    ]
    
    mode_combo = ui_factory.create_combobox(
        display_frame, 
        values=[item[0] for item in mode_values],
        state="readonly",
        width=20
    )
    mode_combo.grid(row=1, column=1, padx=5, pady=2, sticky=tk.W)
    
    # 设置当前值
    current_mode = dpi_manager.current_mode.value
    for display_name, mode_value in mode_values:
        if mode_value == current_mode:
            mode_combo.set(display_name)
            break
    
    widgets["display_mode_combo"] = mode_combo
    widgets["display_mode_values"] = mode_values  # 保存映射关系
    
    # 推荐模式提示
    recommended_mode = dpi_manager.recommend_display_mode()
    recommended_text = f"推荐模式: "
    for display_name, mode_value in mode_values:
        if mode_value == recommended_mode.value:
            recommended_text += display_name
            break
    
    recommend_label = ui_factory.create_label(display_frame, text=recommended_text, foreground="blue")
    recommend_label.grid(row=2, column=0, columnspan=2, sticky="w", pady=(5, 0))
    widgets["display_recommend_label"] = recommend_label
    
    # 应用按钮和重启按钮
    button_subframe = ui_factory.create_frame(display_frame)
    button_subframe.grid(row=3, column=0, columnspan=2, pady=(10, 0), sticky="w")
    
    apply_display_button = ui_factory.create_button(
        button_subframe,
        text="应用显示设置"
    )
    apply_display_button.pack(side=tk.LEFT, padx=(0, 10))
    widgets["apply_display_button"] = apply_display_button
    
    restart_app_button = ui_factory.create_button(
        button_subframe,
        text="重启应用"
    )
    restart_app_button.pack(side=tk.LEFT)
    widgets["restart_app_button"] = restart_app_button

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