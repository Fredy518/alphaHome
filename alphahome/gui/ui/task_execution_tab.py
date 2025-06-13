"""
任务运行标签页 UI

负责创建"任务运行"标签页的全部Tkinter控件。
"""
import tkinter as tk
from tkinter import ttk
from typing import Dict, Any
from ..utils.layout_manager import create_task_status_column_manager
from ..utils.dpi_aware_ui import get_ui_factory

# 尝试导入 tkcalendar
try:
    from tkcalendar import DateEntry
    HAS_TKCALENDAR = True
except ImportError:
    HAS_TKCALENDAR = False


def create_task_execution_tab(
    parent: ttk.Frame, main_ui_elements: Dict[str, tk.Widget]
) -> Dict[str, tk.Widget]:
    """创建"任务运行"标签页的Tkinter布局。"""
    widgets = {}

    # --- 左侧控制面板 ---
    left_panel = ttk.Frame(parent, width=300)
    left_panel.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10), pady=5)
    left_panel.pack_propagate(False)

    # --- 执行模式框架 ---
    mode_frame = ttk.LabelFrame(left_panel, text="执行模式", padding=10)
    mode_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 10))

    exec_mode = tk.StringVar(value="智能增量")
    widgets["exec_mode"] = exec_mode

    # 事件现在由 main_window.py 绑定
    rb1 = ttk.Radiobutton(
        mode_frame,
        text="智能增量",
        variable=exec_mode,
        value="智能增量",
    )
    rb1.pack(anchor=tk.W)
    widgets["exec_mode_rb1"] = rb1

    rb2 = ttk.Radiobutton(
        mode_frame,
        text="手动增量",
        variable=exec_mode,
        value="手动增量",
    )
    rb2.pack(anchor=tk.W)
    widgets["exec_mode_rb2"] = rb2

    rb3 = ttk.Radiobutton(
        mode_frame,
        text="全量导入",
        variable=exec_mode,
        value="全量导入",
    )
    rb3.pack(anchor=tk.W)
    widgets["exec_mode_rb3"] = rb3

    # --- 日期选择框架 (仅在'手动增量'模式下显示) ---
    date_frame = ttk.LabelFrame(left_panel, text="日期范围 (手动增量)", padding=10)
    # 不立即 pack，由 main_window.py 绑定
    widgets["date_frame"] = date_frame

    ttk.Label(date_frame, text="开始日期:").grid(row=0, column=0, sticky=tk.W, pady=2)
    ttk.Label(date_frame, text="结束日期:").grid(row=1, column=0, sticky=tk.W, pady=2)

    if HAS_TKCALENDAR:
        start_date_entry = DateEntry(date_frame, date_pattern="y-m-d", width=15)
        end_date_entry = DateEntry(date_frame, date_pattern="y-m-d", width=15)
    else:
        start_date_entry = ttk.Entry(date_frame, width=17)
        end_date_entry = ttk.Entry(date_frame, width=17)

    start_date_entry.grid(row=0, column=1, padx=5, pady=2)
    end_date_entry.grid(row=1, column=1, padx=5, pady=2)
    widgets["start_date_entry"] = start_date_entry
    widgets["end_date_entry"] = end_date_entry

    # --- 运行控制框架 ---
    run_frame = ttk.Frame(left_panel, padding=(0, 10))
    run_frame.pack(side=tk.TOP, fill=tk.X, pady=(20, 0))

    # 注意：按钮的 command 现在在 main_window.py 的 bind_events 中设置
    run_button = ttk.Button(
        run_frame,
        text="▶ 运行任务",
        style="Accent.TButton",
    )
    run_button.pack(fill=tk.X, ipady=5)
    widgets["run_tasks_button"] = run_button

    stop_button = ttk.Button(
        run_frame,
        text="■ 停止任务",
    )
    stop_button.pack(fill=tk.X, pady=(5, 0))
    widgets["stop_button"] = stop_button

    # --- 右侧主显示区域 ---
    right_panel = ttk.Frame(parent)
    right_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, pady=5)

    # 将右侧区域分割为上下两部分
    paned_window = ttk.PanedWindow(right_panel, orient=tk.VERTICAL)
    paned_window.pack(fill=tk.BOTH, expand=True)

    # 上部分：任务状态 Treeview
    status_frame = ttk.LabelFrame(paned_window, text="任务状态", padding=10)
    paned_window.add(status_frame, weight=2)

    # 添加状态控制框架
    status_control_frame = ttk.Frame(status_frame)
    status_control_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 5))

    # 状态显示模式指示器
    status_mode_label = ttk.Label(status_control_frame, text="当前会话任务", foreground="blue")
    status_mode_label.pack(side=tk.LEFT)

    # 历史任务切换按钮
    history_toggle_button = ttk.Button(
        status_control_frame,
        text="显示历史任务",
        width=12
    )
    history_toggle_button.pack(side=tk.RIGHT)
    widgets["history_toggle_button"] = history_toggle_button
    widgets["status_mode_label"] = status_mode_label

    status_columns = ("name", "status", "update_time", "details")
    
    # 使用DPI感知的UI工厂创建Treeview
    ui_factory = get_ui_factory()
    task_status_tree = ui_factory.create_treeview(status_frame, columns=status_columns, show="headings")
    
    task_status_tree.heading("name", text="任务名称")
    task_status_tree.heading("status", text="最后状态")
    task_status_tree.heading("update_time", text="更新时间")
    task_status_tree.heading("details", text="详情")
    
    # 初始化动态列宽管理器
    status_column_manager = create_task_status_column_manager(task_status_tree)
    task_status_tree._column_manager = status_column_manager  # 保存引用以便后续使用
    
    # 注释掉静态列宽配置，完全由动态管理器控制
    # task_status_tree.column("name", width=200, stretch=True)
    # task_status_tree.column("status", width=80, anchor=tk.CENTER)
    # task_status_tree.column("update_time", width=140, anchor=tk.CENTER)
    # task_status_tree.column("details", width=300, stretch=True)

    vsb_status = ttk.Scrollbar(status_frame, orient="vertical", command=task_status_tree.yview)
    hsb_status = ttk.Scrollbar(status_frame, orient="horizontal", command=task_status_tree.xview)
    task_status_tree.configure(yscrollcommand=vsb_status.set, xscrollcommand=hsb_status.set)
    
    task_status_tree.grid(row=1, column=0, sticky="nsew")
    vsb_status.grid(row=1, column=1, sticky="ns")
    hsb_status.grid(row=2, column=0, sticky="ew")
    status_frame.grid_rowconfigure(1, weight=1)
    status_frame.grid_columnconfigure(0, weight=1)
    widgets["task_status_tree"] = task_status_tree
    
    # 启用动态列宽管理器
    status_column_manager.bind_resize_event()
    # 延迟配置列宽以确保父容器已完全初始化
    task_status_tree.after_idle(status_column_manager.configure_columns)

    # 下部分：日志视图
    log_frame = ttk.LabelFrame(paned_window, text="运行日志", padding=10)
    paned_window.add(log_frame, weight=1)
    
    # 使用DPI感知的Text widget
    log_view = ui_factory.create_text(log_frame, wrap=tk.WORD, state=tk.DISABLED, height=10)
    vsb_log = ttk.Scrollbar(log_frame, orient="vertical", command=log_view.yview)
    log_view.configure(yscrollcommand=vsb_log.set)
    
    log_view.grid(row=0, column=0, sticky="nsew")
    vsb_log.grid(row=0, column=1, sticky="ns")
    log_frame.grid_rowconfigure(0, weight=1)
    log_frame.grid_columnconfigure(0, weight=1)
    widgets["log_view"] = log_view


    # 初始隐藏日期选择
    # The initial state is handled by the handler after binding
    date_frame.pack_forget()

    return widgets 