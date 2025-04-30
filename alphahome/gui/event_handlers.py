import tkinter as tk
from tkinter import ttk, messagebox, font as tkFont
from datetime import datetime, timedelta
from . import controller
from typing import Dict, Any, List
import operator # For sorting
import urllib.parse # For URL parsing/building
import logging

# 尝试导入 tkcalendar
try:
    from tkcalendar import DateEntry
    HAS_TKCALENDAR = True
except ImportError:
    HAS_TKCALENDAR = False
    print("提示: 未找到 tkcalendar 库 (pip install tkcalendar)，日期将使用普通输入框。")


# --- Module level storage for filtering and sorting ---
_full_task_list_data: List[Dict[str, Any]] = [] # Stores the complete list from controller
_current_sort_col: str = 'name' # Default sort column
_current_sort_reverse: bool = False # Default sort direction
_ALL_TYPES_OPTION = "所有类型"

# --- Tkinter 控件创建和布局 (骨架) ---

def create_task_list_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"任务列表"标签页的 Tkinter 布局 (含过滤和排序)"""
    widgets = {} # 存储此标签页创建的控件
    hsb = None # 防御性初始化

    # --- 顶部按钮框架 ---
    top_frame = ttk.Frame(parent)
    top_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))

    refresh_button = ttk.Button(top_frame, text="刷新列表", command=handle_refresh_tasks)
    refresh_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets['refresh_button'] = refresh_button

    select_all_button = ttk.Button(top_frame, text="全选", command=lambda w=widgets: handle_select_all(w))
    select_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets['select_all_button'] = select_all_button

    deselect_all_button = ttk.Button(top_frame, text="取消全选", command=lambda w=widgets: handle_deselect_all(w))
    deselect_all_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets['deselect_all_button'] = deselect_all_button

    # 添加类型过滤下拉框
    ttk.Label(top_frame, text=" 类型过滤:").pack(side=tk.LEFT, padx=(15, 5))
    type_filter_combo = ttk.Combobox(top_frame, values=[_ALL_TYPES_OPTION], state="readonly", width=15)
    type_filter_combo.set(_ALL_TYPES_OPTION) # 默认选中
    type_filter_combo.pack(side=tk.LEFT, padx=(0, 5))
    type_filter_combo.bind("<<ComboboxSelected>>", lambda event: handle_type_filter_change(widgets)) # 绑定事件
    widgets['type_filter_combo'] = type_filter_combo

    # --- Treeview (表格) 框架 ---
    tree_frame = ttk.Frame(parent)
    tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    # 定义字体
    tree_font = tkFont.Font(family="Microsoft YaHei UI", size=10) # 或 "Segoe UI"

    # 定义列
    columns = ('selected', 'type', 'name', 'description', 'update_time')
    tree = ttk.Treeview(tree_frame, columns=columns, show='headings') # show='headings' 隐藏第一列空白列

    # 应用字体到 Treeview (通过 style)
    style = ttk.Style()
    default_rowheight = int(tree_font.metrics("linespace") * 1.3) # 稍微再调大一点行高
    style.configure("Treeview", font=tree_font, rowheight=default_rowheight)
    style.configure("Treeview.Heading", font=tree_font) # 应用到表头字体

    # 定义列标题和宽度
    tree.heading('selected', text='选择')
    tree.heading('type', text='类型', command=lambda: handle_sort_column(widgets, 'type'))
    tree.heading('name', text='名称', command=lambda: handle_sort_column(widgets, 'name'))
    tree.heading('description', text='描述', command=lambda: handle_sort_column(widgets, 'description'))
    tree.heading('update_time', text='更新时间', command=lambda: handle_sort_column(widgets, 'update_time'))

    tree.column('selected', width=50, anchor=tk.CENTER, stretch=False)
    tree.column('type', width=100, stretch=False)
    tree.column('name', width=200, stretch=False)
    tree.column('description', width=300, stretch=True)
    tree.column('update_time', width=150, anchor=tk.CENTER)

    # 添加滚动条
    vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    # 布局 Treeview 和滚动条
    tree.grid(row=0, column=0, sticky='nsew')
    vsb.grid(row=0, column=1, sticky='ns')
    hsb.grid(row=1, column=0, sticky='ew')

    tree_frame.grid_rowconfigure(0, weight=1)
    tree_frame.grid_columnconfigure(0, weight=1)

    # 绑定点击事件来切换选择状态 (回调函数待实现具体逻辑)
    tree.bind('<ButtonRelease-1>', lambda event: handle_task_tree_click(event, tree)) # 使用 ButtonRelease 更可靠

    widgets['task_tree'] = tree # 存储 Treeview 引用

    # 初始加载提示 (将在 process_controller_update 中被实际数据替换)
    tree.insert('', tk.END, values=('', '', '正在加载...', '', ''), tags=('loading',))

    # Add attributes to tree for sorting state
    tree._last_sort_col = _current_sort_col
    tree._last_sort_reverse = _current_sort_reverse

    return widgets

def create_storage_settings_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"存储设置"标签页的 Tkinter 布局"""
    widgets = {}

    # --- PostgreSQL 框架 ---
    db_frame = ttk.LabelFrame(parent, text="PostgreSQL 设置", padding="10")
    db_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

    db_labels = ["主机:", "端口:", "数据库名:", "用户名:", "密码:"]
    db_keys = ['db_host', 'db_port', 'db_name', 'db_user', 'db_password']
    for i, (label_text, key) in enumerate(zip(db_labels, db_keys)):
        lbl = ttk.Label(db_frame, text=label_text, width=12, anchor=tk.W)
        lbl.grid(row=i, column=0, padx=5, pady=2, sticky=tk.W)
        # 重新启用数据库输入框
        entry = ttk.Entry(db_frame, width=40, show='*' if key == 'db_password' else None, state='normal') # state='normal'
        entry.grid(row=i, column=1, padx=5, pady=2, sticky=tk.EW)
        widgets[key] = entry

    db_frame.grid_columnconfigure(1, weight=1)

    # --- Tushare 框架 ---
    ts_frame = ttk.LabelFrame(parent, text="Tushare 设置", padding="10")
    ts_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

    ts_lbl = ttk.Label(ts_frame, text="Tushare Token:", width=15, anchor=tk.W)
    ts_lbl.grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)
    # Tushare Token 输入框保持可用
    ts_entry = ttk.Entry(ts_frame, width=50)
    ts_entry.grid(row=0, column=1, padx=5, pady=2, sticky=tk.EW)
    widgets['tushare_token'] = ts_entry

    ts_frame.grid_columnconfigure(1, weight=1)

    # --- 底部按钮框架 ---
    button_frame = ttk.Frame(parent, padding=(0, 10))
    button_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

    # 加载按钮现在只加载 Tushare Token
    load_button = ttk.Button(button_frame, text="加载当前设置", command=lambda: handle_load_settings(widgets))
    load_button.pack(side=tk.LEFT, padx=(0, 10))
    widgets['load_settings_button'] = load_button

    # 保存按钮现在只保存 Tushare Token
    save_button = ttk.Button(button_frame, text="保存设置", command=lambda: handle_save_settings(widgets))
    save_button.pack(side=tk.LEFT)
    widgets['save_settings_button'] = save_button

    return widgets

def create_task_execution_tab(parent: ttk.Frame, main_ui_elements: Dict[str, tk.Widget]) -> Dict[str, tk.Widget]:
    """创建"任务运行"标签页的 Tkinter 布局"""
    widgets = {} # 本地widgets字典，只包含此tab创建的控件

    # --- 运行控制框架 ---
    control_frame = ttk.LabelFrame(parent, text="运行控制", padding="10")
    control_frame.pack(side=tk.TOP, fill=tk.X, pady=5)

    # --- 第 0 行: 执行模式 ---
    ttk.Label(control_frame, text="执行模式:").grid(row=0, column=0, padx=(0, 5), pady=5, sticky=tk.W)
    modes = ["智能增量", "手动增量", "全量导入"]
    exec_mode_combo = ttk.Combobox(control_frame, values=modes, state="readonly", width=12) # 调整宽度
    exec_mode_combo.set(modes[0]) # 默认选中第一个
    exec_mode_combo.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
    # 绑定模式切换事件，使用 lambda 传递 widgets
    exec_mode_combo.bind("<<ComboboxSelected>>", lambda event: handle_exec_mode_change(widgets))
    widgets['exec_mode_combo'] = exec_mode_combo

    # --- 第 1 行: 手动增量日期 (初始隐藏) ---
    manual_date_label = ttk.Label(control_frame, text="开始:") # 修改标签文本
    manual_date_label.grid(row=1, column=0, padx=(0, 5), pady=(5,5), sticky=tk.W) # 移到 row=1, col=0, 增加 pady
    widgets['manual_date_label'] = manual_date_label

    if HAS_TKCALENDAR:
        manual_date_entry = DateEntry(control_frame, width=12, background='darkblue',
                                      foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd',
                                      state='disabled')
        manual_date_entry.grid(row=1, column=1, padx=5, pady=(5,5), sticky=tk.W) # 移到 row=1, col=1, 增加 pady
    else:
        manual_date_var = tk.StringVar(value=datetime.now().strftime('%Y-%m-%d'))
        manual_date_entry = ttk.Entry(control_frame, width=12, state='disabled', textvariable=manual_date_var)
        manual_date_entry.grid(row=1, column=1, padx=5, pady=(5,5), sticky=tk.W) # 移到 row=1, col=1, 增加 pady
    widgets['manual_date_entry'] = manual_date_entry

    manual_end_date_label = ttk.Label(control_frame, text="结束:") # 修改标签文本
    manual_end_date_label.grid(row=1, column=2, padx=(15, 5), pady=(5,5), sticky=tk.W) # 移到 row=1, col=2, 增加 pady
    widgets['manual_end_date_label'] = manual_end_date_label

    today_str = datetime.now().strftime('%Y-%m-%d')
    if HAS_TKCALENDAR:
        manual_end_date_entry = DateEntry(control_frame, width=12, background='darkblue',
                                          foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd',
                                          state='disabled')
        manual_end_date_entry.set_date(datetime.now())
        manual_end_date_entry.grid(row=1, column=3, padx=5, pady=(5,5), sticky=tk.W) # 移到 row=1, col=3, 增加 pady
    else:
        manual_end_date_var = tk.StringVar(value=today_str)
        manual_end_date_entry = ttk.Entry(control_frame, width=12, state='disabled', textvariable=manual_end_date_var)
        manual_end_date_entry.grid(row=1, column=3, padx=5, pady=(5,5), sticky=tk.W) # 移到 row=1, col=3, 增加 pady
    widgets['manual_end_date_entry'] = manual_end_date_entry

    # --- 第 2 行: 运行/停止按钮 (靠右) ---
    button_container = ttk.Frame(control_frame)
    button_container.grid(row=0, column=4, rowspan=3, padx=(20, 0), sticky='e') 

    run_button = ttk.Button(button_container, text="运行选中任务", command=lambda: handle_run_tasks(main_ui_elements))
    run_button.pack(side=tk.LEFT, padx=(0, 5))
    widgets['run_button'] = run_button

    stop_button = ttk.Button(button_container, text="停止执行", state='disabled', command=lambda: handle_stop_tasks(main_ui_elements))
    stop_button.pack(side=tk.LEFT, padx=(0, 0))
    widgets['stop_button'] = stop_button

    control_frame.grid_columnconfigure(4, weight=1)

    # 初始隐藏日期控件 (需要从本地 widgets 获取)
    handle_exec_mode_change(widgets) # handle_exec_mode_change 仍然使用本地 widgets

    # --- 运行状态框架 ---
    run_frame = ttk.LabelFrame(parent, text="运行状态", padding="10")
    run_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=5)

    run_columns = ('type', 'name', 'status', 'progress', 'start', 'end')
    run_tree = ttk.Treeview(run_frame, columns=run_columns, show='headings')

    run_tree.heading('type', text='类型')
    run_tree.heading('name', text='名称')
    run_tree.heading('status', text='状态')
    run_tree.heading('progress', text='进度')
    run_tree.heading('start', text='开始时间')
    run_tree.heading('end', text='结束时间')

    run_tree.column('type', width=100, stretch=False)
    run_tree.column('name', width=200, stretch=True)
    run_tree.column('status', width=100, stretch=False)
    run_tree.column('progress', width=80, anchor=tk.E, stretch=False)
    run_tree.column('start', width=150, anchor=tk.CENTER, stretch=False)
    run_tree.column('end', width=150, anchor=tk.CENTER, stretch=False)

    run_vsb = ttk.Scrollbar(run_frame, orient="vertical", command=run_tree.yview)
    run_hsb = ttk.Scrollbar(run_frame, orient="horizontal", command=run_tree.xview)
    run_tree.configure(yscrollcommand=run_vsb.set, xscrollcommand=run_hsb.set)

    run_tree.grid(row=0, column=0, sticky='nsew')
    run_vsb.grid(row=0, column=1, sticky='ns')
    run_hsb.grid(row=1, column=0, sticky='ew')

    run_frame.grid_rowconfigure(0, weight=1)
    run_frame.grid_columnconfigure(0, weight=1)

    widgets['run_tree'] = run_tree

    return widgets

def create_task_log_tab(parent: ttk.Frame) -> Dict[str, tk.Widget]:
    """创建"任务日志"标签页的 Tkinter 布局"""
    widgets = {}

    log_frame = ttk.Frame(parent) # 使用普通 Frame，不需要 LabelFrame
    log_frame.pack(fill=tk.BOTH, expand=True)

    log_text = tk.Text(log_frame, wrap=tk.WORD, state='disabled',
                       background='#F0F0F0', # 使用标准浅灰色
                       foreground='black',
                       borderwidth=1, relief="sunken") # relief=sunken 效果类似
    vsb = ttk.Scrollbar(log_frame, orient="vertical", command=log_text.yview)
    log_text.configure(yscrollcommand=vsb.set)

    # 使用 grid 布局 Text 和 Scrollbar
    log_text.grid(row=0, column=0, sticky='nsew')
    vsb.grid(row=0, column=1, sticky='ns')

    # 配置 grid 权重使 Text 控件可伸缩
    log_frame.grid_rowconfigure(0, weight=1)
    log_frame.grid_columnconfigure(0, weight=1)

    widgets['log_text'] = log_text

    return widgets


# --- Tkinter 事件回调函数 (骨架) ---
# --- Tkinter 事件回调函数 ---

def handle_refresh_tasks(main_ui_elements: Dict[str, tk.Widget]):
    """处理刷新列表按钮点击"""
    print("回调：刷新任务列表...")
    controller.request_task_list()
    # 更新状态栏
    statusbar = main_ui_elements.get('statusbar')
    if statusbar: statusbar.config(text="正在刷新任务列表...")

def handle_select_all(main_ui_elements: Dict[str, tk.Widget]):
    """处理全选按钮点击 (仅选择当前可见的任务)"""
    print("回调：请求全选可见任务...")
    tree = main_ui_elements.get('task_tree')
    if tree and isinstance(tree, ttk.Treeview):
        visible_item_ids = tree.get_children('')
        visible_task_names = []
        for item_id in visible_item_ids:
            try:
                values = tree.item(item_id, 'values')
                if values and len(values) > 2:
                    visible_task_names.append(values[2])
            except Exception as e:
                print(f"获取任务名称时出错 (item: {item_id}): {e}")
        print(f"  将选择 {len(visible_task_names)} 个可见任务: {visible_task_names}")
        controller.request_select_specific(visible_task_names)
    else:
        print("错误：无法在 widgets 中找到任务列表 Treeview ('task_tree')。")

def handle_deselect_all(main_ui_elements: Dict[str, tk.Widget]):
    """处理取消全选按钮点击 (仅取消选择当前可见的任务)"""
    print("回调：请求取消全选可见任务...")
    tree = main_ui_elements.get('task_tree')
    if tree and isinstance(tree, ttk.Treeview):
        visible_item_ids = tree.get_children('')
        visible_task_names = []
        for item_id in visible_item_ids:
            try:
                values = tree.item(item_id, 'values')
                if values and len(values) > 2:
                    visible_task_names.append(values[2])
            except Exception as e:
                print(f"获取任务名称时出错 (item: {item_id}): {e}")
        print(f"  将取消选择 {len(visible_task_names)} 个可见任务: {visible_task_names}")
        controller.request_deselect_specific(visible_task_names)
    else:
        print("错误：无法在 widgets 中找到任务列表 Treeview ('task_tree')。")

def handle_task_tree_click(event, tree: ttk.Treeview):
    """处理任务列表 Treeview 点击事件以切换选择状态"""
    region = tree.identify("region", event.x, event.y)
    # 只在点击单元格时触发，避免点击标题或其他区域
    if region == "cell":
        item_id = tree.identify_row(event.y)
        if item_id:
            # 获取当前行的值，我们需要任务名称来查找索引
            # 假设任务名称在第3列 (index 2)
            try:
                current_values = tree.item(item_id, 'values')
                task_name = current_values[2] # 获取任务名称

                # 从 controller 获取缓存的任务列表来找到对应的索引
                current_task_list = controller.get_cached_task_list()

                task_index = -1
                if current_task_list:
                    for idx, task_info in enumerate(current_task_list):
                        if task_info.get('name') == task_name:
                            task_index = idx
                            break

                if task_index != -1:
                    print(f"回调：切换任务 '{task_name}' (索引 {task_index}) 的选择状态")
                    controller.toggle_task_selection(task_index)
                else:
                     print(f"错误：无法在 controller 缓存中找到任务 '{task_name}'")
                     # 可以考虑给用户一个提示，或者只是记录日志

            except IndexError:
                print("错误：处理 Treeview 点击时无法获取任务名称 (列索引可能不正确)。")
            except Exception as e:
                print(f"处理任务列表点击时发生意外错误: {e}")

def handle_load_settings(main_ui_elements: Dict[str, tk.Widget]):
    """加载配置并填充存储设置标签页的各个字段"""
    print("回调：加载设置...")
    try:
        # 从控制器获取设置 ({'database': {'url': ...}, 'api': {'token': ...}})
        settings = controller.get_current_settings()
        print(f"DEBUG: 从控制器获取的设置: {settings}")

        # 检查必要控件是否存在
        db_keys = ['db_host', 'db_port', 'db_name', 'db_user', 'db_password']
        token_key = 'tushare_token'
        all_keys = db_keys + [token_key]
        missing_keys = [k for k in all_keys if k not in main_ui_elements or not hasattr(main_ui_elements[k], 'get')]
        
        if missing_keys:
            messagebox.showwarning("加载错误", f"存储设置界面的输入框控件未找到: {', '.join(missing_keys)}")
            return

        # 清空所有输入框
        for key in all_keys:
            try:
                main_ui_elements[key].config(state=tk.NORMAL) # Ensure enabled before deleting
                main_ui_elements[key].delete(0, tk.END)
            except tk.TclError as e:
                 print(f"警告：清空输入框 {key} 时出错: {e} (控件可能已被禁用或销毁?)")

        # 解析并填充
        db_config = settings.get('database', {})
        api_config = settings.get('api', {})
        db_url = db_config.get('url') # 可能为 None 或空字符串
        tushare_token = api_config.get('tushare_token', '') # 默认空字符串

        db_values = {
            'db_host': '',
            'db_port': '',
            'db_name': '',
            'db_user': '',
            'db_password': ''
        }

        statusbar = main_ui_elements.get('statusbar')
        status_msg = ""

        if db_url:
            print(f"DEBUG: 尝试解析 DB URL: {db_url}")
            try:
                parsed_url = urllib.parse.urlparse(db_url)
                db_values['db_host'] = parsed_url.hostname or ''
                db_values['db_port'] = str(parsed_url.port or '') # Port might be None
                db_values['db_user'] = parsed_url.username or ''
                db_values['db_password'] = parsed_url.password or ''
                db_values['db_name'] = parsed_url.path.lstrip('/') or ''
                print(f"DEBUG: 解析成功: {db_values}")
                status_msg = "数据库设置已加载。"
            except Exception as e:
                print(f"错误：解析 URL \"{db_url}\" 失败: {e}")
                messagebox.showwarning("URL 解析错误", f"加载的数据库 URL 无法解析:\n{db_url}\n错误: {e}\n请检查格式或重新输入。")
                status_msg = "数据库 URL 解析失败，请重新输入。"
                # 保留 db_values 为空字符串，因为解析失败
        else:
             print("DEBUG: 未找到 DB URL 配置。")
             # 如果 URL 为空，db_values 保持为空字符串

        # 填充数据库字段
        for key in db_keys:
            main_ui_elements[key].insert(0, db_values[key])

        # 填充 Token 字段
        main_ui_elements[token_key].insert(0, tushare_token)
        if tushare_token:
             print("DEBUG: Tushare Token 已加载。")
             if status_msg: status_msg += " " # 添加空格分隔
             status_msg += "Tushare Token 已加载。"
        else:
             print("DEBUG: 未找到 Tushare Token 配置。")

        # 设置状态栏消息
        if not db_url and not tushare_token:
            status_msg = "未找到配置文件或配置为空，请输入配置信息。"
        elif not status_msg: # 如果 URL 和 Token 都为空，但前面没有设置消息
             status_msg = "设置已加载 (空)。"

        if statusbar:
            statusbar.config(text=status_msg)
        print(f"INFO: {status_msg}")

    except Exception as e:
        print(f"加载设置时发生未预料的错误: {e}")
        messagebox.showerror("加载错误", f"加载设置时出错: {e}")
        if main_ui_elements.get('statusbar'):
            main_ui_elements['statusbar'].config(text=f"加载设置错误: {e}")

def handle_save_settings(main_ui_elements: Dict[str, tk.Widget]):
    """从界面提取 Token 和 DB 字段，构建 URL 并请求保存"""
    print("回调：请求保存设置...")
    try:
        settings_to_send = {}

        # 1. 获取 Tushare Token
        token_key = 'tushare_token'
        if token_key in main_ui_elements and hasattr(main_ui_elements[token_key], 'get'):
            settings_to_send['tushare_token'] = main_ui_elements[token_key].get()
        else:
            print(f"警告：保存设置时缺少控件或 get 方法 '{token_key}'")
            messagebox.showerror("内部错误", f"无法找到 Tushare Token 输入控件。")
            return

        # 2. 获取数据库字段值
        db_values = {}
        db_keys = ['db_host', 'db_port', 'db_name', 'db_user', 'db_password']
        all_db_fields_present = True
        for key in db_keys:
            if key in main_ui_elements and hasattr(main_ui_elements[key], 'get'):
                db_values[key] = main_ui_elements[key].get()
            else:
                print(f"警告：保存设置时缺少控件或 get 方法 '{key}'")
                messagebox.showerror("内部错误", f"无法找到数据库输入控件 '{key}'。")
                all_db_fields_present = False
                db_values[key] = '' # Assign empty on error to allow URL construction attempt

        # 3. 构建数据库 URL (如果所有字段都获取到了)
        # 允许用户清空所有字段来表示不设置URL
        new_db_url = ''
        if any(db_values.values()): # Only construct URL if at least one field is non-empty
            # Simple validation: require host and dbname at minimum?
            if not db_values['db_host'] or not db_values['db_name']:
                messagebox.showerror("输入错误", "数据库主机和数据库名是必填项。")
                return
            
            user = db_values['db_user']
            password = db_values['db_password']
            host = db_values['db_host']
            port = db_values['db_port']
            dbname = db_values['db_name']

            # Basic URL construction (consider edge cases like empty password)
            user_pass = f"{urllib.parse.quote(user)}:{urllib.parse.quote(password)}@" if user or password else ""
            host_part = host
            if port:
                host_part += f":{port}"
            
            # Use urllib.parse.urlunparse for safer construction if needed, but manual is often clearer here
            new_db_url = f"postgresql://{user_pass}{host_part}/{urllib.parse.quote(dbname)}"
            print(f"DEBUG: Constructed DB URL: {new_db_url}")
        else:
            print("DEBUG: All DB fields empty, setting URL to empty string.")


        settings_to_send['database_url'] = new_db_url

        # 4. 调用 controller 异步保存设置 (不再检查返回值)
        controller.save_settings(settings_to_send)
        print("保存请求已发送至后台，请查看日志和状态栏确认结果。") # 更新提示

    except tk.TclError as e:
        messagebox.showerror("界面错误", f"获取设置输入框内容时出错: {e}")
    except Exception as e:
        messagebox.showerror("保存错误", f"保存设置时发生意外错误: {e}")

def handle_exec_mode_change(widgets: Dict[str, tk.Widget]):
    """处理执行模式下拉框选择变化的事件。"""
    selected_mode = widgets['exec_mode_combo'].get()

    manual_date_label = widgets.get('manual_date_label')
    manual_date_entry = widgets.get('manual_date_entry')
    manual_end_date_label = widgets.get('manual_end_date_label') # 获取结束日期标签
    manual_end_date_entry = widgets.get('manual_end_date_entry') # 获取结束日期输入

    if manual_date_label and manual_date_entry and manual_end_date_label and manual_end_date_entry:
        if selected_mode == "手动增量":
            # 显示开始和结束日期控件
            manual_date_label.grid()
            manual_date_entry.config(state='normal')
            manual_date_entry.grid()
            manual_end_date_label.grid() # 显示结束日期标签
            manual_end_date_entry.config(state='normal') # 启用结束日期输入
            manual_end_date_entry.grid() # 显示结束日期输入
            # 如果使用的是普通 Entry，可能需要显式调用 grid()
            if not HAS_TKCALENDAR:
                manual_date_entry.grid()
                manual_end_date_entry.grid()

        else:
            # 隐藏开始和结束日期控件
            manual_date_label.grid_remove()
            manual_date_entry.config(state='disabled')
            manual_date_entry.grid_remove()
            manual_end_date_label.grid_remove() # 隐藏结束日期标签
            manual_end_date_entry.config(state='disabled') # 禁用结束日期输入
            manual_end_date_entry.grid_remove() # 隐藏结束日期输入
    else:
        print("错误: 未找到手动日期控件。")

def handle_run_tasks(main_ui_elements: Dict[str, tk.Widget]):
    """处理"运行选中任务"按钮点击事件。"""
    task_tree = main_ui_elements.get('task_tree')
    exec_mode_combo = main_ui_elements.get('exec_mode_combo')
    manual_date_entry = main_ui_elements.get('manual_date_entry')
    manual_end_date_entry = main_ui_elements.get('manual_end_date_entry') # 获取结束日期控件
    run_button = main_ui_elements.get('run_button')
    stop_button = main_ui_elements.get('stop_button')
    statusbar = main_ui_elements.get('statusbar')

    if not all([task_tree, exec_mode_combo, manual_date_entry, manual_end_date_entry, run_button, stop_button, statusbar]):
        messagebox.showerror("错误", "界面元素不完整，无法运行任务。")
        # 增加调试信息
        missing = []
        if not task_tree: missing.append('task_tree')
        if not exec_mode_combo: missing.append('exec_mode_combo')
        if not manual_date_entry: missing.append('manual_date_entry')
        if not manual_end_date_entry: missing.append('manual_end_date_entry')
        if not run_button: missing.append('run_button')
        if not stop_button: missing.append('stop_button')
        if not statusbar: missing.append('statusbar')
        print(f"DEBUG: handle_run_tasks 中缺失的控件: {missing} (字典 main_ui_elements 中存在 keys: {list(main_ui_elements.keys())})") # 打印具体缺失和现有keys
        return

    # 1. 获取选中的任务
    selected_task_names = []
    for item_id in task_tree.get_children():
        values = task_tree.item(item_id, 'values')
        # 假设 'selected' 列是第一列 (索引 0)
        if values and values[0] == '✓': # 或者您用来表示选中的标记
            # 假设 'name' 列是第三列 (索引 2)
            if len(values) > 2:
                selected_task_names.append(values[2])

    if not selected_task_names:
        messagebox.showwarning("提示", "请至少选择一个任务。")
        return

    # 2. 获取执行模式和日期
    mode = exec_mode_combo.get()
    start_date = None
    end_date = None # 初始化 end_date

    if mode == "手动增量":
        try:
            start_date = manual_date_entry.get()
            end_date = manual_end_date_entry.get() # 获取结束日期
            # 简单验证日期格式 (YYYY-MM-DD)
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d') # 验证结束日期
            # 替换 '-' 为 '' 以匹配 Tushare API 格式
            start_date = start_date.replace('-', '')
            end_date = end_date.replace('-', '') # 格式化结束日期
        except ValueError:
            messagebox.showerror("错误", "手动增量模式下，请输入有效的开始和结束日期 (YYYY-MM-DD)。")
            return

    # 3. 禁用运行按钮，启用停止按钮
    run_button.config(state='disabled')
    stop_button.config(state='normal')
    statusbar.config(text=f"准备运行 {len(selected_task_names)} 个任务...")

    # 4. 请求控制器执行任务 (现在传递 end_date)
    controller.request_task_execution(mode, start_date, end_date)

def handle_stop_tasks(main_ui_elements: Dict[str, tk.Widget]):
    """处理停止任务按钮点击"""
    print("回调：请求停止任务...")
    try:
        if 'stop_button' in main_ui_elements:
            main_ui_elements['stop_button'].config(state=tk.DISABLED) # 立即禁用防止重复点击
        controller.request_stop_execution()
        print("停止请求已发送。")
        # 状态栏和按钮状态将由 RUN_COMPLETED 或其他后续消息处理
    except Exception as e:
        print(f"停止任务时发生错误: {e}")
        # 如果出错，可以考虑手动启用停止按钮？或者让用户等待
        # if 'stop_button' in main_ui_elements: main_ui_elements['stop_button'].config(state=tk.NORMAL)

def handle_type_filter_change(widgets: Dict[str, tk.Widget]):
    """处理类型过滤下拉框选择变化"""
    print("回调：类型过滤更改")
    _update_task_tree_display(widgets)

def handle_sort_column(widgets: Dict[str, tk.Widget], col: str):
    """处理点击 Treeview 列标题进行排序"""
    global _current_sort_col, _current_sort_reverse
    tree = widgets.get('task_tree')
    if not tree or not isinstance(tree, ttk.Treeview): return

    # 切换排序方向或列
    if col == _current_sort_col:
        _current_sort_reverse = not _current_sort_reverse
    else:
        _current_sort_col = col
        _current_sort_reverse = False # 默认升序

    print(f"回调：请求按列 '{_current_sort_col}' 排序, reverse={_current_sort_reverse}")
    # 更新 tree 属性 (可选, 但有助于调试或复杂逻辑)
    tree._last_sort_col = _current_sort_col
    tree._last_sort_reverse = _current_sort_reverse

    # 调用更新函数，它会处理排序和显示
    _update_task_tree_display(widgets)

# --- Helper function to update Treeview based on filter and sort ---
def _update_task_tree_display(widgets: Dict[str, tk.Widget]):
    """根据当前过滤和排序状态更新任务列表 Treeview 的显示"""
    global _full_task_list_data, _current_sort_col, _current_sort_reverse

    tree = widgets.get('task_tree')
    filter_combo = widgets.get('type_filter_combo')

    if not tree or not isinstance(tree, ttk.Treeview) or \
       not filter_combo or not isinstance(filter_combo, ttk.Combobox):
        print("错误：更新 Treeview 时缺少必要的控件。")
        return

    selected_filter_type = filter_combo.get()
    print(f"DEBUG: 更新 Treeview: 过滤类型='{selected_filter_type}', 排序依据='{_current_sort_col}', reverse={_current_sort_reverse}")

    # 1. 过滤数据
    display_data = []
    if selected_filter_type == _ALL_TYPES_OPTION:
        display_data = _full_task_list_data # 复制一份以免修改原始数据?
    else:
        display_data = [task for task in _full_task_list_data if task.get('type') == selected_filter_type]

    # 2. 排序数据
    if _current_sort_col:
        try:
            # 使用 operator.itemgetter 获取排序键
            # 注意：Treeview 列标识符需要与 _full_task_list_data 中的字典键匹配
            # 列: selected, type, name, description, update_time
            # 数据: {'selected': bool, 'type': str, 'name': str, 'description': str, 'latest_update_time': str}, ...
            if _current_sort_col == 'selected':
                # Sort by selected status ('✓' vs '') - assuming data has boolean 'selected'
                display_data.sort(key=lambda task: not task.get('selected', False), reverse=_current_sort_reverse)
            elif _current_sort_col == 'update_time':
                # Special sorting for update time (handle 'N/A', 'No Data', etc.)
                def sort_key(task):
                    val = task.get('latest_update_time', 'N/A') # Get value from task dict
                    if val in ["N/A", "N/A (DB Error)", "N/A (Query Error)", "N/A (No Table)"]:
                        return datetime.min if not _current_sort_reverse else datetime.max
                    elif val == "No Data":
                        return datetime.min + timedelta(seconds=1) if not _current_sort_reverse else datetime.max - timedelta(seconds=1)
                    try:
                        # Attempt to parse the datetime string
                        return datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # Fallback for unparsable strings
                        return datetime.min if not _current_sort_reverse else datetime.max
                display_data.sort(key=sort_key, reverse=_current_sort_reverse)
            else:
                # Default string sort (case-insensitive) for type, name, description
                def sort_key(task):
                    # Get the value, default to empty string if key missing
                    val = task.get(_current_sort_col, '')
                    # Ensure value is string for lower()
                    return str(val).lower()
                try:
                    display_data.sort(key=sort_key, reverse=_current_sort_reverse)
                except KeyError:
                    print(f"警告：排序键 '{_current_sort_col}' 在任务数据中不存在。")
                except Exception as e:
                    print(f"默认排序时出错: {e}")
        except KeyError:
            print(f"警告：排序键 '{_current_sort_col}' 在任务数据中不存在。")
        except Exception as e:
            print(f"排序时出错: {e}")

    # --- Update Heading Indicators --- 
    for c in tree["columns"]:
        current_heading = tree.heading(c, "text")
        # Remove existing sort indicators (▲▼)
        current_heading = current_heading.replace(' ▲', '').replace(' ▼', '')
        if c == _current_sort_col:
            indicator = ' ▲' if not _current_sort_reverse else ' ▼'
            tree.heading(c, text=current_heading + indicator)
        else:
            tree.heading(c, text=current_heading)
    # --- End Heading Update --- 

    # 3. 更新 Treeview 显示
    # 清空现有内容
    tree.delete(*tree.get_children())

    # 插入过滤和排序后的数据
    for task_info in display_data:
        selected_char = '✓' if task_info.get('selected', False) else ''
        # Prepare values tuple
        values = (
            selected_char,
            task_info.get('type', ''),
            task_info.get('name', ''),
            task_info.get('description', ''),
            task_info.get('latest_update_time', 'N/A')
        )
        tree.insert('', tk.END, values=values, tags=('selected' if selected_char else 'normal',))

    print(f"DEBUG: Treeview 更新完成，显示 {len(display_data)} 条记录。")

# --- 处理来自 Controller 的更新 (骨架) ---
# ... (process_controller_update 骨架保持不变)
# --- 处理来自 Controller 的更新 ---

# 映射 task_name 到 treeview item id (用于运行状态更新)
run_task_item_map: Dict[str, str] = {}

def process_controller_update(root: tk.Tk, ui_elements: Dict[str, tk.Widget], update_type: str, data: Any):
    """根据从控制器接收到的更新类型处理UI更新。"""
    # print(f"DEBUG: process_controller_update - Type: {update_type}, Data: {data}") # Optional: Add for detailed debugging
    statusbar = ui_elements.get('statusbar')
    task_tree = ui_elements.get('task_tree')
    log_text = ui_elements.get('log_text')
    run_status_tree = ui_elements.get('run_tree') # Get run status tree
    run_button = ui_elements.get('run_button') # Get run button
    stop_button = ui_elements.get('stop_button') # Get stop button

    try:
        if update_type == 'STATUS':
            if statusbar:
                statusbar.config(text=str(data))
            else:
                print(f"警告：无法更新状态栏，缺少或类型错误的 statusbar 控件。")
                
        elif update_type == 'ERROR':
            messagebox.showerror("后台错误", str(data))
            if statusbar:
                 statusbar.config(text=f"错误: {str(data)[:100]}") # 显示部分错误信息

        elif update_type == 'LOG_ENTRY':
            if log_text and isinstance(log_text, tk.Text):
                log_text.config(state=tk.NORMAL)
                log_text.insert(tk.END, str(data) + '\n')
                log_text.see(tk.END) # 滚动到底部
                log_text.config(state=tk.DISABLED)
            else:
                print(f"日志：{data}") # 如果日志控件无效，打印到控制台

        elif update_type == 'TASK_LIST_UPDATE':
            if task_tree and isinstance(task_tree, ttk.Treeview):
                global _full_task_list_data # Access the global cache
                _full_task_list_data = data # Store the full data
                
                # 更新类型过滤器选项
                type_filter_combo = ui_elements.get('type_filter_combo')
                if type_filter_combo:
                    all_types = sorted(list(set(item.get('type', 'unknown') for item in data))) if data else []
                    current_filter = type_filter_combo.get()
                    type_filter_combo['values'] = [_ALL_TYPES_OPTION] + all_types
                    # Try to preserve selection, default to ALL if previous selection disappears
                    if current_filter not in type_filter_combo['values']:
                         type_filter_combo.set(_ALL_TYPES_OPTION)
                         
                # Update the displayed tree based on current filter/sort
                _update_task_tree_display(ui_elements) # Call helper to apply filter/sort
                
                if statusbar:
                    statusbar.config(text=f"任务列表已更新 ({len(data)} 个任务)")
            else:
                print("警告：无法更新任务列表，缺少或类型错误的 task_tree 控件。")
                
        elif update_type == 'TASK_TIMESTAMP_UPDATE':
            if isinstance(data, dict) and 'name' in data and 'latest_update_time' in data:
                task_name = data['name']
                new_time = data['latest_update_time']
                cache_updated = False
                # Update the global cache directly
                for task_item in _full_task_list_data:
                    if task_item.get('name') == task_name:
                        task_item['latest_update_time'] = new_time
                        cache_updated = True
                        logging.debug(f"Received timestamp update for {task_name}: {new_time}") # Use logging
                        break
                if cache_updated:
                    # Refresh the treeview display using the updated global cache
                    _update_task_tree_display(ui_elements)
                else:
                     logging.warning(f"收到任务 {task_name} 的时间戳更新，但在缓存中未找到该任务。") # Use logging
            else:
                logging.warning(f"收到无效的 TASK_TIMESTAMP_UPDATE 数据: {data}") # Use logging
                
        elif update_type == 'TASK_EXECUTION_COMPLETE':
            # Legacy or potentially used for final state update?
            # Let's rely on TASKS_FINISHED for button state and STATUS for bar text
            # Maybe update the run table one last time?
            if run_status_tree and isinstance(data, dict):
                 _update_run_status_table(run_status_tree, data)
            if statusbar:
                # Final status message is handled by the 'STATUS' type sent just before this
                pass # statusbar.config(text="任务执行完成")
        
        # --- !!! NEW/MODIFIED HANDLERS !!! --- 
        
        elif update_type == 'RUN_TABLE_INIT':
            if run_status_tree and isinstance(data, list):
                # Clear existing items
                run_status_tree.delete(*run_status_tree.get_children())
                # Insert new items
                for item_data in data:
                    if isinstance(item_data, dict):
                        # Define the order of columns for the values tuple
                        # Must match the order in create_task_execution_tab
                        # Columns: ('type', 'name', 'status', 'progress', 'start', 'end', 'details')
                        values_tuple = (
                            item_data.get('type', 'N/A'),
                            item_data.get('name', 'Unknown'),
                            item_data.get('status', 'PENDING'),
                            item_data.get('progress', ''),
                            item_data.get('start', ''),
                            item_data.get('end', ''),
                            item_data.get('details', '')
                        )
                        # Use 'name' as the item ID (iid)
                        run_status_tree.insert('', tk.END, iid=item_data.get('name'), values=values_tuple)
            else:
                 print(f"警告：无法初始化运行状态表，缺少控件或数据格式错误 (Expected list): {type(data)}")
                 
        elif update_type == 'TASK_RUN_UPDATE':
             if run_status_tree and isinstance(data, dict): # Data is the full status dict {name: status_dict}
                 _update_run_status_table(run_status_tree, data)
             else:
                 print(f"警告：无法更新运行状态表，缺少控件或数据格式错误 (Expected dict): {type(data)}")
                 
        elif update_type == 'TASK_PROGRESS_UPDATE':
             if run_status_tree and isinstance(data, tuple) and len(data) == 2:
                 task_name, progress_float = data
                 if run_status_tree.exists(task_name):
                     try:
                         # Format progress and update only the progress cell
                         progress_str = f"{progress_float:.0%}"
                         run_status_tree.set(task_name, 'progress', progress_str)
                     except ValueError:
                          print(f"警告：无法格式化进度值: {progress_float}")
                     except Exception as e:
                          print(f"警告：更新任务 '{task_name}' 进度时出错: {e}")
                 # else: Task might have finished/disappeared before progress update arrived
             else:
                 print(f"警告：无法更新进度，缺少控件或数据格式错误 (Expected tuple(name, progress)): {type(data)}")
                 
        elif update_type == 'TASKS_FINISHED':
             # Update button states
             if run_button and isinstance(run_button, ttk.Button):
                 run_button.config(state=tk.NORMAL)
             else:
                  print("警告：无法启用运行按钮，缺少或类型错误。")
             if stop_button and isinstance(stop_button, ttk.Button):
                  stop_button.config(state=tk.DISABLED)
             else:
                  print("警告：无法禁用停止按钮，缺少或类型错误。")
             # Final status bar text is handled by the 'STATUS' message type

        # --- END OF NEW/MODIFIED HANDLERS --- 

    except Exception as e:
        # Log the exception from the update processing itself
        logging.exception(f"处理控制器更新 '{update_type}' 时发生内部错误")
        # Optionally show a generic error in status bar
        if statusbar:
            statusbar.config(text=f"处理UI更新时发生错误: {type(e).__name__}")

# --- Helper function to update run status table --- 
def _update_run_status_table(tree: ttk.Treeview, status_data: Dict[str, Dict[str, Any]]):
    """Helper to update the run status treeview based on status dict."""
    for task_name, status_dict in status_data.items():
        if tree.exists(task_name):
            try:
                tree.set(task_name, 'status', status_dict.get('status', ''))
                tree.set(task_name, 'progress', status_dict.get('progress', ''))
                tree.set(task_name, 'start', status_dict.get('start', ''))
                tree.set(task_name, 'end', status_dict.get('end', ''))
                tree.set(task_name, 'details', status_dict.get('details', ''))
            except Exception as e:
                 print(f"警告：更新运行状态表行 '{task_name}' 时出错: {e}")
        # else: Task might not be in the table if RUN_TABLE_INIT hasn't been processed yet

# --- 其他事件处理函数 (保持不变或根据需要修改) ---
# ... (handle_refresh_tasks, handle_select_all, etc.) ...
