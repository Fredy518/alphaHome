import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import logging
import time # 用于关闭时短暂等待
import platform
import ctypes

# --- DPI Awareness (Windows specific) ---
def enable_dpi_awareness():
    if platform.system() == "Windows":
        try:
            # Try modern DPI awareness context (Windows 10 version 1607+)
            # DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2 = -4
            ctypes.windll.shcore.SetProcessDpiAwarenessContext(ctypes.c_ssize_t(-4))
            logging.info("已启用 Per Monitor V2 DPI Awareness Context。")
        except (AttributeError, OSError):
            try:
                # Try older DPI awareness setting (Windows Vista+)
                ctypes.windll.user32.SetProcessDPIAware()
                logging.info("已启用 System DPI Awareness。")
            except (AttributeError, OSError):
                logging.warning("无法设置 DPI Awareness。在高分屏上界面可能模糊。")

enable_dpi_awareness() # 在创建 Tk 窗口前调用
# --- End DPI Awareness ---

# 配置基本日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 导入其他 GUI 组件和控制器
from . import event_handlers, controller

# --- 全局变量存储控件引用 ---
# 这样做可以简化 event_handlers 中更新控件的代码，避免传递大量参数
# 但要注意可能引起的命名冲突或管理复杂性
# 另一种方法是在 event_handlers 中创建类来管理其控件
ui_elements = {}

def run_gui():
    print("--- TEST: run_gui function called successfully! ---")
    """初始化并运行主 Tkinter GUI 窗口。"""
    global ui_elements

    # 创建主窗口
    root = tk.Tk()
    root.title("AlphaHome 数据管理工具(Tkinter版)")
    root.geometry("1000x800") # 设置初始大小

    # --- 初始化控制器 ---
    try:
        controller.initialize_controller()
    except Exception as e:
        logging.exception("无法初始化后台控制器")
        messagebox.showerror("初始化错误", f"无法初始化后台控制器: {e}\n请检查日志文件。应用程序将退出。")
        root.destroy()
        return

    # --- 创建主界面布局 ---
    # 使用 Notebook 实现标签页
    notebook = ttk.Notebook(root)
    ui_elements['notebook'] = notebook # 存储引用

    # 为每个标签页创建 Frame
    tab1_frame = ttk.Frame(notebook, padding="10")
    tab2_frame = ttk.Frame(notebook, padding="10")
    tab3_frame = ttk.Frame(notebook, padding="10")
    tab4_frame = ttk.Frame(notebook, padding="10")

    notebook.add(tab1_frame, text='任务列表')
    notebook.add(tab2_frame, text='存储设置')
    notebook.add(tab3_frame, text='任务运行')
    notebook.add(tab4_frame, text='任务日志')

    notebook.pack(expand=True, fill='both', padx=5, pady=5)

    # --- 创建状态栏 ---
    statusbar_frame = ttk.Frame(root, relief=tk.SUNKEN, padding=(2, 2))
    statusbar = ttk.Label(statusbar_frame, text="准备就绪", anchor=tk.W)
    statusbar.pack(side=tk.LEFT, fill=tk.X, expand=True)
    statusbar_frame.pack(side=tk.BOTTOM, fill=tk.X)
    ui_elements['statusbar'] = statusbar # 存储引用

    # --- 填充标签页内容 (调用 event_handlers) ---
    try:
        ui_elements.update(event_handlers.create_task_list_tab(tab1_frame))
        ui_elements.update(event_handlers.create_storage_settings_tab(tab2_frame))
        ui_elements.update(event_handlers.create_task_execution_tab(tab3_frame, ui_elements))
        ui_elements.update(event_handlers.create_task_log_tab(tab4_frame))
    except Exception as e:
        logging.exception("创建界面布局时出错")
        messagebox.showerror("布局错误", f"创建界面布局时出错: {e}\n应用程序将退出。")
        root.destroy()
        return

    # --- 队列检查函数 ---
    def check_queue():
        """定期检查来自控制器线程的响应队列。"""
        try:
            updates = controller.check_for_updates() # 获取所有待处理更新
            for update_type, data in updates:
                # 调用 event_handlers 中的处理函数（需要修改）
                event_handlers.process_controller_update(root, ui_elements, update_type, data)
        except Exception as e:
            logging.exception("处理控制器更新时出错")
            # 不在这里弹窗，避免过多中断，错误应在 process_controller_update 中处理
            if 'statusbar' in ui_elements:
                 ui_elements['statusbar'].config(text=f"处理更新时出错: {e}")
        finally:
            # 再次安排检查
            root.after(100, check_queue) # 每 100ms 检查一次

    # --- 窗口关闭处理 ---
    def on_closing():
        """处理窗口关闭事件。"""
        if messagebox.askokcancel("退出", "确定要退出 AlphaHome 数据管理工具吗？"):
            logging.info("请求关闭...")
            if 'statusbar' in ui_elements:
                ui_elements['statusbar'].config(text="正在关闭后台服务...")
            root.update_idletasks() # 确保状态栏更新显示
            controller.request_shutdown()
            # 给后台一点时间处理关闭请求
            shutdown_wait_start = time.time()
            while controller.is_backend_running() and (time.time() - shutdown_wait_start < 3): # 最多等3秒
                root.update() # 处理 Tkinter 事件，允许队列消息流动
                time.sleep(0.1)
                # 可以在这里最后一次检查队列，处理关闭日志
                updates = controller.check_for_updates()
                for update_type, data in updates:
                     event_handlers.process_controller_update(root, ui_elements, update_type, data)


            logging.info("销毁窗口。")
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing) # 绑定关闭事件

    # --- 初始化加载 ---
    try:
        logging.info("请求初始任务列表和设置...")
        controller.request_task_list()
        # --> 实际调用加载设置的回调函数来填充初始值
        print(f"DEBUG: main_window - ui_elements keys before load: {list(ui_elements.keys())}")
        event_handlers.handle_load_settings(ui_elements)
        print("DEBUG: main_window - handle_load_settings called.")
    except Exception as e:
        logging.error(f"初始化加载数据时出错: {e}")
        if 'statusbar' in ui_elements:
            ui_elements['statusbar'].config(text=f"初始化加载数据时出错: {e}")

    # --- 启动队列检查 ---
    root.after(100, check_queue)

    # --- 运行主事件循环 ---
    logging.info("启动 GUI 主循环。")
    root.mainloop()
    logging.info("GUI 主循环结束。")


if __name__ == '__main__':
    # 允许直接运行此文件进行测试
    # 确保项目根目录在 PYTHONPATH 中，或者使用 python -m gui.main_window 从项目根目录运行
    run_gui() 
