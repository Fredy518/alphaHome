import asyncio
import ctypes
import platform
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any, Dict

from async_tkinter_loop import async_handler, async_mainloop

from ..common.logging_utils import get_logger, setup_logging
from ..common.task_system import UnifiedTaskFactory
from . import controller
from .handlers import (
    data_collection as data_collection_handler,
    data_processing as data_processing_handler,
    storage_settings as storage_settings_handler,
    task_execution as task_execution_handler,
    task_log as task_log_handler,
)
from .ui import (
    data_collection_tab,
    data_processing_tab,
    storage_settings_tab,
    task_execution_tab,
    task_log_tab,
)
from .utils.screen_utils import get_window_geometry_string, center_window_on_screen, position_window_top_left
from .utils.dpi_manager import initialize_dpi_manager, get_dpi_manager, DisplayMode
from .utils.dpi_aware_ui import initialize_ui_factory

# --- DPI Awareness ---
def enable_dpi_awareness():
    if platform.system() == "Windows":
        try:
            # Use Per Monitor V2 DPI awareness if available (Windows 10+)
            ctypes.windll.shcore.SetProcessDpiAwarenessContext(ctypes.c_ssize_t(-4))
            get_logger("main_window").info(
                "已启用 Per Monitor V2 DPI Awareness Context。"
            )
            return True
        except (AttributeError, OSError):
            try:
                # Fallback for older Windows versions
                ctypes.windll.user32.SetProcessDPIAware()
                get_logger("main_window").info("已启用 System DPI Awareness。")
                return True
            except (AttributeError, OSError):
                get_logger("main_window").warning("无法设置 DPI Awareness。")
                return False
    return False

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AlphaHome - Intelligent Investment Research System")
        
        # 初始化DPI管理系统
        self.dpi_manager = initialize_dpi_manager()
        self.ui_factory = initialize_ui_factory()
        
        # 智能设置窗口尺寸 - 强制使用更大的默认尺寸
        geometry_str = get_window_geometry_string(self)
        
        # 解析几何字符串并确保最小尺寸
        if 'x' in geometry_str:
            width_str, height_str = geometry_str.split('x')
            width = int(width_str)
            height = int(height_str)
            
            # 在4K高DPI环境下强制使用更大尺寸
            if self.dpi_manager.dpi_info.scale_factor >= 2.0:
                # 4K环境：确保至少1800x1000
                width = max(width, 1800)
                height = max(height, 1000)
            elif self.dpi_manager.dpi_info.scale_factor >= 1.5:
                # 高DPI环境：确保至少1600x900
                width = max(width, 1600)
                height = max(height, 900)
            else:
                # 标准环境：确保至少1400x850
                width = max(width, 1400)
                height = max(height, 850)
            
            geometry_str = f"{width}x{height}"
        
        self.geometry(geometry_str)
        
        # 设置最小窗口尺寸（DPI感知）
        min_width, min_height = self.ui_factory.get_scaled_dimensions(1200, 800)
        self.minsize(min_width, min_height)
        
        # 窗口定位到左上角
        self.after_idle(lambda: position_window_top_left(self))

        self.ui_elements = {}

        self.create_widgets()
        self.bind_events()

        # Backend controller will be initialized in initial_async_load

        # Schedule the initial async tasks to run shortly after the mainloop starts
        self.after(50, async_handler(self.initial_async_load))

    def create_widgets(self):
        notebook = ttk.Notebook(self)
        self.ui_elements["notebook"] = notebook

        data_collection_frame = ttk.Frame(notebook, padding="10")
        data_processing_frame = ttk.Frame(notebook, padding="10")
        storage_settings_frame = ttk.Frame(notebook, padding="10")
        task_execution_frame = ttk.Frame(notebook, padding="10")
        task_log_frame = ttk.Frame(notebook, padding="10")

        notebook.add(data_collection_frame, text="数据采集")
        notebook.add(data_processing_frame, text="数据处理")
        notebook.add(task_execution_frame, text="任务运行与状态")
        notebook.add(task_log_frame, text="任务日志")
        notebook.add(storage_settings_frame, text="存储与设置")

        notebook.pack(expand=True, fill="both", padx=5, pady=5)

        # Create tabs and populate ui_elements
        self.ui_elements.update(
            data_collection_tab.create_data_collection_tab(data_collection_frame)
        )
        self.ui_elements.update(
            data_processing_tab.create_data_processing_tab(data_processing_frame)
        )
        self.ui_elements.update(
            task_execution_tab.create_task_execution_tab(
                task_execution_frame, self.ui_elements
            )
        )
        self.ui_elements.update(
            storage_settings_tab.create_storage_settings_tab(storage_settings_frame)
        )

        # Handler dictionary for task log tab needs to be created before passing
        log_handlers = {
            "handle_clear_log": lambda: task_log_handler.handle_clear_log(
                self.ui_elements
            )
        }
        self.ui_elements.update(
            task_log_tab.create_task_log_tab(task_log_frame, log_handlers)
        )

    def bind_events(self):
        # Storage Settings
        self.ui_elements["load_settings_button"].config(
            command=async_handler(controller.handle_request, "GET_STORAGE_SETTINGS")
        )
        self.ui_elements["save_settings_button"].config(
            command=async_handler(self.save_storage_settings)
        )
        self.ui_elements["test_db_button"].config(
            command=async_handler(
                storage_settings_handler.handle_test_db_connection,
                self.ui_elements,
            )
        )

        # Data Collection Binds
        self.ui_elements["collection_refresh_button"].config(
            command=lambda: data_collection_handler.handle_refresh_collection_tasks(
                self.ui_elements
            )
        )
        self.ui_elements["collection_data_source_combo"].bind(
            "<<ComboboxSelected>>",
            lambda e: data_collection_handler.handle_collection_data_source_filter_change(
                self.ui_elements
            ),
        )
        self.ui_elements["collection_task_type_combo"].bind(
            "<<ComboboxSelected>>",
            lambda e: data_collection_handler.handle_collection_type_filter_change(
                self.ui_elements
            ),
        )
        self.ui_elements["collection_select_all_button"].config(
            command=lambda: data_collection_handler.handle_select_all_collection(
                self.ui_elements
            )
        )
        self.ui_elements["collection_deselect_all_button"].config(
            command=lambda: data_collection_handler.handle_deselect_all_collection(
                self.ui_elements
            )
        )
        self.ui_elements["collection_filter_entry"].bind(
            "<KeyRelease>",
            lambda e: data_collection_handler.handle_collection_name_filter_change(
                self.ui_elements
            ),
        )
        self.ui_elements["collection_task_tree"].bind(
            "<ButtonRelease-1>",
            lambda event: data_collection_handler.handle_collection_task_tree_click(
                event, self.ui_elements["collection_task_tree"]
            ),
        )

        # Bind sort headers for collection task tree
        for col in ("data_source", "type", "name", "description", "latest_update_time"):
            self.ui_elements["collection_task_tree"].heading(
                col,
                text=self.ui_elements["collection_task_tree"].heading(col)["text"],
                command=lambda c=col: data_collection_handler.handle_collection_sort_column(
                    self.ui_elements, c
                ),
            )

        # Data Processing Binds
        self.ui_elements["processing_refresh_button"].config(
            command=lambda: data_processing_handler.handle_refresh_processing_tasks(
                self.ui_elements
            )
        )
        self.ui_elements["processing_select_all_button"].config(
            command=lambda: data_processing_handler.handle_select_all_processing(
                self.ui_elements
            )
        )
        self.ui_elements["processing_deselect_all_button"].config(
            command=lambda: data_processing_handler.handle_deselect_all_processing(
                self.ui_elements
            )
        )
        self.ui_elements["processing_task_tree"].bind(
            "<ButtonRelease-1>",
            lambda event: data_processing_handler.handle_processing_task_tree_click(
                event, self.ui_elements["processing_task_tree"]
            ),
        )

        # Task Execution Binds
        self.ui_elements["run_tasks_button"].config(
            command=async_handler(self.run_selected_tasks)
        )
        self.ui_elements["stop_button"].config(
            command=lambda: task_execution_handler.handle_stop_tasks(self.ui_elements)
        )
        # 绑定历史任务切换按钮
        self.ui_elements["history_toggle_button"].config(
            command=lambda: task_execution_handler.handle_toggle_history_mode(self.ui_elements)
        )
        # Bind radio buttons for exec mode
        rb1 = self.ui_elements.get("exec_mode_rb1")
        rb2 = self.ui_elements.get("exec_mode_rb2")
        rb3 = self.ui_elements.get("exec_mode_rb3")
        if rb1:
            rb1.config(
                command=lambda: task_execution_handler.handle_exec_mode_change(
                    self.ui_elements
                )
            )
        if rb2:
            rb2.config(
                command=lambda: task_execution_handler.handle_exec_mode_change(
                    self.ui_elements
                )
            )
        if rb3:
            rb3.config(
                command=lambda: task_execution_handler.handle_exec_mode_change(
                    self.ui_elements
                )
            )
        # Call once to set initial state
        task_execution_handler.handle_exec_mode_change(self.ui_elements)

        # Window close
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # 绑定窗口大小变化事件，支持表格列宽动态调整
        self.bind('<Configure>', self._on_window_configure)
        
        # 绑定显示设置按钮
        self.ui_elements["apply_display_button"].config(
            command=self.apply_display_settings
        )
        
        # 绑定重启应用按钮
        self.ui_elements["restart_app_button"].config(
            command=self.restart_application
        )

    def initialize_backend_controller(self):
        controller.initialize_controller(self.handle_controller_response)

    async def initial_async_load(self):
        """Runs initial data loading tasks after the event loop has started."""
        # 首先初始化控制器
        await controller.initialize_controller(self.handle_controller_response)
        
        # 然后加载初始数据
        await controller.handle_request("GET_COLLECTION_TASKS")
        await controller.handle_request("GET_PROCESSING_TASKS")
        await controller.handle_request("GET_STORAGE_SETTINGS")

    def handle_controller_response(self, command: str, data: Any):
        """
        Routes updates from the backend controller to the appropriate UI handler.
        This acts as a central dispatcher.
        """
        logger = get_logger("main_window")
        logger.debug(f"UI received command: {command}")

        # Note: The handler functions must have signatures that match the arguments provided.
        # The arguments are passed as a list.
        command_map = {
            "LOG": (task_log_handler.update_task_log, [self.ui_elements, data]),
            "TASK_STATUS_UPDATE": (
                task_execution_handler.update_task_run_status,
                [self.ui_elements, data],
            ),
            "COLLECTION_TASK_LIST_UPDATE": (
                data_collection_handler.update_collection_task_list_ui,
                [self.ui_elements, data],
            ),
            "PROCESSING_TASK_LIST_UPDATE": (
                data_processing_handler.update_processing_task_list_ui,
                [self, self.ui_elements, data],
            ),
            "STORAGE_SETTINGS_UPDATE": (
                storage_settings_handler.update_storage_settings_display,
                [self.ui_elements, data],
            ),
            "PROCESSING_REFRESH_COMPLETE": (
                data_processing_handler.handle_processing_refresh_complete,
                [self.ui_elements, data],
            ),
            "STATUS": (
                self._handle_status_update,
                [data],
            ),
            "COLLECTION_REFRESH_COMPLETE": (
                self._handle_collection_refresh_complete,
                [self.ui_elements],
            ),
        }

        if command in command_map:
            handler, args = command_map[command]
            try:
                # Schedule the handler to run in the Tkinter event loop
                self.after(0, lambda h=handler, a=args: h(*a))
            except Exception as e:
                logger.error(
                    f"Error executing handler for command '{command}': {e}",
                    exc_info=True,
                )
                messagebox.showerror(
                    "UI Handler Error",
                    f"An error occurred while processing the command '{command}':\n\n{e}",
                )

        elif command == "ERROR":
            error_message = data if isinstance(data, str) else str(data)
            messagebox.showerror("Backend Error", error_message)
        else:
            logger.warning(f"UI received unknown command: {command}")

    async def save_storage_settings(self):
        settings_data = storage_settings_handler.get_settings_from_ui(
            self.ui_elements
        )
        await controller.handle_request("SAVE_STORAGE_SETTINGS", settings_data)

    async def run_selected_tasks(self):
        params = task_execution_handler.get_execution_params(self.ui_elements)
        if params:
            await controller.handle_request(
                "RUN_TASKS",
                {
                    "tasks_to_run": params["tasks_to_run"],
                    "start_date": params["start_date"],
                    "end_date": params["end_date"],
                    "exec_mode": params["exec_mode"],
                },
            )

    def on_closing(self):
        if messagebox.askokcancel("Exit", "Are you sure you want to exit?"):
            self.destroy()

    def _handle_status_update(self, status_message: str):
        """Handle general status updates from the backend."""
        logger = get_logger("main_window")
        logger.info(f"Status update: {status_message}")
        # You could update a status bar here if you have one

    def _handle_collection_refresh_complete(self, ui_elements: Dict[str, tk.Widget]):
        """Handle collection refresh completion."""
        refresh_button = ui_elements.get("collection_refresh_button")
        if refresh_button:
            refresh_button.config(state=tk.NORMAL)
        logger = get_logger("main_window")
        logger.info("Collection task refresh completed")
        
    def _on_window_configure(self, event):
        """处理窗口大小变化事件，触发表格列宽重新计算"""
        # 只处理主窗口的configure事件，避免子组件事件干扰
        if event.widget == self:
            # 延迟执行以避免频繁调用，并确保布局稳定
            self.after_idle(self._reconfigure_all_tables)
            
    def _reconfigure_all_tables(self):
        """重新配置所有表格的列宽"""
        try:
            # 重新配置数据采集表格
            collection_tree = self.ui_elements.get("collection_task_tree")
            if collection_tree and hasattr(collection_tree, '_column_manager'):
                collection_tree._column_manager.configure_columns()
                
            # 重新配置任务状态表格
            status_tree = self.ui_elements.get("task_status_tree")
            if status_tree and hasattr(status_tree, '_column_manager'):
                status_tree._column_manager.configure_columns()
                
        except Exception as e:
            logger = get_logger("main_window")
            logger.warning(f"重新配置表格列宽时出错: {e}")
    
    def refresh_for_dpi_change(self):
        """DPI模式切换时刷新所有UI元素"""
        logger = get_logger("main_window")
        logger.info("开始刷新UI以适配DPI变化")
        
        try:
            # 刷新UI工厂
            from .utils.dpi_aware_ui import refresh_ui_factory
            refresh_ui_factory()
            
            # 刷新所有表格的列管理器
            collection_tree = self.ui_elements.get("collection_task_tree")
            if collection_tree and hasattr(collection_tree, '_column_manager'):
                collection_tree._column_manager.refresh_for_dpi_change()
                
            status_tree = self.ui_elements.get("task_status_tree")
            if status_tree and hasattr(status_tree, '_column_manager'):
                status_tree._column_manager.refresh_for_dpi_change()
            
            # 强制重新布局
            self.update_idletasks()
            self._reconfigure_all_tables()
            
            logger.info("DPI适配刷新完成")
            
        except Exception as e:
            logger.error(f"DPI适配刷新失败: {e}")
    
    def apply_display_settings(self):
        """应用显示设置"""
        logger = get_logger("main_window")
        
        try:
            # 获取选择的显示模式
            mode_combo = self.ui_elements.get("display_mode_combo")
            mode_values = self.ui_elements.get("display_mode_values")
            
            if not mode_combo or not mode_values:
                logger.warning("显示设置控件未找到")
                return
            
            selected_display_name = mode_combo.get()
            selected_mode_value = None
            
            # 查找对应的模式值
            for display_name, mode_value in mode_values:
                if display_name == selected_display_name:
                    selected_mode_value = mode_value
                    break
            
            if selected_mode_value is None:
                logger.warning(f"无效的显示模式选择: {selected_display_name}")
                return
            
            # 转换为DisplayMode枚举
            try:
                new_mode = DisplayMode(selected_mode_value)
            except ValueError:
                logger.error(f"无效的显示模式值: {selected_mode_value}")
                return
            
            # 应用新的显示模式
            current_mode = self.dpi_manager.current_mode
            if new_mode != current_mode:
                logger.info(f"切换显示模式: {current_mode.value} -> {new_mode.value}")
                
                # 设置新模式
                self.dpi_manager.set_display_mode(new_mode)
                
                # 刷新UI
                self.refresh_for_dpi_change()
                
                # 更新显示信息
                self.update_display_info()
                
                # 强制重新绘制整个窗口
                self.update()
                self.update_idletasks()
                
                # 显示成功消息
                from tkinter import messagebox
                messagebox.showinfo(
                    "显示设置", 
                    f"显示模式已切换为: {selected_display_name}\n\n"
                    f"界面已自动调整以适配新的显示模式。\n"
                    f"如果部分元素显示异常，请重启应用程序。"
                )
            else:
                logger.info("显示模式未变化，无需切换")
                
        except Exception as e:
            logger.error(f"应用显示设置失败: {e}")
            from tkinter import messagebox
            messagebox.showerror("错误", f"应用显示设置时发生错误:\n{e}")
    
    def restart_application(self):
        """重启应用程序"""
        from tkinter import messagebox
        import sys
        import os
        import subprocess
        
        logger = get_logger("main_window")
        
        # 确认重启
        if messagebox.askyesno(
            "重启应用", 
            "重启应用程序将关闭当前窗口并重新启动。\n\n确定要继续吗？"
        ):
            try:
                logger.info("用户确认重启应用程序")
                
                # 获取当前Python可执行文件和脚本路径
                python_exe = sys.executable
                script_path = os.path.abspath(sys.argv[0])
                
                # 如果是通过模块运行的，使用run.py
                if script_path.endswith('__main__.py') or 'alphahome' in script_path:
                    # 查找run.py文件
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    project_root = os.path.dirname(os.path.dirname(current_dir))
                    run_script = os.path.join(project_root, 'run.py')
                    
                    if os.path.exists(run_script):
                        script_path = run_script
                    else:
                        logger.warning(f"未找到run.py，使用当前脚本: {script_path}")
                
                logger.info(f"重启命令: {python_exe} {script_path}")
                
                # 启动新进程
                subprocess.Popen([python_exe, script_path], 
                               cwd=os.path.dirname(script_path),
                               creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0)
                
                # 关闭当前应用
                logger.info("启动新进程成功，关闭当前应用")
                self.destroy()
                
            except Exception as e:
                logger.error(f"重启应用失败: {e}")
                messagebox.showerror("重启失败", f"重启应用程序时发生错误:\n{e}")
    
    def update_display_info(self):
        """更新显示信息标签"""
        try:
            display_info_label = self.ui_elements.get("display_info_label")
            if display_info_label:
                info_text = f"当前分辨率: {self.dpi_manager.dpi_info.logical_resolution[0]}x{self.dpi_manager.dpi_info.logical_resolution[1]}\n"
                info_text += f"DPI缩放: {self.dpi_manager.dpi_info.scale_factor:.0%}\n"
                info_text += f"高DPI环境: {'是' if self.dpi_manager.dpi_info.is_high_dpi else '否'}"
                display_info_label.config(text=info_text)
        except Exception as e:
            logger = get_logger("main_window")
            logger.warning(f"更新显示信息失败: {e}")

def main():
    """
    Synchronous entry point that initializes async services and starts the GUI.
    """
    async def init_and_run():
        # Initialize services first
        try:
            setup_logging(
                log_level="INFO",
                log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                date_format="%H:%M:%S",
            )
            await UnifiedTaskFactory.initialize()
        except Exception as e:
            messagebox.showerror(
                "Fatal Error", f"Core application services failed to initialize.\n\nError: {e}"
            )
            return

        # 启用DPI感知（必须在创建窗口前）
        dpi_success = enable_dpi_awareness()
        if dpi_success:
            get_logger("main_window").info("DPI感知启用成功")
        else:
            get_logger("main_window").warning("DPI感知启用失败，可能影响高DPI显示效果")
        
        app = MainWindow()
        
        # Import and use the async mainloop correctly
        from async_tkinter_loop import main_loop
        await main_loop(app)

    # Run everything in a single async context
    asyncio.run(init_and_run())


def run_gui():
    """
    Entry point for running the GUI application.
    This function is called from run.py and provides a simple interface.
    """
    main()


if __name__ == "__main__":
    main()