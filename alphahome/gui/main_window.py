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

# --- DPI Awareness ---
def enable_dpi_awareness():
    if platform.system() == "Windows":
        try:
            # Use Per Monitor V2 DPI awareness if available (Windows 10+)
            ctypes.windll.shcore.SetProcessDpiAwarenessContext(ctypes.c_ssize_t(-4))
            get_logger("main_window").info(
                "已启用 Per Monitor V2 DPI Awareness Context。"
            )
        except (AttributeError, OSError):
            try:
                # Fallback for older Windows versions
                ctypes.windll.user32.SetProcessDPIAware()
                get_logger("main_window").info("已启用 System DPI Awareness。")
            except (AttributeError, OSError):
                get_logger("main_window").warning("无法设置 DPI Awareness。")

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AlphaHome - Intelligent Investment Research System")
        self.geometry("1300x800")

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
        for col in ("type", "name", "description", "latest_update_time"):
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

        enable_dpi_awareness()
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