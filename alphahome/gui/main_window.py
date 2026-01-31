"""
AlphaHome主窗口模块

本模块定义了AlphaHome智能量化投研系统的主窗口类MainWindow，
负责应用程序的GUI初始化、事件循环管理和前后端通信协调。

## 主要功能

### 窗口管理
- 自动检测和适配高DPI环境
- 智能窗口尺寸设置
- 窗口位置管理

### 架构组织
- 通过Mixin模式分离职责：
  - WindowEventsMixin: 事件绑定管理
  - WindowDpiMixin: DPI感知和显示设置
  - WindowLayoutMixin: UI布局和组件创建

### 异步集成
- 与async-tkinter-loop集成支持异步操作
- 异步初始化后端服务
- 异步数据加载和任务执行

### 前后端通信
- 统一的控制器响应处理机制
- 命令模式的事件分发
- 异常处理和错误显示

## 使用方式

```python
# 直接运行主窗口
if __name__ == "__main__":
    main()

# 通过模块接口启动
from alphahome.gui.main_window import run_gui
run_gui()
```

## 依赖组件
- tkinter: GUI基础库
- async-tkinter-loop: 异步事件循环支持
- controller: 后端业务逻辑控制器
- handlers: 各功能域的事件处理器
- ui: 标签页UI组件创建器
- utils: DPI管理和界面工具
- mixins: 功能混入类
"""

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
    data_collection_handler,
    feature_update_handler,
    storage_settings_handler,
    task_execution_handler,
    task_log_handler,
)
from .ui import (
    data_collection_tab,
    feature_update_tab,
    storage_settings_tab,
    task_execution_tab,
    task_log_tab,
)
from .utils.screen_utils import get_window_geometry_string, center_window_on_screen, position_window_top_left
from .utils.dpi_manager import initialize_dpi_manager, get_dpi_manager, DisplayMode
from .utils.dpi_aware_ui import initialize_ui_factory
from .mixins import WindowEventsMixin, WindowDpiMixin, WindowLayoutMixin

# --- DPI Awareness ---
def enable_dpi_awareness():
    """
    启用Windows系统的DPI感知功能
    
    在创建主窗口前调用，确保应用程序能够正确处理高DPI环境。
    优先尝试使用Per Monitor V2模式，回退到System DPI模式。
    
    Returns:
        bool: 是否成功启用DPI感知
    """
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

class MainWindow(WindowEventsMixin, WindowDpiMixin, WindowLayoutMixin, tk.Tk):
    """
    AlphaHome主窗口类
    
    ## Mixin职责分工
    - WindowEventsMixin: 处理UI事件绑定和用户交互
    - WindowDpiMixin: 管理DPI感知和显示设置
    - WindowLayoutMixin: 负责UI组件创建和布局管理
    
    ## 主要属性
    - dpi_manager: DPI环境管理器
    - ui_factory: DPI感知的UI组件工厂
    - ui_elements: UI组件字典，存储所有界面元素的引用
    
    ## 异步初始化流程
    1. 同步创建UI界面
    2. 异步初始化后端控制器
    3. 异步加载初始数据
    """
    
    def __init__(self):
        """
        初始化主窗口
        
        ## 执行窗口基本设置、DPI初始化、UI创建和事件绑定。
        ## 异步任务会在事件循环启动后自动执行。
        """
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

# create_widgets方法已移动到WindowLayoutMixin

    # bind_events方法已移动到WindowEventsMixin

    def initialize_backend_controller(self):
        """
        初始化后端控制器
        
        ## 设置控制器的响应回调函数，建立前后端通信链路。
        """
        controller.initialize_controller(self.handle_controller_response)

    async def initial_async_load(self):
        """
        执行初始异步加载任务
        
        ## 在事件循环启动后执行的异步初始化流程：
        1. 初始化后端控制器
        2. 加载数据采集任务列表
        3. 加载特征列表
        4. 加载存储设置
        """
        # 首先初始化控制器
        await controller.initialize_controller(self.handle_controller_response)
        
        # 然后加载初始数据
        await controller.handle_request("GET_COLLECTION_TASKS")
        await controller.handle_request("GET_FEATURES")
        await controller.handle_request("GET_STORAGE_SETTINGS")

    def handle_controller_response(self, command: str, data: Any):
        """
        处理来自后端控制器的响应
        
        ## 作为中央分发器，将后端的更新路由到相应的UI处理器。
        ## 所有处理器函数都会在Tkinter事件循环中执行，确保线程安全。
        
        Args:
            command (str): 命令类型，用于路由到对应的处理器
            data (Any): 命令携带的数据
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
            "STORAGE_SETTINGS_UPDATE": (
                storage_settings_handler.update_storage_settings_display,
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
            # Feature Update Responses
            "FEATURE_LIST_UPDATE": (
                feature_update_handler.update_feature_list_ui,
                [self, self.ui_elements, data],
            ),
            "FEATURE_REFRESH_COMPLETE": (
                feature_update_handler.handle_feature_refresh_complete,
                [self.ui_elements, data],
            ),
            "FEATURE_OPERATION_COMPLETE": (
                feature_update_handler.handle_feature_operation_complete,
                [self.ui_elements, data],
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
        """
        保存存储设置
        
        ## 从UI获取存储设置数据并发送到后端保存。
        """
        settings_data = storage_settings_handler.get_settings_from_ui(
            self.ui_elements
        )
        await controller.handle_request("SAVE_STORAGE_SETTINGS", settings_data)

    async def run_selected_tasks(self):
        """
        运行选中的任务

        ## 从UI获取任务执行参数并发送到后端执行。
        """
        params = task_execution_handler.get_execution_params(self.ui_elements)
        if params:
            await controller.handle_request(
                "RUN_TASKS",
                {
                    "tasks_to_run": params["tasks_to_run"],
                    "start_date": params["start_date"],
                    "end_date": params["end_date"],
                    "exec_mode": params["exec_mode"],
                    "use_insert_mode": params["use_insert_mode"],
                },
            )

    def on_closing(self):
        """
        处理窗口关闭事件
        
        ## 在用户关闭窗口时显示确认对话框。
        """
        if messagebox.askokcancel("Exit", "Are you sure you want to exit?"):
            self.destroy()

    def _handle_status_update(self, status_message: str):
        """
        处理通用状态更新
        
        Args:
            status_message (str): 状态消息内容
        """
        logger = get_logger("main_window")
        logger.info(f"Status update: {status_message}")
        # You could update a status bar here if you have one

    def _handle_collection_refresh_complete(self, ui_elements: Dict[str, tk.Widget]):
        """
        处理数据采集刷新完成事件
        
        ## 重新启用刷新按钮，恢复用户交互。
        
        Args:
            ui_elements (Dict[str, tk.Widget]): UI元素字典
        """
        refresh_button = ui_elements.get("collection_refresh_button")
        if refresh_button:
            refresh_button.config(state=tk.NORMAL)
        logger = get_logger("main_window")
        logger.info("Collection task refresh completed")
        
    # DPI相关方法已移动到WindowDpiMixin:
    # - _on_window_configure
    # - _reconfigure_all_tables  
    # - refresh_for_dpi_change
    # - apply_display_settings
    # - restart_application
    # - update_display_info

def main():
    """
    应用程序主入口点
    
    ## 初始化异步服务并启动GUI的同步入口点。
    ## 负责核心服务初始化、DPI感知启用和异步事件循环启动。
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
    GUI应用程序运行入口点
    
    ## 此函数从run.py调用，提供简单的接口启动GUI应用。
    """
    main()


if __name__ == "__main__":
    main()