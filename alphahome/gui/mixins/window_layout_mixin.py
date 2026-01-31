"""
窗口布局和组件创建Mixin

负责处理MainWindow的UI组件创建和布局管理，包括：
- 创建主要的Notebook容器
- 创建各个标签页的Frame
- 调用各个标签页的UI创建函数
- 管理ui_elements字典
"""

from tkinter import ttk
from ..ui import (
    data_collection_tab,
    feature_update_tab,
    storage_settings_tab,
    task_execution_tab,
    task_log_tab,
)
from ..handlers import task_log_handler


class WindowLayoutMixin:
    """窗口布局和组件创建Mixin类"""
    
    def create_widgets(self):
        """创建所有UI组件和标签页"""
        notebook = ttk.Notebook(self)
        self.ui_elements["notebook"] = notebook

        data_collection_frame = ttk.Frame(notebook, padding="10")
        feature_update_frame = ttk.Frame(notebook, padding="10")
        storage_settings_frame = ttk.Frame(notebook, padding="10")
        task_execution_frame = ttk.Frame(notebook, padding="10")
        task_log_frame = ttk.Frame(notebook, padding="10")

        notebook.add(data_collection_frame, text="数据采集")
        notebook.add(feature_update_frame, text="特征更新")
        notebook.add(task_execution_frame, text="任务运行与状态")
        notebook.add(task_log_frame, text="任务日志")
        notebook.add(storage_settings_frame, text="存储与设置")

        notebook.pack(expand=True, fill="both", padx=5, pady=5)

        # Create tabs and populate ui_elements
        self.ui_elements.update(
            data_collection_tab.create_data_collection_tab(data_collection_frame)
        )
        self.ui_elements.update(
            feature_update_tab.create_feature_update_tab(feature_update_frame)
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