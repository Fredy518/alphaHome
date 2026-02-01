"""
窗口事件绑定Mixin

负责处理MainWindow的所有UI事件绑定逻辑，包括：
- 存储设置按钮事件
- 数据采集相关事件
- 特征更新相关事件
- 任务执行相关事件
- 窗口管理事件
"""

from async_tkinter_loop import async_handler
from .. import controller
from ..handlers import (
    data_collection_handler,
    feature_update_handler,
    storage_settings_handler,
    task_execution_handler,
)


class WindowEventsMixin:
    """窗口事件绑定Mixin类"""
    
    def bind_events(self):
        """绑定所有UI组件的事件处理器"""
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
                event, self.ui_elements
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

        # Feature Update Binds
        self.ui_elements["feature_refresh_button"].config(
            command=lambda: feature_update_handler.handle_refresh_features(
                self.ui_elements
            )
        )
        self.ui_elements["feature_select_all_button"].config(
            command=lambda: feature_update_handler.handle_select_all_features(
                self.ui_elements
            )
        )
        self.ui_elements["feature_deselect_all_button"].config(
            command=lambda: feature_update_handler.handle_deselect_all_features(
                self.ui_elements
            )
        )
        self.ui_elements["feature_refresh_selected_button"].config(
            command=lambda: feature_update_handler.handle_refresh_selected_features(
                self.ui_elements
            )
        )
        self.ui_elements["feature_full_refresh_button"].config(
            command=lambda: feature_update_handler.handle_full_refresh_selected_features(
                self.ui_elements
            )
        )
        self.ui_elements["feature_create_missing_button"].config(
            command=lambda: feature_update_handler.handle_create_missing_features(
                self.ui_elements
            )
        )
        self.ui_elements["feature_category_combobox"].bind(
            "<<ComboboxSelected>>",
            lambda e: feature_update_handler.handle_category_filter_change(
                self.ui_elements
            ),
        )
        self.ui_elements["feature_storage_type_combobox"].bind(
            "<<ComboboxSelected>>",
            lambda e: feature_update_handler.handle_storage_type_filter_change(
                self.ui_elements
            ),
        )
        self.ui_elements["feature_tree"].bind(
            "<ButtonRelease-1>",
            lambda event: feature_update_handler.handle_feature_tree_click(
                event, self.ui_elements
            ),
        )
        
        # 绑定特征表格列头排序
        feature_tree = self.ui_elements["feature_tree"]
        for col in ("name", "description", "category", "storage_type", "status", "row_count", "last_refresh"):
            feature_tree.heading(
                col,
                text=feature_tree.heading(col)["text"],
                command=lambda c=col: feature_update_handler.handle_column_sort(
                    c, self.ui_elements
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

        # Bind INSERT mode checkbox
        insert_mode_cb = self.ui_elements.get("insert_mode_cb")
        if insert_mode_cb:
            insert_mode_cb.config(
                command=lambda: task_execution_handler.handle_insert_mode_change(
                    self.ui_elements
                )
            )
        # Call once to set initial state
        task_execution_handler.handle_insert_mode_change(self.ui_elements)

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