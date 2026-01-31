"""
GUI业务逻辑处理器模块

本模块包含GUI各个功能域的业务逻辑处理器，
将原本集中在controller.py和event_handlers.py中的功能
按职责分离到不同的处理器中。

主要处理器：
- task_execution_handler: 任务执行逻辑
- data_collection_handler: 数据采集处理
- feature_update_handler: 特征更新逻辑
- storage_settings_handler: 存储设置管理
- task_log_handler: 任务日志处理
"""

# Import modules for compatibility
from .data_collection_handler import (
    handle_collection_sort_column,
    handle_collection_task_tree_click,
    handle_collection_type_filter_change,
    handle_collection_data_source_filter_change,
    handle_deselect_all_collection,
    handle_refresh_collection_tasks,
    handle_select_all_collection,
    update_collection_task_list_ui,
)
from .storage_settings_handler import (
    get_settings_from_ui,
    handle_test_db_connection,
    update_storage_settings_display,
)
from .task_execution_handler import (
    handle_clear_task_run,
    handle_exec_mode_change,
    handle_execute_tasks,
    handle_stop_tasks,
    update_task_run_status,
)
from .task_log_handler import handle_clear_log, update_task_log
from .feature_update_handler import (
    handle_category_filter_change,
    handle_create_missing_features,
    handle_deselect_all_features,
    handle_feature_operation_complete,
    handle_feature_refresh_complete,
    handle_feature_tree_click,
    handle_refresh_features,
    handle_refresh_selected_features,
    handle_select_all_features,
    update_feature_list_ui,
)

__all__ = [
    # data_collection_handler
    "update_collection_task_list_ui",
    "handle_refresh_collection_tasks",
    "handle_select_all_collection",
    "handle_deselect_all_collection",
    "handle_collection_task_tree_click",
    "handle_collection_type_filter_change",
    "handle_collection_sort_column",
    # storage_settings_handler
    "update_storage_settings_display",
    "get_settings_from_ui",
    "handle_test_db_connection",
    # task_execution_handler
    "handle_clear_task_run",
    "handle_execute_tasks",
    "handle_stop_tasks",
    "update_task_run_status",
    "handle_exec_mode_change",
    # task_log_handler
    "update_task_log",
    "handle_clear_log",
    # feature_update_handler
    "update_feature_list_ui",
    "handle_feature_refresh_complete",
    "handle_refresh_features",
    "handle_select_all_features",
    "handle_deselect_all_features",
    "handle_feature_tree_click",
    "handle_category_filter_change",
    "handle_refresh_selected_features",
    "handle_create_missing_features",
    "handle_feature_operation_complete",
]
