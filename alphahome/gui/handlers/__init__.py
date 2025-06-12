"""
GUI业务逻辑处理器模块

本模块包含GUI各个功能域的业务逻辑处理器，
将原本集中在controller.py和event_handlers.py中的功能
按职责分离到不同的处理器中。

主要处理器：
- task_execution: 任务执行逻辑
- data_collection: 数据采集处理
- data_processing: 数据处理逻辑
- storage_settings: 存储设置管理
- task_list: 任务列表管理
- task_log: 任务日志处理
- sorting_manager: 排序和过滤逻辑
- controller_updater: 统一的控制器更新处理
"""

from . import (
    data_collection,
    data_processing,
    storage_settings,
    task_execution,
    task_log,
)
from .data_collection import (
    handle_collection_sort_column,
    handle_collection_task_tree_click,
    handle_collection_type_filter_change,
    handle_deselect_all_collection,
    handle_refresh_collection_tasks,
    handle_select_all_collection,
    update_collection_task_list_ui,
)
from .data_processing import (
    handle_deselect_all_processing,
    handle_processing_refresh_complete,
    handle_processing_task_tree_click,
    handle_refresh_processing_tasks,
    handle_select_all_processing,
    update_processing_task_list_ui,
)
from .storage_settings import (
    get_settings_from_ui,
    handle_test_db_connection,
    update_storage_settings_display,
)
from .task_execution import (
    handle_clear_task_run,
    handle_exec_mode_change,
    handle_execute_tasks,
    handle_stop_tasks,
    update_task_run_status,
)
from .task_log import handle_clear_log, update_task_log

__all__ = [
    # data_collection
    "update_collection_task_list_ui",
    "handle_refresh_collection_tasks",
    "handle_select_all_collection",
    "handle_deselect_all_collection",
    "handle_collection_task_tree_click",
    "handle_collection_type_filter_change",
    "handle_collection_sort_column",
    # data_processing
    "update_processing_task_list_ui",
    "handle_processing_refresh_complete",
    "handle_refresh_processing_tasks",
    "handle_select_all_processing",
    "handle_deselect_all_processing",
    "handle_processing_task_tree_click",
    # storage_settings
    "update_storage_settings_display",
    "get_settings_from_ui",
    "handle_test_db_connection",
    # task_execution
    "handle_clear_task_run",
    "handle_execute_tasks",
    "handle_stop_tasks",
    "update_task_run_status",
    "handle_exec_mode_change",
    # task_log
    "update_task_log",
    "handle_clear_log",
]

# 为了保持向后兼容，这里可以添加重要处理器的导入
# 但在重构完成前，我们先保持空的状态 