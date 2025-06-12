"""
GUI UI组件创建模块

本模块包含各个标签页的UI组件创建逻辑，
将原本在event_handlers.py中的UI创建代码
按标签页分离到不同的文件中。

主要UI组件：
- task_list_tab: 任务列表标签页UI
- data_collection_tab: 数据采集标签页UI  
- data_processing_tab: 数据处理标签页UI
- storage_settings_tab: 存储设置标签页UI
- task_execution_tab: 任务执行标签页UI
- task_log_tab: 任务日志标签页UI
"""

# UI组件的统一导入将在重构完成后添加 

from .storage_settings_tab import create_storage_settings_tab
from .task_execution_tab import create_task_execution_tab
from .task_log_tab import create_task_log_tab

__all__ = [
    "create_data_collection_tab",
    "create_data_processing_tab",
    "create_storage_settings_tab",
    "create_task_execution_tab",
    "create_task_log_tab",
] 