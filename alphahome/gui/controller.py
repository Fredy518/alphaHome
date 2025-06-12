import asyncio
from typing import Any, Callable, Dict, List, Optional

from ..common.logging_utils import get_logger
from ..common.db_manager import create_async_manager, create_sync_manager
from .controller_logic import (
    data_collection,
    data_processing,
    storage_settings,
    task_execution,
)

logger = get_logger(__name__)

# --- Module-level State ---
db_manager = None
_response_callback: Optional[Callable] = None


async def reinitialize_db_and_reload_data():
    """Reinitialize database connection and reload all data."""
    global db_manager
    
    settings = storage_settings.get_current_settings()
    if settings:
        db_url = settings.get("database", {}).get("url")
        if db_url:
            try:
                db_manager = create_async_manager(db_url)
                logger.info("Controller: DB Manager created successfully.")
            except Exception as e:
                logger.error(f"Controller: Failed to create DB Manager: {e}")
                db_manager = None
        else:
            logger.warning("Controller: No database URL found in settings.")
    else:
        logger.warning("Controller: No settings loaded.")


async def initialize_controller(response_callback):
    """Initialize the controller with all backend logic modules."""
    global _response_callback, db_manager
    _response_callback = response_callback
    
    logger.info("正在初始化所有后端控制器逻辑模块...")
    
    # Initialize all controller logic modules
    data_collection.initialize_data_collection(response_callback)
    storage_settings.initialize_storage_settings(response_callback)
    data_processing.initialize_data_processing(response_callback)
    task_execution.set_response_callback(response_callback)
    
    # 初始化任务执行会话
    task_execution.initialize_session()
    
    logger.info("所有控制器逻辑模块已初始化。")
    
    # Perform initial DB connection and data load
    await reinitialize_db_and_reload_data()
    
    logger.info("控制器初始化完成。")


# --- Handlers that call the core logic ---

async def handle_get_all_task_status():
    """Handles request to get all task statuses."""
    if db_manager:
        await task_execution.get_all_task_status(db_manager)
    else:
        logger.warning("Request to get task status, but DB manager is not initialized.")
        if _response_callback:
            _response_callback("LOG", {"level": "error", "message": "数据库未连接。"})


async def handle_run_tasks(
    tasks_to_run: List[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    exec_mode: str,
):
    """Handles request to run tasks."""
    if db_manager:
        await task_execution.run_tasks(
            db_manager, tasks_to_run, start_date, end_date, exec_mode
        )
    else:
        logger.error("Request to run tasks, but DB manager is not initialized.")
        if _response_callback:
            _response_callback("LOG", {"level": "error", "message": "数据库未连接，无法执行任务。"})


async def handle_get_collection_tasks():
    """Handles request to get data collection tasks."""
    await data_collection.handle_get_collection_tasks()


async def handle_get_processing_tasks():
    """Handles request to get data processing tasks."""
    await data_processing.handle_get_processing_tasks()


async def handle_request(command: str, data: Optional[Dict[str, Any]] = None):
    """
    Main request handler for the controller.
    Dispatches commands to the appropriate logic handlers.
    """
    logger.debug(f"Controller received command: {command} with data: {data}")
    data = data or {}

    try:
        if command == "GET_ALL_TASK_STATUS":
            await handle_get_all_task_status()
        elif command == "RUN_TASKS":
            await handle_run_tasks(
                tasks_to_run=data.get("tasks_to_run", []),
                start_date=data.get("start_date"),
                end_date=data.get("end_date"),
                exec_mode=data.get("exec_mode", "serial"),
            )
        elif command == "GET_COLLECTION_TASKS":
            await handle_get_collection_tasks()
        elif command == "TOGGLE_COLLECTION_SELECT":
            # This is a synchronous call to the logic module
            data_collection.toggle_task_select(data.get("row_index"))
        elif command == "GET_PROCESSING_TASKS":
            await handle_get_processing_tasks()
        elif command == "TOGGLE_PROCESSING_SELECT":
            # This is a synchronous call to the logic module
            data_processing.toggle_processing_select(data.get("row_index"))
        elif command == "GET_STORAGE_SETTINGS":
            await storage_settings.handle_get_storage_settings()
        elif command == "SAVE_STORAGE_SETTINGS":
            await storage_settings.handle_save_storage_settings(data)
            # After saving, re-initialize the DB connection with the new settings
            await reinitialize_db_and_reload_data()
        else:
            logger.warning(f"Unknown command received: {command}")
            if _response_callback:
                _response_callback("LOG", {"level": "warning", "message": f"收到未知命令: {command}"})
        
    except Exception as e:
        logger.error(f"Error handling command '{command}': {e}", exc_info=True)
        if _response_callback:
            _response_callback("LOG", {"level": "error", "message": f"处理命令 '{command}' 时出错: {e}"})


# --- 添加缺失的controller请求函数 ---

def request_collection_tasks():
    """请求获取数据采集任务列表。"""
    asyncio.create_task(handle_request("GET_COLLECTION_TASKS"))

def request_processing_tasks():
    """请求获取数据处理任务列表。"""
    asyncio.create_task(handle_request("GET_PROCESSING_TASKS"))

def request_all_task_status():
    """请求获取所有任务状态。"""
    asyncio.create_task(handle_request("GET_ALL_TASK_STATUS"))