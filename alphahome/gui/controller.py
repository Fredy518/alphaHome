"""
AlphaHome GUI控制器模块

本模块作为GUI前端和后端业务逻辑之间的中间层，负责：
- 前后端通信协调
- 业务逻辑服务组织
- 数据库连接管理
- 异步任务调度
- 错误处理和状态同步

## 架构设计

### 控制器职责
- **服务初始化**: 初始化各业务服务模块
- **请求分发**: 将GUI请求路由到相应的服务处理器
- **响应回调**: 统一管理从后端到前端的数据更新
- **资源管理**: 管理数据库连接和系统资源

### 服务模块集成
- `task_registry_service`: 任务注册、发现和元数据管理
- `task_execution_service`: 任务执行引擎、流程控制和状态跟踪
- `configuration_service`: 配置管理、数据库连接测试

### 异步支持
- 支持异步任务执行，避免UI冻结
- 异步数据库操作
- 异步服务初始化

## 主要功能

### 数据库管理
- 动态数据库连接初始化
- 自动schema迁移检查
- 数据库错误处理

### 任务管理  
- 数据采集任务的获取和执行
- 数据处理任务的管理
- 任务状态监控和更新

### 配置管理
- 存储设置的保存和加载
- 数据库连接配置
- 系统设置同步

## 使用方式

```python
# 初始化控制器
await initialize_controller(response_callback)

# 处理请求
await handle_request("GET_COLLECTION_TASKS")
await handle_request("RUN_TASKS", task_data)
```
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional

from ..common.config_manager import _config_manager as config_manager
from ..common.db_manager import DBManager  # 添加 DBManager 导入
from ..common.logging_utils import get_logger, setup_logging
from ..common.schema_migrator import run_migration_check, run_refactoring_check
from ..common.task_system import UnifiedTaskFactory, task_factory
from .services import (
    task_registry_service,
    configuration_service,
    task_execution_service,
    feature_service,
)

logger = get_logger(__name__)

# --- Module-level State ---
db_manager = None
_response_callback: Optional[Callable] = None


async def initialize_controller(response_callback):
    """
    初始化控制器及所有后端业务逻辑模块

    设置响应回调函数，初始化各个服务模块，建立数据库连接，
    确保系统准备好处理来自GUI的请求。

    Args:
        response_callback (Callable): GUI响应回调函数，用于向前端发送数据更新
    """
    global _response_callback, db_manager
    _response_callback = response_callback

    # 导入fetchers模块以触发任务注册（修复Phase 1-3中GUI不显示任务的问题）
    logger.info("正在导入数据采集任务模块...")
    import alphahome.fetchers  # 触发任务注册

    logger.info("正在初始化所有后端控制器逻辑模块...")
    
    # Initialize all controller logic modules
    task_registry_service.initialize_task_registry(response_callback)
    configuration_service.initialize_storage_settings(response_callback)
    task_execution_service.set_response_callback(response_callback)
    feature_service.initialize_feature_service(response_callback)
    
    # 初始化任务执行会话
    task_execution_service.initialize_session()
    
    logger.info("所有控制器逻辑模块已初始化。")
    
    # Perform initial DB connection and data load using the factory's reload method
    try:
        await UnifiedTaskFactory.reload_config()
        db_manager = UnifiedTaskFactory.get_db_manager()
        if db_manager:
             # 在数据库连接成功后，立即执行 schema 迁移检查
            logger.info("Controller: Preparing to run schema migration check...")
            all_tasks_dict = UnifiedTaskFactory.get_tasks_by_type(None)
            task_classes = list(all_tasks_dict.values())
            await run_migration_check(db_manager, task_classes)
            await run_refactoring_check(db_manager, task_classes)  # 已恢复：PASS1表名重命名功能
            logger.info("Schema migration check completed.")
    except Exception as e:
        logger.error(f"Controller: Initial UnifiedTaskFactory setup failed: {e}", exc_info=True)
        db_manager = None

    logger.info("控制器初始化完成。")


# --- Handlers that call the core logic ---

async def handle_get_all_task_status():
    """
    处理获取所有任务状态的请求
    
    如果数据库管理器已初始化，则获取所有任务的当前状态；
    否则记录警告并通知前端数据库未连接。
    """
    if db_manager:
        await task_execution_service.get_all_task_status(db_manager)
    else:
        logger.warning("Request to get task status, but DB manager is not initialized.")
        if _response_callback:
            _response_callback("LOG", {"level": "error", "message": "数据库未连接。"})


async def handle_run_tasks(
    tasks_to_run: List[Dict[str, Any]],
    start_date: Optional[str],
    end_date: Optional[str],
    exec_mode: str,
    use_insert_mode: bool = False,
):
    """
    处理运行任务的请求

    验证数据库连接后，将任务提交给任务执行服务进行处理。
    支持串行和并行执行模式。

    Args:
        tasks_to_run (List[Dict[str, Any]]): 要执行的任务列表
        start_date (Optional[str]): 开始日期
        end_date (Optional[str]): 结束日期
        exec_mode (str): 执行模式（"serial" 或 "parallel"）
        use_insert_mode (bool): 是否使用INSERT模式（跳过重复数据检查）
    """
    if db_manager:
        await task_execution_service.run_tasks(
            db_manager, tasks_to_run, start_date, end_date, exec_mode, use_insert_mode
        )
    else:
        logger.error("Request to run tasks, but DB manager is not initialized.")
        if _response_callback:
            _response_callback("LOG", {"level": "error", "message": "数据库未连接，无法执行任务。"})


def handle_stop_tasks():
    """
    处理停止正在运行任务的请求
    
    向任务执行服务发送停止信号，中止当前正在执行的任务。
    """
    logger.info("Request to stop tasks received")
    task_execution_service.stop_tasks()


async def handle_get_collection_tasks():
    """
    处理获取数据采集任务列表的请求
    
    委托给任务注册服务处理，获取所有可用的数据采集任务。
    """
    await task_registry_service.handle_get_collection_tasks()


async def handle_get_features():
    """
    处理获取特征列表的请求
    
    委托给特征服务处理，获取所有已注册的特征配方。
    """
    await feature_service.handle_get_features()


async def handle_refresh_features(feature_names: List[str], strategy: str = "default"):
    """
    处理刷新指定特征视图的请求
    
    Args:
        feature_names: 要刷新的特征名称列表
        strategy: 刷新策略 ("default", "full", "incremental")
    """
    await feature_service.handle_refresh_features(feature_names, strategy=strategy)


async def handle_create_features(feature_names: List[str]):
    """
    处理创建指定特征视图的请求
    
    Args:
        feature_names: 要创建的特征名称列表
    """
    await feature_service.handle_create_features(feature_names)


async def handle_request(command: str, data: Optional[Dict[str, Any]] = None):
    """
    控制器主请求处理器
    
    将命令分发到相应的逻辑处理器，支持多种类型的GUI请求。
    所有异常都会被捕获并记录，确保系统稳定性。
    
    Args:
        command (str): 命令类型，决定调用哪个处理器
        data (Optional[Dict[str, Any]]): 命令携带的数据参数
        
    ## 支持的命令类型
    - GET_ALL_TASK_STATUS: 获取所有任务状态
    - RUN_TASKS: 执行任务
    - STOP_TASKS: 停止任务
    - GET_COLLECTION_TASKS: 获取数据采集任务
    - GET_PROCESSING_TASKS: 获取数据处理任务
    - TOGGLE_COLLECTION_SELECT: 切换采集任务选择状态
    - TOGGLE_PROCESSING_SELECT: 切换处理任务选择状态
    - GET_STORAGE_SETTINGS: 获取存储设置
    - SAVE_STORAGE_SETTINGS: 保存存储设置
    """
    logger.debug(f"Controller received command: {command} with data: {data}")
    data = data or {}

    try:
        if command == "GET_ALL_TASK_STATUS":
            await handle_get_all_task_status()

        elif command == "RUN_TASKS":
            await handle_run_tasks(
                data.get("tasks_to_run", []),
                data.get("start_date"),
                data.get("end_date"),
                data.get("exec_mode", "serial"),
                data.get("use_insert_mode", False),
            )

        elif command == "STOP_TASKS":
            handle_stop_tasks()

        elif command == "GET_COLLECTION_TASKS":
            await handle_get_collection_tasks()

        elif command == "TOGGLE_COLLECTION_SELECT":
            row_index = data.get("row_index", -1)
            if row_index >= 0:
                task_registry_service.toggle_collection_select(row_index)

        elif command == "GET_STORAGE_SETTINGS":
            await configuration_service.handle_get_storage_settings()

        elif command == "SAVE_STORAGE_SETTINGS":
            # The configuration service now handles saving and reloading.
            await configuration_service.handle_save_settings(data)
            
            # After reloading, we need to refresh the UI task list
            await handle_get_collection_tasks()

        elif command == "TEST_DB_CONNECTION":
            db_url = data.get("db_url")
            if db_url:
                await configuration_service.test_database_connection(db_url)
            else:
                logger.warning("Test DB connection request received with no URL.")
        
        # --- 特征更新相关命令 ---
        elif command == "GET_FEATURES":
            await handle_get_features()
        
        elif command == "REFRESH_FEATURES":
            feature_names = data.get("feature_names", [])
            strategy = data.get("strategy", "default")
            if feature_names:
                await handle_refresh_features(feature_names, strategy=strategy)
        
        elif command == "CREATE_FEATURES":
            feature_names = data.get("feature_names", [])
            if feature_names:
                await handle_create_features(feature_names)
            
        else:
            logger.warning(f"Unknown command: {command}")
            
    except Exception as e:
        logger.error(f"Error handling command '{command}': {e}", exc_info=True)
        if _response_callback:
            _response_callback(
                "LOG", {"level": "error", "message": f"处理命令 '{command}' 时发生意外错误: {e}"}
            )


# --- 添加缺失的controller请求函数 ---

def request_collection_tasks():
    """
    请求获取数据采集任务列表
    
    创建异步任务来获取数据采集任务，避免阻塞当前线程。
    """
    asyncio.create_task(handle_request("GET_COLLECTION_TASKS"))

def request_all_task_status():
    """
    请求获取所有任务状态
    
    创建异步任务来获取任务状态，避免阻塞当前线程。
    """
    asyncio.create_task(handle_request("GET_ALL_TASK_STATUS"))


def request_feature_list():
    """
    请求获取特征列表
    
    创建异步任务来获取特征配方列表，避免阻塞当前线程。
    """
    asyncio.create_task(handle_request("GET_FEATURES"))


def request_refresh_features(feature_names: List[str], strategy: str = "default"):
    """
    请求刷新指定特征视图
    
    Args:
        feature_names: 要刷新的特征名称列表
        strategy: 刷新策略 ("default", "full", "incremental")
    """
    asyncio.create_task(handle_request("REFRESH_FEATURES", {
        "feature_names": feature_names,
        "strategy": strategy
    }))


def request_create_features(feature_names: List[str]):
    """
    请求创建指定特征视图
    
    Args:
        feature_names: 要创建的特征名称列表
    """
    asyncio.create_task(handle_request("CREATE_FEATURES", {"feature_names": feature_names}))