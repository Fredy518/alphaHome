# alphahome/derivatives/derivative_task_factory.py

import asyncio
import importlib
import inspect
import logging
import os
import pkgutil
from typing import Dict, Type, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor

from alphahome.fetchers.db_manager import DBManager # 从 fetchers 模块导入真实的 DBManager
from .base_derivative_task import BaseDerivativeTask # 导入衍生品任务基类

logger = logging.getLogger(__name__) # 模块级日志记录器

# 全局注册表，用于存储已注册的衍生品任务类
_derivative_tasks_registry: Dict[str, Type[BaseDerivativeTask]] = {}
# 全局共享的数据库管理器实例
_db_manager_instance: Optional[DBManager] = None
# 全局共享的线程池执行器实例
_thread_pool_executor_instance: Optional[ThreadPoolExecutor] = None
# 标记工厂是否已显式初始化完成
_is_initialized: bool = False
# 标记任务发现是否已尝试执行
_discovery_done: bool = False

def derivative_task_register(name: Optional[str] = None, **attrs):
    """
    衍生品任务注册装饰器。

    用于将继承自 BaseDerivativeTask 的类注册到任务工厂中。

    参数:
        name (Optional[str]): 任务的自定义名称。如果未提供，则使用类名。
        **attrs: 其他要附加到任务类上的属性。
    """
    def decorator(cls: Type[BaseDerivativeTask]):
        task_name = name or getattr(cls, 'name', None) # 获取任务名，优先使用装饰器参数，其次是类的name属性
        if not task_name: # 如果两者都无，则使用类名
            task_name = cls.__name__
            if not hasattr(cls, 'name'): # 如果类的name属性不存在，则设置它
                setattr(cls, 'name', task_name)
                
        if task_name in _derivative_tasks_registry:
            logger.warning(f"衍生品任务 {task_name} 已注册，将被覆盖。")
        
        if not issubclass(cls, BaseDerivativeTask):
            raise TypeError(f"类 {cls.__name__} 必须继承自 BaseDerivativeTask才能注册。")

        _derivative_tasks_registry[task_name] = cls
        logger.debug(f"衍生品任务 '{task_name}' (类 {cls.__name__}) 已注册。")
        
        # 将装饰器中指定的其他属性设置到类上
        for attr_name, attr_value in attrs.items():
            setattr(cls, attr_name, attr_value)
        return cls
    return decorator


class DerivativeTaskFactory:
    """
    衍生品任务工厂类。
    负责创建和管理衍生品任务实例，并管理共享的 DBManager 和 ThreadPoolExecutor。
    """

    @staticmethod
    async def initialize(
        db_dsn: Optional[str] = None, 
        loop: Optional[asyncio.AbstractEventLoop] = None,
        max_workers_threadpool: Optional[int] = None,
        external_db_manager: Optional[DBManager] = None, # 允许外部传入DBManager实例
        force_reinitialize: bool = False # 是否强制重新初始化
    ):
        """
        异步初始化任务工厂及共享资源（DBManager, ThreadPoolExecutor）。
        这个方法应该是幂等的，除非 force_reinitialize 为 True。

        参数:
            db_dsn (Optional[str]): 数据库连接字符串。
            loop (Optional[asyncio.AbstractEventLoop]): 事件循环。
            max_workers_threadpool (Optional[int]): 线程池的最大工作线程数。
            external_db_manager (Optional[DBManager]): (可选) 外部提供的DBManager实例。
            force_reinitialize (bool): 如果为True，则强制关闭并重新初始化工厂。
        """
        global _is_initialized, _db_manager_instance, _thread_pool_executor_instance, _discovery_done
        
        if _is_initialized and not force_reinitialize:
            logger.info("DerivativeTaskFactory 已经初始化，跳过。")
            return
        
        if force_reinitialize:
            logger.warning("强制重新初始化 DerivativeTaskFactory。")
            await DerivativeTaskFactory.shutdown() # 清理之前的状态

        # 初始化 DBManager
        if external_db_manager:
            _db_manager_instance = external_db_manager
            logger.info("DerivativeTaskFactory 使用外部提供的 DBManager 实例进行初始化。")
        elif db_dsn:
            _db_manager_instance = DBManager(dsn=db_dsn, loop=loop) 
            await _db_manager_instance.connect() # 确保连接成功
            logger.info(f"DerivativeTaskFactory 使用 DSN 初始化了新的 DBManager。")
        elif _db_manager_instance is None: # 如果没有DSN、外部实例，且当前实例也为空
            # 尝试从 FetcherTaskFactory 获取 DBManager (如果可用且已初始化)
            try:
                from alphahome.fetchers.task_factory import TaskFactory as FetcherTaskFactory
                fetcher_db_manager = FetcherTaskFactory.get_db_manager()
                if fetcher_db_manager: 
                    _db_manager_instance = fetcher_db_manager
                    logger.info("DerivativeTaskFactory 复用来自 FetcherTaskFactory 的 DBManager 实例。")
                else:
                    # FetcherTaskFactory 可能已导入但其DBManager未初始化
                    raise ValueError("FetcherTaskFactory 的 DBManager 不可用或未初始化。")
            except (ImportError, ValueError, RuntimeError) as e:
                logger.error(f"DerivativeTaskFactory 初始化错误: DBManager 必须通过 db_dsn, external_db_manager 提供，或从已初始化的 FetcherTaskFactory 获取。错误: {e}")
                raise ValueError("DerivativeTaskFactory 的 DBManager 不可用。") from e
        else:
            # 如果 _db_manager_instance 已存在 (例如，在强制重新初始化之前未完全清除，或在其他地方已设置)
            logger.info("DerivativeTaskFactory 复用现有的 DBManager 实例。")
        
        # 初始化 ThreadPoolExecutor
        if not _thread_pool_executor_instance or force_reinitialize:
            num_cpus = os.cpu_count() or 1 # 获取CPU核心数作为默认值
            workers = max_workers_threadpool or num_cpus
            _thread_pool_executor_instance = ThreadPoolExecutor(max_workers=workers)
            logger.info(f"DerivativeTaskFactory 初始化了 ThreadPoolExecutor，最大工作线程数: {workers}。")

        # 执行任务发现
        if not _discovery_done or force_reinitialize: # 只有在未发现过或强制重新初始化时才执行
            DerivativeTaskFactory._discover_tasks()
        
        _is_initialized = True
        logger.info("DerivativeTaskFactory 初始化成功。")

    @staticmethod
    async def shutdown():
        """
        关闭任务工厂及相关共享资源。
        """
        global _is_initialized, _db_manager_instance, _thread_pool_executor_instance, _discovery_done
        if not _is_initialized:
            # logger.info("DerivativeTaskFactory 未初始化或已关闭，无需操作。")
            return

        if _thread_pool_executor_instance:
            _thread_pool_executor_instance.shutdown(wait=True) # 等待所有线程完成
            _thread_pool_executor_instance = None
            logger.info("DerivativeTaskFactory 的 ThreadPoolExecutor 已关闭。")
        
        # 关于DBManager的关闭：
        # 为避免复杂的生命周期管理和所有权问题，此工厂不应主动关闭一个可能是共享的DBManager实例。
        # DBManager的关闭应由其最初的创建者（例如主应用或FetcherTaskFactory）负责。
        # 此处仅释放引用，不调用 _db_manager_instance.close()，除非有明确的策略规定此工厂拥有该实例。
        # _db_manager_instance = None # 如果需要明确断开引用

        _derivative_tasks_registry.clear() # 清空任务注册表
        _discovery_done = False
        _is_initialized = False
        logger.info("DerivativeTaskFactory 关闭成功。")

    @staticmethod
    def _discover_tasks(tasks_module_path_str: str = "alphahome.derivatives.tasks"):
        """
        私有方法，用于发现并加载指定路径下的所有衍生品任务。
        任务通过模块内的 @derivative_task_register 装饰器自动注册。
        """
        global _discovery_done # 修改全局变量状态
        logger.debug(f"开始从模块路径 {tasks_module_path_str} 发现衍生品任务...")
        try:
            tasks_module = importlib.import_module(tasks_module_path_str)
            count_before = len(_derivative_tasks_registry)
            
            # 遍历模块路径下的所有包和模块
            for _, modname, ispkg in pkgutil.walk_packages(
                path=tasks_module.__path__, 
                prefix=tasks_module.__name__ + '.',
                onerror=lambda name: logger.error(f"导入衍生品任务包 {name} 时出错。") # 处理导入包本身的错误
            ):
                if not ispkg: # 如果不是包 (即是一个模块文件)
                    try:
                        importlib.import_module(modname) # 导入模块以触发装饰器注册
                    except ImportError as e:
                        logger.error(f"导入衍生品任务模块 {modname} 时出错: {e}", exc_info=False) 
            count_after = len(_derivative_tasks_registry)
            logger.info(f"衍生品任务发现完成。新发现 {count_after - count_before} 个任务。总计已注册: {count_after} 个。")
        except ImportError as e:
            logger.error(f"无法导入基础任务模块路径 {tasks_module_path_str}: {e}")
        _discovery_done = True

    @staticmethod
    def get_db_manager() -> Optional[DBManager]:
        """返回共享的 DBManager 实例。"""
        if not _is_initialized and not _db_manager_instance: # 如果在初始化前访问
            logger.warning("DBManager 在 DerivativeTaskFactory 初始化之前被访问。请先调用 initialize()。")
        return _db_manager_instance

    @staticmethod
    def get_executor() -> Optional[ThreadPoolExecutor]:
        """返回共享的 ThreadPoolExecutor 实例。"""
        if not _is_initialized and not _thread_pool_executor_instance:
            logger.warning("ThreadPoolExecutor 在 DerivativeTaskFactory 初始化之前被访问。请先调用 initialize()。")
        return _thread_pool_executor_instance

    @staticmethod
    async def get_task(task_name: str, **kwargs) -> Optional[BaseDerivativeTask]:
        """
        根据任务名称获取一个已初始化的衍生品任务实例。
        如果工厂未初始化，会尝试进行默认初始化。

        参数:
            task_name (str): 要获取的任务的注册名称。
            **kwargs: 传递给任务构造函数的额外参数。
        返回:
            一个 BaseDerivativeTask 的实例，如果找到并成功初始化的话；否则返回 None。
        """
        if not _is_initialized:
            logger.warning("DerivativeTaskFactory.get_task 在显式初始化之前被调用。尝试进行默认初始化...")
            try:
                # 尝试使用无参数（或依赖FetcherTaskFactory）的方式初始化
                await DerivativeTaskFactory.initialize() 
            except Exception as e:
                 logger.error(f"get_task 中的默认初始化失败: {e}。无法提供任务。")
                 return None
            if not _is_initialized: # 再次检查初始化状态
                 logger.error("默认初始化尝试失败。无法提供任务。")
                 return None
        
        if not _discovery_done: # 确保任务已发现
            logger.warning("任务发现在 get_task 时仍未完成，强制重新执行发现。")
            DerivativeTaskFactory._discover_tasks()

        task_class = _derivative_tasks_registry.get(task_name)
        if task_class:
            db_m = DerivativeTaskFactory.get_db_manager()
            executor = DerivativeTaskFactory.get_executor()
            if not db_m: # DBManager 是必需的
                 logger.error(f"无法创建任务 {task_name}: DBManager 不可用 (是否已正确初始化?)。")
                 return None
            try:
                # 为每个任务实例创建一个特定的logger，方便追踪
                task_specific_logger = logging.getLogger(f"DerivativeTask.{task_name}") 
                return task_class(db_manager=db_m, executor=executor, logger=task_specific_logger, **kwargs)
            except Exception as e:
                logger.error(f"实例化衍生品任务 {task_name} 时出错: {e}", exc_info=True)
                return None
        else:
            logger.error(f"衍生品任务 '{task_name}' 未在注册表中找到。当前已注册任务: {list(_derivative_tasks_registry.keys())}")
            return None

    @staticmethod
    def get_all_task_names() -> List[str]:
        """返回所有已注册衍生品任务的名称列表。"""
        if not _discovery_done and not _is_initialized: # 如果在初始化和发现完成前调用
             logger.warning("get_all_task_names 在任务发现前调用，可能列表不完整。尝试执行发现...")
             DerivativeTaskFactory._discover_tasks()
        return sorted(list(_derivative_tasks_registry.keys()))

    @staticmethod
    def get_task_names_by_type(task_type_filter: str) -> List[str]:
        """
        根据任务类型 (task_cls.task_type 属性) 筛选已注册的任务名称。
        """
        if not _discovery_done and not _is_initialized:
             DerivativeTaskFactory._discover_tasks()
        
        matched_tasks = []
        for name, task_cls in _derivative_tasks_registry.items():
            if getattr(task_cls, 'task_type', None) == task_type_filter:
                matched_tasks.append(name)
        return sorted(matched_tasks)

# 提供一个便捷的顶层函数来获取任务，封装对工厂方法的调用
async def get_derivative_task(task_name: str, **kwargs) -> Optional[BaseDerivativeTask]:
    """便捷函数，用于异步获取衍生品任务实例。"""
    return await DerivativeTaskFactory.get_task(task_name, **kwargs)
 