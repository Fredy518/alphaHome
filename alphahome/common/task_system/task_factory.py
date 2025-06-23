import inspect
import logging
from typing import Any, Dict, List, Optional, Type

from ..config_manager import get_database_url, get_task_config, get_tushare_token, reload_config as _reload_config
from ..db_manager import DBManager
from .base_task import BaseTask

logger = logging.getLogger("unified_task_factory")


class UnifiedTaskFactory:
    """统一任务工厂类

    管理所有类型任务实例的创建、数据库连接和分类管理。
    支持fetch、processor等多种任务类型的统一管理。
    """

    # 类变量
    _db_manager: Optional[DBManager] = None
    _task_instances: Dict[str, BaseTask] = {}
    _task_registry: Dict[str, Type[BaseTask]] = {}
    _initialized: bool = False

    @classmethod
    def register_task(cls, task_name, task_class):
        """注册任务类到工厂"""
        if task_name in cls._task_registry:
            logger.debug(f"任务 {task_name} 已注册，跳过重复注册。")
            return
        cls._task_registry[task_name] = task_class
        
        # 新增：记录任务类型信息
        task_type = getattr(task_class, 'task_type', 'unknown')
        logger.debug(f"注册任务类型: {task_name} (类型: {task_type})")

    @classmethod
    async def initialize(cls, db_url=None):
        """初始化任务工厂，连接数据库"""
        # 如果没有提供连接字符串，尝试从配置获取
        if db_url is None:
            db_url = get_database_url()  # 可能返回 None

        # 检查 db_url 是否有效
        if not db_url:
            logger.error(
                "无法获取有效的数据库连接 URL (配置文件或环境变量均未设置)，UnifiedTaskFactory 初始化失败。"
            )
            cls._db_manager = None
            cls._initialized = False
            return  # 或者直接返回，保持未初始化状态

        # 只有在获得有效 db_url 后才继续
        logger.info(f"尝试使用数据库 URL 初始化 UnifiedTaskFactory: {db_url}")
        try:
            cls._db_manager = DBManager(db_url)
            await cls._db_manager.connect()
            cls._initialized = True
            logger.info(f"UnifiedTaskFactory 初始化成功: db_url={db_url}")
        except Exception as e:
            logger.exception(f"使用 URL {db_url} 连接数据库失败")
            cls._db_manager = None
            cls._initialized = False
            # 向上抛出异常，让 controller 知道初始化失败
            raise ConnectionError(f"连接数据库失败: {e}") from e

    @classmethod
    async def reload_config(cls):
        """重新加载配置并重新初始化数据库连接。"""
        logger.info("开始重新加载 UnifiedTaskFactory 配置...")

        try:
            # 1. 关闭现有数据库连接 (如果存在)
            if cls._db_manager:
                logger.info("正在关闭现有数据库连接...")
                try:
                    await cls._db_manager.close()
                    logger.info("现有数据库连接已关闭。")
                except Exception as close_err:
                    logger.error(
                        f"关闭现有数据库连接时出错: {close_err}，继续尝试重新加载..."
                    )
                finally:
                    cls._db_manager = None  # 无论关闭是否成功，都置为 None
            else:
                logger.info("没有需要关闭的现有数据库连接。")

            # 2. 重新加载配置（这会清空 ConfigManager 的缓存）
            logger.info("正在重新加载配置...")
            new_config = _reload_config()
            new_db_url = new_config.get("database", {}).get("url")

            if not new_db_url:
                logger.error("新配置中缺少有效的数据库 URL，无法重新初始化。")
                cls._initialized = False
                cls._db_manager = None
                raise ValueError("新配置中缺少数据库 URL")

            logger.info(f"加载到新的数据库 URL: {new_db_url}")

            # 3. 使用新 URL 重新初始化 DBManager
            logger.info("正在使用新 URL 创建新的 DBManager 实例...")
            cls._db_manager = DBManager(new_db_url)
            await cls._db_manager.connect()  # 连接失败会抛出异常

            # 4. 清空旧的任务实例缓存，因为它们可能持有旧的 db_manager 或旧配置
            logger.info("正在清空旧的任务实例缓存...")
            cls._task_instances.clear()

            cls._initialized = True  # 标记为已初始化
            logger.info("UnifiedTaskFactory 配置重新加载完成。")

        except Exception as e:
            logger.exception(
                "重新加载 UnifiedTaskFactory 配置时发生错误。UnifiedTaskFactory 可能处于不稳定状态。"
            )
            cls._initialized = False  # 标记为未初始化以表示错误状态
            cls._db_manager = None  # 确保 db_manager 也被清理
            raise  # 重新抛出异常，让 controller 处理

    @classmethod
    async def shutdown(cls):
        """关闭数据库连接"""
        # 关闭数据库连接
        if cls._db_manager:
            await cls._db_manager.close()
            cls._db_manager = None
            logger.info("数据库连接已关闭")

        cls._task_instances.clear()
        cls._initialized = False
        logger.info("UnifiedTaskFactory 已关闭")

    @classmethod
    def get_db_manager(cls):
        """获取数据库管理器实例"""
        if not cls._initialized:
            raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")
        return cls._db_manager

    @classmethod
    def get_all_task_names(cls) -> List[str]:
        """获取所有已注册的任务名称列表"""
        if not cls._initialized:
            raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")

        all_tasks = list(cls._task_registry.keys())
        logger.debug(f"获取到所有 {len(all_tasks)} 个已注册任务: {all_tasks}")
        return all_tasks

    @classmethod
    def get_tasks_by_type(cls, task_type: Optional[str] = None) -> Dict[str, Type[BaseTask]]:
        """新增：按任务类型获取任务字典"""
        if not cls._initialized:
            raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")
            
        if task_type is None:
            return cls._task_registry.copy()
            
        filtered_tasks = {}
        for name, task_class in cls._task_registry.items():
            if hasattr(task_class, 'task_type') and task_class.task_type == task_type:
                filtered_tasks[name] = task_class
        
        logger.debug(f"获取到 {len(filtered_tasks)} 个 {task_type} 类型任务: {list(filtered_tasks.keys())}")
        return filtered_tasks

    @classmethod
    def get_task_names_by_type(cls, task_type: Optional[str] = None) -> List[str]:
        """新增：按任务类型获取任务名称列表"""
        if not cls._initialized:
            raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")
            
        if task_type is None:
            return cls.get_all_task_names()
            
        filtered_tasks = []
        for name, task_class in cls._task_registry.items():
            if hasattr(task_class, 'task_type') and task_class.task_type == task_type:
                filtered_tasks.append(name)
        
        logger.debug(f"获取到 {len(filtered_tasks)} 个 {task_type} 类型任务: {filtered_tasks}")
        return sorted(filtered_tasks)

    @classmethod
    def get_task_types(cls) -> List[str]:
        """新增：获取所有任务类型"""
        if not cls._initialized:
            raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")
            
        types = set()
        for task_class in cls._task_registry.values():
            if hasattr(task_class, 'task_type'):
                types.add(task_class.task_type)
        
        result = sorted(list(types))
        logger.debug(f"获取到 {len(result)} 种任务类型: {result}")
        return result

    @classmethod
    def get_task_info(cls, task_name: str) -> Dict[str, Any]:
        """新增：获取任务详细信息"""
        if not cls._initialized:
            raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")
            
        if task_name not in cls._task_registry:
            raise ValueError(f"未注册的任务类型: {task_name}")
            
        task_class = cls._task_registry[task_name]
        return {
            "name": task_name,
            "type": getattr(task_class, 'task_type', 'unknown'),
            "description": getattr(task_class, 'description', ''),
            "table_name": getattr(task_class, 'table_name', None),
            "source_tables": getattr(task_class, 'source_tables', []),
            "dependencies": getattr(task_class, 'dependencies', []),
            "class_name": task_class.__name__,
        }

    @classmethod
    async def create_task_instance(cls, task_name: str, **task_init_kwargs: Any) -> BaseTask:
        """
        创建一个新的一次性任务实例，不使用缓存。
        允许在运行时传递特定参数来初始化任务。

        Args:
            task_name (str): 要创建的任务的名称。
            **task_init_kwargs: 传递给任务构造函数的关键字参数
                                 (例如: update_type, start_date, end_date)。

        Returns:
            BaseTask: 一个新创建和配置的任务实例。
        """
        if not cls._initialized:
            logger.warning("Factory not initialized. Attempting to initialize now.")
            await cls.initialize()
            if not cls._initialized:
                raise RuntimeError("UnifiedTaskFactory ailed to initialize.")

        if task_name not in cls._task_registry:
            raise ValueError(
                f"未注册的任务类型: {task_name}，请先调用 register_task 方法注册"
            )

        task_class = cls._task_registry[task_name]
        
        # 准备构造函数所需的所有参数
        constructor_kwargs = task_init_kwargs.copy()
        
        # 注入固定的依赖项
        constructor_kwargs['db_connection'] = cls._db_manager

        # 如果是 Tushare 任务，注入 API Token
        init_params = inspect.signature(task_class.__init__).parameters
        if "api_token" in init_params:
            constructor_kwargs["api_token"] = get_tushare_token()

        # 注入任务特定配置
        task_config = get_task_config(task_name)
        if "task_config" not in constructor_kwargs:
            constructor_kwargs["task_config"] = task_config
        else:
            # 合并传入的配置和文件中的配置
            constructor_kwargs["task_config"] = {**task_config, **constructor_kwargs["task_config"]}

        logger.debug(f"Creating instance of '{task_name}' with kwargs: {constructor_kwargs}")
        
        # 创建新实例
        task_instance = task_class(**constructor_kwargs)
        
        return task_instance

    @classmethod
    async def get_task(cls, task_name: str):
        """获取任务实例"""
        if not cls._initialized:
            await cls.initialize()

        if task_name not in cls._task_instances:
            # 如果任务实例不存在，创建它
            if task_name not in cls._task_registry:
                raise ValueError(
                    f"未注册的任务类型: {task_name}，请先调用 register_task 方法注册"
                )

            # 获取任务特定配置
            task_config = get_task_config(task_name)

            # 使用注册表中的任务类创建实例
            task_class = cls._task_registry[task_name]

            # 检查任务类构造函数是否接受api_token参数
            init_params = inspect.signature(task_class.__init__).parameters
            constructor_kwargs = {}
            if "api_token" in init_params:
                # 如果接受api_token，则传递它
                constructor_kwargs["api_token"] = get_tushare_token()
            
            task_instance = task_class(cls._db_manager, **constructor_kwargs)

            # 设置任务特定配置（如果任务类支持）
            if hasattr(task_instance, "set_config") and callable(task_instance.set_config):
                task_instance.set_config(task_config)
                logger.debug(f"应用任务特定配置: {task_name}")

            cls._task_instances[task_name] = task_instance
            logger.debug(f"创建任务实例: {task_name} (类型: {getattr(task_class, 'task_type', 'unknown')})")

        return cls._task_instances[task_name]

    @classmethod
    def get_registry_stats(cls) -> Dict[str, Any]:
        """新增：获取注册表统计信息"""
        if not cls._initialized:
            raise RuntimeError("UnifiedTaskFactory 尚未初始化，请先调用 initialize() 方法")
            
        stats = {
            "total_tasks": len(cls._task_registry),
            "task_types": cls.get_task_types(),
            "tasks_by_type": {}
        }
        
        for task_type in stats["task_types"]:
            stats["tasks_by_type"][task_type] = len(cls.get_task_names_by_type(task_type))
            
        return stats


# 为了向后兼容，保留原有TaskFactory的别名
TaskFactory = UnifiedTaskFactory


# 便捷函数
async def get_task(task_name: str) -> BaseTask:
    """便捷函数，获取任务实例"""
    return await UnifiedTaskFactory.get_task(task_name)


def get_tasks_by_type(task_type: Optional[str] = None) -> Dict[str, Type[BaseTask]]:
    """便捷函数，按类型获取任务字典"""
    # 移除Fallback逻辑，直接调用工厂方法
    return UnifiedTaskFactory.get_tasks_by_type(task_type)


def get_task_names_by_type(task_type: Optional[str] = None) -> List[str]:
    """便捷函数，按类型获取任务名称列表"""
    # 移除Fallback逻辑，直接调用工厂方法
    return UnifiedTaskFactory.get_task_names_by_type(task_type)


def get_task_types() -> List[str]:
    """便捷函数，获取所有任务类型"""
    # 移除Fallback逻辑，直接调用工厂方法
    return UnifiedTaskFactory.get_task_types() 