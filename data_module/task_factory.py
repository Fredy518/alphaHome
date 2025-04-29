import logging
import os
import json
from typing import List, Optional, Dict, Type, Any

from .base_task import Task
from .db_manager import DBManager
from .tasks import *  # 导入所有任务类

# 配置文件路径
CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')

# 读取配置文件
def load_config():
    # 首先尝试读取配置文件
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"读取配置文件失败: {e}，使用环境变量或默认值")
    
    # 如果配置文件不存在或读取失败，返回默认配置
    return {
        "database": {
            "url": os.environ.get('DATABASE_URL', 'postgresql://postgres:wuhao123@localhost:5432/tusharedb')
        },
        "api": {
            "tushare_token": os.environ.get('TUSHARE_TOKEN', '')
        },
        "tasks": {}  # 任务配置为空，让任务类使用自己的默认值
    }

# 获取配置值的函数
def get_database_url():
    return load_config()["database"]["url"]

def get_tushare_token():
    return load_config()["api"]["tushare_token"]

def get_task_config(task_name, key=None, default=None):
    """获取任务特定配置
    
    Args:
        task_name: 任务名称
        key: 配置键名，如果为None则返回整个任务配置
        default: 默认值，当配置不存在时返回
    
    Returns:
        任务配置或特定配置值
        
    Note:
        如果配置文件中没有指定任务配置，将返回空字典，让任务类使用自己的默认值
    """
    config = load_config()
    task_config = config.get("tasks", {}).get(task_name, {})
    
    if key is None:
        return task_config
    return task_config.get(key, default)

logger = logging.getLogger('task_factory')

class TaskFactory:
    """任务工厂类
    
    管理任务实例的创建、数据库连接
    """
    
    # 类变量
    _db_manager = None
    _task_instances = {}
    _task_registry = {}
    _initialized = False
    
    @classmethod
    def register_task(cls, task_name, task_class):
        """注册任务类型
        
        Args:
            task_name: 任务名称
            task_class: 任务类
        """
        cls._task_registry[task_name] = task_class
        logger.debug(f"注册任务类型: {task_name}")
    
    @classmethod
    async def initialize(cls, db_url=None):
        """初始化任务工厂，连接数据库"""
        if cls._initialized:
            logger.debug("数据库连接已存在，跳过初始化")
            return
        # 如果没有提供连接字符串，使用配置中的默认值
        if db_url is None:
            db_url = get_database_url()
        
        cls._db_manager = DBManager(db_url)
        await cls._db_manager.connect()
        
        cls._initialized = True
        logger.info(f"TaskFactory 已初始化: db_url={db_url}")
    
    @classmethod
    async def reload_config(cls):
        """重新加载配置并重新初始化数据库连接。"""
        logger.info("开始重新加载 TaskFactory 配置...")
        if not cls._initialized or not cls._db_manager:
            logger.warning("TaskFactory 尚未初始化，无法重载。将执行首次初始化。")
            await cls.initialize()
            return

        try:
            # 1. 关闭现有数据库连接
            logger.info("正在关闭现有数据库连接...")
            await cls._db_manager.close() # 假设 db_manager 有 close 方法
            logger.info("现有数据库连接已关闭。")

            # 2. 加载新配置
            logger.info("正在加载新配置...")
            new_config = load_config()
            new_db_url = new_config.get("database", {}).get("url")
            new_token = new_config.get("api", {}).get("tushare_token")

            if not new_db_url:
                logger.error("新配置中缺少有效的数据库 URL，无法重新初始化。")
                # 标记为未初始化状态？或者保持旧状态？ 标记为未初始化可能更安全
                cls._initialized = False
                cls._db_manager = None
                raise ValueError("新配置中缺少数据库 URL")

            logger.info(f"加载到新的数据库 URL: {new_db_url}")
            # 注意：这里没有显式更新 Token，因为 Token 通常在任务实例化时传递
            # 如果需要全局更新 Token，可以在这里存储 cls._tushare_token = new_token

            # 3. 使用新 URL 重新初始化 DBManager
            # 方案 A: 重新创建 DBManager 实例 (如果 DBManager 设计为不可变或重新连接复杂)
            logger.info("正在使用新 URL 创建新的 DBManager 实例...")
            cls._db_manager = DBManager(new_db_url)
            await cls._db_manager.connect()

            # 方案 B: 如果 DBManager 支持更新 URL 并重新连接 (需要 DBManager 支持)
            # logger.info("正在更新 DBManager 的 URL 并重新连接...")
            # await cls._db_manager.reconnect(new_db_url) # 假设有 reconnect 方法

            # 4. 清空旧的任务实例缓存，它们可能持有旧的 db_manager 引用
            # 或者如果任务设计得好，可以不用清空，取决于任务如何获取 db_manager
            logger.info("正在清空旧的任务实例缓存...")
            cls._task_instances.clear()

            cls._initialized = True # 确保标记为已初始化
            logger.info("TaskFactory 配置重新加载完成。")

        except Exception as e:
            logger.exception("重新加载 TaskFactory 配置时发生错误。TaskFactory 可能处于不稳定状态。")
            cls._initialized = False # 标记为未初始化以表示错误状态
            cls._db_manager = None # 丢弃可能无效的 db_manager
            # 可以选择向上抛出异常，或者让调用者检查状态
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
        logger.info("TaskFactory 已关闭")
    
    @classmethod
    def get_db_manager(cls):
        """获取数据库管理器实例"""
        if not cls._initialized:
            raise RuntimeError("TaskFactory 尚未初始化，请先调用 initialize() 方法")
        return cls._db_manager
    
    @classmethod
    def get_all_task_names(cls) -> List[str]:
        """获取所有已注册的任务名称列表"""
        if not cls._initialized:
            raise RuntimeError("TaskFactory 尚未初始化，请先调用 initialize() 方法")
        
        all_tasks = list(cls._task_registry.keys())
        logger.debug(f"获取到所有 {len(all_tasks)} 个已注册任务: {all_tasks}")
        return all_tasks
    
    @classmethod
    async def get_task(cls, task_name: str):
        """获取任务实例"""
        if not cls._initialized:
            await cls.initialize()
            
        if task_name not in cls._task_instances:
            # 如果任务实例不存在，创建它
            if task_name not in cls._task_registry:
                raise ValueError(f"未注册的任务类型: {task_name}，请先调用 register_task 方法注册")
                
            # 获取任务特定配置
            task_config = get_task_config(task_name)
            
            # 使用注册表中的任务类创建实例
            task_class = cls._task_registry[task_name]
            
            # 检查任务类构造函数是否接受api_token参数
            import inspect
            init_params = inspect.signature(task_class.__init__).parameters
            if 'api_token' in init_params:
                # 如果接受api_token，则传递它
                task_instance = task_class(
                    cls._db_manager,
                    api_token=get_tushare_token()
                )
            else:
                # 如果不接受api_token，则只传递db_manager
                task_instance = task_class(cls._db_manager)
            
            # 设置任务特定配置（如果任务类支持）
            if hasattr(task_instance, 'set_config') and callable(getattr(task_instance, 'set_config')):
                task_instance.set_config(task_config)
                logger.debug(f"应用任务特定配置: {task_name}")
                
            cls._task_instances[task_name] = task_instance
            logger.debug(f"创建任务实例: {task_name}")
                
        return cls._task_instances[task_name]

# 导入装饰器模块中的函数，将已装饰的任务注册到工厂
from .task_decorator import register_tasks_to_factory

# 初始化并注册所有通过装饰器注册的任务
register_tasks_to_factory()

# 便捷函数
async def get_task(task_name: str) -> Task:
    """便捷函数，获取任务实例"""
    return await TaskFactory.get_task(task_name)
