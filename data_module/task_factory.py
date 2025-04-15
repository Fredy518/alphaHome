import logging
from typing import Dict, Type, Optional, Any
import os
import json

from .base_task import Task
from .db_manager import DBManager
from .tasks.stock.daily import StockDailyTask
from .tasks.stock.daily_basic import StockDailyBasicTask
# 导入其他任务类

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
    
    管理任务实例的创建和数据库连接
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
        logger.info(f"数据库连接已初始化: {db_url}")
    
    @classmethod
    async def shutdown(cls):
        """关闭数据库连接"""
        if cls._db_manager:
            await cls._db_manager.close()
            cls._db_manager = None
            cls._task_instances.clear()
            cls._initialized = False
            logger.info("数据库连接已关闭")
    
    @classmethod
    def register_task(cls, task_name: str, task_class: Type[Task]):
        """注册新的任务类型"""
        cls._task_registry[task_name] = task_class
        logger.debug(f"注册任务类型: {task_name}")
    
    @classmethod
    def get_db_manager(cls):
        """获取数据库管理器实例"""
        if not cls._initialized:
            raise RuntimeError("TaskFactory 尚未初始化，请先调用 initialize() 方法")
        return cls._db_manager
    
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
            task_instance = task_class(
                cls._db_manager,
                api_token=get_tushare_token()
            )
            
            # 设置任务特定配置（如果任务类支持）
            if hasattr(task_instance, 'set_config') and callable(getattr(task_instance, 'set_config')):
                task_instance.set_config(task_config)
                logger.debug(f"应用任务特定配置: {task_name}")
                
            cls._task_instances[task_name] = task_instance
            logger.debug(f"创建任务实例: {task_name}")
                
        return cls._task_instances[task_name]

# 注册默认的任务类型
TaskFactory.register_task('stock_daily', StockDailyTask)
TaskFactory.register_task('stock_daily_basic', StockDailyBasicTask)
# 在这里注册其他任务类型

# 便捷函数
async def get_task(task_name: str) -> Task:
    """便捷函数，获取任务实例"""
    return await TaskFactory.get_task(task_name)
