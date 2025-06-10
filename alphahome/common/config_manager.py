import json
import logging
import os
import shutil
from threading import Lock
from typing import Any, Dict, Optional

import appdirs

logger = logging.getLogger("config_manager")


class ConfigManager:
    """统一的配置管理器 - 单例模式"""

    _instance = None
    _lock = Lock()

    # 应用配置常量
    APP_NAME = "alphahome"
    APP_AUTHOR = "trademaster"

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ConfigManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        # 配置文件路径
        self.config_dir = appdirs.user_config_dir(self.APP_NAME, self.APP_AUTHOR)
        self.config_file = os.path.join(self.config_dir, "config.json")

        # 配置缓存
        self._config_cache = None
        self._config_loaded = False

        self._initialized = True

    def load_config(self) -> Dict[str, Any]:
        """加载配置文件，支持缓存和环境变量回退"""
        if self._config_loaded and self._config_cache is not None:
            logger.debug("从缓存加载配置。")
            return self._config_cache

        # 配置迁移逻辑
        self._migrate_old_config()

        logger.info(f"尝试从用户配置路径加载设置: {self.config_file}")

        config_data = {}
        # 读取配置文件
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
            except Exception as e:
                logger.warning(
                    f"读取配置文件 {self.config_file} 失败: {e}，使用环境变量或默认值"
                )
        else:
            logger.warning(f"配置文件 {self.config_file} 未找到，将尝试环境变量。")

        # 处理环境变量回退
        db_url = config_data.get("database", {}).get("url")
        tushare_token = config_data.get("api", {}).get("tushare_token")

        if not db_url:
            logger.info(
                "配置文件中未找到数据库 URL，尝试从环境变量 DATABASE_URL 加载。"
            )
            db_url_from_env = os.environ.get("DATABASE_URL")
            if db_url_from_env:
                logger.info("成功从环境变量 DATABASE_URL 加载数据库 URL。")
                db_url = db_url_from_env
            else:
                logger.warning("配置文件和环境变量均未设置有效的数据库 URL。")

        if not tushare_token:
            tushare_token_from_env = os.environ.get("TUSHARE_TOKEN")
            if tushare_token_from_env:
                logger.info("从环境变量 TUSHARE_TOKEN 加载 Tushare Token。")
                tushare_token = tushare_token_from_env

        # 构建完整配置结构
        final_config = {
            "database": {"url": db_url},
            "api": {"tushare_token": tushare_token or ""},
            "tasks": config_data.get("tasks", {}),
            "backtesting": config_data.get("backtesting", {}),
        }

        self._config_cache = final_config
        self._config_loaded = True
        logger.debug("配置已加载并缓存。")
        return self._config_cache

    def reload_config(self):
        """重新加载配置并清空缓存"""
        logger.info("开始重新加载配置...")
        self._config_cache = None
        self._config_loaded = False
        logger.info("配置缓存已清除，将重新加载。")
        return self.load_config()

    def _migrate_old_config(self):
        """迁移旧配置文件到新路径"""
        try:
            # 定义旧路径组件
            OLD_APP_NAME = "alphaHomeApp"
            OLD_APP_AUTHOR = "YourAppNameOrAuthor"
            old_config_dir = appdirs.user_config_dir(OLD_APP_NAME, OLD_APP_AUTHOR)
            old_config_file_path = os.path.join(old_config_dir, "config.json")

            # 检查是否需要迁移
            if os.path.exists(old_config_file_path) and not os.path.exists(
                self.config_file
            ):
                logger.info(f"检测到旧配置文件: {old_config_file_path}")
                logger.info(f"将尝试迁移到新路径: {self.config_file}")
                try:
                    # 确保新目录存在
                    os.makedirs(self.config_dir, exist_ok=True)
                    # 移动文件
                    shutil.move(old_config_file_path, self.config_file)
                    logger.info("配置文件已成功迁移到新路径。")
                except (IOError, OSError, shutil.Error) as move_err:
                    logger.warning(f"迁移旧配置文件失败: {move_err}")
        except Exception as migration_err:
            logger.error(f"检查或迁移旧配置文件时发生意外错误: {migration_err}")

    def get_database_url(self) -> Optional[str]:
        """获取数据库连接URL"""
        return self.load_config()["database"]["url"]

    def get_tushare_token(self) -> str:
        """获取Tushare API Token"""
        return self.load_config()["api"]["tushare_token"]

    def get_task_config(
        self, task_name: str, key: Optional[str] = None, default: Any = None
    ) -> Any:
        """获取任务特定配置

        Args:
            task_name: 任务名称
            key: 配置键名，如果为None则返回整个任务配置
            default: 默认值，当配置不存在时返回

        Returns:
            任务配置或特定配置值
        """
        config = self.load_config()
        task_config = config.get("tasks", {}).get(task_name, {})

        if key is None:
            return task_config
        return task_config.get(key, default)

    def get_backtesting_config(
        self, key: Optional[str] = None, default: Any = None
    ) -> Any:
        """获取回测模块配置

        Args:
            key: 配置键名，如果为None则返回整个回测配置
            default: 默认值，当配置不存在时返回

        Returns:
            回测配置或特定配置值
        """
        config = self.load_config()
        backtesting_config = config.get("backtesting", {})

        if key is None:
            return backtesting_config
        return backtesting_config.get(key, default)


# 全局配置管理器实例
_config_manager = ConfigManager()


# 便捷函数接口
def load_config() -> Dict[str, Any]:
    """加载配置文件"""
    return _config_manager.load_config()


def reload_config() -> Dict[str, Any]:
    """重新加载配置文件"""
    return _config_manager.reload_config()


def get_database_url() -> Optional[str]:
    """获取数据库连接URL"""
    return _config_manager.get_database_url()


def get_tushare_token() -> str:
    """获取Tushare API Token"""
    return _config_manager.get_tushare_token()


def get_task_config(
    task_name: str, key: Optional[str] = None, default: Any = None
) -> Any:
    """获取任务特定配置"""
    return _config_manager.get_task_config(task_name, key, default)


def get_backtesting_config(key: Optional[str] = None, default: Any = None) -> Any:
    """获取回测模块配置"""
    return _config_manager.get_backtesting_config(key, default)
