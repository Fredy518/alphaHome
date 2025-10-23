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

        # 配置文件路径 - 使用用户主目录下的 .alphahome 目录
        self.config_dir = os.path.expanduser("~/.alphahome")
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
                # 尝试使用UTF-8编码读取
                with open(self.config_file, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
            except UnicodeDecodeError:
                # 如果UTF-8解码失败，尝试使用系统默认编码或其他编码
                try:
                    with open(self.config_file, "r", encoding="gbk") as f:
                        config_data = json.load(f)
                    logger.warning("配置文件使用GBK编码，建议转换为UTF-8编码")
                except Exception as e2:
                    logger.warning(f"尝试GBK编码也失败: {e2}，使用环境变量或默认值")
                    config_data = {}
            except Exception as e:
                logger.warning(
                    f"读取配置文件 {self.config_file} 失败: {e}，使用环境变量或默认值"
                )
        else:
            logger.warning(f"配置文件 {self.config_file} 未找到，将尝试环境变量。")

        # 使用从文件加载的配置作为基础
        final_config = config_data

        # 确保顶层键存在，以避免 KeyErrors
        final_config.setdefault("database", {})
        final_config.setdefault("api", {})
        final_config.setdefault("backtesting", {})

        # 如果配置文件中缺少，则尝试从环境变量加载
        if not final_config["database"].get("url"):
            db_url_from_env = os.environ.get("DATABASE_URL")
            if db_url_from_env:
                logger.info("成功从环境变量 DATABASE_URL 加载数据库 URL。")
                final_config["database"]["url"] = db_url_from_env
            else:
                logger.warning("配置文件和环境变量均未设置有效的数据库 URL。")

        if not final_config["api"].get("tushare_token"):
            tushare_token_from_env = os.environ.get("TUSHARE_TOKEN")
            if tushare_token_from_env:
                logger.info("从环境变量 TUSHARE_TOKEN 加载 Tushare Token。")
                final_config["api"]["tushare_token"] = tushare_token_from_env

        # Hikyuu 数据目录配置：优先配置文件，其次环境变量
        if not final_config["backtesting"].get("hikyuu_data_dir"):
            hikyuu_dir_from_env = os.environ.get("HIKYUU_DATA_DIR")
            if hikyuu_dir_from_env:
                logger.info("从环境变量 HIKYUU_DATA_DIR 加载 Hikyuu 数据目录。")
                final_config["backtesting"]["hikyuu_data_dir"] = hikyuu_dir_from_env

        self._config_cache = final_config
        self._config_loaded = True
        logger.debug(f"配置已加载并缓存: {final_config}")
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
            # 第一步：从旧的alphaHomeApp路径迁移（向后兼容）
            OLD_APP_NAME = "alphaHomeApp"
            OLD_APP_AUTHOR = "YourAppNameOrAuthor"
            old_config_dir = appdirs.user_config_dir(OLD_APP_NAME, OLD_APP_AUTHOR)
            old_config_file_path = os.path.join(old_config_dir, "config.json")

            # 检查是否需要从旧路径迁移
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
                    logger.info("配置文件已成功从旧路径迁移到新路径。")
                    return  # 迁移完成，直接返回
                except (IOError, OSError, shutil.Error) as move_err:
                    logger.warning(f"迁移旧配置文件失败: {move_err}")

            # 第二步：从旧的trademaster/alphahome路径迁移到新的~/.alphahome路径
            legacy_config_dir = appdirs.user_config_dir(self.APP_NAME, self.APP_AUTHOR)
            legacy_config_file = os.path.join(legacy_config_dir, "config.json")

            # 检查是否需要从遗留路径迁移
            if os.path.exists(legacy_config_file) and not os.path.exists(self.config_file):
                logger.info(f"检测到遗留配置文件: {legacy_config_file}")
                logger.info(f"将尝试迁移到新路径: {self.config_file}")
                try:
                    # 确保新目录存在
                    os.makedirs(self.config_dir, exist_ok=True)
                    # 复制文件而不是移动，以防万一出现问题
                    shutil.copy2(legacy_config_file, self.config_file)
                    logger.info("配置文件已成功从遗留路径迁移到新路径。")
                    logger.info("保留原文件以防需要回滚。如确认迁移成功，可手动删除旧配置文件。")
                except (IOError, OSError, shutil.Error) as move_err:
                    logger.warning(f"迁移遗留配置文件失败: {move_err}")

        except Exception as migration_err:
            logger.error(f"检查或迁移配置文件时发生意外错误: {migration_err}")

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

    def get_hikyuu_data_dir(self) -> Optional[str]:
        """获取 Hikyuu 数据目录

        Returns:
            配置中的 Hikyuu 数据目录路径，若未配置返回 None
        """
        cfg = self.load_config()
        return cfg.get("backtesting", {}).get("hikyuu_data_dir")


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


def get_hikyuu_data_dir() -> Optional[str]:
    """获取 Hikyuu 数据目录"""
    return _config_manager.get_hikyuu_data_dir()
