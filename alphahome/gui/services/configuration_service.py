"""
存储设置处理器

负责处理所有与存储设置相关的逻辑，包括：
- 加载和保存配置文件 (config.json)
- 管理 PostgreSQL 和 Tushare 的配置信息
- 与控制器协调，触发配置重载
"""
import json
import os
from typing import Callable, Dict, Optional, Any

import appdirs

from ...common.logging_utils import get_logger
from ...common.task_system import UnifiedTaskFactory
from ...common.db_manager import create_async_manager

logger = get_logger(__name__)

# --- 配置路径管理 ---
# 使用用户主目录下的 .alphahome 目录作为配置目录
CONFIG_DIR = os.path.expanduser("~/.alphahome")
CONFIG_FILE_PATH = os.path.join(CONFIG_DIR, "config.json")

# --- 回调函数 ---
_send_response_callback: Optional[Callable] = None


def initialize_storage_settings(response_callback: Callable):
    """初始化存储设置模块，设置回调函数。"""
    global _send_response_callback
    _send_response_callback = response_callback
    os.makedirs(CONFIG_DIR, exist_ok=True)  # 确保配置目录存在
    logger.info(f"存储设置处理器已初始化。配置文件路径: {CONFIG_FILE_PATH}")


async def handle_get_storage_settings():
    """处理获取存储设置的请求，加载并发送到UI。"""
    try:
        settings = get_current_settings()
        if _send_response_callback:
            _send_response_callback("STORAGE_SETTINGS_UPDATE", settings)
            _send_response_callback("LOG", {"level": "info", "message": "存储设置已加载并发送到UI。"})
    except Exception as e:
        logger.exception("处理获取存储设置时出错。")
        if _send_response_callback:
            _send_response_callback("ERROR", f"获取存储设置失败: {e}")


def get_current_settings() -> Dict:
    """从JSON文件加载当前设置。"""
    try:
        if os.path.exists(CONFIG_FILE_PATH):
            with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
                settings = json.load(f)
                logger.info(f"成功从 {CONFIG_FILE_PATH} 加载设置。")
                return settings
        else:
            logger.warning(f"配置文件不存在: {CONFIG_FILE_PATH}。返回空设置。")
            return {}
    except (json.JSONDecodeError, IOError) as e:
        logger.exception(f"加载配置文件 {CONFIG_FILE_PATH} 时出错")
        if _send_response_callback:
            _send_response_callback("ERROR", f"加载配置文件失败: {e}")
        return {}


async def handle_save_settings(settings_from_gui: Dict):
    """处理保存设置的逻辑，并触发配置重载。"""
    try:
        with open(CONFIG_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(settings_from_gui, f, indent=4, ensure_ascii=False)
        logger.info(f"设置已成功保存到 {CONFIG_FILE_PATH}")
        if _send_response_callback:
            _send_response_callback("LOG_ENTRY", "设置已成功保存。")

        # 触发后台任务配置重载
        logger.info("正在触发 TaskFactory 配置重载...")
        await UnifiedTaskFactory.reload_config()
        logger.info("TaskFactory 配置重载完成。")

        if _send_response_callback:
            _send_response_callback("LOG_ENTRY", "后台任务配置已根据新设置重新加载。")
            _send_response_callback("STATUS", "设置已保存并重新加载。")

    except IOError as e:
        logger.exception(f"保存配置文件到 {CONFIG_FILE_PATH} 时出错")
        if _send_response_callback:
            _send_response_callback("ERROR", f"保存配置文件失败: {e}")
    except Exception as e:
        logger.exception("调用 TaskFactory.reload_config() 时出错")
        if _send_response_callback:
            _send_response_callback(
                "ERROR", f"重载任务配置失败: {e}. 请重启应用使设置完全生效。"
            )


async def test_database_connection(db_url: str) -> Dict[str, Any]:
    """
    尝试使用给定的URL连接到数据库。

    Args:
        db_url: 要测试的数据库连接字符串。

    Returns:
        一个包含 'status' ('success' or 'error') 和 'message' 的字典。
    """
    if not db_url:
        return {"status": "error", "message": "数据库连接URL不能为空。"}

    temp_manager = None
    try:
        logger.info(f"正在尝试使用URL连接到数据库: {db_url[:db_url.find('@')]}...")
        temp_manager = create_async_manager(db_url)
        
        # test_connection 返回布尔值，需要检查其结果
        is_connected = await temp_manager.test_connection()
        
        if is_connected:
            logger.info("数据库连接测试成功。")
            return {"status": "success", "message": "数据库连接成功！"}
        else:
            # test_connection 内部已经记录了详细错误
            logger.warning("数据库连接测试返回失败状态。")
            return {"status": "error", "message": "数据库连接验证失败。请检查日志获取详细信息。"}
            
    except Exception as e:
        # 这个except块主要捕获 create_async_manager 或其他意外错误
        logger.error(f"数据库连接测试过程中发生意外错误: {e}", exc_info=True)
        return {"status": "error", "message": f"数据库连接失败: {e}"}
    finally:
        if temp_manager:
            await temp_manager.close()
            logger.info("临时数据库管理器连接已关闭。") 