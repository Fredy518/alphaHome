"""
日志管理模块

提供统一的日志配置接口，支持命令行级别的日志配置。
"""

import logging
from alphahome.common.logging_utils import setup_logging as setup_common_logging

def setup_cli_logging(log_level: str = "INFO") -> logging.Logger:
    """
    设置CLI日志配置。
    
    Args:
        log_level: 日志级别 (DEBUG/INFO/WARNING/ERROR)
        
    Returns:
        配置好的 logger 实例
    """
    # 使用公共的日志配置；尽量避免重复 reset（会导致输出多次“初始化完成”）
    # 但仍需确保 log_level 生效：即使 logging_utils 已初始化，也要更新 root logger 的 level。
    setup_common_logging(log_level=log_level, log_to_file=False, reset=False)

    # 强制同步 root logger 的 level，避免 setup_logging(reset=False) 时因“已初始化”而跳过更新
    if isinstance(log_level, str):
        level = getattr(logging, log_level.upper(), logging.INFO)
    else:
        level = int(log_level)
    logging.getLogger().setLevel(level)
    return logging.getLogger(__name__)


def get_cli_logger(name: str) -> logging.Logger:
    """获取指定名称的 logger。"""
    return logging.getLogger(name)
