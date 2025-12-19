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
    # 使用公共的日志配置，仅传递日志级别
    setup_common_logging(
        log_level=log_level,
        log_to_file=False,
        reset=True  # 强制重置以覆盖任何之前的配置
    )
    return logging.getLogger(__name__)


def get_cli_logger(name: str) -> logging.Logger:
    """获取指定名称的 logger。"""
    return logging.getLogger(name)
