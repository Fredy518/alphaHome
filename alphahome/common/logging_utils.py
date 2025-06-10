#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
通用日志管理工具。

提供统一的日志配置、获取和扩展功能，用于整个项目的日志管理。
推荐所有模块通过本工具获取 logger，而不是直接使用 logging 模块。

用法示例:
    from ..common.logging_utils import get_logger, setup_logging

    # 在主入口处初始化日志配置（仅调用一次）
    setup_logging(log_level="INFO", log_to_file=False)

    # 在各个模块中获取 logger
    logger = get_logger(__name__)  # 推荐用法，使用模块名
    logger = get_logger("custom_name")  # 兼容自定义名称

    logger.info("信息日志")
    logger.warning("警告日志")
    logger.error("错误日志")
"""

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Union

# 配置项默认值
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOG_DIR = "logs"

# 记录日志配置状态，避免重复初始化
_logging_initialized = False
_log_config = {
    "level": DEFAULT_LOG_LEVEL,
    "format": DEFAULT_LOG_FORMAT,
    "date_format": DEFAULT_DATE_FORMAT,
    "log_to_file": False,
    "log_dir": DEFAULT_LOG_DIR,
}


def setup_logging(
    log_level: Union[int, str] = DEFAULT_LOG_LEVEL,
    log_format: str = DEFAULT_LOG_FORMAT,
    date_format: str = DEFAULT_DATE_FORMAT,
    log_to_file: bool = False,
    log_dir: str = DEFAULT_LOG_DIR,
    log_filename: Optional[str] = None,
    reset: bool = False,
) -> None:
    """
    配置全局日志。推荐在应用启动时调用一次。

    Args:
        log_level: 日志级别，可以是字符串（"DEBUG", "INFO"等）或 logging 常量（logging.DEBUG等）
        log_format: 日志格式
        date_format: 日期格式
        log_to_file: 是否将日志写入文件
        log_dir: 日志文件目录
        log_filename: 日志文件名，默认为 app_{当前日期}.log
        reset: 是否强制重置日志配置

    Returns:
        None
    """
    global _logging_initialized, _log_config

    # 已经初始化且不需要重置，则直接返回
    if _logging_initialized and not reset:
        logging.info(
            "日志系统已经初始化，跳过重复配置。设置 reset=True 可强制重新配置。"
        )
        return

    # 转换字符串日志级别到 logging 常量
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), DEFAULT_LOG_LEVEL)

    # 更新配置
    _log_config["level"] = log_level
    _log_config["format"] = log_format
    _log_config["date_format"] = date_format
    _log_config["log_to_file"] = log_to_file
    _log_config["log_dir"] = log_dir

    # 移除所有现有处理器
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 设置根日志级别
    root_logger.setLevel(log_level)

    # 创建格式化器
    formatter = logging.Formatter(log_format, date_format)

    # 添加控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 如果需要写入文件
    if log_to_file:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        if log_filename is None:
            current_date = datetime.now().strftime("%Y%m%d")
            log_filename = f"app_{current_date}.log"

        log_file_path = os.path.join(log_dir, log_filename)
        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        logging.info(f"日志将同时写入文件: {log_file_path}")

    _logging_initialized = True
    logging.info(f"日志系统初始化完成，级别: {logging.getLevelName(log_level)}")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    获取指定名称的 logger。
    如果 name 为 None，则返回根 logger。

    Args:
        name: logger 名称，推荐使用 __name__ 以便追踪日志来源
              如需兼容之前的命名风格，也可传入自定义字符串

    Returns:
        logging.Logger: 日志记录器实例
    """
    if not _logging_initialized:
        # 自动调用默认配置，确保至少有基础日志功能
        setup_logging()

    logger = logging.getLogger(name)

    # 如果没有显式设置级别，使用 root logger 的级别
    if not logger.level:
        logger.setLevel(_log_config["level"])

    return logger


def get_current_log_config() -> Dict[str, Any]:
    """
    获取当前日志配置。

    Returns:
        Dict[str, Any]: 当前日志配置字典副本
    """
    return _log_config.copy()


# 为了向后兼容，提供一些直接的日志辅助函数
def log_info(message: str, module_name: Optional[str] = None) -> None:
    """记录一条 INFO 级别的日志。主要用于向后兼容，新代码推荐直接使用 get_logger()"""
    get_logger(module_name).info(message)


def log_warning(message: str, module_name: Optional[str] = None) -> None:
    """记录一条 WARNING 级别的日志。主要用于向后兼容，新代码推荐直接使用 get_logger()"""
    get_logger(module_name).warning(message)


def log_error(
    message: str, module_name: Optional[str] = None, exc_info: bool = False
) -> None:
    """记录一条 ERROR 级别的日志。主要用于向后兼容，新代码推荐直接使用 get_logger()"""
    get_logger(module_name).error(message, exc_info=exc_info)


def log_exception(message: str, module_name: Optional[str] = None) -> None:
    """记录异常信息。主要用于向后兼容，新代码推荐直接使用 get_logger()"""
    get_logger(module_name).exception(message)
