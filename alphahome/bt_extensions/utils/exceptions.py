#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Custom Exceptions for Backtesting Module

自定义异常类，用于backtesting模块的错误处理
"""


class BacktestError(Exception):
    """
    回测框架基础异常类
    """
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        
    def __str__(self):
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class DataError(BacktestError):
    """
    数据相关异常
    """
    def __init__(self, message: str, table_name: str = None, ts_code: str = None):
        super().__init__(message, "DATA_ERROR")
        self.table_name = table_name
        self.ts_code = ts_code
        
    def __str__(self):
        details = []
        if self.table_name:
            details.append(f"表名: {self.table_name}")
        if self.ts_code:
            details.append(f"代码: {self.ts_code}")
        
        if details:
            return f"{super().__str__()} ({', '.join(details)})"
        return super().__str__()


class ConfigError(BacktestError):
    """
    配置相关异常
    """
    def __init__(self, message: str, config_key: str = None):
        super().__init__(message, "CONFIG_ERROR") 
        self.config_key = config_key
        
    def __str__(self):
        if self.config_key:
            return f"{super().__str__()} (配置项: {self.config_key})"
        return super().__str__()


 