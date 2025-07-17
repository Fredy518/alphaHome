#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自定义异常 - Backtrader增强工具
"""

class BtExtensionsError(Exception):
    """bt_extensions 模块所有自定义异常的基类"""
    def __init__(self, message: str, **kwargs):
        super().__init__(message)
        self.message = message
        self.details = kwargs

    def __str__(self):
        if not self.details:
            return self.message
        
        details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
        return f"{self.message} ({details_str})"

class DataFeedError(BtExtensionsError):
    """数据供给相关错误"""
    pass

class RunnerExecutionError(BtExtensionsError):
    """并行执行器相关错误"""
    pass

class CacheOperationError(BtExtensionsError):
    """缓存操作相关错误"""
    pass

class BatchLoadingError(BtExtensionsError):
    """批量数据加载相关错误"""
    pass

class StrategyParameterError(BtExtensionsError):
    """策略参数或配置相关错误"""
    pass
