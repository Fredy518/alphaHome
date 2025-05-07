# alphahome/derivatives/__init__.py
# 这个文件使得 derivatives 目录可以作为一个 Python 包被导入。
# 它同时定义了这个包对外暴露的公共接口。

from .base_derivative_task import BaseDerivativeTask
from .derivative_task_factory import (
    DerivativeTaskFactory, 
    get_derivative_task,      # 便捷函数，用于获取任务实例
    derivative_task_register  # 装饰器，用于注册新的衍生品任务
)

# 定义当使用 from alphahome.derivatives import * 时，哪些名称会被导入
__all__ = [
    "BaseDerivativeTask",       # 衍生品任务的基类
    "DerivativeTaskFactory",    # 衍生品任务工厂类
    "get_derivative_task",      # 获取任务实例的便捷函数
    "derivative_task_register", # 任务注册装饰器
] 