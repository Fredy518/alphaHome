"""
GUI Services Module
GUI服务模块

为GUI控制器提供业务逻辑服务接口。

重组后的服务架构：
- task_registry_service: 任务注册、发现、元数据管理
- task_execution_service: 任务执行引擎、流程控制、状态跟踪
- configuration_service: 配置管理、数据库连接测试
"""

# 导入重组后的服务模块
from . import task_registry_service
from . import task_execution_service
from . import configuration_service

__all__ = [
    "task_registry_service",
    "task_execution_service", 
    "configuration_service",
] 