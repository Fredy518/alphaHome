"""
GUI Services Module
GUI服务模块

为GUI控制器提供业务逻辑服务接口。

重组后的服务架构：
- task_registry_service: 任务注册、发现、元数据管理
- task_execution_service: 任务执行引擎、流程控制、状态跟踪
- configuration_service: 配置管理、数据库连接测试
- pit_service: PIT任务、审计、覆盖率和单股诊断
"""

__all__ = [
    "task_registry_service",
    "task_execution_service",
    "configuration_service",
    "feature_service",
    "factor_service",
    "pit_service",
]


def __getattr__(name):
    if name in __all__:
        from importlib import import_module
        module = import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(name)
