"""
物化视图基础设施模块

提供物化视图系统的核心组件：
- MaterializedViewTask: 物化视图任务基类
- MaterializedViewSQL: SQL 模板生成器
- MaterializedViewValidator: 数据质量检查器
- MaterializedViewRefresh: 刷新执行器
- MaterializedViewMonitor: 监控器
"""

from .base_task import MaterializedViewTask
from .sql_templates import MaterializedViewSQL
from .validator import MaterializedViewValidator
from .refresh import MaterializedViewRefresh
from .monitor import MaterializedViewMonitor
from .evaluation_criteria import (
    EvaluationDimension,
    EvaluationCriteria,
    SuitabilityScore,
    SuitabilityLevel,
)
from .alerting import (
    MaterializedViewAlerting,
    AlertSeverity,
    AlertType,
)

__all__ = [
    'MaterializedViewTask',
    'MaterializedViewSQL',
    'MaterializedViewValidator',
    'MaterializedViewRefresh',
    'MaterializedViewMonitor',
    'EvaluationDimension',
    'EvaluationCriteria',
    'SuitabilityScore',
    'SuitabilityLevel',
    'MaterializedViewAlerting',
    'AlertSeverity',
    'AlertType',
]
