"""Barra computation tasks.

独立于 processors 模块的 Barra 任务，直接继承 common/task_system 的 BaseTask。

提供的任务:
- BarraExposuresDailyTask: 日度因子暴露计算（MVP 版）
- BarraExposuresFullTask: 日度因子暴露计算（Full 版）
- BarraFactorReturnsDailyTask: 日度因子收益回归

使用方式:
    from alphahome.barra.tasks import BarraExposuresDailyTask, BarraFactorReturnsDailyTask
    
    task = BarraExposuresDailyTask(db)
    result = await task.execute(trade_date="2025-01-01")
"""

from .barra_exposures_daily import BarraExposuresDailyTask
from .barra_exposures_full import BarraExposuresFullTask
from .barra_factor_returns_daily import BarraFactorReturnsDailyTask

__all__ = [
    "BarraExposuresDailyTask",
    "BarraExposuresFullTask",
    "BarraFactorReturnsDailyTask",
]
