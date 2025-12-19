"""
因子计算模块

提供各类因子并行计算的功能模块。
"""

from .p_factor import run_parallel_p_factor_calculation

__all__ = [
    'run_parallel_p_factor_calculation',
]