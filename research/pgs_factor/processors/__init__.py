"""
PGS因子模块数据处理层
===================

数据处理层负责各种数据转换、计算和验证操作。

组件说明：
- pit_processor: PIT数据处理器，负责原始数据到PIT数据的转换和清洗
- factor_calculator: 因子计算器，负责P/G/S因子的具体计算逻辑
- data_validator: 数据验证器，负责数据质量检查和异常检测

处理流程：
原始数据 → pit_processor → PIT数据 → factor_calculator → 因子数据
                                          ↓
                                   data_validator
"""

# P/G 因子计算器实现（移除财务指标计算器，避免硬依赖）
try:
    from .production_p_factor_calculator import ProductionPFactorCalculator
except Exception:
    ProductionPFactorCalculator = None  # 兼容环境

try:
    from .production_g_factor_calculator import ProductionGFactorCalculator
except Exception:
    ProductionGFactorCalculator = None  # 兼容环境

# 仅导出现存组件，移除已废弃的财务指标别名
__all__ = [name for name, obj in [
    ('ProductionPFactorCalculator', ProductionPFactorCalculator),
    ('ProductionGFactorCalculator', ProductionGFactorCalculator),
] if obj is not None]
