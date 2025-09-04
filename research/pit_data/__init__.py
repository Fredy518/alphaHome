"""
PIT 数据模块（Layer 2 + 2.5）
- 提供 PIT 基础表的采集/清洗/维护
- 提供通用财务指标（MVP）计算与维护（不含因子打分逻辑）
"""

# 便捷导出常用管理器
from .pit_income_quarterly_manager import PITIncomeQuarterlyManager
from .pit_balance_quarterly_manager import PITBalanceQuarterlyManager
from .pit_industry_classification_manager import PITIndustryClassificationManager
from .pit_financial_indicators_manager import PITFinancialIndicatorsManager

# 基础配置与基类
from .base import PITConfig

__all__ = [
    'PITIncomeQuarterlyManager',
    'PITBalanceQuarterlyManager',
    'PITIndustryClassificationManager',
    'PITFinancialIndicatorsManager',
    'PITConfig',
]

