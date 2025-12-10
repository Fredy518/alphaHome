#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PIT 处理任务子包
----------------

包含将四个 PIT Manager 适配为 ProcessorTaskBase 的任务实现。
导入模块以触发 @task_register 注册。
"""

# 导入四个任务以触发注册
from .pit_balance_quarterly_task import PITBalanceQuarterlyTask  # noqa: F401
from .pit_income_quarterly_task import PITIncomeQuarterlyTask  # noqa: F401
from .pit_financial_indicators_task import PITFinancialIndicatorsTask  # noqa: F401
from .pit_industry_classification_task import PITIndustryClassificationTask  # noqa: F401

__all__ = [
    "PITBalanceQuarterlyTask",
    "PITIncomeQuarterlyTask",
    "PITFinancialIndicatorsTask",
    "PITIndustryClassificationTask",
]


