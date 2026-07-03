"""Registered PIT financial statement and indicator tasks."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager
from alphahome.pit.pit_cashflow_quarterly_manager import PITCashflowQuarterlyManager
from alphahome.pit.pit_financial_indicators_manager import PITFinancialIndicatorsManager
from alphahome.pit.pit_income_quarterly_manager import PITIncomeQuarterlyManager


@task_register()
class PITIncomeQuarterlyTask(PITTask):
    name = "pit_income_quarterly"
    table_name = "pit_income_quarterly"
    description = "PIT利润表季度数据"
    contract = PITTaskContract(
        task_name=name,
        domain="financials",
        source_tables=("tushare.fina_income", "tushare.fina_express", "tushare.fina_forecast"),
        output_table="pit.pit_income_quarterly",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range", "single_backfill", "audit_only"),
        manager_class=PITIncomeQuarterlyManager,
    )


@task_register()
class PITBalanceQuarterlyTask(PITTask):
    name = "pit_balance_quarterly"
    table_name = "pit_balance_quarterly"
    description = "PIT资产负债表季度数据"
    contract = PITTaskContract(
        task_name=name,
        domain="financials",
        source_tables=("tushare.fina_balancesheet", "tushare.fina_express"),
        output_table="pit.pit_balance_quarterly",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range", "single_backfill", "audit_only"),
        manager_class=PITBalanceQuarterlyManager,
    )


@task_register()
class PITCashflowQuarterlyTask(PITTask):
    name = "pit_cashflow_quarterly"
    table_name = "pit_cashflow_quarterly"
    description = "PIT现金流量表季度数据"
    contract = PITTaskContract(
        task_name=name,
        domain="financials",
        source_tables=("tushare.fina_cashflow",),
        output_table="pit.pit_cashflow_quarterly",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range", "single_backfill", "audit_only"),
        manager_class=PITCashflowQuarterlyManager,
    )


@task_register()
class PITFinancialIndicatorsTask(PITTask):
    name = "pit_financial_indicators"
    table_name = "pit_financial_indicators"
    description = "PIT财务指标"
    contract = PITTaskContract(
        task_name=name,
        domain="financials",
        source_tables=("pit.pit_income_quarterly", "pit.pit_balance_quarterly"),
        output_table="pit.pit_financial_indicators",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date", "data_source"),
        dependencies=("pit_income_quarterly", "pit_balance_quarterly"),
        supported_modes=("incremental", "full_backfill", "manual_range", "single_backfill", "audit_only"),
        manager_class=PITFinancialIndicatorsManager,
    )


__all__ = [
    "PITIncomeQuarterlyTask",
    "PITBalanceQuarterlyTask",
    "PITCashflowQuarterlyTask",
    "PITFinancialIndicatorsTask",
]
