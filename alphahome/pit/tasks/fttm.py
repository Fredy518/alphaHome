"""Registered stock and SW industry FTTM PIT tasks."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_industry_fttm_manager import PITIndustryFTTMManager
from alphahome.pit.pit_stock_fttm_manager import PITStockFTTMManager


@task_register()
class PITStockFTTMMonthlyTask(PITTask):
    name = "pit_stock_fttm_monthly"
    table_name = "pit_stock_fttm_monthly"
    description = "个股机构FTTM月末PIT快照"
    contract = PITTaskContract(
        task_name=name,
        domain="stock_fttm",
        source_tables=(
            "rawdata.stock_report_rc",
            "rawdata.stock_dailybasic",
        ),
        output_table="pit.pit_stock_fttm_monthly",
        pit_time_key="obs_date",
        primary_keys=("ts_code", "org_name", "obs_date"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=PITStockFTTMManager,
        audit_entity_keys=("ts_code",),
        audit_denominator="pit_time_active_stocks",
    )


@task_register()
class PITIndustryFTTMMonthlyTask(PITTask):
    name = "pit_industry_fttm_monthly"
    table_name = "pit_industry_fttm_monthly"
    description = "申万一级、二级行业FTTM月末PIT快照"
    contract = PITTaskContract(
        task_name=name,
        domain="industry_fttm",
        source_tables=(
            "pit.pit_stock_fttm_monthly",
            "pit.pit_industry_classification",
            "rawdata.stock_dailybasic",
            "rawdata.stock_basic",
        ),
        output_table="pit.pit_industry_fttm_monthly",
        pit_time_key="obs_date",
        primary_keys=(
            "obs_date",
            "classification_source",
            "industry_level",
            "industry_code",
            "weight_basis",
        ),
        dependencies=("pit_stock_fttm_monthly", "pit_industry_classification"),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=PITIndustryFTTMManager,
        audit_entity_keys=(
            "classification_source",
            "industry_level",
            "industry_code",
            "weight_basis",
        ),
        audit_denominator="pit_time_structural_industries",
    )


__all__ = ["PITIndustryFTTMMonthlyTask", "PITStockFTTMMonthlyTask"]
