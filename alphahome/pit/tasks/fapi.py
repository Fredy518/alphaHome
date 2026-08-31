"""Registered source-adapted industry FAPI PIT task."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_industry_fapi_manager import PITIndustryFAPIManager


@task_register()
class PITIndustryFAPIMonthlyTask(PITTask):
    name = "pit_industry_fapi_monthly"
    table_name = "pit_industry_fapi_monthly"
    description = "申万一级、二级行业相对中证800的FAPI月末PIT快照"
    contract = PITTaskContract(
        task_name=name,
        domain="industry_fapi",
        source_tables=(
            "pit.pit_stock_fttm_monthly",
            "pit.pit_industry_classification",
            "rawdata.stock_dailybasic",
            "rawdata.index_weight",
        ),
        output_table="pit.pit_industry_fapi_monthly",
        pit_time_key="obs_date",
        primary_keys=(
            "obs_date",
            "classification_source",
            "industry_level",
            "industry_code",
            "benchmark_code",
            "method_version",
        ),
        dependencies=("pit_stock_fttm_monthly", "pit_industry_classification"),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=PITIndustryFAPIManager,
        audit_entity_keys=(
            "classification_source",
            "industry_level",
            "industry_code",
        ),
        audit_denominator="pit_time_structural_industries",
    )


__all__ = ["PITIndustryFAPIMonthlyTask"]
