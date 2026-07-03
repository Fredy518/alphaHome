"""Registered PIT industry tasks."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_industry_classification_manager import PITIndustryClassificationManager


@task_register()
class PITIndustryClassificationTask(PITTask):
    name = "pit_industry_classification"
    table_name = "pit_industry_classification"
    description = "PIT行业分类月度快照"
    contract = PITTaskContract(
        task_name=name,
        domain="industry",
        source_tables=("tushare.index_swmember", "tushare.index_cimember"),
        output_table="pit.pit_industry_classification",
        pit_time_key="obs_date",
        primary_keys=("ts_code", "obs_date", "data_source"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=PITIndustryClassificationManager,
    )


__all__ = ["PITIndustryClassificationTask"]
