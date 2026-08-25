"""Registered important-index and all-A FTTM PIT task."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_index_fttm_manager import PITIndexFTTMManager


@task_register()
class PITIndexFTTMMonthlyTask(PITTask):
    name = "pit_index_fttm_monthly"
    table_name = "pit_index_fttm_monthly"
    description = "重要指数与全A的FTTM月末PIT快照"
    contract = PITTaskContract(
        task_name=name,
        domain="index_fttm",
        source_tables=(
            "pit.pit_stock_fttm_monthly",
            "rawdata.index_weight",
            "rawdata.stock_dailybasic",
            "rawdata.stock_basic",
        ),
        output_table="pit.pit_index_fttm_monthly",
        pit_time_key="obs_date",
        primary_keys=(
            "obs_date",
            "universe_type",
            "universe_code",
            "weight_basis",
        ),
        dependencies=("pit_stock_fttm_monthly",),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=PITIndexFTTMManager,
        audit_entity_keys=("universe_type", "universe_code", "weight_basis"),
        audit_denominator="configured_index_fttm_universes",
    )


__all__ = ["PITIndexFTTMMonthlyTask"]
