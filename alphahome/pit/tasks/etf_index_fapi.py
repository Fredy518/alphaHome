"""Registered ETF-index FAPI PIT task."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_etf_index_fapi_manager import PITETFIndexFAPIMonthlyManager


@task_register()
class PITETFIndexFAPIMonthlyTask(PITTask):
    name = "pit_etf_index_fapi_monthly"
    table_name = "pit_etf_index_fapi_monthly"
    description = "ETF跟踪指数相对中证800的FAPI月末PIT快照"
    contract = PITTaskContract(
        task_name=name,
        domain="etf_index_fapi",
        source_tables=(
            "pit.pit_etf_index_members_monthly",
            "pit.pit_stock_fttm_monthly",
            "rawdata.stock_dailybasic",
            "rawdata.index_weight",
        ),
        output_table="pit.pit_etf_index_fapi_monthly",
        pit_time_key="obs_date",
        primary_keys=("obs_date", "index_code", "benchmark_code", "method_version"),
        dependencies=("pit_etf_index_members_monthly", "pit_stock_fttm_monthly"),
        supported_modes=("incremental", "full_backfill", "manual_range"),
        manager_class=PITETFIndexFAPIMonthlyManager,
        audit_entity_keys=("index_code", "benchmark_code", "method_version"),
        audit_denominator="etf_index_member_source_pairs",
    )


__all__ = ["PITETFIndexFAPIMonthlyTask"]
