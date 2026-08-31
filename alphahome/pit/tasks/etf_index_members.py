"""Registered PIT task for ETF-tracked index constituents."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_etf_index_members_manager import (
    PITETFIndexMembersMonthlyManager,
)


@task_register()
class PITETFIndexMembersMonthlyTask(PITTask):
    name = "pit_etf_index_members_monthly"
    table_name = "pit_etf_index_members_monthly"
    description = "ETF跟踪指数成分月末PIT快照"
    contract = PITTaskContract(
        task_name=name,
        domain="etf_index_members",
        source_tables=(
            "rawdata.index_weight",
            "rawdata.fund_etf_basic",
            "rawdata.fund_etf_index",
            "rawdata.fund_portfolio",
        ),
        output_table="pit.pit_etf_index_members_monthly",
        pit_time_key="obs_date",
        primary_keys=("obs_date", "index_code", "ts_code", "method_version"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range"),
        manager_class=PITETFIndexMembersMonthlyManager,
        audit_entity_keys=("index_code", "ts_code", "method_version"),
        audit_denominator="etf_index_member_source_pairs",
    )


__all__ = ["PITETFIndexMembersMonthlyTask"]
