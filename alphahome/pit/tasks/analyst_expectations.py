"""Registered PIT tasks for analyst expectations and realized surprise."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_earnings_surprise_annual_manager import (
    PITEarningsSurpriseAnnualManager,
)
from alphahome.pit.pit_stock_consensus_fy_manager import (
    PITStockConsensusFYMonthlyManager,
)


@task_register()
class PITStockConsensusFYMonthlyTask(PITTask):
    name = "pit_stock_consensus_fy_monthly"
    table_name = "pit_stock_consensus_fy_monthly"
    description = "个股固定财年分析师一致预期月末PIT快照"
    contract = PITTaskContract(
        task_name=name,
        domain="stock_consensus_fy",
        source_tables=("rawdata.stock_report_rc",),
        output_table="pit.pit_stock_consensus_fy_monthly",
        pit_time_key="obs_date",
        primary_keys=("obs_date", "ts_code", "target_year"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=PITStockConsensusFYMonthlyManager,
        audit_entity_keys=("ts_code",),
        audit_denominator="pit_time_active_stocks",
    )


@task_register()
class PITEarningsSurpriseAnnualTask(PITTask):
    name = "pit_earnings_surprise_annual"
    table_name = "pit_earnings_surprise_annual"
    description = "年度实际盈利相对公告前固定财年一致预期"
    contract = PITTaskContract(
        task_name=name,
        domain="earnings_surprise",
        source_tables=(
            "pit.pit_income_quarterly",
            "pit.pit_stock_consensus_fy_monthly",
        ),
        output_table="pit.pit_earnings_surprise_annual",
        pit_time_key="ann_date",
        primary_keys=("ts_code", "end_date", "ann_date"),
        dependencies=(
            "pit_income_quarterly",
            "pit_stock_consensus_fy_monthly",
        ),
        supported_modes=("incremental", "full_backfill", "manual_range", "audit_only"),
        manager_class=PITEarningsSurpriseAnnualManager,
        audit_entity_keys=("ts_code", "end_date"),
        audit_denominator="annual_report_events_at_ann_date",
    )


__all__ = ["PITEarningsSurpriseAnnualTask", "PITStockConsensusFYMonthlyTask"]
