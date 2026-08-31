"""Registered PIT tasks for cross-market index A-share proxies."""

from __future__ import annotations

from alphahome.common.task_system.task_decorator import task_register
from alphahome.pit.base.pit_task import PITTask, PITTaskContract
from alphahome.pit.pit_etf_index_a_share_proxy_fapi_manager import (
    PITETFIndexAShareProxyFAPIMonthlyManager,
)
from alphahome.pit.pit_etf_index_a_share_proxy_members_manager import (
    PITETFIndexAShareProxyMembersMonthlyManager,
)


@task_register()
class PITETFIndexAShareProxyMembersMonthlyTask(PITTask):
    name = "pit_etf_index_a_share_proxy_members_monthly"
    table_name = "pit_etf_index_members_monthly"
    description = "跨市场ETF指数A股子样本成分月末PIT代理"
    contract = PITTaskContract(
        task_name=name,
        domain="etf_index_a_share_proxy_members",
        source_tables=(
            "rawdata.index_weight",
            "rawdata.fund_etf_basic",
            "rawdata.fund_etf_index",
        ),
        output_table="pit.pit_etf_index_members_monthly",
        pit_time_key="obs_date",
        primary_keys=("obs_date", "index_code", "ts_code", "method_version"),
        dependencies=(),
        supported_modes=("incremental", "full_backfill", "manual_range"),
        manager_class=PITETFIndexAShareProxyMembersMonthlyManager,
        audit_entity_keys=("index_code", "ts_code", "method_version"),
        audit_denominator="registered_cross_market_a_share_proxy_members",
    )


@task_register()
class PITETFIndexAShareProxyFAPIMonthlyTask(PITTask):
    name = "pit_etf_index_a_share_proxy_fapi_monthly"
    table_name = "pit_etf_index_fapi_monthly"
    description = "跨市场ETF指数A股子样本FAPI与预期ROE月末PIT代理"
    contract = PITTaskContract(
        task_name=name,
        domain="etf_index_a_share_proxy_fapi",
        source_tables=(
            "pit.pit_etf_index_members_monthly",
            "pit.pit_stock_fttm_monthly",
            "rawdata.stock_dailybasic",
            "rawdata.index_weight",
        ),
        output_table="pit.pit_etf_index_fapi_monthly",
        pit_time_key="obs_date",
        primary_keys=("obs_date", "index_code", "benchmark_code", "method_version"),
        dependencies=(
            "pit_etf_index_a_share_proxy_members_monthly",
            "pit_stock_fttm_monthly",
        ),
        supported_modes=("incremental", "full_backfill", "manual_range"),
        manager_class=PITETFIndexAShareProxyFAPIMonthlyManager,
        audit_entity_keys=("index_code", "benchmark_code", "method_version"),
        audit_denominator="registered_cross_market_a_share_proxy_indices",
    )


__all__ = [
    "PITETFIndexAShareProxyMembersMonthlyTask",
    "PITETFIndexAShareProxyFAPIMonthlyTask",
]
