#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""AkShare 公募基金概况任务（fund_overview_em）。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.akshare.akshare_task import AkShareTask
from ....common.task_system.task_decorator import task_register
from .akshare_fund_fee_utils import (
    AkShareFundCodeBatchMixin,
    current_snapshot_date,
    normalize_fund_code,
    parse_percent,
    row_to_json,
)


@task_register()
class AkShareFundOverviewEmTask(AkShareFundCodeBatchMixin, AkShareTask):
    """获取天天基金单基金概况和运作费率字段。"""

    domain = "fund"
    name = "akshare_fund_overview_em"
    description = "天天基金-公募基金概况（AkShare fund_overview_em）"
    table_name = "fund_overview_em"
    data_source = "akshare"

    primary_keys = ["fund_code", "snapshot_date"]
    date_column = "snapshot_date"
    default_start_date = "20000101"

    api_name = "fund_overview_em"

    column_mapping = {
        "基金全称": "full_name",
        "基金简称": "fund_name",
        "基金代码": "fund_code_text",
        "基金类型": "fund_type",
        "发行日期": "issue_date_text",
        "成立日期/规模": "establishment_text",
        "净资产规模": "net_asset_size_text",
        "份额规模": "share_size_text",
        "基金管理人": "management",
        "基金托管人": "custodian",
        "基金经理人": "fund_manager",
        "成立来分红": "dividend_text",
        "管理费率": "management_fee_text",
        "托管费率": "custodian_fee_text",
        "销售服务费率": "sales_service_fee_text",
        "最高认购费率": "max_subscription_fee_text",
        "业绩比较基准": "benchmark",
        "跟踪标的": "tracking_target",
    }

    schema_def = {
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "full_name": {"type": "VARCHAR(200)"},
        "fund_name": {"type": "VARCHAR(120)"},
        "fund_code_text": {"type": "VARCHAR(120)"},
        "fund_type": {"type": "VARCHAR(80)"},
        "issue_date_text": {"type": "VARCHAR(80)"},
        "establishment_text": {"type": "VARCHAR(160)"},
        "net_asset_size_text": {"type": "VARCHAR(160)"},
        "share_size_text": {"type": "VARCHAR(160)"},
        "management": {"type": "VARCHAR(120)"},
        "custodian": {"type": "VARCHAR(120)"},
        "fund_manager": {"type": "VARCHAR(200)"},
        "dividend_text": {"type": "VARCHAR(160)"},
        "management_fee_text": {"type": "VARCHAR(80)"},
        "custodian_fee_text": {"type": "VARCHAR(80)"},
        "sales_service_fee_text": {"type": "VARCHAR(80)"},
        "max_subscription_fee_text": {"type": "VARCHAR(80)"},
        "management_fee_rate_pct": {"type": "NUMERIC(12,6)"},
        "custodian_fee_rate_pct": {"type": "NUMERIC(12,6)"},
        "sales_service_fee_rate_pct": {"type": "NUMERIC(12,6)"},
        "max_subscription_fee_rate_pct": {"type": "NUMERIC(12,6)"},
        "benchmark": {"type": "TEXT"},
        "tracking_target": {"type": "TEXT"},
        "snapshot_date": {"type": "DATE", "constraints": "NOT NULL"},
        "raw_json": {"type": "TEXT"},
    }

    indexes = [
        {"name": "idx_fund_overview_em_fund_code", "columns": "fund_code"},
        {"name": "idx_fund_overview_em_snapshot", "columns": "snapshot_date"},
        {"name": "idx_fund_overview_em_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["fund_code"].notna(), "基金代码不能为空"),
        (lambda df: df["snapshot_date"].notna(), "快照日期不能为空"),
    ]
    validation_mode = "report"

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        codes = await self._resolve_fund_codes(**kwargs)
        return [{"fund_code": code, "symbol": code} for code in codes]

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        data = await self.api.call(
            func_name=self.api_name,
            stop_event=stop_event,
            symbol=params["symbol"],
        )
        if data is None or data.empty:
            return None
        transformed = self.data_transformer.process_data(data)
        return self.process_data(transformed, **params)

    def process_data(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        if {"fund_code", "snapshot_date", "management_fee_rate_pct"}.issubset(data.columns):
            schema_columns = [col for col in self.schema_def if col in data.columns]
            return data[schema_columns].copy()

        fund_code = normalize_fund_code(kwargs.get("fund_code") or kwargs.get("symbol"))
        if not fund_code and "fund_code" in data.columns:
            fund_code = normalize_fund_code(data["fund_code"].iloc[0])
        if not fund_code:
            return pd.DataFrame()

        data = data.copy()
        data["fund_code"] = fund_code
        data["management_fee_rate_pct"] = data.get("management_fee_text", pd.Series(index=data.index)).apply(parse_percent)
        data["custodian_fee_rate_pct"] = data.get("custodian_fee_text", pd.Series(index=data.index)).apply(parse_percent)
        data["sales_service_fee_rate_pct"] = data.get("sales_service_fee_text", pd.Series(index=data.index)).apply(parse_percent)
        data["max_subscription_fee_rate_pct"] = data.get("max_subscription_fee_text", pd.Series(index=data.index)).apply(parse_percent)
        data["snapshot_date"] = current_snapshot_date()
        data["raw_json"] = data.apply(row_to_json, axis=1)

        if len(data) > 1:
            data = data.tail(1).copy()
        schema_columns = [col for col in self.schema_def if col in data.columns]
        return data[schema_columns].copy()


__all__ = ["AkShareFundOverviewEmTask"]
