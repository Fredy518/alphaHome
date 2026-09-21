#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""AkShare 雪球蛋卷基金交易规则明细任务。"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from ...sources.akshare.akshare_api import AkShareAPIError
from ...sources.akshare.akshare_task import AkShareTask
from ....common.task_system.task_decorator import task_register
from .akshare_fund_fee_utils import (
    AkShareFundCodeBatchMixin,
    normalize_fund_code,
    parse_amount_condition_wan,
    parse_holding_days_condition,
    row_to_json,
)


@task_register()
class AkShareFundIndividualDetailInfoXqTask(AkShareFundCodeBatchMixin, AkShareTask):
    """获取雪球蛋卷基金交易规则，包含买入、卖出和其他费用规则。"""

    domain = "fund"
    name = "akshare_fund_individual_detail_info_xq"
    description = "雪球蛋卷-基金交易规则明细（AkShare fund_individual_detail_info_xq）"
    table_name = "fund_individual_detail_info_xq"
    data_source = "akshare"
    hide_from_gui_collection = True

    primary_keys = ["fund_code", "row_no", "snapshot_date"]
    date_column = "snapshot_date"
    default_start_date = "20000101"

    api_name = "fund_individual_detail_info_xq"
    default_timeout = 10
    default_concurrent_limit = 4
    default_request_interval = 0.15
    missing_payload_fields = (
        "data",
        "declare_rate_table",
        "withdraw_rate_table",
        "other_rate_table",
    )
    rate_table_labels = {
        "declare_rate_table": "买入规则",
        "withdraw_rate_table": "卖出规则",
        "other_rate_table": "其他费用",
    }

    column_mapping = {
        "费用类型": "fee_type",
        "条件或名称": "condition_or_name",
        "费用": "fee_value",
    }

    schema_def = {
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "row_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "fee_type": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "condition_or_name": {"type": "TEXT"},
        "fee_value": {"type": "NUMERIC(18,6)"},
        "fee_unit_hint": {"type": "VARCHAR(40)"},
        "condition_min_amount_wan": {"type": "NUMERIC(18,4)"},
        "condition_max_amount_wan": {"type": "NUMERIC(18,4)"},
        "condition_min_holding_days": {"type": "NUMERIC(12,4)"},
        "condition_max_holding_days": {"type": "NUMERIC(12,4)"},
        "snapshot_date": {"type": "DATE", "constraints": "NOT NULL"},
        "raw_json": {"type": "TEXT"},
    }

    indexes = [
        {"name": "idx_fund_detail_xq_fund_code", "columns": "fund_code"},
        {"name": "idx_fund_detail_xq_fee_type", "columns": "fee_type"},
        {"name": "idx_fund_detail_xq_snapshot", "columns": "snapshot_date"},
        {"name": "idx_fund_detail_xq_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["fund_code"].notna(), "基金代码不能为空"),
        (lambda df: df["fee_type"].notna(), "费用类型不能为空"),
        (lambda df: df["snapshot_date"].notna(), "快照日期不能为空"),
    ]
    validation_mode = "report"

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        codes = await self._resolve_fund_codes(**kwargs)
        timeout = (
            kwargs.get("timeout")
            or getattr(self, "task_specific_config", {}).get("timeout")
            or self.default_timeout
        )
        batches = [
            {"fund_code": code, "symbol": code, "timeout": float(timeout)}
            for code in codes
        ]
        return await self._exclude_existing_month_batches(
            batches,
            key_fields=("fund_code",),
            **kwargs,
        )

    async def fetch_batch(
        self, params: Dict[str, Any], stop_event=None
    ) -> Optional[pd.DataFrame]:
        try:
            data = await self.api.call(
                func_name=self.api_name,
                stop_event=stop_event,
                symbol=params["symbol"],
                timeout=params.get("timeout"),
            )
        except AkShareAPIError as exc:
            missing_field = self._missing_payload_field_from_error(exc)
            if missing_field:
                data = await self._fetch_available_rate_tables(
                    params["symbol"],
                    timeout=params.get("timeout"),
                    stop_event=stop_event,
                )
                if data is not None and not data.empty:
                    self.logger.info(
                        "%s: fund_code=%s 缺少 %s 字段，已保留其余 %s 行费率规则。",
                        self.name,
                        params.get("fund_code") or params.get("symbol"),
                        missing_field,
                        len(data),
                    )
                else:
                    self.logger.info(
                        "%s: fund_code=%s 雪球蛋卷详情缺少 %s 字段且无其他费率规则，按无数据跳过。",
                        self.name,
                        params.get("fund_code") or params.get("symbol"),
                        missing_field,
                    )
                    return None
            else:
                raise
        if data is None or data.empty:
            return None
        transformed = self.data_transformer.process_data(data)
        return self.process_data(transformed, **params)

    async def _fetch_available_rate_tables(
        self,
        symbol: str,
        *,
        timeout: Optional[float],
        stop_event=None,
    ) -> Optional[pd.DataFrame]:
        """Recover the valid fee sections when AkShare assumes every section exists."""
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("操作被用户取消")
        try:
            return await asyncio.to_thread(
                self._read_available_rate_tables,
                symbol,
                timeout,
            )
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            raise AkShareAPIError(
                f"akshare.{self.api_name} 缺字段后的兼容解析失败: {exc}"
            ) from exc

    @classmethod
    def _read_available_rate_tables(
        cls,
        symbol: str,
        timeout: Optional[float],
    ) -> Optional[pd.DataFrame]:
        response = requests.get(
            f"https://danjuanfunds.com/djapi/fund/detail/{symbol}",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/80.0.3987.149 Safari/537.36"
                )
            },
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        rates = data.get("fund_rates") if isinstance(data, dict) else None
        if not isinstance(rates, dict):
            return None

        frames: List[pd.DataFrame] = []
        for field, label in cls.rate_table_labels.items():
            rows = rates.get(field)
            if not rows:
                continue
            frame = pd.DataFrame.from_records(rows)
            missing = {"name", "value"} - set(frame.columns)
            if missing:
                raise ValueError(f"{field} 缺少字段: {sorted(missing)}")
            frame = frame[["name", "value"]].copy()
            frame.insert(0, "费用类型", label)
            frame.rename(
                columns={"name": "条件或名称", "value": "费用"},
                inplace=True,
            )
            frames.append(frame)
        if not frames:
            return None
        combined = pd.concat(frames, ignore_index=True)
        combined["费用"] = pd.to_numeric(combined["费用"], errors="coerce")
        return combined

    def _missing_payload_field_from_error(self, exc: Exception) -> Optional[str]:
        message = str(exc)
        for field in self.missing_payload_fields:
            if f"'{field}'" in message or f'"{field}"' in message:
                return field
        return None

    def process_data(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        if {
            "fund_code",
            "row_no",
            "fee_type",
            "snapshot_date",
            "fee_unit_hint",
        }.issubset(data.columns):
            schema_columns = [col for col in self.schema_def if col in data.columns]
            return data[schema_columns].copy()

        fund_code = normalize_fund_code(kwargs.get("fund_code") or kwargs.get("symbol"))
        if not fund_code and "fund_code" in data.columns:
            fund_code = normalize_fund_code(data["fund_code"].iloc[0])
        if not fund_code:
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        snapshot_date = self._resolve_snapshot_date(**kwargs)
        for idx, row in data.reset_index(drop=True).iterrows():
            fee_type = row.get("fee_type")
            condition = row.get("condition_or_name")
            fee_value = pd.to_numeric(row.get("fee_value"), errors="coerce")
            min_amount = max_amount = None
            min_days = max_days = None
            fee_unit_hint = "pct"

            if str(fee_type) == "买入规则":
                min_amount, max_amount = parse_amount_condition_wan(condition)
                if pd.notna(fee_value) and float(fee_value) >= 100:
                    fee_unit_hint = "yuan_per_txn"
            elif str(fee_type) == "卖出规则":
                min_days, max_days = parse_holding_days_condition(condition)
            elif str(fee_type) == "其他费用":
                fee_unit_hint = "pct_per_year"

            rows.append(
                {
                    "fund_code": fund_code,
                    "row_no": idx + 1,
                    "fee_type": None if pd.isna(fee_type) else str(fee_type),
                    "condition_or_name": None if pd.isna(condition) else str(condition),
                    "fee_value": None if pd.isna(fee_value) else float(fee_value),
                    "fee_unit_hint": fee_unit_hint,
                    "condition_min_amount_wan": min_amount,
                    "condition_max_amount_wan": max_amount,
                    "condition_min_holding_days": min_days,
                    "condition_max_holding_days": max_days,
                    "snapshot_date": snapshot_date,
                    "raw_json": row_to_json(row),
                }
            )

        normalized = pd.DataFrame(rows)
        schema_columns = [col for col in self.schema_def if col in normalized.columns]
        return normalized[schema_columns].copy()


__all__ = ["AkShareFundIndividualDetailInfoXqTask"]
