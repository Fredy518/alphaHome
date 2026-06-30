#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 公募基金费率任务（fund_fee_em）

覆盖天天基金费率页面的当前费率表：
- 申购费率（前端）
- 赎回费率
- 运作费用

该接口是当前快照，不提供严格历史费率变更链路；因此以 snapshot_date 入库。
"""

from __future__ import annotations

import asyncio
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ...sources.akshare.akshare_api import AkShareAPIError, ak
from ...sources.akshare.akshare_task import AkShareTask
from ....common.task_system.task_decorator import task_register
from .akshare_fund_fee_utils import (
    AkShareFundCodeBatchMixin,
    normalize_fund_code,
    parse_amount_condition_wan,
    parse_flat_fee_yuan,
    parse_holding_days_condition,
    parse_operation_period,
    parse_percent,
    row_to_json,
    split_original_discount_fee,
)


@task_register()
class AkShareFundFeeEmTask(AkShareFundCodeBatchMixin, AkShareTask):
    """获取单基金当前费率表（AkShare fund_fee_em）。"""

    domain = "fund"
    name = "akshare_fund_fee_em"
    description = "天天基金-公募基金费率表（AkShare fund_fee_em）"
    table_name = "fund_fee_em"
    data_source = "akshare"

    primary_keys = ["fund_code", "indicator", "row_no", "snapshot_date"]
    date_column = "snapshot_date"
    default_start_date = "20000101"

    api_name = "fund_fee_em"
    default_indicators = ("申购费率（前端）", "赎回费率", "运作费用")
    known_optional_indicators = {"申购费率（前端）"}
    default_stream_save_batch_size = 3000

    schema_def = {
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "indicator": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "row_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "rule_type": {"type": "VARCHAR(40)", "constraints": "NOT NULL"},
        "item_name": {"type": "VARCHAR(80)"},
        "condition_text": {"type": "TEXT"},
        "condition_min_amount_wan": {"type": "NUMERIC(18,4)"},
        "condition_max_amount_wan": {"type": "NUMERIC(18,4)"},
        "condition_min_holding_days": {"type": "NUMERIC(12,4)"},
        "condition_max_holding_days": {"type": "NUMERIC(12,4)"},
        "fee_text": {"type": "TEXT"},
        "original_rate_pct": {"type": "NUMERIC(12,6)"},
        "discount_rate_pct": {"type": "NUMERIC(12,6)"},
        "flat_fee_amount_yuan": {"type": "NUMERIC(18,4)"},
        "fee_rate_pct": {"type": "NUMERIC(12,6)"},
        "fee_unit": {"type": "VARCHAR(40)"},
        "operation_period": {"type": "VARCHAR(40)"},
        "snapshot_date": {"type": "DATE", "constraints": "NOT NULL"},
        "raw_json": {"type": "TEXT"},
    }

    indexes = [
        {"name": "idx_fund_fee_em_fund_code", "columns": "fund_code"},
        {"name": "idx_fund_fee_em_indicator", "columns": "indicator"},
        {"name": "idx_fund_fee_em_snapshot", "columns": "snapshot_date"},
        {"name": "idx_fund_fee_em_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["fund_code"].notna(), "基金代码不能为空"),
        (lambda df: df["indicator"].notna(), "费率指标不能为空"),
        (lambda df: df["snapshot_date"].notna(), "快照日期不能为空"),
    ]
    validation_mode = "report"

    async def _pre_execute(self, stop_event=None, **kwargs: Any) -> None:
        await super()._pre_execute(stop_event=stop_event, **kwargs)
        await self._ensure_fee_text_is_text()

    async def _ensure_fee_text_is_text(self) -> None:
        try:
            if await self.db.table_exists(self):
                rows = await self.db.fetch(
                    """
                    SELECT data_type, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = $1
                      AND table_name = $2
                      AND column_name = 'fee_text'
                    """,
                    self.data_source,
                    self.table_name,
                )
                if not rows:
                    return
                if rows[0]["data_type"] == "text":
                    return

                rawdata_view_sql = self._rawdata_view_sql()
                if self.db.pool is None:
                    await self.db.connect()
                async with self.db.pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute('DROP VIEW IF EXISTS "rawdata"."fund_fee_em"')
                        await conn.execute(
                            f"""
                            ALTER TABLE {self.get_full_table_name()}
                            ALTER COLUMN fee_text TYPE TEXT
                            """
                        )
                        await conn.execute(rawdata_view_sql)
                self.logger.info("%s: 已将 fee_text 字段扩展为 TEXT。", self.name)
        except Exception as exc:
            self.logger.warning("%s: 扩展 fee_text 字段为 TEXT 失败，将继续执行: %s", self.name, exc)

    @staticmethod
    def _rawdata_view_sql() -> str:
        return """
            CREATE OR REPLACE VIEW "rawdata"."fund_fee_em" AS
            SELECT fund_code,
                   indicator,
                   row_no,
                   rule_type,
                   item_name,
                   condition_text,
                   condition_min_amount_wan,
                   condition_max_amount_wan,
                   condition_min_holding_days,
                   condition_max_holding_days,
                   fee_text,
                   original_rate_pct,
                   discount_rate_pct,
                   flat_fee_amount_yuan,
                   fee_rate_pct,
                   fee_unit,
                   operation_period,
                   snapshot_date,
                   raw_json,
                   update_time
            FROM "akshare"."fund_fee_em"
        """

    def _resolve_indicators(self, **kwargs: Any) -> List[str]:
        configured = kwargs.get("indicators") or getattr(self, "task_specific_config", {}).get("indicators")
        indicators: List[str] = []
        if isinstance(configured, str):
            indicators = [item.strip() for item in configured.replace(";", ",").split(",") if item.strip()]
        elif isinstance(configured, (list, tuple, set)):
            indicators = [str(item).strip() for item in configured if str(item).strip()]
        if not indicators:
            indicators = list(self.default_indicators)
        return list(dict.fromkeys(indicators))

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        codes = await self._resolve_fund_codes(**kwargs)
        indicators = self._resolve_indicators(**kwargs)
        batches = [
            {"fund_code": code, "symbol": code, "indicator": indicator}
            for code in codes
            for indicator in indicators
        ]
        return await self._exclude_existing_snapshot_batches(batches, **kwargs)

    async def _exclude_existing_snapshot_batches(
        self,
        batches: List[Dict[str, Any]],
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """In SMART mode, resume the current month's snapshot by missing pairs."""
        return await self._exclude_existing_month_batches(
            batches,
            key_fields=("fund_code", "indicator"),
            **kwargs,
        )

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        if params.get("indicator") in self.known_optional_indicators:
            data = await self._call_optional_indicator_once(params, stop_event=stop_event)
        else:
            data = await self.api.call(
                func_name=self.api_name,
                stop_event=stop_event,
                symbol=params["symbol"],
                indicator=params["indicator"],
            )
        if data is None or data.empty:
            return None
        transformed = self.data_transformer.process_data(data)
        return self.process_data(transformed, **params)

    async def _call_optional_indicator_once(
        self,
        params: Dict[str, Any],
        stop_event=None,
    ) -> Optional[pd.DataFrame]:
        """Fetch optional indicator once; known missing pages are normal no-data cases."""
        if ak is None:
            raise AkShareAPIError("akshare 库未安装，请使用 'pip install akshare' 安装")
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("操作被用户取消")

        await self.api._wait_for_rate_limit()
        try:
            result = await asyncio.to_thread(
                ak.fund_fee_em,
                symbol=params["symbol"],
                indicator=params["indicator"],
            )
        except KeyError as exc:
            missing_key = str(exc.args[0]) if exc.args else str(exc).strip("'\"")
            if missing_key == params["indicator"]:
                fallback = await self._call_purchase_fee_title_fallback(params, stop_event=stop_event)
                if fallback is not None and not fallback.empty:
                    return fallback
                self.logger.info(
                    "%s: fund_code=%s 缺少可选费率指标 %s，按无数据跳过。",
                    self.name,
                    params.get("fund_code") or params.get("symbol"),
                    params["indicator"],
                )
                return None
            raise

        if result is None:
            self.logger.warning("akshare.%s 返回 None，参数: %s", self.api_name, params)
            return None
        if isinstance(result, pd.DataFrame):
            self.logger.info("akshare.%s 成功返回 %s 行数据", self.api_name, len(result))
        return result

    async def _call_purchase_fee_title_fallback(
        self,
        params: Dict[str, Any],
        stop_event=None,
    ) -> Optional[pd.DataFrame]:
        if params.get("indicator") != "申购费率（前端）":
            return None
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("操作被用户取消")

        await self.api._wait_for_rate_limit()
        data = await asyncio.to_thread(self._read_fund_fee_table_by_title, params["symbol"], "申购费率")
        if data is None or data.empty:
            return None
        self.logger.info(
            "%s: fund_code=%s 使用页面标题“申购费率”回退解析成功，返回 %s 行数据。",
            self.name,
            params.get("fund_code") or params.get("symbol"),
            len(data),
        )
        return data

    @staticmethod
    def _read_fund_fee_table_by_title(symbol: str, title: str) -> Optional[pd.DataFrame]:
        url = f"https://fundf10.eastmoney.com/jjfl_{symbol}.html"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, features="html.parser")
        for title_elem in soup.find_all(name="h4", class_="t"):
            title_text = title_elem.get_text(strip=True)
            if title_text != title:
                continue
            next_table = title_elem.find_next("table")
            if next_table is None:
                return None
            return pd.read_html(StringIO(str(next_table)))[0]
        return None

    def process_data(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        if {"fund_code", "indicator", "row_no", "rule_type", "snapshot_date"}.issubset(data.columns):
            schema_columns = [col for col in self.schema_def if col in data.columns]
            return data[schema_columns].copy()

        fund_code = normalize_fund_code(kwargs.get("fund_code") or kwargs.get("symbol"))
        indicator = kwargs.get("indicator")
        if "fund_code" in data.columns and not fund_code:
            fund_code = normalize_fund_code(data["fund_code"].iloc[0])
        if "indicator" in data.columns and not indicator:
            indicator = str(data["indicator"].iloc[0])
        if not fund_code or not indicator:
            return pd.DataFrame()

        snapshot_date = self._resolve_snapshot_date(**kwargs)
        if indicator == "运作费用":
            normalized = self._normalize_operation_fee(data, fund_code, indicator, snapshot_date)
        else:
            normalized = self._normalize_schedule_fee(data, fund_code, indicator, snapshot_date)

        schema_columns = [col for col in self.schema_def if col in normalized.columns]
        return normalized[schema_columns].copy()

    def _normalize_schedule_fee(
        self,
        data: pd.DataFrame,
        fund_code: str,
        indicator: str,
        snapshot_date: str,
    ) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []

        for idx, row in data.reset_index(drop=True).iterrows():
            columns = list(data.columns)
            condition_col = columns[0]
            fee_col = columns[1] if len(columns) > 1 else columns[0]
            condition_text = row.get(condition_col)
            fee_text = row.get(fee_col)
            original_rate, discount_rate, flat_fee, fee_unit = split_original_discount_fee(fee_text)

            min_amount = max_amount = None
            min_days = max_days = None
            rule_type = "fee_schedule"
            if "金额" in str(condition_col) or "申购" in indicator or "认购" in indicator:
                rule_type = "amount_fee"
                min_amount, max_amount = parse_amount_condition_wan(condition_text)
            elif "期限" in str(condition_col) or "赎回" in indicator:
                rule_type = "holding_period_fee"
                min_days, max_days = parse_holding_days_condition(condition_text)

            rows.append(
                {
                    "fund_code": fund_code,
                    "indicator": indicator,
                    "row_no": idx + 1,
                    "rule_type": rule_type,
                    "condition_text": None if pd.isna(condition_text) else str(condition_text),
                    "condition_min_amount_wan": min_amount,
                    "condition_max_amount_wan": max_amount,
                    "condition_min_holding_days": min_days,
                    "condition_max_holding_days": max_days,
                    "fee_text": None if pd.isna(fee_text) else str(fee_text),
                    "original_rate_pct": original_rate,
                    "discount_rate_pct": discount_rate,
                    "flat_fee_amount_yuan": flat_fee,
                    "fee_rate_pct": discount_rate if discount_rate is not None else original_rate,
                    "fee_unit": fee_unit,
                    "snapshot_date": snapshot_date,
                    "raw_json": row_to_json(row),
                }
            )

        return pd.DataFrame(rows)

    def _normalize_operation_fee(
        self,
        data: pd.DataFrame,
        fund_code: str,
        indicator: str,
        snapshot_date: str,
    ) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        row_no = 1

        for _, row in data.reset_index(drop=True).iterrows():
            values = [row.get(col) for col in data.columns]
            for i in range(0, len(values), 2):
                item_name = values[i] if i < len(values) else None
                fee_text = values[i + 1] if i + 1 < len(values) else None
                if item_name is None or pd.isna(item_name) or str(item_name).strip() == "":
                    continue
                rate = parse_percent(fee_text)
                rows.append(
                    {
                        "fund_code": fund_code,
                        "indicator": indicator,
                        "row_no": row_no,
                        "rule_type": "operation_fee",
                        "item_name": str(item_name).strip(),
                        "fee_text": None if fee_text is None or pd.isna(fee_text) else str(fee_text).strip(),
                        "flat_fee_amount_yuan": parse_flat_fee_yuan(fee_text),
                        "fee_rate_pct": rate,
                        "fee_unit": "pct_per_period" if rate is not None else "",
                        "operation_period": parse_operation_period(fee_text),
                        "snapshot_date": snapshot_date,
                        "raw_json": row_to_json(row),
                    }
                )
                row_no += 1

        return pd.DataFrame(rows)


__all__ = ["AkShareFundFeeEmTask"]
