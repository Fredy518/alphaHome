#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Shared helpers for AkShare public-fund fee tasks."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Iterable, List, Optional

import pandas as pd

from ....common.constants import UpdateTypes


def current_snapshot_date() -> str:
    """Return the local collection date used by snapshot-style fund fee tasks."""
    return datetime.now().strftime("%Y-%m-%d")


def normalize_fund_code(value: Any) -> Optional[str]:
    """Normalize common fund code shapes to the six-digit code AkShare expects."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if "." in text:
        text = text.split(".", 1)[0]
    match = re.search(r"\d{6}", text)
    return match.group(0) if match else text


def parse_code_list(value: Any) -> List[str]:
    """Parse fund code config from a list, tuple, set, or comma-separated string."""
    if value is None or value == "":
        return []
    if isinstance(value, str):
        raw_items: Iterable[Any] = re.split(r"[,;\s]+", value)
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = [value]

    codes: List[str] = []
    for item in raw_items:
        code = normalize_fund_code(item)
        if code:
            codes.append(code)
    return list(dict.fromkeys(codes))


class AkShareFundCodeBatchMixin:
    """Batch helper for AkShare APIs that fetch one fund at a time."""

    default_code_batch_size = 5000
    default_concurrent_limit = 2
    default_continue_on_stream_batch_failure = True
    default_stream_update_types = (UpdateTypes.FULL, UpdateTypes.SMART)
    smart_refresh_interval_days = 1

    async def _resolve_fund_codes(self, **kwargs: Any) -> List[str]:
        configured = (
            kwargs.get("fund_codes")
            or getattr(self, "task_specific_config", {}).get("fund_codes")
            or getattr(self, "task_specific_config", {}).get("codes")
        )
        codes = parse_code_list(configured)
        if not codes:
            query = """
                SELECT ts_code
                FROM tushare.fund_basic
                WHERE status = 'L'
                ORDER BY ts_code
            """
            rows = await self.db.fetch(query)
            codes = [normalize_fund_code(row["ts_code"]) for row in rows]
            codes = [code for code in codes if code]

        max_codes = kwargs.get("max_codes") or getattr(self, "task_specific_config", {}).get("max_codes")
        if max_codes not in (None, ""):
            codes = codes[: max(0, int(max_codes))]
        return list(dict.fromkeys(codes))


def parse_percent(value: Any) -> Optional[float]:
    """Extract a percent number from strings such as '1.20%（每年）'."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text in {"---", "--", "-", "nan", "None"}:
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*%", text)
    return float(match.group(1)) if match else None


def parse_flat_fee_yuan(value: Any) -> Optional[float]:
    """Extract a flat per-transaction fee from strings such as '每笔1000元'."""
    if value is None or pd.isna(value):
        return None
    match = re.search(r"每笔\s*(\d+(?:\.\d+)?)\s*元", str(value))
    return float(match.group(1)) if match else None


def split_original_discount_fee(value: Any) -> tuple[Optional[float], Optional[float], Optional[float], str]:
    """Parse Eastmoney fee text into original rate, discounted rate, and flat fee."""
    text = "" if value is None or pd.isna(value) else str(value).strip()
    if "|" in text:
        left, right = [part.strip() for part in text.split("|", 1)]
        original = parse_percent(left)
        discount = parse_percent(right)
        flat = parse_flat_fee_yuan(left) or parse_flat_fee_yuan(right)
        unit = "yuan_per_txn" if flat is not None else "pct"
        return original, discount, flat, unit

    flat = parse_flat_fee_yuan(text)
    rate = parse_percent(text)
    unit = "yuan_per_txn" if flat is not None else ("pct" if rate is not None else None)
    return rate, None, flat, unit or ""


def parse_operation_period(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    match = re.search(r"[（(]([^）)]+)[）)]", str(value))
    return match.group(1).strip() if match else None


def parse_amount_condition_wan(text: Any) -> tuple[Optional[float], Optional[float]]:
    """Best-effort parser for amount ranges expressed in 万元."""
    if text is None or pd.isna(text):
        return None, None
    value = str(text).strip()
    nums = [float(item) for item in re.findall(r"(\d+(?:\.\d+)?)\s*万", value)]
    if not nums:
        return None, None
    if value.startswith("小于"):
        return None, nums[0]
    if "大于等于" in value and "小于" in value and len(nums) >= 2:
        return nums[0], nums[1]
    if "大于等于" in value:
        return nums[0], None
    if len(nums) >= 2:
        return nums[0], nums[1]
    return None, nums[0]


def parse_holding_days_condition(text: Any) -> tuple[Optional[float], Optional[float]]:
    """Best-effort parser for holding-period ranges expressed in days."""
    if text is None or pd.isna(text):
        return None, None
    value = str(text).strip()
    nums = [float(item) for item in re.findall(r"(\d+(?:\.\d+)?)\s*天", value)]
    if not nums:
        return None, None
    if value.startswith("小于"):
        return None, nums[0]
    if "大于等于" in value and "小于" in value and len(nums) >= 2:
        return nums[0], nums[1]
    if "大于等于" in value:
        return nums[0], None
    if "<持有期限<" in value and len(nums) >= 2:
        return nums[0], nums[1]
    if "持有期限<" in value:
        return None, nums[-1]
    if "天<持有期限" in value:
        return nums[0], None
    if len(nums) >= 2:
        return nums[0], nums[1]
    return None, nums[0]


def row_to_json(row: pd.Series) -> str:
    """Serialize one raw row with Chinese text preserved."""
    return json.dumps(row.to_dict(), ensure_ascii=False, default=str)
