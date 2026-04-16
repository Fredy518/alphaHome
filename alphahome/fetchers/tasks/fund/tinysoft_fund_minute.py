#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft 场内基金分钟级行情任务

说明：
- 复用股票分钟任务的流式抓取、批次生成与自适应拆分逻辑
- 默认从 fund_basic / fund_etf_basic 加载场内基金（ETF、LOF 等）代码
- 当前仅覆盖 SH/SZ 场内基金代码
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare.batch_utils import generate_natural_day_batches, normalize_date_range
from ...sources.tinysoft import TinySoftTask
from ....common.task_system.task_decorator import task_register
from ..stock.tinysoft_stock_minute import (
    TinySoftStockMinuteTask,
    _to_datetime_bound,
    normalize_ts_code,
    ts_code_to_tinysoft_symbol,
)


@task_register()
class TinySoftFundMinuteTask(TinySoftStockMinuteTask):
    """获取场内基金分钟线行情（Tinysoft）。"""

    domain = "fund"
    name = "tinysoft_fund_minute"
    description = "获取场内基金分钟级行情数据（Tinysoft）"
    table_name = "fund_minute"
    primary_keys = ["ts_code", "trade_time"]
    date_column = "trade_time"
    default_start_date = "20240101"

    default_symbol_source_tables = [
        "tushare.fund_basic",
        "rawdata.fund_basic",
        "tushare.fund_etf_basic",
        "rawdata.fund_etf_basic",
    ]
    default_enable_quality_checks = False
    default_quality_checks_table = "tinysoft.fund_minute_quality_checks"
    default_use_config_symbols = False
    default_normalize_units = True
    default_volume_scale = 0.01
    default_amount_scale = 0.001
    default_symbols: List[str] = []

    indexes = [
        {"name": "idx_tinysoft_fund_minute_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_minute_time", "columns": "trade_time"},
        {"name": "idx_tinysoft_fund_minute_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_fund_minute_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["trade_time"].notna(), "trade_time 不能为空"),
        (lambda df: df["open"] >= 0, "开盘价不能为负"),
        (lambda df: df["high"] >= 0, "最高价不能为负"),
        (lambda df: df["low"] >= 0, "最低价不能为负"),
        (lambda df: df["close"] >= 0, "收盘价不能为负"),
        (lambda df: df["volume"] >= 0, "成交量不能为负"),
        (lambda df: df["amount"] >= 0, "成交额不能为负"),
        (lambda df: df["high"] >= df["low"], "最高价不能小于最低价"),
    ]

    async def _load_market_symbols_from_db(self, *, silent: bool = False) -> List[str]:
        """从数据库加载场内基金代码。"""
        if not self.db:
            return []

        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue

                query = f"""
                SELECT ts_code
                FROM "{schema}"."{table_name}"
                WHERE ts_code ~ '^[0-9]{{6}}\\.(SH|SZ)$'
                """
                if "market" in columns:
                    query += " AND market = 'E'"
                if "status" in columns:
                    query += " AND status = 'L'"
                query += " ORDER BY ts_code"

                rows = await self.db.fetch(query)
                if not rows:
                    continue

                symbols: List[str] = []
                for row in rows:
                    value = None
                    if isinstance(row, dict):
                        value = row.get("ts_code")
                    else:
                        try:
                            value = row["ts_code"]
                        except Exception:
                            value = getattr(row, "ts_code", None)

                    if value is None:
                        continue
                    try:
                        normalized = normalize_ts_code(str(value))
                    except Exception:
                        continue
                    if normalized.endswith((".SH", ".SZ")):
                        symbols.append(normalized)

                symbols = list(dict.fromkeys(symbols))
                if symbols:
                    if not silent:
                        self.logger.info(
                            "任务 %s: 从 %s 加载到 %s 个场内基金代码（默认全市场模式）。",
                            self.name,
                            table,
                            len(symbols),
                        )
                    return symbols
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载场内基金列表失败: %s", table, e)

        return []

    async def _resolve_symbols(self, **kwargs) -> List[str]:
        symbols = self._parse_symbol_list(kwargs.get("ts_codes"))
        if not symbols:
            symbols = self._parse_symbol_list(kwargs.get("ts_code"))

        symbols_from_runtime = bool(symbols)
        use_config_symbols = self._parse_bool(
            kwargs.get(
                "use_config_symbols",
                self.task_specific_config.get("use_config_symbols", self.default_use_config_symbols),
            ),
            default=self.default_use_config_symbols,
        )

        if not symbols and use_config_symbols:
            symbols = self._parse_symbol_list(self.task_specific_config.get("ts_codes"))
            if not symbols:
                symbols = self._parse_symbol_list(self.task_specific_config.get("ts_code"))

        if not symbols:
            symbols = await self._load_market_symbols_from_db()

        if not symbols:
            symbols = self.default_symbols.copy()

        max_symbols = kwargs.get("max_symbols")
        if max_symbols is None and use_config_symbols and not symbols_from_runtime:
            max_symbols = self.task_specific_config.get("max_symbols")

        if max_symbols is not None:
            try:
                max_count = max(1, int(max_symbols))
                symbols = symbols[:max_count]
            except (TypeError, ValueError):
                pass

        return symbols

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        start_date, end_date = normalize_date_range(
            start_date=kwargs.get("start_date"),
            end_date=kwargs.get("end_date"),
            default_start_date=self.default_start_date,
            logger=self.logger,
            task_name=self.name,
        )

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info("起始日期 (%s) 晚于结束日期 (%s)，无需执行任务。", start_date, end_date)
            return []

        symbols = await self._resolve_symbols(**kwargs)
        if not symbols:
            self.logger.warning("未获取到有效基金代码，任务将跳过。")
            return []

        batch_days = self._parse_positive_int(
            kwargs.get("batch_days", self.task_specific_config.get("batch_days", self.default_batch_days)),
            self.default_batch_days,
        )

        date_batches = await generate_natural_day_batches(
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_days,
            logger=self.logger,
        )
        if not date_batches:
            return []

        final_batches: List[Dict[str, Any]] = []
        for ts_code in symbols:
            stock = ts_code_to_tinysoft_symbol(ts_code)
            for batch in date_batches:
                final_batches.append(
                    {
                        "ts_code": ts_code,
                        "stock": stock,
                        "cycle": self.cycle,
                        "begin_time": _to_datetime_bound(batch["start_date"], is_start=True),
                        "end_time": _to_datetime_bound(batch["end_date"], is_start=False),
                        "fields": self.fields,
                        "service": self.service,
                        "timeout_ms": self.query_timeout_ms,
                    }
                )

        return final_batches

    def process_data(self, data, **kwargs):
        df = super().process_data(data, **kwargs)
        if df is None or df.empty:
            return df

        normalize_units = self._parse_bool(
            kwargs.get(
                "normalize_units",
                self.task_specific_config.get("normalize_units", self.default_normalize_units),
            ),
            default=self.default_normalize_units,
        )
        if normalize_units:
            volume_scale = self._parse_float(
                kwargs.get(
                    "volume_scale",
                    self.task_specific_config.get("volume_scale", self.default_volume_scale),
                ),
                self.default_volume_scale,
            )
            amount_scale = self._parse_float(
                kwargs.get(
                    "amount_scale",
                    self.task_specific_config.get("amount_scale", self.default_amount_scale),
                ),
                self.default_amount_scale,
            )
            if "volume" in df.columns:
                df["volume"] = df["volume"] * volume_scale
            if "amount" in df.columns:
                df["amount"] = df["amount"] * amount_scale

        return df

    async def _post_execute(self, result, stop_event=None, **kwargs):
        # 当前不复用股票分钟的质量检查 SQL。
        await TinySoftTask._post_execute(self, result, stop_event=stop_event, **kwargs)


__all__ = ["TinySoftFundMinuteTask"]
