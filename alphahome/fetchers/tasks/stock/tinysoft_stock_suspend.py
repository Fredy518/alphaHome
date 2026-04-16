#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft 停牌事件任务（C1）

口径：
- 使用股票.特别提示-停牌（表 127，InfoArray）
- 按股票拉取停牌历史记录，并按执行窗口过滤
"""

import asyncio
import re
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from ...sources.tinysoft import TinySoftTask
from ...sources.tushare.batch_utils import normalize_date_range
from ....common.task_system.task_decorator import task_register
from .tinysoft_stock_minute import (
    normalize_ts_code,
    tinysoft_symbol_to_ts_code,
    ts_code_to_tinysoft_symbol,
)


@task_register()
class TinySoftStockSuspendTask(TinySoftTask):
    """获取 A 股停牌事件（Tinysoft）。"""

    domain = "stock"
    name = "tinysoft_stock_suspend"
    description = "获取A股停牌事件（Tinysoft）"
    table_name = "stock_suspend"
    primary_keys = ["ts_code", "trade_date", "event_text"]
    date_column = "trade_date"
    default_start_date = "20150101"
    smart_lookback_days = 30

    default_concurrent_limit = 2
    default_query_timeout_ms = 45_000
    default_request_interval = 0.2
    default_cycle = "日线"
    default_symbol_batch_size = 50
    default_skip_failed_symbols = True
    default_use_config_symbols = False
    default_symbols: List[str] = []
    default_symbol_source_tables = ["tushare.stock_basic", "rawdata.stock_basic"]

    default_infoarray_table_id = 127
    default_include_empty_events = False

    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "suspend_start_date": {"type": "DATE"},
        "suspend_start_time": {"type": "VARCHAR(20)"},
        "suspend_end_date": {"type": "DATE"},
        "suspend_end_time": {"type": "VARCHAR(20)"},
        "suspend_term": {"type": "VARCHAR(50)"},
        "suspend_reason": {"type": "TEXT"},
        "event_type": {"type": "VARCHAR(20)"},
        "event_text": {"type": "TEXT", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
    }

    indexes = [
        {"name": "idx_tinysoft_stock_suspend_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_stock_suspend_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_suspend_type", "columns": "event_type"},
        {"name": "idx_tinysoft_stock_suspend_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["event_text"].notna(), "event_text 不能为空"),
    ]

    validation_mode = "report"

    @staticmethod
    def _parse_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off", ""}:
                return False
        return default

    @staticmethod
    def _parse_positive_int(value: Any, default: int, *, min_value: int = 1) -> int:
        try:
            parsed = int(value)
            return max(min_value, parsed)
        except (TypeError, ValueError):
            return max(min_value, int(default))

    @staticmethod
    def _parse_symbol_list(raw_symbols: Any) -> List[str]:
        symbols: List[str] = []
        if raw_symbols is None:
            return symbols

        if isinstance(raw_symbols, str):
            raw_items: Iterable[str] = re.split(r"[,\s;]+", raw_symbols)
        elif isinstance(raw_symbols, (list, tuple, set)):
            raw_items = [str(x) for x in raw_symbols]
        else:
            raw_items = [str(raw_symbols)]

        for item in raw_items:
            raw = str(item).strip()
            if not raw:
                continue
            try:
                symbols.append(normalize_ts_code(raw))
            except ValueError:
                continue

        return list(dict.fromkeys(symbols))

    async def _load_market_symbols_from_db(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []

        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue

                has_list_status = "list_status" in columns
                query = f"""
                SELECT ts_code
                FROM "{schema}"."{table_name}"
                WHERE ts_code ~ '^[0-9]{{6}}\\.(SH|SZ|BJ)$'
                """
                if has_list_status:
                    query += " AND list_status = 'L'"
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
                        symbols.append(normalize_ts_code(str(value)))
                    except Exception:
                        continue

                symbols = list(dict.fromkeys(symbols))
                if symbols:
                    if not silent:
                        self.logger.info(
                            "任务 %s: 从 %s 加载到 %s 个A股代码（默认全市场模式）。",
                            self.name,
                            table,
                            len(symbols),
                        )
                    return symbols
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载股票列表失败: %s", table, e)

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
                symbols = symbols[: max(1, int(max_symbols))]
            except Exception:
                self.logger.warning("max_symbols 配置无效: %s", max_symbols)

        return symbols

    @staticmethod
    def _to_date(value: Any) -> Optional[pd.Timestamp]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit() and len(text) == 8:
            return pd.to_datetime(text, format="%Y%m%d", errors="coerce")
        return pd.to_datetime(text, errors="coerce")

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
            self.logger.warning("未获取到有效股票代码，任务将跳过。")
            return []

        symbol_batch_size = self._parse_positive_int(
            kwargs.get(
                "symbol_batch_size",
                self.task_specific_config.get("symbol_batch_size", self.default_symbol_batch_size),
            ),
            self.default_symbol_batch_size,
        )

        table_id = self._parse_positive_int(
            kwargs.get(
                "infoarray_table_id",
                self.task_specific_config.get("infoarray_table_id", self.default_infoarray_table_id),
            ),
            self.default_infoarray_table_id,
        )

        symbol_groups = [
            symbols[i : i + symbol_batch_size]
            for i in range(0, len(symbols), symbol_batch_size)
        ]

        final_batches: List[Dict[str, Any]] = []
        for symbol_group in symbol_groups:
            symbol_pairs = [
                {"ts_code": ts_code, "stock": ts_code_to_tinysoft_symbol(ts_code)}
                for ts_code in symbol_group
            ]
            if not symbol_pairs:
                continue

            final_batches.append(
                {
                    "symbol_pairs": symbol_pairs,
                    "start_date": start_date,
                    "end_date": end_date,
                    "infoarray_table_id": table_id,
                    "service": self.service,
                    "timeout_ms": self.query_timeout_ms,
                }
            )

        self.logger.info(
            "任务 %s: 生成 %s 个批次（%s 个标的, %s 个标的组）",
            self.name,
            len(final_batches),
            len(symbols),
            len(symbol_groups),
        )
        return final_batches

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        symbol_pairs = params.get("symbol_pairs")
        if not isinstance(symbol_pairs, list) or not symbol_pairs:
            raise ValueError(f"Tinysoft 停牌批次参数缺失 symbol_pairs: {params}")

        table_id = self._parse_positive_int(
            params.get("infoarray_table_id", self.default_infoarray_table_id),
            self.default_infoarray_table_id,
        )
        skip_failed_symbols = self._parse_bool(
            params.get(
                "skip_failed_symbols",
                self.task_specific_config.get("skip_failed_symbols", self.default_skip_failed_symbols),
            ),
            default=self.default_skip_failed_symbols,
        )
        timeout_ms = self._parse_positive_int(
            params.get("timeout_ms", self.query_timeout_ms),
            self.query_timeout_ms,
        )
        use_service = params.get("service", self.service)

        merged_frames: List[pd.DataFrame] = []
        for pair in symbol_pairs:
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("Tinysoft 停牌批次拉取被取消")

            if not isinstance(pair, dict):
                continue
            ts_code = str(pair.get("ts_code") or "").strip()
            stock = str(pair.get("stock") or "").strip()
            if not ts_code or not stock:
                continue

            try:
                df = await self.api.call_dataframe(
                    "infoarray",
                    table_id,
                    stock=stock,
                    service=use_service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
            except Exception as e:
                if not skip_failed_symbols:
                    raise
                self.logger.warning("Tinysoft 停牌拉取失败（跳过）: %s, 错误: %s", ts_code, e)
                continue

            if df is None or df.empty:
                continue

            one = df.copy()
            if "ts_code" not in one.columns:
                one["ts_code"] = ts_code
            if "StockID" not in one.columns and "stockid" not in {str(c).lower() for c in one.columns}:
                one["StockID"] = stock

            merged_frames.append(one)

        if not merged_frames:
            return None

        combined = pd.concat(merged_frames, ignore_index=True)
        return combined

    @staticmethod
    def _safe_event_type(text: str) -> str:
        raw = str(text or "")
        if "复牌" in raw:
            return "resume"
        if "停牌" in raw or "临时停" in raw:
            return "suspend"
        return "other"

    def process_data(self, data, **kwargs):
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()

        rename_map = {
            "StockID": "tsl_code",
            "stockid": "tsl_code",
            "证券代码": "tsl_code",
            "停牌开始日": "suspend_start_date",
            "停牌开始时间": "suspend_start_time",
            "停牌截止日": "suspend_end_date",
            "停牌截止时间": "suspend_end_time",
            "停牌期限": "suspend_term",
            "停牌原因": "suspend_reason",
        }
        for src, dst in list(rename_map.items()):
            if src in df.columns:
                df.rename(columns={src: dst}, inplace=True)

        if "ts_code" not in df.columns and "tsl_code" in df.columns:
            df["ts_code"] = df["tsl_code"].map(tinysoft_symbol_to_ts_code)
        if "ts_code" not in df.columns:
            self.logger.error("Tinysoft 停牌任务返回数据缺少 ts_code/StockID 列，无法入库。")
            return pd.DataFrame()

        def _safe_normalize(value: Any) -> Optional[str]:
            try:
                return normalize_ts_code(str(value))
            except Exception:
                return None

        df["ts_code"] = df["ts_code"].map(_safe_normalize)
        df = df.dropna(subset=["ts_code"])
        if df.empty:
            return pd.DataFrame()

        if "tsl_code" not in df.columns:
            df["tsl_code"] = df["ts_code"].map(ts_code_to_tinysoft_symbol)

        if "suspend_start_date" not in df.columns:
            self.logger.warning("Tinysoft 停牌任务缺少停牌开始日字段，跳过该批次。")
            return pd.DataFrame()

        df["suspend_start_date"] = df["suspend_start_date"].map(self._to_date)
        df["suspend_end_date"] = df.get("suspend_end_date", None)
        if "suspend_end_date" in df.columns:
            df["suspend_end_date"] = df["suspend_end_date"].map(self._to_date)

        df["trade_date"] = pd.to_datetime(df["suspend_start_date"], errors="coerce").dt.date
        df = df.dropna(subset=["trade_date"])
        if df.empty:
            return pd.DataFrame()

        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        if start_date:
            start_dt = pd.to_datetime(str(start_date), errors="coerce")
            if not pd.isna(start_dt):
                start_bound = start_dt.date()
                df = df[df["trade_date"] >= start_bound]
        if end_date:
            end_dt = pd.to_datetime(str(end_date), errors="coerce")
            if not pd.isna(end_dt):
                end_bound = end_dt.date()
                df = df[df["trade_date"] <= end_bound]
        if df.empty:
            return pd.DataFrame()

        if "suspend_reason" not in df.columns:
            df["suspend_reason"] = None
        df["suspend_reason"] = df["suspend_reason"].astype(str).str.strip()

        df["event_text"] = df["suspend_reason"]
        include_empty = self._parse_bool(
            kwargs.get(
                "include_empty_events",
                self.task_specific_config.get("include_empty_events", self.default_include_empty_events),
            ),
            default=self.default_include_empty_events,
        )
        if not include_empty:
            df = df[
                (~df["event_text"].isna())
                & (df["event_text"] != "")
                & (df["event_text"].str.lower() != "none")
                & (df["event_text"].str.lower() != "nan")
            ].copy()
            if df.empty:
                return pd.DataFrame()

        if "suspend_start_time" in df.columns:
            df["suspend_start_time"] = df["suspend_start_time"].astype(str).str.strip()
        if "suspend_end_time" in df.columns:
            df["suspend_end_time"] = df["suspend_end_time"].astype(str).str.strip()
        if "suspend_term" in df.columns:
            df["suspend_term"] = df["suspend_term"].astype(str).str.strip()

        df["event_type"] = df["event_text"].map(self._safe_event_type)

        table_id = self._parse_positive_int(
            kwargs.get(
                "infoarray_table_id",
                self.task_specific_config.get("infoarray_table_id", self.default_infoarray_table_id),
            ),
            self.default_infoarray_table_id,
        )
        df["source_table_id"] = table_id

        df = super().process_data(df, **kwargs)
        target_columns = [c for c in self.schema_def.keys() if c in df.columns]
        df = df[target_columns]
        df = df.drop_duplicates(subset=self.primary_keys, keep="last")
        return df


__all__ = ["TinySoftStockSuspendTask"]
