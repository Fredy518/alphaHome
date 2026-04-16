#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft 指数分钟级行情任务

说明：
- 复用股票分钟任务的流式抓取、自适应分组与分钟线处理骨架
- 指数保留双码：`index_code_raw`（Tinysoft 原码）与 `index_ts_code`（可映射时）
- 默认从 index_basic 加载可直接映射到 Tinysoft 的国内指数代码
- 依据数据字典中的访问代码样式，如 `SH000001`、`SZ399106`、`CSI000300`
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from ...sources.tinysoft import TinySoftTask
from ...sources.tushare.batch_utils import (
    generate_natural_day_batches,
    generate_trade_day_batches,
    normalize_date_range,
)
from ....common.task_system.task_decorator import task_register
from ..stock.tinysoft_stock_minute import TinySoftStockMinuteTask, _to_datetime_bound


INDEX_TINYSOFT_PREFIXES = ("CSI", "CNI", "SW", "CI", "SH", "SZ")
INDEX_TS_SUFFIXES = frozenset(INDEX_TINYSOFT_PREFIXES)
INDEX_CODE_PATTERN = re.compile(r"^[A-Z0-9]{4,12}$")


def normalize_index_ts_code(ts_code: str) -> str:
    """规范化指数 ts_code，如 000300.CSI。"""
    raw = str(ts_code or "").strip().upper()
    if not raw:
        raise ValueError("index ts_code 不能为空")
    if "." not in raw:
        raise ValueError(f"无法识别的指数代码格式: {ts_code}")

    code, suffix = raw.rsplit(".", 1)
    if suffix not in INDEX_TS_SUFFIXES:
        raise ValueError(f"不支持的指数后缀: {suffix}")
    if not INDEX_CODE_PATTERN.fullmatch(code):
        raise ValueError(f"不支持的指数主代码: {code}")

    return f"{code}.{suffix}"


def index_ts_code_to_tinysoft_symbol(ts_code: str) -> str:
    """000300.CSI -> CSI000300。"""
    normalized = normalize_index_ts_code(ts_code)
    code, suffix = normalized.rsplit(".", 1)
    return f"{suffix}{code}"


def normalize_tinysoft_index_symbol(symbol: str) -> str:
    """规范化 Tinysoft 指数原码，如 CSI000300。"""
    raw = str(symbol or "").strip().upper()
    if not raw:
        raise ValueError("Tinysoft 指数代码不能为空")

    for prefix in sorted(INDEX_TINYSOFT_PREFIXES, key=len, reverse=True):
        if raw.startswith(prefix):
            code = raw[len(prefix):]
            if INDEX_CODE_PATTERN.fullmatch(code):
                return raw
            break

    raise ValueError(f"无法识别的 Tinysoft 指数代码: {symbol}")


def tinysoft_index_symbol_to_ts_code(symbol: str) -> Optional[str]:
    """CSI000300 -> 000300.CSI。"""
    try:
        raw = normalize_tinysoft_index_symbol(symbol)
    except ValueError:
        return None

    for prefix in sorted(INDEX_TINYSOFT_PREFIXES, key=len, reverse=True):
        if raw.startswith(prefix):
            return f"{raw[len(prefix):]}.{prefix}"
    return None


def _parse_index_symbol(value: Any) -> Optional[Dict[str, str]]:
    raw = str(value or "").strip().upper()
    if not raw:
        return None

    if "." in raw:
        try:
            ts_code = normalize_index_ts_code(raw)
        except ValueError:
            return None
        return {"ts_code": ts_code, "stock": index_ts_code_to_tinysoft_symbol(ts_code)}

    try:
        stock = normalize_tinysoft_index_symbol(raw)
    except ValueError:
        return None

    ts_code = tinysoft_index_symbol_to_ts_code(stock)
    return {"ts_code": ts_code or stock, "stock": stock}


@task_register()
class TinySoftIndexMinuteTask(TinySoftStockMinuteTask):
    """获取指数分钟线行情（Tinysoft）。"""

    domain = "index"
    name = "tinysoft_index_minute"
    description = "获取指数分钟级行情数据（Tinysoft）"
    table_name = "index_minute"
    primary_keys = ["index_code_raw", "trade_time"]
    date_column = "trade_time"
    default_start_date = "20240101"

    default_enable_quality_checks = False
    default_quality_checks_table = "tinysoft.index_minute_quality_checks"
    default_symbol_source_tables = ["tushare.index_basic", "rawdata.index_basic"]
    default_symbol_batch_size = 50
    default_use_config_symbols = False
    default_use_trade_day_batches = False
    default_adaptive_symbol_grouping = False
    default_all_symbols_in_one_group = False
    default_exchange = "SSE"
    default_normalize_units = True
    default_volume_scale = 0.01
    default_amount_scale = 0.001
    default_symbols: List[str] = []

    schema_def = {
        "index_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "index_ts_code": {"type": "VARCHAR(20)"},
        "trade_time": {"type": "TIMESTAMP", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "volume": {"type": "NUMERIC(20,4)"},
        "amount": {"type": "NUMERIC(20,4)"},
    }

    indexes = [
        {"name": "idx_tinysoft_index_minute_raw_code", "columns": "index_code_raw"},
        {"name": "idx_tinysoft_index_minute_ts_code", "columns": "index_ts_code"},
        {"name": "idx_tinysoft_index_minute_time", "columns": "trade_time"},
        {"name": "idx_tinysoft_index_minute_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_index_minute_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["index_code_raw"].notna(), "index_code_raw 不能为空"),
        (lambda df: df["trade_time"].notna(), "trade_time 不能为空"),
        (lambda df: df["open"] >= 0, "开盘价不能为负"),
        (lambda df: df["high"] >= 0, "最高价不能为负"),
        (lambda df: df["low"] >= 0, "最低价不能为负"),
        (lambda df: df["close"] >= 0, "收盘价不能为负"),
        (lambda df: df["volume"] >= 0, "成交量不能为负"),
        (lambda df: df["amount"] >= 0, "成交额不能为负"),
        (lambda df: df["high"] >= df["low"], "最高价不能小于最低价"),
    ]

    @staticmethod
    def _parse_symbol_pairs(raw_symbols: Any) -> List[Dict[str, str]]:
        if raw_symbols is None:
            return []

        if isinstance(raw_symbols, str):
            raw_items: Iterable[str] = re.split(r"[,\s;]+", raw_symbols)
        elif isinstance(raw_symbols, (list, tuple, set)):
            raw_items = [str(x) for x in raw_symbols]
        else:
            raw_items = [str(raw_symbols)]

        pairs: List[Dict[str, str]] = []
        seen = set()
        for item in raw_items:
            pair = _parse_index_symbol(item)
            if not pair:
                continue
            dedup_key = pair["stock"]
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            pairs.append(pair)

        return pairs

    async def _load_market_symbol_pairs_from_db(self, *, silent: bool = False) -> List[Dict[str, str]]:
        """从数据库加载可直接映射到 Tinysoft 的指数代码。"""
        if not self.db:
            return []

        suffix_pattern = "|".join(INDEX_TINYSOFT_PREFIXES)
        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue

                query = f"""
                SELECT ts_code
                FROM "{schema}"."{table_name}"
                WHERE ts_code ~ '^[A-Z0-9]{{4,12}}\\.({suffix_pattern})$'
                ORDER BY ts_code
                """

                rows = await self.db.fetch(query)
                if not rows:
                    continue

                pairs: List[Dict[str, str]] = []
                seen = set()
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
                    pair = _parse_index_symbol(value)
                    if not pair:
                        continue
                    if pair["stock"] in seen:
                        continue
                    seen.add(pair["stock"])
                    pairs.append(pair)

                if pairs:
                    if not silent:
                        self.logger.info(
                            "任务 %s: 从 %s 加载到 %s 个可映射指数代码（默认全市场模式）。",
                            self.name,
                            table,
                            len(pairs),
                        )
                    return pairs
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载指数列表失败: %s", table, e)

        return []

    async def _resolve_symbol_pairs(self, **kwargs) -> List[Dict[str, str]]:
        pairs = self._parse_symbol_pairs(kwargs.get("ts_codes"))
        if not pairs:
            pairs = self._parse_symbol_pairs(kwargs.get("ts_code"))
        if not pairs:
            pairs = self._parse_symbol_pairs(kwargs.get("index_codes"))
        if not pairs:
            pairs = self._parse_symbol_pairs(kwargs.get("index_code_raw"))

        symbols_from_runtime = bool(pairs)
        symbol_scope_source = "runtime" if symbols_from_runtime else "unknown"

        use_config_symbols = self._parse_bool(
            kwargs.get(
                "use_config_symbols",
                self.task_specific_config.get("use_config_symbols", self.default_use_config_symbols),
            ),
            default=self.default_use_config_symbols,
        )
        if not pairs and use_config_symbols:
            pairs = self._parse_symbol_pairs(self.task_specific_config.get("ts_codes"))
            if not pairs:
                pairs = self._parse_symbol_pairs(self.task_specific_config.get("ts_code"))
            if not pairs:
                pairs = self._parse_symbol_pairs(self.task_specific_config.get("index_codes"))
            if not pairs:
                pairs = self._parse_symbol_pairs(self.task_specific_config.get("index_code_raw"))
            if pairs:
                symbol_scope_source = "config"
                self.logger.info(
                    "任务 %s: 使用配置指数列表，共 %s 个代码。",
                    self.name,
                    len(pairs),
                )

        if not pairs:
            pairs = await self._load_market_symbol_pairs_from_db()
            if pairs:
                symbol_scope_source = "market"

        if not pairs:
            pairs = self._parse_symbol_pairs(self.default_symbols)
            if pairs:
                symbol_scope_source = "fallback_default"

        max_symbols = kwargs.get("max_symbols")
        max_symbols_applied = False
        if max_symbols is None and use_config_symbols and not symbols_from_runtime:
            max_symbols = self.task_specific_config.get("max_symbols")

        if max_symbols is not None:
            try:
                max_count = max(1, int(max_symbols))
                pairs = pairs[:max_count]
                max_symbols_applied = True
                self.logger.info(
                    "任务 %s: 应用 max_symbols=%s，最终执行 %s 个指数代码。",
                    self.name,
                    max_count,
                    len(pairs),
                )
            except (TypeError, ValueError):
                self.logger.warning("max_symbols 配置无效: %s", max_symbols)

        if max_symbols_applied and symbol_scope_source == "market":
            symbol_scope_source = "market_with_max_symbols"

        self._last_symbol_scope_limited = bool(
            symbols_from_runtime
            or (use_config_symbols and symbol_scope_source == "config")
            or max_symbols_applied
            or symbol_scope_source == "fallback_default"
        )
        self._last_symbol_scope_source = symbol_scope_source
        self._last_resolved_symbol_count = len(pairs)
        self._last_max_symbols_applied = max_symbols_applied

        return pairs

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

        symbol_pairs = await self._resolve_symbol_pairs(**kwargs)
        if not symbol_pairs:
            self.logger.warning("未获取到有效指数代码，任务将跳过。")
            return []

        batch_days = self._parse_positive_int(
            kwargs.get("batch_days", self.default_batch_days),
            self.default_batch_days,
        )
        symbol_batch_size = self._parse_positive_int(
            kwargs.get("symbol_batch_size", self.default_symbol_batch_size),
            self.default_symbol_batch_size,
        )

        use_trade_day_batches = self._parse_bool(
            kwargs.get("use_trade_day_batches", self.default_use_trade_day_batches),
            default=self.default_use_trade_day_batches,
        )
        exchange = str(kwargs.get("exchange") or self.default_exchange)
        if use_trade_day_batches:
            date_batches = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_days,
                exchange=exchange,
                logger=self.logger,
            )
            if not date_batches:
                self.logger.warning(
                    "任务 %s: 交易日历批次为空，回退自然日批次。",
                    self.name,
                )
                date_batches = await generate_natural_day_batches(
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=batch_days,
                    logger=self.logger,
                )
        else:
            date_batches = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_days,
                logger=self.logger,
            )

        if not date_batches:
            return []

        adaptive_symbol_grouping = self._parse_bool(
            kwargs.get("adaptive_symbol_grouping", self.default_adaptive_symbol_grouping),
            default=self.default_adaptive_symbol_grouping,
        )
        all_symbols_in_one_group = self._parse_bool(
            kwargs.get("all_symbols_in_one_group", self.default_all_symbols_in_one_group),
            default=self.default_all_symbols_in_one_group,
        )

        if adaptive_symbol_grouping and (
            all_symbols_in_one_group or str(kwargs.get("symbol_batch_size", "")).strip() == "0"
        ):
            symbol_groups = [symbol_pairs]
        else:
            symbol_groups = [
                symbol_pairs[i : i + symbol_batch_size]
                for i in range(0, len(symbol_pairs), symbol_batch_size)
            ]

        final_batches: List[Dict[str, Any]] = []
        for symbol_group in symbol_groups:
            if not symbol_group:
                continue

            for batch in date_batches:
                batch_params: Dict[str, Any] = {
                    "symbol_pairs": symbol_group,
                    "cycle": self.cycle,
                    "begin_time": _to_datetime_bound(batch["start_date"], is_start=True),
                    "end_time": _to_datetime_bound(batch["end_date"], is_start=False),
                    "fields": self.fields,
                    "service": self.service,
                    "timeout_ms": self.query_timeout_ms,
                    "adaptive_symbol_grouping": adaptive_symbol_grouping,
                }

                if len(symbol_group) == 1:
                    batch_params["ts_code"] = symbol_group[0]["ts_code"]
                    batch_params["stock"] = symbol_group[0]["stock"]

                final_batches.append(batch_params)

        self.logger.info(
            "任务 %s: 生成 %s 个批次（%s 个指数, %s 个标的组 x %s 个时间批次）",
            self.name,
            len(final_batches),
            len(symbol_pairs),
            len(symbol_groups),
            len(date_batches),
        )
        return final_batches

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        symbol_pairs = params.get("symbol_pairs")
        if isinstance(symbol_pairs, list) and symbol_pairs:
            merged_frames: List[pd.DataFrame] = []
            for pair in symbol_pairs:
                if stop_event and stop_event.is_set():
                    break

                stock = (pair or {}).get("stock")
                if not stock:
                    continue

                single_params = params.copy()
                single_params.pop("symbol_pairs", None)
                single_params["stock"] = stock
                single_params["ts_code"] = (
                    (pair or {}).get("ts_code")
                    or tinysoft_index_symbol_to_ts_code(stock)
                    or stock
                )

                frame = await super().fetch_batch(single_params, stop_event=stop_event)
                if frame is None or frame.empty:
                    continue
                merged_frames.append(frame)

            if not merged_frames:
                return None
            return pd.concat(merged_frames, ignore_index=True)

        return await super().fetch_batch(params, stop_event=stop_event)

    def process_data(self, data, **kwargs):
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()

        rename_map: Dict[str, str] = {}
        for col in df.columns:
            lower = str(col).lower()
            if lower == "date":
                rename_map[col] = "trade_time"
            elif lower == "stockid":
                rename_map[col] = "index_code_raw"
            elif lower == "vol":
                rename_map[col] = "volume"

        if rename_map:
            df.rename(columns=rename_map, inplace=True)

        if "trade_time" not in df.columns:
            self.logger.error("Tinysoft 返回数据缺少 date/trade_time 列，无法入库。")
            return pd.DataFrame()

        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
        df = df.dropna(subset=["trade_time"])
        if df.empty:
            return pd.DataFrame()
        if getattr(df["trade_time"].dtype, "tz", None) is not None:
            df["trade_time"] = df["trade_time"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)

        if "index_code_raw" not in df.columns and "ts_code" in df.columns:
            df["index_code_raw"] = df["ts_code"].apply(
                lambda x: index_ts_code_to_tinysoft_symbol(x) if _parse_index_symbol(x) else None
            )

        if "index_code_raw" not in df.columns:
            self.logger.error("Tinysoft 返回数据缺少 StockID/index_code_raw 列，无法入库。")
            return pd.DataFrame()

        def _safe_raw(value: Any) -> Optional[str]:
            try:
                return normalize_tinysoft_index_symbol(str(value))
            except Exception:
                return None

        def _safe_ts(value: Any) -> Optional[str]:
            try:
                return normalize_index_ts_code(str(value))
            except Exception:
                return None

        df["index_code_raw"] = df["index_code_raw"].map(_safe_raw)
        df = df.dropna(subset=["index_code_raw"])
        if df.empty:
            return pd.DataFrame()

        if "index_ts_code" not in df.columns:
            candidate = df.get("ts_code")
            if candidate is not None:
                df["index_ts_code"] = candidate.map(_safe_ts)
            else:
                df["index_ts_code"] = None

        derived_ts_code = df["index_code_raw"].map(tinysoft_index_symbol_to_ts_code).map(_safe_ts)
        df["index_ts_code"] = df["index_ts_code"].where(df["index_ts_code"].notna(), derived_ts_code)
        df["trade_date"] = df["trade_time"].dt.date

        df = TinySoftTask.process_data(self, df, **kwargs)

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

        target_columns = [c for c in self.schema_def.keys() if c in df.columns]
        df = df[target_columns]
        df = df.drop_duplicates(subset=self.primary_keys, keep="last")
        if not df.empty and "trade_date" in df.columns:
            self._last_processed_start_date = self._parse_date(df["trade_date"].min())
            self._last_processed_end_date = self._parse_date(df["trade_date"].max())

        return df

    async def _post_execute(self, result, stop_event=None, **kwargs):
        # 当前不复用股票分钟的质量检查 SQL。
        await TinySoftTask._post_execute(self, result, stop_event=stop_event, **kwargs)


__all__ = [
    "TinySoftIndexMinuteTask",
    "index_ts_code_to_tinysoft_symbol",
    "normalize_index_ts_code",
    "normalize_tinysoft_index_symbol",
    "tinysoft_index_symbol_to_ts_code",
]
