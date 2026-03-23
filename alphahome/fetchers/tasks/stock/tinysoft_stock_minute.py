#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft 分钟级 A 股行情任务

说明：
- 使用 pyTSL query 接口按“股票 × 日期区间”拉取分钟线
- 默认拉取 1 分钟线，可通过 task_config.cycle 覆盖为 5 分钟等
"""

import re
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from ...sources.tinysoft import TinySoftTask
from ...sources.tushare.batch_utils import generate_natural_day_batches, normalize_date_range
from ....common.task_system.task_decorator import task_register


def normalize_ts_code(ts_code: str) -> str:
    """规范化 ts_code（如 000001.SZ）。"""
    raw = str(ts_code or "").strip().upper()
    if not raw:
        raise ValueError("ts_code 不能为空")

    if "." in raw:
        code, suffix = raw.split(".", 1)
    else:
        m = re.match(r"^([A-Z]{2})(\d{6})$", raw)
        if not m:
            raise ValueError(f"无法识别的股票代码格式: {ts_code}")
        suffix = m.group(1)
        code = m.group(2)

    if not re.fullmatch(r"\d{6}", code):
        raise ValueError(f"股票代码必须是6位数字: {ts_code}")
    if suffix not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"不支持的交易所后缀: {suffix}")

    return f"{code}.{suffix}"


def ts_code_to_tinysoft_symbol(ts_code: str) -> str:
    """000001.SZ -> SZ000001。"""
    normalized = normalize_ts_code(ts_code)
    code, suffix = normalized.split(".", 1)
    return f"{suffix}{code}"


def tinysoft_symbol_to_ts_code(stockid: str) -> Optional[str]:
    """SZ000001 -> 000001.SZ。"""
    raw = str(stockid or "").strip().upper()
    m = re.match(r"^(SH|SZ|BJ)(\d{6})$", raw)
    if not m:
        return None
    return f"{m.group(2)}.{m.group(1)}"


def _to_datetime_bound(date_value: str, *, is_start: bool) -> str:
    dt = pd.to_datetime(str(date_value), errors="raise")
    time_part = "09:30:00" if is_start else "15:00:00"
    return f"{dt.strftime('%Y-%m-%d')} {time_part}"


@task_register()
class TinySoftStockMinuteTask(TinySoftTask):
    """获取 A 股分钟线行情（Tinysoft）。"""

    domain = "stock"
    name = "tinysoft_stock_minute"
    description = "获取A股分钟级行情数据（Tinysoft）"
    table_name = "stock_minute"
    primary_keys = ["ts_code", "trade_time"]
    date_column = "trade_time"
    default_start_date = "20240101"
    smart_lookback_days = 2

    default_concurrent_limit = 2
    default_query_timeout_ms = 45_000
    default_request_interval = 0.2
    default_cycle = "1分钟线"
    default_batch_days = 1
    default_symbols = ["000001.SZ"]

    # pyTSL 返回字段建议包含 date / StockID / OHLC / vol / amount
    fields = ["date", "StockID", "open", "high", "low", "close", "vol", "amount"]

    column_mapping = {
        "vol": "volume",
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "date": "trade_time",
    }

    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        "amount": float,
    }

    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
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
        {"name": "idx_tinysoft_stock_minute_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_stock_minute_time", "columns": "trade_time"},
        {"name": "idx_tinysoft_stock_minute_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_minute_update_time", "columns": "update_time"},
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

    validation_mode = "report"

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

    def _resolve_symbols(self, **kwargs) -> List[str]:
        symbols = self._parse_symbol_list(kwargs.get("ts_codes"))
        if not symbols:
            symbols = self._parse_symbol_list(kwargs.get("ts_code"))
        if not symbols:
            symbols = self._parse_symbol_list(self.task_specific_config.get("ts_codes"))
        if not symbols:
            symbols = self.default_symbols.copy()

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
            self.logger.info(
                "起始日期 (%s) 晚于结束日期 (%s)，无需执行任务。",
                start_date,
                end_date,
            )
            return []

        symbols = self._resolve_symbols(**kwargs)
        if not symbols:
            self.logger.warning("未获取到有效股票代码，任务将跳过。")
            return []

        batch_days = self.task_specific_config.get("batch_days", self.default_batch_days)
        try:
            batch_days = max(1, int(batch_days))
        except (TypeError, ValueError):
            batch_days = self.default_batch_days

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
            for b in date_batches:
                final_batches.append(
                    {
                        "ts_code": ts_code,
                        "stock": stock,
                        "cycle": self.cycle,
                        "begin_time": _to_datetime_bound(b["start_date"], is_start=True),
                        "end_time": _to_datetime_bound(b["end_date"], is_start=False),
                        "fields": self.fields,
                        "service": self.service,
                        "timeout_ms": self.query_timeout_ms,
                    }
                )

        self.logger.info(
            "任务 %s: 生成 %s 个批次（%s 个标的 x %s 个时间批次）",
            self.name,
            len(final_batches),
            len(symbols),
            len(date_batches),
        )
        return final_batches

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        data = await super().fetch_batch(params, stop_event=stop_event)
        if data is None or data.empty:
            return None

        df = data.copy()
        if "ts_code" not in df.columns:
            df["ts_code"] = params.get("ts_code")

        lower_cols = {c.lower() for c in df.columns}
        if "stockid" not in lower_cols and params.get("stock"):
            df["StockID"] = params["stock"]

        return df

    def process_data(self, data, **kwargs):
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()

        rename_map: Dict[str, str] = {}
        for col in df.columns:
            lower = col.lower()
            if lower == "date":
                rename_map[col] = "trade_time"
            elif lower == "stockid":
                rename_map[col] = "tsl_code"
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

        if "ts_code" not in df.columns and "tsl_code" in df.columns:
            df["ts_code"] = df["tsl_code"].map(tinysoft_symbol_to_ts_code)

        if "ts_code" not in df.columns:
            self.logger.error("Tinysoft 返回数据缺少 ts_code/StockID 列，无法入库。")
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

        df["trade_date"] = df["trade_time"].dt.date

        # 仅保留 A 股后缀
        df = df[df["ts_code"].str.endswith((".SH", ".SZ", ".BJ"), na=False)].copy()
        if df.empty:
            return pd.DataFrame()

        df = super().process_data(df, **kwargs)

        required_columns = {"ts_code", "trade_time", "trade_date"}
        if not required_columns.issubset(df.columns):
            self.logger.error(
                "处理后数据缺少必要列: %s",
                sorted(required_columns - set(df.columns)),
            )
            return pd.DataFrame()

        # 仅保留 schema 中定义的列，避免写入多余字段
        target_columns = [c for c in self.schema_def.keys() if c in df.columns]
        df = df[target_columns]
        df = df.drop_duplicates(subset=self.primary_keys, keep="last")

        return df


__all__ = ["TinySoftStockMinuteTask"]
