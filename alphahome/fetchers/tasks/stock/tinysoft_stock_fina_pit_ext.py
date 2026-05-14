#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft 财务 PIT 扩展任务（C3）

口径：
- 以财报表为主（默认股票.主要财务指标，表 42，InfoArray）
- 按“公布日/截止日”展开成长表，保留字段ID与 report(...) 表达式
"""

from __future__ import annotations

import asyncio
import json
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
class TinySoftStockFinaPitExtTask(TinySoftTask):
    """获取财务时点扩展字段（Tinysoft）。"""

    domain = "stock"
    name = "tinysoft_stock_fina_pit_ext"
    description = "获取财务时点一致性扩展字段（Tinysoft）"
    table_name = "fina_pit_ext"
    primary_keys = ["ts_code", "trade_date", "finance_source", "metric_name"]
    date_column = "trade_date"
    default_start_date = "20100101"
    smart_lookback_days = 30

    default_concurrent_limit = 2
    default_query_timeout_ms = 45_000
    default_request_interval = 0.2
    default_cycle = "日线"
    default_symbol_batch_size = 30
    default_skip_failed_symbols = True
    default_use_config_symbols = False
    default_symbols: List[str] = []
    default_symbol_source_tables = ["tushare.stock_basic", "rawdata.stock_basic"]
    default_include_empty_metrics = False

    default_metric_profiles: List[Dict[str, Any]] = [
        {
            "finance_source": "report_42_main",
            "table_id": 42,
            "metric_defs": [
                {"metric_name": "eps_diluted", "field_id": 42002, "field_name": "每股收益(摊薄)"},
                {"metric_name": "bps", "field_id": 42006, "field_name": "每股净资产"},
                {"metric_name": "roe_diluted", "field_id": 42012, "field_name": "净资产收益率(摊薄)(%)"},
                {"metric_name": "netprofit_excl_nr", "field_id": 42017, "field_name": "扣除非经常性损益后的净利润"},
            ],
        }
    ]

    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "report_date": {"type": "DATE"},
        "ann_date": {"type": "DATE"},
        "finance_source": {"type": "VARCHAR(40)", "constraints": "NOT NULL"},
        "metric_name": {"type": "VARCHAR(80)", "constraints": "NOT NULL"},
        "metric_expr": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
        "metric_field_id": {"type": "INTEGER"},
        "metric_value": {"type": "NUMERIC(24,8)"},
        "metric_text": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "metric_map_json": {"type": "JSONB"},
    }

    indexes = [
        {"name": "idx_tinysoft_fina_pit_ext_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fina_pit_ext_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_fina_pit_ext_source", "columns": "finance_source"},
        {"name": "idx_tinysoft_fina_pit_ext_metric", "columns": "metric_name"},
        {"name": "idx_tinysoft_fina_pit_ext_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["finance_source"].notna(), "finance_source 不能为空"),
        (lambda df: df["metric_name"].notna(), "metric_name 不能为空"),
        (lambda df: df["metric_expr"].notna(), "metric_expr 不能为空"),
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
            except Exception:
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
    def _normalize_metric_defs(raw: Any) -> List[Dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        defs: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            metric_name = str(item.get("metric_name") or "").strip()
            field_name = str(item.get("field_name") or "").strip()
            metric_expr = str(item.get("metric_expr") or "").strip()
            field_id = item.get("field_id")
            field_id_int = None
            try:
                if field_id is not None and str(field_id).strip() != "":
                    field_id_int = int(field_id)
            except Exception:
                field_id_int = None
            if not metric_name:
                continue
            if not field_name and not metric_expr:
                continue
            defs.append(
                {
                    "metric_name": metric_name,
                    "field_name": field_name,
                    "field_id": field_id_int,
                    "metric_expr": metric_expr,
                }
            )
        return defs

    def _resolve_metric_profiles(self, **kwargs: Any) -> List[Dict[str, Any]]:
        raw_profiles = kwargs.get("metric_profiles", self.task_specific_config.get("metric_profiles"))
        if not isinstance(raw_profiles, list) or not raw_profiles:
            raw_profiles = self.default_metric_profiles

        profiles: List[Dict[str, Any]] = []
        for item in raw_profiles:
            if not isinstance(item, dict):
                continue
            source = str(item.get("finance_source") or "").strip() or "unknown"
            table_id = self._parse_positive_int(item.get("table_id", 42), 42)

            metric_defs = self._normalize_metric_defs(item.get("metric_defs"))
            if not metric_defs:
                raw_metric_map = item.get("metric_map")
                if isinstance(raw_metric_map, dict):
                    converted: List[Dict[str, Any]] = []
                    for metric_name, expr in raw_metric_map.items():
                        m_name = str(metric_name or "").strip()
                        m_expr = str(expr or "").strip()
                        if not m_name or not m_expr:
                            continue
                        converted.append(
                            {
                                "metric_name": m_name,
                                "field_name": m_expr,
                                "field_id": None,
                                "metric_expr": m_expr,
                            }
                        )
                    metric_defs = converted

            if not metric_defs:
                continue

            profiles.append(
                {
                    "finance_source": source,
                    "table_id": table_id,
                    "metric_defs": metric_defs,
                }
            )

        if not profiles:
            profiles = self.default_metric_profiles
        return profiles

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

    @staticmethod
    def _build_where_clause(start_date: Any) -> Optional[str]:
        """构造 infotable WHERE 子句，按公布日或截止日做服务端过滤。"""
        if not start_date:
            return None
        try:
            dt = pd.to_datetime(str(start_date), errors="raise")
            d = dt.strftime("%Y%m%d")
            return f'["公布日"]>={d} or ["截止日"]>={d}'
        except Exception:
            return None

    @staticmethod
    def _is_non_empty(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, float) and pd.isna(value):
            return False
        text = str(value).strip()
        if not text:
            return False
        return text.lower() not in {"none", "nan", "null"}

    @staticmethod
    def _to_float_or_none(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            if isinstance(value, str):
                text = value.strip().replace(",", "")
                if not text or text.lower() in {"none", "nan", "null"}:
                    return None
                return float(text)
            if isinstance(value, (int, float)):
                if pd.isna(value):
                    return None
                return float(value)
            return float(value)
        except Exception:
            return None

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

        metric_profiles = self._resolve_metric_profiles(**kwargs)
        if not metric_profiles:
            self.logger.warning("未获取到有效 metric_profiles，任务将跳过。")
            return []

        symbol_batch_size = self._parse_positive_int(
            kwargs.get(
                "symbol_batch_size",
                self.task_specific_config.get("symbol_batch_size", self.default_symbol_batch_size),
            ),
            self.default_symbol_batch_size,
        )

        symbol_groups = [
            symbols[i : i + symbol_batch_size]
            for i in range(0, len(symbols), symbol_batch_size)
        ]

        final_batches: List[Dict[str, Any]] = []
        for profile in metric_profiles:
            for symbol_group in symbol_groups:
                symbol_pairs = [
                    {"ts_code": ts_code, "stock": ts_code_to_tinysoft_symbol(ts_code)}
                    for ts_code in symbol_group
                ]
                if not symbol_pairs:
                    continue
                final_batches.append(
                    {
                        "finance_source": profile["finance_source"],
                        "table_id": profile["table_id"],
                        "metric_defs": profile["metric_defs"],
                        "symbol_pairs": symbol_pairs,
                        "start_date": start_date,
                        "end_date": end_date,
                        "service": self.service,
                        "timeout_ms": self.query_timeout_ms,
                    }
                )

        self.logger.info(
            "任务 %s: 生成 %s 个批次（%s 个标的, %s 个标的组 x %s 个财务源）",
            self.name,
            len(final_batches),
            len(symbols),
            len(symbol_groups),
            len(metric_profiles),
        )
        return final_batches

    async def fetch_batch(self, params: Dict[str, Any], stop_event=None) -> Optional[pd.DataFrame]:
        symbol_pairs = params.get("symbol_pairs")
        if not isinstance(symbol_pairs, list) or not symbol_pairs:
            raise ValueError(f"Tinysoft 财务批次参数缺失 symbol_pairs: {params}")

        table_id = self._parse_positive_int(params.get("table_id", 42), 42)
        finance_source = str(params.get("finance_source") or "unknown").strip() or "unknown"
        metric_defs = self._normalize_metric_defs(params.get("metric_defs"))

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

        where_clause = self._build_where_clause(params.get("start_date"))

        merged_frames: List[pd.DataFrame] = []
        for pair in symbol_pairs:
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("Tinysoft 财务批次拉取被取消")

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
                    where_clause=where_clause,
                    service=use_service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
            except Exception as e:
                if not skip_failed_symbols:
                    raise
                self._record_skipped_symbol(ts_code, e)
                self.logger.warning("Tinysoft 财务拉取失败（跳过）: %s, 错误: %s", ts_code, e)
                continue

            if df is None or df.empty:
                continue

            one = df.copy()
            if "ts_code" not in one.columns:
                one["ts_code"] = ts_code
            if "StockID" not in one.columns and "stockid" not in {str(c).lower() for c in one.columns}:
                one["StockID"] = stock
            one["finance_source"] = finance_source
            one["source_table_id"] = table_id
            one["metric_defs"] = [metric_defs] * len(one)
            merged_frames.append(one)

        if not merged_frames:
            return None

        return pd.concat(merged_frames, ignore_index=True)

    def process_data(self, data, **kwargs):
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()
        rename_map = {
            "StockID": "tsl_code",
            "stockid": "tsl_code",
            "证券代码": "tsl_code",
            "截止日": "report_date_raw",
            "公布日": "ann_date_raw",
        }
        for src, dst in list(rename_map.items()):
            if src in df.columns:
                df.rename(columns={src: dst}, inplace=True)

        if "ts_code" not in df.columns and "tsl_code" in df.columns:
            df["ts_code"] = df["tsl_code"].map(tinysoft_symbol_to_ts_code)
        if "ts_code" not in df.columns:
            self.logger.error("Tinysoft 财务任务返回数据缺少 ts_code/StockID 列，无法入库。")
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

        if "finance_source" not in df.columns:
            df["finance_source"] = "unknown"
        df["finance_source"] = df["finance_source"].astype(str).str.strip().replace("", "unknown")

        if "source_table_id" not in df.columns:
            df["source_table_id"] = 42
        df["source_table_id"] = df["source_table_id"].apply(lambda x: self._parse_positive_int(x, 42))

        df["report_date"] = df.get("report_date_raw", None).map(self._to_date)
        df["ann_date"] = df.get("ann_date_raw", None).map(self._to_date)
        df["trade_date"] = df["ann_date"].where(df["ann_date"].notna(), df["report_date"])
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
        df["ann_date"] = pd.to_datetime(df["ann_date"], errors="coerce").dt.date
        df = df.dropna(subset=["trade_date"])
        if df.empty:
            return pd.DataFrame()

        start_bound = None
        end_bound = None
        start_date = getattr(self, "_effective_start_date", None) or kwargs.get("start_date")
        end_date = getattr(self, "_effective_end_date", None) or kwargs.get("end_date")
        if start_date:
            dt = pd.to_datetime(str(start_date), errors="coerce")
            if not pd.isna(dt):
                start_bound = dt.date()
        if end_date:
            dt = pd.to_datetime(str(end_date), errors="coerce")
            if not pd.isna(dt):
                end_bound = dt.date()
        if start_bound:
            df = df[df["trade_date"] >= start_bound]
        if end_bound:
            df = df[df["trade_date"] <= end_bound]
        if df.empty:
            return pd.DataFrame()

        include_empty = self._parse_bool(
            kwargs.get(
                "include_empty_metrics",
                self.task_specific_config.get("include_empty_metrics", self.default_include_empty_metrics),
            ),
            default=self.default_include_empty_metrics,
        )

        output_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            metric_defs = self._normalize_metric_defs(row.get("metric_defs"))
            if not metric_defs:
                continue

            report_date = row.get("report_date")
            report_date_int = None
            if report_date:
                try:
                    report_date_int = int(pd.Timestamp(report_date).strftime("%Y%m%d"))
                except Exception:
                    report_date_int = None

            for metric_def in metric_defs:
                metric_name = metric_def.get("metric_name")
                field_name = metric_def.get("field_name")
                field_id = metric_def.get("field_id")

                raw_value = None
                if field_name and field_name in row.index:
                    raw_value = row.get(field_name)

                if not include_empty and not self._is_non_empty(raw_value):
                    continue

                metric_text = None
                if self._is_non_empty(raw_value):
                    metric_text = str(raw_value).strip()

                metric_expr = metric_def.get("metric_expr")
                if not metric_expr:
                    if field_id and report_date_int:
                        metric_expr = f"report({field_id},{report_date_int})"
                    elif field_name:
                        metric_expr = field_name
                    else:
                        metric_expr = str(metric_name)

                output_rows.append(
                    {
                        "ts_code": row.get("ts_code"),
                        "tsl_code": row.get("tsl_code"),
                        "trade_date": row.get("trade_date"),
                        "report_date": row.get("report_date"),
                        "ann_date": row.get("ann_date"),
                        "finance_source": row.get("finance_source"),
                        "metric_name": metric_name,
                        "metric_expr": metric_expr,
                        "metric_field_id": field_id,
                        "metric_value": self._to_float_or_none(raw_value),
                        "metric_text": metric_text,
                        "source_table_id": row.get("source_table_id"),
                        "metric_map_json": json.dumps(
                            {
                                "table_id": row.get("source_table_id"),
                                "field_id": field_id,
                                "field_name": field_name,
                                "report_date": report_date_int,
                            },
                            ensure_ascii=False,
                        ),
                    }
                )

        if not output_rows:
            return pd.DataFrame()

        out_df = pd.DataFrame(output_rows)
        out_df = super().process_data(out_df, **kwargs)
        target_columns = [c for c in self.schema_def.keys() if c in out_df.columns]
        out_df = out_df[target_columns]
        out_df = out_df.drop_duplicates(subset=self.primary_keys, keep="last")
        return out_df


__all__ = ["TinySoftStockFinaPitExtTask"]
