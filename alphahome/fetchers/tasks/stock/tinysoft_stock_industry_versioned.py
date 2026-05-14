#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tinysoft 行业分类版本化任务（C2）

口径：
- 使用股票.股票行业分类（表 139，InfoArray）
- 基于入选/剔除日期重建行业层级快照
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Set

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
class TinySoftStockIndustryVersionedTask(TinySoftTask):
    """获取 A 股行业分类版本快照（Tinysoft）。"""

    domain = "stock"
    name = "tinysoft_stock_industry_versioned"
    description = "获取A股行业分类版本快照（Tinysoft）"
    table_name = "stock_industry_versioned"
    primary_keys = ["ts_code", "trade_date", "industry_source"]
    date_column = "trade_date"
    default_start_date = "20100101"
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

    default_infoarray_table_id = 139
    default_source_codes: List[str] = []
    default_include_empty_records = False

    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "industry_source": {"type": "VARCHAR(40)", "constraints": "NOT NULL"},
        "industry_l1": {"type": "VARCHAR(200)"},
        "industry_l2": {"type": "VARCHAR(200)"},
        "industry_l3": {"type": "VARCHAR(200)"},
        "industry_code": {"type": "VARCHAR(80)"},
        "level1_code": {"type": "VARCHAR(80)"},
        "level2_code": {"type": "VARCHAR(80)"},
        "level3_code": {"type": "VARCHAR(80)"},
        "source_name": {"type": "VARCHAR(120)"},
        "source_table_id": {"type": "INTEGER"},
        "field_map_json": {"type": "JSONB"},
    }

    indexes = [
        {"name": "idx_tinysoft_stock_industry_versioned_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_stock_industry_versioned_date", "columns": "trade_date"},
        {"name": "idx_tinysoft_stock_industry_versioned_source", "columns": "industry_source"},
        {"name": "idx_tinysoft_stock_industry_versioned_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["trade_date"].notna(), "trade_date 不能为空"),
        (lambda df: df["industry_source"].notna(), "industry_source 不能为空"),
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

    @staticmethod
    def _parse_source_codes(raw: Any) -> Set[str]:
        if raw is None:
            return set()
        values: Iterable[str]
        if isinstance(raw, str):
            values = re.split(r"[,\s;]+", raw)
        elif isinstance(raw, (list, tuple, set)):
            values = [str(x) for x in raw]
        else:
            values = [str(raw)]
        result: Set[str] = set()
        for item in values:
            code = str(item).strip().upper()
            if code:
                result.add(code)
        return result

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

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() in {"none", "nan", "null"}:
            return None
        return text

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

        source_codes = self._parse_source_codes(
            kwargs.get("source_codes", self.task_specific_config.get("source_codes", self.default_source_codes))
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
                    "source_codes": sorted(source_codes),
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
            raise ValueError(f"Tinysoft 行业批次参数缺失 symbol_pairs: {params}")

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
                raise asyncio.CancelledError("Tinysoft 行业批次拉取被取消")

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
                self._record_skipped_symbol(ts_code, e)
                self.logger.warning("Tinysoft 行业拉取失败（跳过）: %s, 错误: %s", ts_code, e)
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

        return pd.concat(merged_frames, ignore_index=True)

    def _build_snapshots(
        self,
        group_df: pd.DataFrame,
        *,
        start_bound: Optional[date],
        end_bound: Optional[date],
        include_empty: bool,
        source_table_id: int,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        g = group_df.copy()
        g = g.dropna(subset=["entry_date_dt"])
        if g.empty:
            return rows

        snapshot_dates: Set[date] = set(g["entry_date_dt"].dt.date.dropna().tolist())
        if start_bound:
            snapshot_dates.add(start_bound)

        for snap_date in sorted(snapshot_dates):
            if start_bound and snap_date < start_bound:
                continue
            if end_bound and snap_date > end_bound:
                continue

            snap_ts = pd.Timestamp(snap_date)
            active = g[
                (g["entry_date_dt"] <= snap_ts)
                & (
                    g["remove_date_dt"].isna()
                    | (g["remove_date_dt"] >= snap_ts)
                )
            ].copy()
            if active.empty:
                continue

            out: Dict[str, Any] = {
                "ts_code": g.iloc[0].get("ts_code"),
                "tsl_code": g.iloc[0].get("tsl_code"),
                "trade_date": snap_date,
                "industry_source": g.iloc[0].get("source_attr_code") or "unknown",
                "source_name": g.iloc[0].get("source_attr_name"),
                "industry_l1": None,
                "industry_l2": None,
                "industry_l3": None,
                "industry_code": None,
                "level1_code": None,
                "level2_code": None,
                "level3_code": None,
                "source_table_id": source_table_id,
            }

            for level, name_key, code_key in [
                (1, "industry_l1", "level1_code"),
                (2, "industry_l2", "level2_code"),
                (3, "industry_l3", "level3_code"),
            ]:
                level_df = active[active["level"] == level].copy()
                if level_df.empty:
                    continue
                level_df["latest_flag"] = level_df["latest_flag"].fillna(0)
                level_df = level_df.sort_values(
                    ["entry_date_dt", "latest_flag", "industry_attr_code"],
                    ascending=[True, True, True],
                )
                last = level_df.iloc[-1]
                out[name_key] = self._clean_text(last.get("industry_attr_name"))
                out[code_key] = self._clean_text(last.get("industry_attr_code"))

            out["industry_code"] = out["level3_code"] or out["level2_code"] or out["level1_code"]
            out["field_map_json"] = json.dumps(
                {
                    "table_id": source_table_id,
                    "date_field": "入选日期",
                    "remove_field": "剔除日期",
                    "level_fields": {
                        "1": {"name": "industry_l1", "code": "level1_code"},
                        "2": {"name": "industry_l2", "code": "level2_code"},
                        "3": {"name": "industry_l3", "code": "level3_code"},
                    },
                },
                ensure_ascii=False,
            )

            if not include_empty and not any([out["industry_l1"], out["industry_l2"], out["industry_l3"], out["industry_code"]]):
                continue

            rows.append(out)

        return rows

    def process_data(self, data, **kwargs):
        if data is None or data.empty:
            return pd.DataFrame()

        df = data.copy()
        if "证券代码" in df.columns:
            for alias_col in ("StockID", "stockid"):
                if alias_col in df.columns:
                    df.drop(columns=[alias_col], inplace=True)

        rename_map = {
            "StockID": "tsl_code",
            "stockid": "tsl_code",
            "证券代码": "tsl_code",
            "属性代码": "industry_attr_code",
            "属性名称": "industry_attr_name",
            "级数": "level",
            "入选日期": "entry_date",
            "剔除日期": "remove_date",
            "最新标识": "latest_flag",
            "所属属性代码": "source_attr_code",
            "所属属性名称": "source_attr_name",
        }
        for src, dst in list(rename_map.items()):
            if src in df.columns:
                df.rename(columns={src: dst}, inplace=True)

        if "ts_code" not in df.columns and "tsl_code" in df.columns:
            df["ts_code"] = df["tsl_code"].map(tinysoft_symbol_to_ts_code)
        if "ts_code" not in df.columns:
            self.logger.error("Tinysoft 行业任务返回数据缺少 ts_code/StockID 列，无法入库。")
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

        required = {"industry_attr_code", "industry_attr_name", "level", "entry_date"}
        if not required.issubset(set(df.columns)):
            self.logger.warning("Tinysoft 行业任务缺少关键字段: %s", sorted(required - set(df.columns)))
            return pd.DataFrame()

        df["source_attr_code"] = df.get("source_attr_code", "unknown").apply(lambda x: str(x).strip().upper() if x is not None else "")
        df["source_attr_name"] = df.get("source_attr_name", None).apply(self._clean_text)

        source_codes = self._parse_source_codes(
            kwargs.get("source_codes", self.task_specific_config.get("source_codes", self.default_source_codes))
        )
        if source_codes:
            df = df[df["source_attr_code"].isin(source_codes)].copy()
            if df.empty:
                return pd.DataFrame()

        df["level"] = df["level"].map(self._safe_int)
        df = df[df["level"].isin([1, 2, 3])].copy()
        if df.empty:
            return pd.DataFrame()

        df["entry_date_dt"] = df["entry_date"].map(self._to_date)
        df["remove_date_dt"] = df.get("remove_date", None)
        if "remove_date" in df.columns:
            df["remove_date_dt"] = df["remove_date"].map(self._to_date)
        else:
            df["remove_date_dt"] = pd.NaT

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

        include_empty = self._parse_bool(
            kwargs.get(
                "include_empty_records",
                self.task_specific_config.get("include_empty_records", self.default_include_empty_records),
            ),
            default=self.default_include_empty_records,
        )
        source_table_id = self._parse_positive_int(
            kwargs.get(
                "infoarray_table_id",
                self.task_specific_config.get("infoarray_table_id", self.default_infoarray_table_id),
            ),
            self.default_infoarray_table_id,
        )

        output_rows: List[Dict[str, Any]] = []
        for _, group in df.groupby(["ts_code", "tsl_code", "source_attr_code", "source_attr_name"], dropna=False):
            output_rows.extend(
                self._build_snapshots(
                    group,
                    start_bound=start_bound,
                    end_bound=end_bound,
                    include_empty=include_empty,
                    source_table_id=source_table_id,
                )
            )

        if not output_rows:
            return pd.DataFrame()

        out_df = pd.DataFrame(output_rows)
        out_df = super().process_data(out_df, **kwargs)
        target_columns = [c for c in self.schema_def.keys() if c in out_df.columns]
        out_df = out_df[target_columns]
        out_df = out_df.drop_duplicates(subset=self.primary_keys, keep="last")
        return out_df


__all__ = ["TinySoftStockIndustryVersionedTask"]
