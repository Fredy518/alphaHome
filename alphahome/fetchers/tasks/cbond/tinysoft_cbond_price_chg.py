#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft convertible bond conversion price change events."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import (
    TinySoftP0InfoArrayTask,
    clean_text,
    get_row_value,
    tinysoft_symbol_to_ts_code_any,
    ts_code_to_tinysoft_symbol_any,
)


def _cbond_code_to_tinysoft_symbol(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    raw = text.upper()
    if raw.startswith(("SH", "SZ")):
        return raw
    mapped = ts_code_to_tinysoft_symbol_any(raw)
    if mapped:
        return mapped
    return raw


@task_register()
class TinySoftCBondPriceChgTask(TinySoftP0InfoArrayTask):
    name = "tinysoft_cbond_price_chg"
    description = "获取可转债转股价变动（Tinysoft）"
    table_name = "cbond_price_chg"
    domain = "cbond"
    primary_keys = ["ts_code", "change_date"]
    date_column = "change_date"
    default_start_date = "19900101"
    smart_lookback_days = 550
    default_stream_batches = False
    default_code_batch_size = 500
    default_smart_code_batch_size = 1000
    code_config_keys = ("bond_codes", "cbond_codes", "ts_codes", "ts_code", "codes")
    code_column = "source_code"
    infoarray_table_id = 504
    source_table_name = "债券.可转债转股价变动"
    # Fetch full per-bond history even in SMART mode so derived previous/initial
    # prices are computed from the complete sequence before date-window filtering.
    where_date_field = None
    default_symbol_source_tables = [
        "tushare.cbond_basic",
        "rawdata.cbond_basic",
        "tushare.cbond_price_chg",
        "rawdata.cbond_price_chg",
        "tinysoft.bond_basic_ext",
        "rawdata.bond_basic_ext",
    ]
    field_mapping = {
        "StockID": "source_code",
        "stockid": "source_code",
        "生效日": "change_date",
        "执行日": "execute_date",
        "公布日": "publish_date",
        "转股价": "convertprice_aft",
        "价格变动类型": "change_type",
        "未来不修正开始日": "future_no_revise_start_date",
        "未来不修正截止日": "future_no_revise_end_date",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "bond_short_name": {"type": "VARCHAR(100)"},
        "source_code": {"type": "VARCHAR(30)"},
        "publish_date": {"type": "DATE"},
        "change_date": {"type": "DATE", "constraints": "NOT NULL"},
        "execute_date": {"type": "DATE"},
        "convert_price_initial": {"type": "NUMERIC(15,4)"},
        "convertprice_bef": {"type": "NUMERIC(15,4)"},
        "convertprice_aft": {"type": "NUMERIC(15,4)"},
        "change_type": {"type": "VARCHAR(50)"},
        "future_no_revise_start_date": {"type": "DATE"},
        "future_no_revise_end_date": {"type": "DATE"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_cbond_price_chg_ts_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_cbond_price_chg_change_date", "columns": "change_date"},
        {"name": "idx_tinysoft_cbond_price_chg_publish_date", "columns": "publish_date"},
        {"name": "idx_tinysoft_cbond_price_chg_change_type", "columns": "change_type"},
        {"name": "idx_tinysoft_cbond_price_chg_update_time", "columns": "update_time"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "转债代码不能为空"),
        (lambda df: df["change_date"].notna(), "变动日期不能为空"),
        (
            lambda df: df["convert_price_initial"].fillna(0) >= 0
            if "convert_price_initial" in df.columns
            else True,
            "初始转股价格不能为负数",
        ),
        (
            lambda df: df["convertprice_bef"].fillna(0) >= 0
            if "convertprice_bef" in df.columns
            else True,
            "修正前转股价格不能为负数",
        ),
        (
            lambda df: df["convertprice_aft"].fillna(0) >= 0
            if "convertprice_aft" in df.columns
            else True,
            "修正后转股价格不能为负数",
        ),
    ]
    date_fields = (
        "publish_date",
        "change_date",
        "execute_date",
        "future_no_revise_start_date",
        "future_no_revise_end_date",
    )
    numeric_fields = (
        "convert_price_initial",
        "convertprice_bef",
        "convertprice_aft",
    )
    text_fields = (
        "ts_code",
        "bond_short_name",
        "source_code",
        "change_type",
        "source_table_name",
    )
    rawdata_view_columns = (
        "ts_code",
        "bond_short_name",
        "publish_date",
        "change_date",
        "convert_price_initial",
        "convertprice_bef",
        "convertprice_aft",
        "update_time",
        "source_code",
        "execute_date",
        "change_type",
        "future_no_revise_start_date",
        "future_no_revise_end_date",
        "source_table_id",
        "source_table_name",
        "raw_json",
    )
    validation_mode = "report"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._bond_name_map: Dict[str, str] = {}

    async def _pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs: Any) -> None:
        await super()._pre_execute(stop_event=stop_event, **kwargs)
        self._bond_name_map = await self._load_bond_short_name_map()
        if self.update_type == UpdateTypes.FULL:
            await self._clear_tinysoft_table_if_exists()

    async def _clear_tinysoft_table_if_exists(self) -> None:
        if not self.db:
            return
        try:
            exists = await self.db.check_table_exists(self.data_source, self.table_name)
        except Exception as exc:
            self.logger.warning("检查 %s.%s 是否存在失败，跳过FULL清表: %s", self.data_source, self.table_name, exc)
            return
        if not exists:
            return
        await self.db.execute(f'DELETE FROM "{self.data_source}"."{self.table_name}"')
        self.logger.info("任务 %s: FULL 模式已清空 %s.%s。", self.name, self.data_source, self.table_name)

    async def _create_rawdata_view_if_needed(self) -> None:
        """This Tinysoft table intentionally replaces the archived Tushare source.

        Keep the old Tushare-compatible columns first. PostgreSQL cannot use
        CREATE OR REPLACE VIEW when an existing view changes column names in
        place, so SELECT * is not safe for this source migration.
        """
        try:
            await self.db.ensure_schema_exists("rawdata")
            select_columns = ",\n            ".join(f'"{col}"' for col in self.rawdata_view_columns)
            await self.db.execute(
                f"""
                CREATE OR REPLACE VIEW rawdata."{self.table_name}" AS
                SELECT
                    {select_columns}
                FROM "{self.data_source}"."{self.table_name}";
                """
            )
            await self.db.execute(
                f"""
                COMMENT ON VIEW rawdata."{self.table_name}" IS
                'AUTO_MANAGED: source={self.data_source}.{self.table_name}; compatible_prefix=tushare.cbond_price_chg';
                """
            )
            self.logger.info(
                "已同步 rawdata.%s 视图 -> %s.%s（替代已归档 Tushare 任务）",
                self.table_name,
                self.data_source,
                self.table_name,
            )
        except Exception as exc:
            self.logger.warning("创建 rawdata 视图时出错（不影响数据采集）: %s", exc)

    async def _load_bond_short_name_map(self) -> Dict[str, str]:
        if not self.db:
            return {}

        candidates = [
            ("tinysoft.bond_basic_ext", ("bond_ts_code", "bond_code_raw"), ("bond_short_name",)),
            ("rawdata.bond_basic_ext", ("bond_ts_code", "bond_code_raw"), ("bond_short_name",)),
            ("tushare.cbond_basic", ("ts_code",), ("bond_short_name",)),
            ("rawdata.cbond_basic", ("ts_code",), ("bond_short_name",)),
        ]
        name_map: Dict[str, str] = {}
        for table, code_columns, name_columns in candidates:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns:
                    continue
                available_codes = [col for col in code_columns if col in columns]
                available_names = [col for col in name_columns if col in columns]
                if not available_codes or not available_names:
                    continue
                select_cols = list(dict.fromkeys(available_codes + available_names))
                selects = ", ".join(f'"{col}"' for col in select_cols)
                rows = await self.db.fetch(f'SELECT DISTINCT {selects} FROM "{schema}"."{table_name}"')
                for row in rows or []:
                    name = clean_text(get_row_value(row, available_names[0]))
                    if not name:
                        continue
                    for col in available_codes:
                        ts_code = self._normalize_to_ts_code(get_row_value(row, col))
                        if ts_code and ts_code not in name_map:
                            name_map[ts_code] = name
                            break
            except Exception as exc:
                self.logger.debug("从 %s 加载转债简称映射失败: %s", table, exc)
        return name_map

    @staticmethod
    def _normalize_to_ts_code(value: Any) -> str | None:
        text = clean_text(value)
        if not text:
            return None
        mapped = tinysoft_symbol_to_ts_code_any(text)
        if mapped:
            return mapped
        raw = text.upper()
        if "." in raw:
            code, suffix = raw.rsplit(".", 1)
            if len(code) == 6 and suffix in {"SH", "SZ", "BJ"}:
                return f"{code}.{suffix}"
        return None

    def _get_codes_from_mapping(self, params: Dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        return list(dict.fromkeys(code for code in (_cbond_code_to_tinysoft_symbol(x) for x in raw_codes) if code))

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []

        cbond_sources = [
            ("tushare.cbond_basic", ("ts_code",), None),
            ("rawdata.cbond_basic", ("ts_code",), None),
            ("tushare.cbond_price_chg", ("ts_code",), None),
            ("rawdata.cbond_price_chg", ("ts_code",), None),
        ]
        for table, candidates, where_sql in cbond_sources:
            codes = await self._load_codes_from_table(
                table,
                candidates=candidates,
                where_sql=where_sql,
                silent=silent,
            )
            if codes:
                self.logger.info("任务 %s: 从 %s 加载到 %s 个可转债代码。", self.name, table, len(codes))
                return codes

        for table in ("tinysoft.bond_basic_ext", "rawdata.bond_basic_ext"):
            codes = await self._load_codes_from_bond_basic_ext(table, silent=silent)
            if codes:
                self.logger.info(
                    "任务 %s: 从 %s 按转债字段过滤加载到 %s 个可转债代码。",
                    self.name,
                    table,
                    len(codes),
                )
                return codes
        return []

    async def _load_codes_from_table(
        self,
        table: str,
        *,
        candidates: tuple[str, ...],
        where_sql: str | None = None,
        silent: bool = False,
    ) -> List[str]:
        try:
            schema, table_name = table.split(".", 1)
            columns = await self.db.get_column_names(table)
            if not columns:
                return []
            available = [col for col in candidates if col in columns]
            if not available:
                return []
            selects = ", ".join(f'"{col}"' for col in available)
            query = f'SELECT DISTINCT {selects} FROM "{schema}"."{table_name}"'
            if where_sql:
                query = f"{query} WHERE {where_sql}"
            rows = await self.db.fetch(query)
            codes: List[str] = []
            for row in rows or []:
                for col in available:
                    code = _cbond_code_to_tinysoft_symbol(get_row_value(row, col))
                    if code:
                        codes.append(code)
                        break
            return list(dict.fromkeys(codes))
        except Exception as exc:
            if not silent:
                self.logger.warning("从 %s 加载可转债代码失败: %s", table, exc)
            return []

    async def _load_codes_from_bond_basic_ext(self, table: str, *, silent: bool = False) -> List[str]:
        try:
            columns = await self.db.get_column_names(table)
            if not columns:
                return []

            filter_conditions = []
            for col in (
                "bond_type",
                "bond_short_name",
                "bond_full_name",
                "eval_bond_category",
                "certificate_type",
            ):
                if col in columns:
                    filter_conditions.append(
                        f'("{col}" ILIKE \'%转债%\' OR "{col}" ILIKE \'%可转%\' OR "{col}" ILIKE \'%可交换%\')'
                    )
            if not filter_conditions:
                return []

            code_candidates = tuple(
                col
                for col in ("bond_code_raw", "bond_ts_code", "cbond_code_raw", "cbond_ts_code", "ts_code")
                if col in columns
            )
            if not code_candidates:
                return []

            return await self._load_codes_from_table(
                table,
                candidates=code_candidates,
                where_sql=" OR ".join(filter_conditions),
                silent=silent,
            )
        except Exception as exc:
            if not silent:
                self.logger.warning("从 %s 过滤加载可转债代码失败: %s", table, exc)
            return []

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "source_code" not in df.columns and "request_code" in df.columns:
            df["source_code"] = df["request_code"]
        if "source_code" not in df.columns:
            df["source_code"] = None
        df["source_code"] = df["source_code"].map(clean_text)
        df["ts_code"] = df["source_code"].map(self._normalize_to_ts_code)
        if "bond_short_name" not in df.columns:
            df["bond_short_name"] = df["ts_code"].map(self._bond_name_map)

        if "change_type" in df.columns:
            df["change_type"] = df["change_type"].map(clean_text)

        sort_columns = [
            col for col in ("ts_code", "change_date", "publish_date", "execute_date", "change_type") if col in df.columns
        ]
        if sort_columns:
            df = df.sort_values(by=sort_columns, kind="mergesort", na_position="last")
        if "convertprice_aft" in df.columns:
            df["convertprice_bef"] = df.groupby("ts_code", dropna=False)["convertprice_aft"].shift(1)
            initial_by_code = self._derive_initial_price(df)
            df["convert_price_initial"] = df["ts_code"].map(initial_by_code)
        return df

    @staticmethod
    def _derive_initial_price(df: pd.DataFrame) -> Dict[str, Any]:
        if df.empty or "ts_code" not in df.columns or "convertprice_aft" not in df.columns:
            return {}

        initial: Dict[str, Any] = {}
        type_series = df["change_type"] if "change_type" in df.columns else pd.Series(index=df.index, dtype=object)
        initial_mask = type_series.astype(str).str.contains("初始转股价", na=False)
        for ts_code, group in df[initial_mask].groupby("ts_code", dropna=True):
            values = group["convertprice_aft"].dropna()
            if not values.empty:
                initial[str(ts_code)] = values.iloc[0]

        for ts_code, group in df.groupby("ts_code", dropna=True):
            key = str(ts_code)
            if key in initial:
                continue
            values = group["convertprice_aft"].dropna()
            if not values.empty:
                initial[key] = values.iloc[0]
        return initial


__all__ = ["TinySoftCBondPriceChgTask"]
