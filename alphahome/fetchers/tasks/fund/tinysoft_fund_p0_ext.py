#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tinysoft fund-extension P0 tables."""

from __future__ import annotations

from typing import Any, List

import pandas as pd

from ....common.task_system.task_decorator import task_register
from ..tinysoft_p0_base import (
    TinySoftP0InfoArrayTask,
    clean_text,
    get_row_value,
    map_hsgt_security_to_ts_code,
    tinysoft_symbol_to_ts_code_any,
    ts_code_to_tinysoft_symbol_any,
)


DEFAULT_FUND_CATEGORY_CODES = [
    "TSJJ0209",
    "TSJJ0202",
    "TSJJ0204",
    "TSJJ0201",
    "TSJJ0203",
    "TSJJ0206",
    "TSJJ0207",
    "TSJJ0210",
    "TSJJ020304",
    "TSJJ020205",
    "TSJJ020105",
    "TSJJ020305",
    "TSJJ020902",
    "TSJJ020901",
    "TSJJ020104",
    "TSJJ020106",
    "TSJJ020103",
    "TSJJ020303",
    "TSJJ020306",
    "TSJJ020302",
    "TSJJ020301",
    "TSJJ020307",
    "TSJJ020204",
    "TSJJ020202",
    "TSJJ020201",
    "TSJJ020206",
    "TSJJ020203",
    "TSJJ020401",
    "TSJJ020402",
    "TSJJ020803",
    "TSJJ020802",
    "TSJJ020801",
    "TSJJ020503",
    "TSJJ020501",
]


FOF_TEXT_MARKERS = ("FOF", "基金中基金", "养老目标", "目标日期", "目标风险")
FOF_METADATA_FIELDS = (
    "基金名称",
    "基金简称",
    "基金类型",
    "交易方式",
    "投资风格",
    "投资类型",
    "类别",
    "份额类别",
    "fund_name",
    "fund_short_name",
    "fund_type",
    "trade_mode",
    "invest_style",
    "invest_type",
    "category",
    "share_class",
    "name",
    "fullname",
    "type",
)


class TinySoftFundCodeInfoArrayTask(TinySoftP0InfoArrayTask):
    domain = "fund"
    default_start_date = "20180101"
    smart_lookback_days = 370
    default_code_batch_size = 40
    default_smart_code_batch_size = 120
    code_config_keys = ("ts_codes", "ts_code", "fund_codes", "codes")
    default_symbol_source_tables = [
        "tushare.fund_basic",
        "rawdata.fund_basic",
        "tushare.fund_etf_basic",
        "rawdata.fund_etf_basic",
    ]

    def _get_codes_from_mapping(self, params: dict[str, Any]) -> List[str]:
        raw_codes = super()._get_codes_from_mapping(params)
        codes: List[str] = []
        for raw_code in raw_codes:
            codes.append(ts_code_to_tinysoft_symbol_any(raw_code) or str(raw_code).strip().upper())
        return list(dict.fromkeys(code for code in codes if code))

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
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
                WHERE ts_code ~ '^[0-9]{{6}}\\.(OF|SH|SZ)$'
                """
                if "status" in columns:
                    query += " AND status = 'L'"
                query += " ORDER BY ts_code"
                rows = await self.db.fetch(query)
                codes: List[str] = []
                for row in rows or []:
                    value = get_row_value(row, "ts_code")
                    symbol = ts_code_to_tinysoft_symbol_any(value)
                    if symbol:
                        codes.append(symbol)
                if codes:
                    return list(dict.fromkeys(codes))
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载基金代码失败: %s", table, e)
        return []

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "tsl_code" not in df.columns and "StockID" in df.columns:
            df["tsl_code"] = df["StockID"]
        if "ts_code" not in df.columns and "tsl_code" in df.columns:
            df["ts_code"] = df["tsl_code"].map(tinysoft_symbol_to_ts_code_any)
        return df


def _tinysoft_yes_no_to_bool(value: Any):
    text = clean_text(value)
    if text is None:
        return None
    normalized = text.strip().lower()
    if normalized in {"是", "yes", "y", "true", "1"}:
        return True
    if normalized in {"否", "no", "n", "false", "0"}:
        return False
    return None


def _security_code_to_ts_code(value: Any):
    mapped = tinysoft_symbol_to_ts_code_any(value)
    if mapped:
        return mapped
    return map_hsgt_security_to_ts_code(value)


def _index_or_security_code_to_ts_code(value: Any):
    mapped = tinysoft_symbol_to_ts_code_any(value)
    if mapped:
        return mapped
    text = clean_text(value)
    if not text:
        return None
    raw = text.upper()
    if raw.startswith("CSI") and len(raw) == 9:
        return f"{raw[-6:]}.CSI"
    if raw.startswith("CNI") and len(raw) == 9:
        return f"{raw[-6:]}.CNI"
    return None


def _latest_date_from_columns(df: pd.DataFrame, columns: tuple[str, ...]):
    available = [col for col in columns if col in df.columns]
    if not available:
        return None
    dates = pd.DataFrame({col: pd.to_datetime(df[col], errors="coerce") for col in available})
    return dates.max(axis=1).dt.date


class TinySoftFundMainCodeInfoArrayTask(TinySoftFundCodeInfoArrayTask):
    """Fund report tables that must be queried by main share-class code."""

    default_start_date = "19980101"
    smart_lookback_days = 550
    default_code_batch_size = 50
    default_smart_code_batch_size = 500
    default_stream_batches = True
    code_config_keys = ("fund_codes", "ts_codes", "ts_code", "codes")

    async def _resolve_codes(self, **kwargs: Any) -> List[str]:
        codes = await super()._resolve_codes(**kwargs)
        if not codes:
            return []
        return await self._resolve_main_fund_codes(codes)

    async def _resolve_main_fund_codes(self, codes: List[str]) -> List[str]:
        unique_codes = list(dict.fromkeys(str(code).strip().upper() for code in codes if str(code).strip()))
        if not unique_codes:
            return []

        main_codes: List[str] = []
        unresolved = set(unique_codes)
        batch_size = 1000
        for i in range(0, len(unique_codes), batch_size):
            chunk = unique_codes[i : i + batch_size]
            try:
                df = await self.api.call_dataframe_for_stocks(
                    "infoarray",
                    302,
                    stocks=chunk,
                    where_clause=None,
                    service=self.service,
                    timeout_ms=self.query_timeout_ms,
                )
            except Exception as e:
                self.logger.warning("基金主代码映射查询失败，将使用原始代码继续: %s", e)
                continue

            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                source = clean_text(row.get("StockID") if "StockID" in row.index else row.get("stockid"))
                if source:
                    unresolved.discard(source.strip().upper())

                main_code = clean_text(row.get("不同收费模式基金主代码"))
                if not main_code:
                    main_code = clean_text(row.get("母基金代码"))
                if not main_code:
                    main_code = source
                if not main_code or main_code in {"0", "000000", "00000000"}:
                    continue

                normalized = ts_code_to_tinysoft_symbol_any(main_code) or main_code.strip().upper()
                if normalized:
                    main_codes.append(normalized)

        main_codes.extend(sorted(unresolved))
        resolved = list(dict.fromkeys(code for code in main_codes if code))
        if len(resolved) != len(unique_codes):
            self.logger.info(
                "任务 %s: 基金候选代码经302主代码映射后由 %s 个去重为 %s 个。",
                self.name,
                len(unique_codes),
                len(resolved),
            )
        return resolved


class TinySoftFundParentCodeInfoArrayTask(TinySoftFundCodeInfoArrayTask):
    """Fund report tables queried by own code, except structured funds use parent code."""

    default_start_date = "19980101"
    smart_lookback_days = 550
    default_code_batch_size = 50
    default_smart_code_batch_size = 500
    default_stream_batches = True

    async def _resolve_codes(self, **kwargs: Any) -> List[str]:
        codes = await super()._resolve_codes(**kwargs)
        if not codes:
            return []
        return await self._resolve_parent_fund_codes(codes)

    async def _resolve_parent_fund_codes(self, codes: List[str]) -> List[str]:
        unique_codes = list(dict.fromkeys(str(code).strip().upper() for code in codes if str(code).strip()))
        if not unique_codes:
            return []

        resolved: List[str] = []
        unresolved = set(unique_codes)
        batch_size = 1000
        for i in range(0, len(unique_codes), batch_size):
            chunk = unique_codes[i : i + batch_size]
            try:
                df = await self.api.call_dataframe_for_stocks(
                    "infoarray",
                    302,
                    stocks=chunk,
                    where_clause=None,
                    service=self.service,
                    timeout_ms=self.query_timeout_ms,
                )
            except Exception as e:
                self.logger.warning("基金母代码映射查询失败，将使用原始代码继续: %s", e)
                continue

            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                source = clean_text(row.get("StockID") if "StockID" in row.index else row.get("stockid"))
                if source:
                    unresolved.discard(source.strip().upper())
                parent_code = clean_text(row.get("母基金代码"))
                normalized = ts_code_to_tinysoft_symbol_any(parent_code) if parent_code else None
                resolved.append(normalized or source)

        resolved.extend(sorted(unresolved))
        return list(dict.fromkeys(code for code in resolved if code))


@task_register()
class TinySoftFundBasicExtTask(TinySoftFundCodeInfoArrayTask):
    name = "tinysoft_fund_basic_ext"
    description = "获取基金基本信息扩展字段（Tinysoft）"
    table_name = "fund_basic_ext"
    primary_keys = ["ts_code"]
    date_column = None
    default_start_date = "19900101"
    default_code_batch_size = 100
    default_smart_code_batch_size = 1000
    infoarray_table_id = 302
    source_table_name = "基金.基金基本信息"
    where_date_field = None
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "证券代码": "tsl_code",
        "基金名称": "fund_name",
        "基金简称": "fund_short_name",
        "基金类型": "fund_type",
        "交易方式": "trade_mode",
        "投资风格": "invest_style",
        "投资类型": "invest_type",
        "主动/被动": "active_passive",
        "投资区域": "invest_region",
        "份额类别": "share_class",
        "类别": "category",
        "净值增长率计算方法": "nav_return_method",
        "设立日": "found_date",
        "上市日": "list_date",
        "清算日": "liquidation_date",
        "基金管理人": "management",
        "基金管理人简称": "management_short_name",
        "基金托管人": "custodian",
        "业绩比较基准": "benchmark",
        "标的指数代码": "tracking_index_code_raw",
        "是否ETF联接": "is_etf_feeder",
        "ETF连接目标代码": "etf_target_code_raw",
        "ETF联接目标代码": "etf_target_code_raw",
        "上市地": "list_location",
        "交易代码": "trade_code",
        "不同收费模式基金主代码": "fee_mode_main_code_raw",
        "不同收费模式基金代码": "fee_mode_code_raw",
        "母基金代码": "parent_fund_code_raw",
        "分级A代码": "structured_a_code_raw",
        "分级B代码": "structured_b_code_raw",
        "分级基金类别": "structured_fund_type",
        "分级基金分拆比例": "structured_split_ratio",
        "封转开前基金代码": "pre_open_fund_code_raw",
        "封转开前基金名称": "pre_open_fund_name",
        "封转开后基金代码": "post_open_fund_code_raw",
        "封转开后基金名称": "post_open_fund_name",
        "投资目标": "investment_objective",
        "投资范围": "investment_scope",
        "投资策略": "investment_strategy",
        "风险收益特征": "risk_return_feature",
        "备注": "remark",
        "募集总金额": "raise_total_amount",
        "募集总份额": "raise_total_share",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(200)"},
        "fund_short_name": {"type": "VARCHAR(100)"},
        "fund_type": {"type": "VARCHAR(100)"},
        "trade_mode": {"type": "VARCHAR(100)"},
        "invest_style": {"type": "VARCHAR(100)"},
        "invest_type": {"type": "VARCHAR(100)"},
        "active_passive": {"type": "VARCHAR(30)"},
        "invest_region": {"type": "VARCHAR(100)"},
        "share_class": {"type": "VARCHAR(50)"},
        "category": {"type": "VARCHAR(100)"},
        "nav_return_method": {"type": "VARCHAR(200)"},
        "found_date": {"type": "DATE"},
        "list_date": {"type": "DATE"},
        "liquidation_date": {"type": "DATE"},
        "latest_change_date": {"type": "DATE"},
        "management": {"type": "VARCHAR(200)"},
        "management_short_name": {"type": "VARCHAR(100)"},
        "custodian": {"type": "VARCHAR(200)"},
        "benchmark": {"type": "TEXT"},
        "tracking_index_code_raw": {"type": "VARCHAR(30)"},
        "tracking_index_ts_code": {"type": "VARCHAR(20)"},
        "is_etf_feeder": {"type": "BOOLEAN"},
        "etf_target_code_raw": {"type": "VARCHAR(30)"},
        "etf_target_ts_code": {"type": "VARCHAR(15)"},
        "list_location": {"type": "VARCHAR(50)"},
        "trade_code": {"type": "VARCHAR(30)"},
        "fee_mode_main_code_raw": {"type": "VARCHAR(30)"},
        "fee_mode_main_ts_code": {"type": "VARCHAR(15)"},
        "fee_mode_code_raw": {"type": "VARCHAR(30)"},
        "fee_mode_ts_code": {"type": "VARCHAR(15)"},
        "parent_fund_code_raw": {"type": "VARCHAR(30)"},
        "parent_fund_ts_code": {"type": "VARCHAR(15)"},
        "structured_a_code_raw": {"type": "VARCHAR(30)"},
        "structured_a_ts_code": {"type": "VARCHAR(15)"},
        "structured_b_code_raw": {"type": "VARCHAR(30)"},
        "structured_b_ts_code": {"type": "VARCHAR(15)"},
        "structured_fund_type": {"type": "VARCHAR(100)"},
        "structured_split_ratio": {"type": "NUMERIC(20,8)"},
        "pre_open_fund_code_raw": {"type": "VARCHAR(30)"},
        "pre_open_fund_name": {"type": "VARCHAR(200)"},
        "post_open_fund_code_raw": {"type": "VARCHAR(30)"},
        "post_open_fund_name": {"type": "VARCHAR(200)"},
        "investment_objective": {"type": "TEXT"},
        "investment_scope": {"type": "TEXT"},
        "investment_strategy": {"type": "TEXT"},
        "risk_return_feature": {"type": "TEXT"},
        "remark": {"type": "TEXT"},
        "raise_total_amount": {"type": "NUMERIC(24,8)"},
        "raise_total_share": {"type": "NUMERIC(24,8)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_basic_ext_name", "columns": "fund_short_name"},
        {"name": "idx_tinysoft_fund_basic_ext_type", "columns": "fund_type"},
        {"name": "idx_tinysoft_fund_basic_ext_found", "columns": "found_date"},
        {"name": "idx_tinysoft_fund_basic_ext_main", "columns": "fee_mode_main_ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
    ]
    date_fields = ("found_date", "list_date", "liquidation_date")
    numeric_fields = ("structured_split_ratio", "raise_total_amount", "raise_total_share")
    text_fields = (
        "tsl_code",
        "fund_name",
        "fund_short_name",
        "fund_type",
        "trade_mode",
        "invest_style",
        "invest_type",
        "active_passive",
        "invest_region",
        "share_class",
        "category",
        "nav_return_method",
        "management",
        "management_short_name",
        "custodian",
        "benchmark",
        "tracking_index_code_raw",
        "etf_target_code_raw",
        "list_location",
        "trade_code",
        "fee_mode_main_code_raw",
        "fee_mode_code_raw",
        "parent_fund_code_raw",
        "structured_a_code_raw",
        "structured_b_code_raw",
        "structured_fund_type",
        "pre_open_fund_code_raw",
        "pre_open_fund_name",
        "post_open_fund_code_raw",
        "post_open_fund_name",
        "investment_objective",
        "investment_scope",
        "investment_strategy",
        "risk_return_feature",
        "remark",
    )

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        for raw_col, ts_col in (
            ("tracking_index_code_raw", "tracking_index_ts_code"),
            ("etf_target_code_raw", "etf_target_ts_code"),
            ("fee_mode_main_code_raw", "fee_mode_main_ts_code"),
            ("fee_mode_code_raw", "fee_mode_ts_code"),
            ("parent_fund_code_raw", "parent_fund_ts_code"),
            ("structured_a_code_raw", "structured_a_ts_code"),
            ("structured_b_code_raw", "structured_b_ts_code"),
        ):
            if raw_col in df.columns:
                mapper = (
                    _index_or_security_code_to_ts_code
                    if raw_col == "tracking_index_code_raw"
                    else tinysoft_symbol_to_ts_code_any
                )
                df[ts_col] = df[raw_col].map(mapper)
        if "is_etf_feeder" in df.columns:
            df["is_etf_feeder"] = df["is_etf_feeder"].map(_tinysoft_yes_no_to_bool)
        latest = _latest_date_from_columns(df, ("found_date", "list_date", "liquidation_date"))
        if latest is not None:
            df["latest_change_date"] = latest
        return df


@task_register()
class TinySoftFundManagerExtTask(TinySoftFundCodeInfoArrayTask):
    name = "tinysoft_fund_manager_ext"
    description = "获取基金经理履历扩展字段（Tinysoft）"
    table_name = "fund_manager_ext"
    primary_keys = ["ts_code", "manager_key", "begin_date"]
    date_column = "ann_date"
    default_start_date = "19900101"
    default_code_batch_size = 100
    default_smart_code_batch_size = 1000
    infoarray_table_id = 308
    source_table_name = "基金.基金经理"
    where_date_field = "公布日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "证券代码": "tsl_code",
        "公布日": "ann_date",
        "信息来源": "info_source",
        "姓名": "manager_name",
        "性别": "gender",
        "国籍": "nationality",
        "出生年份": "birth_year",
        "年龄": "age",
        "职务": "position",
        "学历": "education",
        "证券从业经历": "securities_experience",
        "任职日": "begin_date",
        "离职日": "end_date",
        "在任与否": "is_current",
        "简历": "resume",
        "基金经理代码": "manager_code",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(200)"},
        "ann_date": {"type": "DATE"},
        "info_source": {"type": "VARCHAR(200)"},
        "manager_key": {"type": "VARCHAR(120)", "constraints": "NOT NULL"},
        "manager_code": {"type": "VARCHAR(50)"},
        "manager_name": {"type": "VARCHAR(100)"},
        "gender": {"type": "VARCHAR(20)"},
        "nationality": {"type": "VARCHAR(80)"},
        "birth_year": {"type": "VARCHAR(20)"},
        "age": {"type": "INTEGER"},
        "position": {"type": "VARCHAR(200)"},
        "education": {"type": "VARCHAR(100)"},
        "securities_experience": {"type": "TEXT"},
        "begin_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE"},
        "is_current": {"type": "BOOLEAN"},
        "resume": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_manager_ext_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_manager_ext_mgr", "columns": "manager_code"},
        {"name": "idx_tinysoft_fund_manager_ext_ann", "columns": "ann_date"},
        {"name": "idx_tinysoft_fund_manager_ext_begin", "columns": "begin_date"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["manager_key"].notna(), "manager_key 不能为空"),
        (lambda df: df["begin_date"].notna(), "begin_date 不能为空"),
    ]
    date_fields = ("ann_date", "begin_date", "end_date")
    numeric_fields = ("age",)
    integer_fields = ("age",)
    text_fields = (
        "tsl_code",
        "fund_name",
        "info_source",
        "manager_code",
        "manager_name",
        "gender",
        "nationality",
        "birth_year",
        "position",
        "education",
        "securities_experience",
        "resume",
    )

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        code_key = df["manager_code"].map(clean_text) if "manager_code" in df.columns else pd.Series([None] * len(df), index=df.index)
        name_key = df["manager_name"].map(clean_text) if "manager_name" in df.columns else pd.Series([None] * len(df), index=df.index)
        df["manager_key"] = code_key.where(code_key.notna(), name_key)
        if "ann_date" in df.columns and "begin_date" in df.columns:
            df["ann_date"] = df["ann_date"].where(df["ann_date"].notna(), df["begin_date"])
        if "is_current" in df.columns:
            df["is_current"] = df["is_current"].map(_tinysoft_yes_no_to_bool)
        return df


@task_register()
class TinySoftFundFinancialQuarterlyExtTask(TinySoftFundParentCodeInfoArrayTask):
    name = "tinysoft_fund_financial_quarterly_ext"
    description = "获取基金季度财务指标扩展字段（Tinysoft）"
    table_name = "fund_financial_quarterly_ext"
    primary_keys = ["ts_code", "report_date"]
    date_column = "report_date"
    default_start_date = "19980101"
    smart_lookback_days = 550
    default_code_batch_size = 50
    default_smart_code_batch_size = 500
    default_stream_batches = True
    infoarray_table_id = 310
    source_table_name = "基金.基金财务指标(季度)"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "证券代码": "tsl_code",
        "截止日": "report_date",
        "公布日": "ann_date",
        "净收益": "net_income",
        "本期利润": "period_profit",
        "加权平均基金份额本期利润": "weighted_avg_share_profit",
        "单位净收益": "unit_net_income",
        "可分配净收益": "distributable_net_income",
        "单位可分配净收益": "unit_distributable_net_income",
        "资产总值": "total_asset",
        "资产净值": "net_asset",
        "单位资产净值": "unit_net_asset",
        "资产净值收益率(%)": "net_asset_return_pct",
        "资产净值增长率(%)": "net_asset_growth_pct",
        "累计净值增长率(%)": "cum_nav_growth_pct",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(200)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "net_income": {"type": "NUMERIC(24,8)"},
        "period_profit": {"type": "NUMERIC(24,8)"},
        "weighted_avg_share_profit": {"type": "NUMERIC(24,8)"},
        "unit_net_income": {"type": "NUMERIC(24,8)"},
        "distributable_net_income": {"type": "NUMERIC(24,8)"},
        "unit_distributable_net_income": {"type": "NUMERIC(24,8)"},
        "total_asset": {"type": "NUMERIC(24,8)"},
        "net_asset": {"type": "NUMERIC(24,8)"},
        "unit_net_asset": {"type": "NUMERIC(24,8)"},
        "net_asset_return_pct": {"type": "NUMERIC(20,8)"},
        "net_asset_growth_pct": {"type": "NUMERIC(20,8)"},
        "cum_nav_growth_pct": {"type": "NUMERIC(20,8)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_financial_q_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_financial_q_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_financial_q_ann", "columns": "ann_date"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = (
        "net_income",
        "period_profit",
        "weighted_avg_share_profit",
        "unit_net_income",
        "distributable_net_income",
        "unit_distributable_net_income",
        "total_asset",
        "net_asset",
        "unit_net_asset",
        "net_asset_return_pct",
        "net_asset_growth_pct",
        "cum_nav_growth_pct",
    )
    text_fields = ("tsl_code", "fund_name", "remark")


@task_register()
class TinySoftFundFofHoldingDetailTask(TinySoftFundCodeInfoArrayTask):
    name = "tinysoft_fund_fof_holding_detail"
    description = "获取FOF持有基金明细（Tinysoft）"
    table_name = "fund_fof_holding_detail"
    primary_keys = ["ts_code", "report_date", "rank_no", "holding_code_raw"]
    date_column = "report_date"
    default_start_date = "20170101"
    # FOF持仓按季报/中报/年报披露，SMART需要覆盖较长的迟到披露与修正窗口。
    smart_lookback_days = 550
    default_code_batch_size = 50
    default_smart_code_batch_size = 500
    code_config_keys = ("fund_codes", "ts_codes", "ts_code", "codes")
    infoarray_table_id = 349
    source_table_name = "基金.基金明细"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "名称": "holding_name",
        "代码": "holding_code_raw",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
        "是否属于关联基金": "is_related_fund",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "holding_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "holding_ts_code": {"type": "VARCHAR(15)"},
        "holding_name": {"type": "VARCHAR(100)"},
        "quantity": {"type": "NUMERIC(24,4)"},
        "market_value": {"type": "NUMERIC(24,4)"},
        "nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "rank_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "is_related_fund": {"type": "BOOLEAN"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_fof_holding_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_fof_holding_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_fof_holding_raw", "columns": "holding_code_raw"},
        {"name": "idx_tinysoft_fund_fof_holding_ts", "columns": "holding_ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["holding_code_raw"].notna(), "holding_code_raw 不能为空"),
        (lambda df: df["rank_no"].notna(), "rank_no 不能为空"),
    ]
    date_fields = ("report_date",)
    numeric_fields = ("quantity", "market_value", "nav_ratio_pct", "rank_no")
    integer_fields = ("rank_no",)
    text_fields = ("tsl_code", "fund_name", "holding_code_raw", "holding_name")

    async def _load_codes_from_db(self, *, silent: bool = False) -> List[str]:
        codes = await self._load_all_fund_codes_from_db(silent=silent)
        if codes:
            return codes
        return await self._load_local_fof_candidate_codes(silent=silent)

    async def _load_all_fund_codes_from_db(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []

        all_codes: List[str] = []
        source_counts: List[str] = []
        for table in self.default_symbol_source_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue

                query = f"""
                SELECT ts_code
                FROM "{schema}"."{table_name}"
                WHERE ts_code ~ '^[0-9]{{6}}\\.(OF|SH|SZ)$'
                ORDER BY ts_code
                """
                rows = await self.db.fetch(query)
                codes: List[str] = []
                for row in rows or []:
                    symbol = ts_code_to_tinysoft_symbol_any(get_row_value(row, "ts_code"))
                    if symbol:
                        codes.append(symbol)
                codes = list(dict.fromkeys(codes))
                if codes:
                    all_codes.extend(codes)
                    source_counts.append(f"{table}:{len(codes)}")
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载基金代码失败: %s", table, e)

        all_codes = list(dict.fromkeys(all_codes))
        if all_codes and not silent:
            self.logger.info(
                "任务 %s: 从本地基金基础表加载到 %s 个去重基金候选代码（%s），将通过Tinysoft 302识别FOF。",
                self.name,
                len(all_codes),
                ", ".join(source_counts),
            )
        return all_codes

    async def _load_local_fof_candidate_codes(self, *, silent: bool = False) -> List[str]:
        if not self.db:
            return []

        candidate_tables = [
            "tinysoft.fund_basic_ext",
            "rawdata.fund_basic_ext",
            "rawdata.fund_basic",
            "tushare.fund_basic",
        ]
        for table in candidate_tables:
            try:
                schema, table_name = table.split(".", 1)
                columns = await self.db.get_column_names(table)
                if not columns or "ts_code" not in columns:
                    continue

                text_columns = [col for col in FOF_METADATA_FIELDS if col in columns]
                if not text_columns:
                    continue

                fof_filter = " OR ".join(
                    " OR ".join(
                        f"COALESCE(\"{col}\", '') ILIKE '%{marker}%'"
                        for marker in FOF_TEXT_MARKERS
                    )
                    for col in text_columns
                )
                query = f"""
                SELECT ts_code
                FROM "{schema}"."{table_name}"
                WHERE ts_code ~ '^[0-9]{{6}}\\.(OF|SH|SZ)$'
                  AND ({fof_filter})
                ORDER BY ts_code
                """
                rows = await self.db.fetch(query)
                codes: List[str] = []
                for row in rows or []:
                    symbol = ts_code_to_tinysoft_symbol_any(get_row_value(row, "ts_code"))
                    if symbol:
                        codes.append(symbol)
                codes = list(dict.fromkeys(codes))
                if codes:
                    if not silent:
                        self.logger.info("任务 %s: 从 %s 加载到 %s 个本地FOF候选代码。", self.name, table, len(codes))
                    return codes
            except Exception as e:
                if not silent:
                    self.logger.warning("从 %s 加载本地FOF候选代码失败: %s", table, e)
        return []

    async def _resolve_codes(self, **kwargs: Any) -> List[str]:
        codes = await super()._resolve_codes(**kwargs)
        if not codes:
            return []
        return await self._resolve_fof_main_fund_codes(codes)

    @staticmethod
    def _is_fof_metadata_row(row: pd.Series) -> bool:
        for field in FOF_METADATA_FIELDS:
            if field not in row.index:
                continue
            text = clean_text(row.get(field))
            if not text:
                continue
            upper_text = text.upper()
            if any(marker.upper() in upper_text for marker in FOF_TEXT_MARKERS):
                return True
        return False

    async def _resolve_fof_main_fund_codes(self, codes: List[str]) -> List[str]:
        unique_codes = list(dict.fromkeys(str(code).strip().upper() for code in codes if str(code).strip()))
        if not unique_codes:
            return []

        main_codes: List[str] = []
        failed_metadata_batches = 0
        batch_size = 1000
        for i in range(0, len(unique_codes), batch_size):
            chunk = unique_codes[i : i + batch_size]
            try:
                df = await self.api.call_dataframe_for_stocks(
                    "infoarray",
                    302,
                    stocks=chunk,
                    where_clause=None,
                    service=self.service,
                    timeout_ms=self.query_timeout_ms,
                )
            except Exception as e:
                failed_metadata_batches += 1
                self.logger.warning("FOF元数据识别查询失败，将尝试其他批次: %s", e)
                continue

            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                if not self._is_fof_metadata_row(row):
                    continue

                source = clean_text(row.get("StockID") if "StockID" in row.index else row.get("stockid"))
                main_code = clean_text(row.get("不同收费模式基金主代码"))
                if not main_code:
                    main_code = clean_text(row.get("母基金代码"))
                if not main_code:
                    main_code = source
                if not main_code or main_code in {"0", "000000", "00000000"}:
                    continue

                normalized = ts_code_to_tinysoft_symbol_any(main_code) or main_code.strip().upper()
                if normalized:
                    main_codes.append(normalized)

        resolved = list(dict.fromkeys(code for code in main_codes if code))
        if resolved:
            self.logger.info(
                "任务 %s: 从 %s 个基金候选代码中经Tinysoft 302识别出 %s 个FOF主代码。",
                self.name,
                len(unique_codes),
                len(resolved),
            )
            return resolved

        if failed_metadata_batches:
            fallback = await self._load_local_fof_candidate_codes()
            fallback = list(dict.fromkeys(str(code).strip().upper() for code in fallback if str(code).strip()))
            if fallback:
                self.logger.warning(
                    "任务 %s: Tinysoft 302未能完成FOF识别，回退使用 %s 个本地文本FOF候选代码。",
                    self.name,
                    len(fallback),
                )
                return fallback

        self.logger.warning("任务 %s: 未能从 %s 个基金候选代码中识别出FOF代码。", self.name, len(unique_codes))
        return []

    async def _resolve_main_fund_codes(self, codes: List[str]) -> List[str]:
        unique_codes = list(dict.fromkeys(str(code).strip().upper() for code in codes if str(code).strip()))
        if not unique_codes:
            return []

        main_codes: List[str] = []
        unresolved = set(unique_codes)
        batch_size = 1000
        for i in range(0, len(unique_codes), batch_size):
            chunk = unique_codes[i : i + batch_size]
            try:
                df = await self.api.call_dataframe_for_stocks(
                    "infoarray",
                    302,
                    stocks=chunk,
                    where_clause=None,
                    service=self.service,
                    timeout_ms=self.query_timeout_ms,
                )
            except Exception as e:
                self.logger.warning("FOF主代码映射查询失败，将使用原始代码继续: %s", e)
                continue

            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                source = clean_text(row.get("StockID") if "StockID" in row.index else row.get("stockid"))
                if source:
                    unresolved.discard(source.strip().upper())

                main_code = clean_text(row.get("不同收费模式基金主代码"))
                if not main_code:
                    main_code = clean_text(row.get("母基金代码"))
                if not main_code:
                    main_code = source
                if not main_code or main_code in {"0", "000000", "00000000"}:
                    continue

                normalized = ts_code_to_tinysoft_symbol_any(main_code) or main_code.strip().upper()
                if normalized:
                    main_codes.append(normalized)

        main_codes.extend(sorted(unresolved))
        resolved = list(dict.fromkeys(code for code in main_codes if code))
        if len(resolved) != len(unique_codes):
            self.logger.info(
                "任务 %s: FOF候选代码经302主代码映射后由 %s 个去重为 %s 个。",
                self.name,
                len(unique_codes),
                len(resolved),
            )
        return resolved

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "tsl_code" not in df.columns and "StockID" in df.columns:
            df["tsl_code"] = df["StockID"]
        if "ts_code" not in df.columns and "tsl_code" in df.columns:
            df["ts_code"] = df["tsl_code"].map(tinysoft_symbol_to_ts_code_any)
        if "holding_ts_code" not in df.columns and "holding_code_raw" in df.columns:
            df["holding_ts_code"] = df["holding_code_raw"].map(tinysoft_symbol_to_ts_code_any)
        if "is_related_fund" in df.columns:
            df["is_related_fund"] = df["is_related_fund"].map(_tinysoft_yes_no_to_bool)
        return df


@task_register()
class TinySoftFundStockHoldingDetailTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_stock_holding_detail"
    description = "获取基金持股明细（Tinysoft）"
    table_name = "fund_stock_holding_detail"
    primary_keys = ["ts_code", "report_date", "rank_no", "security_code_raw"]
    date_column = "report_date"
    infoarray_table_id = 318
    source_table_name = "基金.持股明细"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "代码": "security_code_raw",
        "名称": "security_name",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
        "其中:指数投资部分数量": "index_invest_quantity",
        "其中：指数投资部分数量": "index_invest_quantity",
        "其中:指数投资部分市值": "index_invest_market_value",
        "其中：指数投资部分市值": "index_invest_market_value",
        "其中:指数投资部分占净值比例(%)": "index_invest_nav_ratio_pct",
        "其中：指数投资部分占净值比例(%)": "index_invest_nav_ratio_pct",
        "其中:积极投资部分数量": "active_invest_quantity",
        "其中：积极投资部分数量": "active_invest_quantity",
        "其中:积极投资部分市值": "active_invest_market_value",
        "其中：积极投资部分市值": "active_invest_market_value",
        "其中:积极投资部分占净值比例(%)": "active_invest_nav_ratio_pct",
        "其中：积极投资部分占净值比例(%)": "active_invest_nav_ratio_pct",
        "板块名称": "board_name",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "security_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "security_ts_code": {"type": "VARCHAR(15)"},
        "security_name": {"type": "VARCHAR(100)"},
        "quantity": {"type": "NUMERIC(24,4)"},
        "market_value": {"type": "NUMERIC(24,4)"},
        "nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "rank_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "index_invest_quantity": {"type": "NUMERIC(24,4)"},
        "index_invest_market_value": {"type": "NUMERIC(24,4)"},
        "index_invest_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "active_invest_quantity": {"type": "NUMERIC(24,4)"},
        "active_invest_market_value": {"type": "NUMERIC(24,4)"},
        "active_invest_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "board_name": {"type": "VARCHAR(100)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_stock_holding_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_stock_holding_fund", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_stock_holding_sec", "columns": "security_ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["security_code_raw"].notna(), "security_code_raw 不能为空"),
        (lambda df: df["rank_no"].notna(), "rank_no 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = (
        "quantity",
        "market_value",
        "nav_ratio_pct",
        "rank_no",
        "index_invest_quantity",
        "index_invest_market_value",
        "index_invest_nav_ratio_pct",
        "active_invest_quantity",
        "active_invest_market_value",
        "active_invest_nav_ratio_pct",
    )
    integer_fields = ("rank_no",)
    text_fields = ("tsl_code", "fund_name", "security_code_raw", "security_name", "board_name", "remark")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        if "security_ts_code" not in df.columns and "security_code_raw" in df.columns:
            df["security_ts_code"] = df["security_code_raw"].map(_security_code_to_ts_code)
        return df


@task_register()
class TinySoftFundIndustryAllocTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_industry_alloc"
    description = "获取基金行业配置（Tinysoft）"
    table_name = "fund_industry_alloc"
    primary_keys = ["ts_code", "report_date", "industry_name"]
    date_column = "report_date"
    infoarray_table_id = 320
    source_table_name = "基金.行业配置"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "行业名称": "industry_name",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "industry_name": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},
        "market_value": {"type": "NUMERIC(24,4)"},
        "nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_industry_alloc_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_industry_alloc_fund", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_industry_alloc_name", "columns": "industry_name"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["industry_name"].notna(), "industry_name 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = ("market_value", "nav_ratio_pct")
    text_fields = ("tsl_code", "fund_name", "industry_name", "remark")


@task_register()
class TinySoftFundAssetAllocTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_asset_alloc"
    description = "获取基金资产配置（Tinysoft）"
    table_name = "fund_asset_alloc"
    primary_keys = ["ts_code", "report_date"]
    date_column = "report_date"
    infoarray_table_id = 322
    source_table_name = "基金.资产配置"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "权益投资": "equity_investment",
        "权益投资占净值比例(%)": "equity_nav_ratio_pct",
        "股票市值": "stock_market_value",
        "股票占净值比例(%)": "stock_nav_ratio_pct",
        "其中：优先股": "preferred_stock_value",
        "其中：优先股占净值比例(%)": "preferred_stock_nav_ratio_pct",
        "其中：存托凭证": "depositary_receipt_value",
        "其中：存托凭证占净值比例(%)": "depositary_receipt_nav_ratio_pct",
        "其中：房地产信托凭证": "reit_value",
        "其中：房地产信托凭证占净值比例(%)": "reit_nav_ratio_pct",
        "基金市值": "fund_market_value",
        "基金市值占净值比例(%)": "fund_nav_ratio_pct",
        "固定收益投资": "fixed_income_investment",
        "固定收益投资占净值比例(%)": "fixed_income_nav_ratio_pct",
        "固定投资收益": "fixed_income_investment",
        "固定投资收益占净值比例(%)": "fixed_income_nav_ratio_pct",
        "债券市值": "bond_market_value",
        "债券占净值比例(%)": "bond_nav_ratio_pct",
        "资产支持证券市值": "abs_market_value",
        "资产支持证券市值占净值比例(%)": "abs_nav_ratio_pct",
        "贵金属投资": "precious_metal_value",
        "贵金属投资占净值比例(%)": "precious_metal_nav_ratio_pct",
        "银行存款和清算备付金市值": "bank_deposit_settlement_value",
        "银行存款和清算备付金占净值比例(%)": "bank_deposit_settlement_nav_ratio_pct",
        "买入返售证券市值": "reverse_repo_value",
        "买入返售证券占净值比例(%)": "reverse_repo_nav_ratio_pct",
        "其中：买断式回购的买入返售金融资产": "outright_reverse_repo_value",
        "其中：买断式回购的买入返售金融资产占净值比例(%)": "outright_reverse_repo_nav_ratio_pct",
        "卖出回购证券市值": "repo_sold_value",
        "卖出回购证券占净值比例(%)": "repo_sold_nav_ratio_pct",
        "货币市场工具": "money_market_instrument_value",
        "货币市场工具占净值比例(%)": "money_market_instrument_nav_ratio_pct",
        "其他资产市值": "other_asset_value",
        "其他资产占净值比例(%)": "other_asset_nav_ratio_pct",
        "金融衍生品市值": "derivative_value",
        "金融衍生品市值占净值比例(%)": "derivative_nav_ratio_pct",
        "其中：远期": "forward_value",
        "其中：远期占净值比例(%)": "forward_nav_ratio_pct",
        "其中：期货": "futures_value",
        "其中：期货占净值比例(%)": "futures_nav_ratio_pct",
        "其中：期权": "option_value",
        "其中：期权占净值比例(%)": "option_nav_ratio_pct",
        "其中：权证": "warrant_value",
        "其中：权证占净值比例(%)": "warrant_nav_ratio_pct",
        "资产净值": "net_asset_value",
        "资产总值": "total_asset_value",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "equity_investment": {"type": "NUMERIC(24,4)"},
        "equity_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "stock_market_value": {"type": "NUMERIC(24,4)"},
        "stock_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "preferred_stock_value": {"type": "NUMERIC(24,4)"},
        "preferred_stock_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "depositary_receipt_value": {"type": "NUMERIC(24,4)"},
        "depositary_receipt_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "reit_value": {"type": "NUMERIC(24,4)"},
        "reit_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "fund_market_value": {"type": "NUMERIC(24,4)"},
        "fund_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "fixed_income_investment": {"type": "NUMERIC(24,4)"},
        "fixed_income_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "bond_market_value": {"type": "NUMERIC(24,4)"},
        "bond_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "abs_market_value": {"type": "NUMERIC(24,4)"},
        "abs_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "precious_metal_value": {"type": "NUMERIC(24,4)"},
        "precious_metal_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "bank_deposit_settlement_value": {"type": "NUMERIC(24,4)"},
        "bank_deposit_settlement_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "reverse_repo_value": {"type": "NUMERIC(24,4)"},
        "reverse_repo_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "outright_reverse_repo_value": {"type": "NUMERIC(24,4)"},
        "outright_reverse_repo_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "repo_sold_value": {"type": "NUMERIC(24,4)"},
        "repo_sold_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "money_market_instrument_value": {"type": "NUMERIC(24,4)"},
        "money_market_instrument_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "other_asset_value": {"type": "NUMERIC(24,4)"},
        "other_asset_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "derivative_value": {"type": "NUMERIC(24,4)"},
        "derivative_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "forward_value": {"type": "NUMERIC(24,4)"},
        "forward_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "futures_value": {"type": "NUMERIC(24,4)"},
        "futures_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "option_value": {"type": "NUMERIC(24,4)"},
        "option_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "warrant_value": {"type": "NUMERIC(24,4)"},
        "warrant_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "net_asset_value": {"type": "NUMERIC(24,4)"},
        "total_asset_value": {"type": "NUMERIC(24,4)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_asset_alloc_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_asset_alloc_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = tuple(
        col
        for col in schema_def
        if col not in {"ts_code", "tsl_code", "fund_name", "report_date", "ann_date", "remark", "source_table_id", "source_table_name", "raw_json"}
    )
    text_fields = ("tsl_code", "fund_name", "remark")


@task_register()
class TinySoftFundBondAllocTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_bond_alloc"
    description = "获取基金债券配置（Tinysoft）"
    table_name = "fund_bond_alloc"
    primary_keys = ["ts_code", "report_date", "bond_category"]
    date_column = "report_date"
    infoarray_table_id = 340
    source_table_name = "基金.债券配置"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "行业名称": "bond_category",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "占债券市值比例(%)": "bond_market_ratio_pct",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "bond_category": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},
        "market_value": {"type": "NUMERIC(24,4)"},
        "nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "bond_market_ratio_pct": {"type": "NUMERIC(20,8)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_bond_alloc_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_bond_alloc_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_bond_alloc_cat", "columns": "bond_category"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["bond_category"].notna(), "bond_category 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = ("market_value", "nav_ratio_pct", "bond_market_ratio_pct")
    text_fields = ("tsl_code", "fund_name", "bond_category", "remark")


@task_register()
class TinySoftFundBondHoldingDetailTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_bond_holding_detail"
    description = "获取基金持债明细（Tinysoft）"
    table_name = "fund_bond_holding_detail"
    primary_keys = ["ts_code", "report_date", "rank_no", "bond_code_raw"]
    date_column = "report_date"
    infoarray_table_id = 342
    source_table_name = "基金.持债明细"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "名称": "bond_name",
        "代码": "bond_code_raw",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
        "债券类型": "bond_type",
        "是否处于转股期": "is_convertible_period",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "bond_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "bond_ts_code": {"type": "VARCHAR(15)"},
        "bond_name": {"type": "VARCHAR(100)"},
        "quantity": {"type": "NUMERIC(24,4)"},
        "market_value": {"type": "NUMERIC(24,4)"},
        "nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "rank_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "bond_type": {"type": "VARCHAR(50)"},
        "is_convertible_period": {"type": "BOOLEAN"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_bond_holding_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_bond_holding_fund", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_bond_holding_bond", "columns": "bond_ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["bond_code_raw"].notna(), "bond_code_raw 不能为空"),
        (lambda df: df["rank_no"].notna(), "rank_no 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = ("quantity", "market_value", "nav_ratio_pct", "rank_no")
    integer_fields = ("rank_no",)
    text_fields = ("tsl_code", "fund_name", "bond_code_raw", "bond_name", "bond_type", "remark")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        if "bond_ts_code" not in df.columns and "bond_code_raw" in df.columns:
            df["bond_ts_code"] = df["bond_code_raw"].map(_security_code_to_ts_code)
        if "is_convertible_period" in df.columns:
            df["is_convertible_period"] = df["is_convertible_period"].map(_tinysoft_yes_no_to_bool)
        return df


@task_register()
class TinySoftFundAbsHoldingDetailTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_abs_holding_detail"
    description = "获取基金资产支持证券明细（Tinysoft）"
    table_name = "fund_abs_holding_detail"
    primary_keys = ["ts_code", "report_date", "rank_no", "asset_code_raw"]
    date_column = "report_date"
    infoarray_table_id = 350
    source_table_name = "基金.资产支持证券明细"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "名称": "asset_name",
        "代码": "asset_code_raw",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "asset_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "asset_ts_code": {"type": "VARCHAR(15)"},
        "asset_name": {"type": "VARCHAR(100)"},
        "quantity": {"type": "NUMERIC(24,4)"},
        "market_value": {"type": "NUMERIC(24,4)"},
        "nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "rank_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_abs_holding_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_abs_holding_fund", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_abs_holding_asset", "columns": "asset_code_raw"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["asset_code_raw"].notna(), "asset_code_raw 不能为空"),
        (lambda df: df["rank_no"].notna(), "rank_no 不能为空"),
    ]
    date_fields = ("report_date",)
    numeric_fields = ("quantity", "market_value", "nav_ratio_pct", "rank_no")
    integer_fields = ("rank_no",)
    text_fields = ("tsl_code", "fund_name", "asset_code_raw", "asset_name")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        if "asset_ts_code" not in df.columns and "asset_code_raw" in df.columns:
            df["asset_ts_code"] = df["asset_code_raw"].map(_security_code_to_ts_code)
        return df


@task_register()
class TinySoftFundCbondHoldingDetailTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_cbond_holding_detail"
    description = "获取基金可转债明细（Tinysoft）"
    table_name = "fund_cbond_holding_detail"
    primary_keys = ["ts_code", "report_date", "rank_no", "cbond_code_raw"]
    date_column = "report_date"
    infoarray_table_id = 354
    source_table_name = "基金.可转债明细"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "名称": "cbond_name",
        "代码": "cbond_code_raw",
        "数量": "quantity",
        "市值": "market_value",
        "占净值比例(%)": "nav_ratio_pct",
        "市值排名": "rank_no",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "cbond_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "cbond_ts_code": {"type": "VARCHAR(15)"},
        "cbond_name": {"type": "VARCHAR(100)"},
        "quantity": {"type": "NUMERIC(24,4)"},
        "market_value": {"type": "NUMERIC(24,4)"},
        "nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "rank_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_cbond_holding_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_cbond_holding_fund", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_cbond_holding_cbond", "columns": "cbond_ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["cbond_code_raw"].notna(), "cbond_code_raw 不能为空"),
        (lambda df: df["rank_no"].notna(), "rank_no 不能为空"),
    ]
    date_fields = ("report_date",)
    numeric_fields = ("quantity", "market_value", "nav_ratio_pct", "rank_no")
    integer_fields = ("rank_no",)
    text_fields = ("tsl_code", "fund_name", "cbond_code_raw", "cbond_name")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        if "cbond_ts_code" not in df.columns and "cbond_code_raw" in df.columns:
            df["cbond_ts_code"] = df["cbond_code_raw"].map(_security_code_to_ts_code)
        return df


@task_register()
class TinySoftFundTopHolderTask(TinySoftFundCodeInfoArrayTask):
    name = "tinysoft_fund_top_holder"
    description = "获取基金主要持有人（Tinysoft）"
    table_name = "fund_top_holder"
    primary_keys = ["ts_code", "report_date", "holder_name", "holder_type"]
    date_column = "report_date"
    default_start_date = "19980101"
    smart_lookback_days = 550
    default_code_batch_size = 80
    default_smart_code_batch_size = 500
    default_stream_batches = True
    infoarray_table_id = 330
    source_table_name = "基金.主要持有人"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "名称": "holder_name",
        "份额": "holding_share",
        "占总份额比例(%)": "total_share_ratio_pct",
        "性质": "holder_type",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "holder_name": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
        "holding_share": {"type": "NUMERIC(24,4)"},
        "total_share_ratio_pct": {"type": "NUMERIC(20,8)"},
        "holder_type": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_top_holder_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_top_holder_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["holder_name"].notna(), "holder_name 不能为空"),
        (lambda df: df["holder_type"].notna(), "holder_type 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = ("holding_share", "total_share_ratio_pct")
    text_fields = ("tsl_code", "fund_name", "holder_name", "holder_type", "remark")


@task_register()
class TinySoftFundHolderStructureTask(TinySoftFundCodeInfoArrayTask):
    name = "tinysoft_fund_holder_structure"
    description = "获取基金持有人结构（Tinysoft）"
    table_name = "fund_holder_structure"
    primary_keys = ["ts_code", "report_date"]
    date_column = "report_date"
    default_start_date = "19980101"
    smart_lookback_days = 550
    default_code_batch_size = 100
    default_smart_code_batch_size = 800
    default_stream_batches = True
    infoarray_table_id = 331
    source_table_name = "基金.持有人结构"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "持有人户数": "holder_count",
        "户均持有份额": "avg_share_per_holder",
        "机构持有份额": "institution_share",
        "机构持有比例(%)": "institution_ratio_pct",
        "个人持有份额": "individual_share",
        "个人持有比例(%)": "individual_ratio_pct",
        "其他持有份额": "other_share",
        "其他持有比例(%)": "other_ratio_pct",
        "备注": "remark",
        "基金管理人从业人员持有份额": "staff_share",
        "基金管理人从业人员持有比例(%)": "staff_ratio_pct",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "holder_count": {"type": "NUMERIC(24,4)"},
        "avg_share_per_holder": {"type": "NUMERIC(24,4)"},
        "institution_share": {"type": "NUMERIC(24,4)"},
        "institution_ratio_pct": {"type": "NUMERIC(20,8)"},
        "individual_share": {"type": "NUMERIC(24,4)"},
        "individual_ratio_pct": {"type": "NUMERIC(20,8)"},
        "other_share": {"type": "NUMERIC(24,4)"},
        "other_ratio_pct": {"type": "NUMERIC(20,8)"},
        "staff_share": {"type": "NUMERIC(24,4)"},
        "staff_ratio_pct": {"type": "NUMERIC(20,8)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_holder_structure_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_holder_structure_code", "columns": "ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
    ]
    date_fields = ("report_date",)
    numeric_fields = tuple(
        col
        for col in schema_def
        if col not in {"ts_code", "tsl_code", "fund_name", "report_date", "remark", "source_table_id", "source_table_name", "raw_json"}
    )
    text_fields = ("tsl_code", "fund_name", "remark")


@task_register()
class TinySoftFundStockTradeSummaryTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_stock_trade_summary"
    description = "获取基金累计买入和卖出（Tinysoft）"
    table_name = "fund_stock_trade_summary"
    primary_keys = ["ts_code", "report_date", "serial_no", "security_code_raw", "change_type"]
    date_column = "report_date"
    default_smart_code_batch_size = 400
    infoarray_table_id = 319
    source_table_name = "基金.累计买入和卖出"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "序号": "serial_no",
        "股票代码": "security_code_raw",
        "股票名称": "security_name",
        "累计买入/卖出金额": "trade_amount",
        "占期初净值比例(%)": "begin_nav_ratio_pct",
        "变动类型": "change_type",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "serial_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "security_code_raw": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "security_ts_code": {"type": "VARCHAR(15)"},
        "security_name": {"type": "VARCHAR(100)"},
        "trade_amount": {"type": "NUMERIC(24,4)"},
        "begin_nav_ratio_pct": {"type": "NUMERIC(20,8)"},
        "change_type": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_stock_trade_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_stock_trade_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_stock_trade_sec", "columns": "security_ts_code"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["serial_no"].notna(), "serial_no 不能为空"),
        (lambda df: df["security_code_raw"].notna(), "security_code_raw 不能为空"),
        (lambda df: df["change_type"].notna(), "change_type 不能为空"),
    ]
    date_fields = ("report_date",)
    numeric_fields = ("serial_no", "trade_amount", "begin_nav_ratio_pct")
    integer_fields = ("serial_no",)
    text_fields = ("tsl_code", "fund_name", "security_code_raw", "security_name", "change_type", "remark")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        if "security_ts_code" not in df.columns and "security_code_raw" in df.columns:
            df["security_ts_code"] = df["security_code_raw"].map(_security_code_to_ts_code)
        return df


@task_register()
class TinySoftFundBrokerSeatTask(TinySoftFundMainCodeInfoArrayTask):
    name = "tinysoft_fund_broker_seat"
    description = "获取基金交易席位情况（Tinysoft）"
    table_name = "fund_broker_seat"
    primary_keys = ["ts_code", "report_date", "broker_name"]
    date_column = "report_date"
    default_smart_code_batch_size = 400
    infoarray_table_id = 332
    source_table_name = "基金.交易席位情况"
    where_date_field = "截止日"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "StockName": "fund_name",
        "截止日": "report_date",
        "公布日": "ann_date",
        "券商名称": "broker_name",
        "交易单元数量": "trading_unit_count",
        "股票成交量": "stock_trade_amount",
        "占股票成交总量比例(%)": "stock_trade_ratio_pct",
        "佣金": "commission",
        "占佣金总量比例(%)": "commission_ratio_pct",
        "债券成交量": "bond_trade_amount",
        "占债券成交总量比例(%)": "bond_trade_ratio_pct",
        "回购成交量": "repo_trade_amount",
        "占回购成交总量比例(%)": "repo_trade_ratio_pct",
        "银行间债券成交量": "interbank_bond_trade_amount",
        "占银行间债券成交总量比例(%)": "interbank_bond_trade_ratio_pct",
        "权证成交金额": "warrant_trade_amount",
        "占权证成交总额比例(%)": "warrant_trade_ratio_pct",
        "基金成交金额": "fund_trade_amount",
        "占基金成交总额比例(%)": "fund_trade_ratio_pct",
        "备注": "remark",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "fund_name": {"type": "VARCHAR(100)"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "broker_name": {"type": "VARCHAR(120)", "constraints": "NOT NULL"},
        "trading_unit_count": {"type": "NUMERIC(20,4)"},
        "stock_trade_amount": {"type": "NUMERIC(24,4)"},
        "stock_trade_ratio_pct": {"type": "NUMERIC(20,8)"},
        "commission": {"type": "NUMERIC(24,4)"},
        "commission_ratio_pct": {"type": "NUMERIC(20,8)"},
        "bond_trade_amount": {"type": "NUMERIC(24,4)"},
        "bond_trade_ratio_pct": {"type": "NUMERIC(20,8)"},
        "repo_trade_amount": {"type": "NUMERIC(24,4)"},
        "repo_trade_ratio_pct": {"type": "NUMERIC(20,8)"},
        "interbank_bond_trade_amount": {"type": "NUMERIC(24,4)"},
        "interbank_bond_trade_ratio_pct": {"type": "NUMERIC(20,8)"},
        "warrant_trade_amount": {"type": "NUMERIC(24,4)"},
        "warrant_trade_ratio_pct": {"type": "NUMERIC(20,8)"},
        "fund_trade_amount": {"type": "NUMERIC(24,4)"},
        "fund_trade_ratio_pct": {"type": "NUMERIC(20,8)"},
        "remark": {"type": "TEXT"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_broker_seat_date", "columns": "report_date"},
        {"name": "idx_tinysoft_fund_broker_seat_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_broker_seat_name", "columns": "broker_name"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["report_date"].notna(), "report_date 不能为空"),
        (lambda df: df["broker_name"].notna(), "broker_name 不能为空"),
    ]
    date_fields = ("report_date", "ann_date")
    numeric_fields = tuple(
        col
        for col in schema_def
        if col not in {"ts_code", "tsl_code", "fund_name", "report_date", "ann_date", "broker_name", "remark", "source_table_id", "source_table_name", "raw_json"}
    )
    text_fields = ("tsl_code", "fund_name", "broker_name", "remark")


class TinySoftFundClassificationInfoBaseTask(TinySoftP0InfoArrayTask):
    domain = "fund"
    default_start_date = "19000101"
    smart_lookback_days = 3700
    default_code_batch_size = 80
    default_smart_code_batch_size = 500
    where_date_field = None
    code_config_keys = ("attr_codes", "category_codes", "codes")
    default_codes = ["YHFL", "TSJJ02", "TSJJ03", *DEFAULT_FUND_CATEGORY_CODES]

    @staticmethod
    def _fill_version_dates(df: pd.DataFrame, *, in_col: str = "in_date", out_col: str = "out_date") -> pd.DataFrame:
        if in_col in df.columns:
            df[in_col] = pd.to_datetime(df[in_col], errors="coerce").dt.date
            df[in_col] = df[in_col].fillna(pd.Timestamp("1900-01-01").date())
        if out_col in df.columns:
            df[out_col] = pd.to_datetime(df[out_col], errors="coerce").dt.date
        date_cols = [col for col in (in_col, out_col) if col in df.columns]
        if date_cols:
            as_ts = pd.DataFrame({col: pd.to_datetime(df[col], errors="coerce") for col in date_cols})
            df["latest_change_date"] = as_ts.max(axis=1).dt.date
            df["latest_change_date"] = df["latest_change_date"].fillna(df[in_col])
        return df


@task_register()
class TinySoftFundClassificationInfoTask(TinySoftFundClassificationInfoBaseTask):
    name = "tinysoft_fund_classification_info"
    description = "获取基金分类信息（Tinysoft）"
    table_name = "fund_classification_info"
    primary_keys = ["attr_code", "level_no", "in_date"]
    date_column = "latest_change_date"
    infoarray_table_id = 355
    source_table_name = "基金.基金分类信息"
    field_mapping = {
        "属性代码": "attr_code",
        "属性名称": "attr_name",
        "级数": "level_no",
        "上级属性代码": "parent_attr_code",
        "上级属性名称": "parent_attr_name",
        "入选日期": "in_date",
        "剔除日期": "out_date",
        "最新标识": "is_latest",
        "所属属性代码": "root_attr_code",
    }
    schema_def = {
        "attr_query_code": {"type": "VARCHAR(50)"},
        "attr_code": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "attr_name": {"type": "VARCHAR(120)"},
        "level_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "parent_attr_code": {"type": "VARCHAR(50)"},
        "parent_attr_name": {"type": "VARCHAR(120)"},
        "in_date": {"type": "DATE", "constraints": "NOT NULL"},
        "out_date": {"type": "DATE"},
        "latest_change_date": {"type": "DATE"},
        "is_latest": {"type": "INTEGER"},
        "root_attr_code": {"type": "VARCHAR(50)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_class_info_attr", "columns": "attr_code"},
        {"name": "idx_tinysoft_fund_class_info_change", "columns": "latest_change_date"},
        {"name": "idx_tinysoft_fund_class_info_root", "columns": "root_attr_code"},
    ]
    validations = [
        (lambda df: df["attr_code"].notna(), "attr_code 不能为空"),
        (lambda df: df["level_no"].notna(), "level_no 不能为空"),
        (lambda df: df["in_date"].notna(), "in_date 不能为空"),
    ]
    date_fields = ("in_date", "out_date")
    numeric_fields = ("level_no", "is_latest")
    integer_fields = ("level_no", "is_latest")
    text_fields = ("attr_code", "attr_name", "parent_attr_code", "parent_attr_name", "root_attr_code")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        if "attr_query_code" not in df.columns and "request_code" in df.columns:
            df["attr_query_code"] = df["request_code"]
        return self._fill_version_dates(df)


@task_register()
class TinySoftFundClassificationMemberTask(TinySoftFundCodeInfoArrayTask):
    name = "tinysoft_fund_classification_member"
    description = "获取基金分类历史映射（Tinysoft）"
    table_name = "fund_classification_member"
    primary_keys = ["ts_code", "attr_code", "level_no", "in_date"]
    date_column = "latest_change_date"
    default_start_date = "19000101"
    smart_lookback_days = 3700
    default_code_batch_size = 80
    default_smart_code_batch_size = 1000
    default_stream_batches = True
    where_date_field = None
    infoarray_table_id = 356
    source_table_name = "基金.基金分类"
    field_mapping = {
        "StockID": "tsl_code",
        "stockid": "tsl_code",
        "证券代码": "tsl_code",
        "属性代码": "attr_code",
        "属性名称": "attr_name",
        "级数": "level_no",
        "入选日期": "in_date",
        "剔除日期": "out_date",
        "最新标识": "is_latest",
        "所属属性代码": "root_attr_code",
        "所属属性名称": "root_attr_name",
    }
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "tsl_code": {"type": "VARCHAR(20)"},
        "attr_code": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "attr_name": {"type": "VARCHAR(120)"},
        "level_no": {"type": "INTEGER", "constraints": "NOT NULL"},
        "in_date": {"type": "DATE", "constraints": "NOT NULL"},
        "out_date": {"type": "DATE"},
        "latest_change_date": {"type": "DATE"},
        "is_latest": {"type": "INTEGER"},
        "root_attr_code": {"type": "VARCHAR(50)"},
        "root_attr_name": {"type": "VARCHAR(120)"},
        "source_table_id": {"type": "INTEGER"},
        "source_table_name": {"type": "VARCHAR(100)"},
        "raw_json": {"type": "TEXT"},
    }
    indexes = [
        {"name": "idx_tinysoft_fund_class_member_code", "columns": "ts_code"},
        {"name": "idx_tinysoft_fund_class_member_attr", "columns": "attr_code"},
        {"name": "idx_tinysoft_fund_class_member_change", "columns": "latest_change_date"},
    ]
    validations = [
        (lambda df: df["ts_code"].notna(), "ts_code 不能为空"),
        (lambda df: df["attr_code"].notna(), "attr_code 不能为空"),
        (lambda df: df["level_no"].notna(), "level_no 不能为空"),
        (lambda df: df["in_date"].notna(), "in_date 不能为空"),
    ]
    date_fields = ("in_date", "out_date")
    numeric_fields = ("level_no", "is_latest")
    integer_fields = ("level_no", "is_latest")
    text_fields = ("tsl_code", "attr_code", "attr_name", "root_attr_code", "root_attr_name")

    def _postprocess_frame(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        df = super()._postprocess_frame(df, **kwargs)
        return TinySoftFundClassificationInfoBaseTask._fill_version_dates(df)


__all__ = [
    "TinySoftFundAbsHoldingDetailTask",
    "TinySoftFundAssetAllocTask",
    "TinySoftFundBasicExtTask",
    "TinySoftFundBondAllocTask",
    "TinySoftFundBondHoldingDetailTask",
    "TinySoftFundBrokerSeatTask",
    "TinySoftFundCbondHoldingDetailTask",
    "TinySoftFundClassificationInfoTask",
    "TinySoftFundClassificationMemberTask",
    "TinySoftFundFinancialQuarterlyExtTask",
    "TinySoftFundFofHoldingDetailTask",
    "TinySoftFundHolderStructureTask",
    "TinySoftFundIndustryAllocTask",
    "TinySoftFundManagerExtTask",
    "TinySoftFundStockHoldingDetailTask",
    "TinySoftFundStockTradeSummaryTask",
    "TinySoftFundTopHolderTask",
]
