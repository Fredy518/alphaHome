#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""中证指数官网日度指数表现任务。

该任务首先服务于宏观风格轮动的 H00922 中证红利全收益指数。官网 performance
接口可能为周末或节假日返回一条或多条绘图锚点；入库前按 AlphaDB 标准交易日历
逐行过滤，避免把这些锚点误当交易日。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from ...base.fetcher_task import FetcherTask
from ...sources.csindex import CSINDEX_PERFORMANCE_URL, CsindexAPI
from ....common.task_system.task_decorator import task_register


@task_register()
class CsindexIndexPerformanceTask(FetcherTask):
    """获取中证指数官网指数日度表现，默认维护 H00922。"""

    domain = "index"
    name = "csindex_index_performance"
    description = "中证指数官网日度表现（默认 H00922 中证红利全收益）"
    table_name = "index_performance"
    data_source = "csindex"

    primary_keys = ["index_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20041231"
    smart_lookback_days = 15
    default_concurrent_limit = 1
    default_calendar_exchange = "SSE"

    default_index_codes = ["H00922"]

    schema_def = {
        "index_code": {"type": "VARCHAR(16)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "index_name_en": {"type": "VARCHAR(160)"},
        "index_name_en_all": {"type": "VARCHAR(240)"},
        "close": {"type": "NUMERIC(20,6)", "constraints": "NOT NULL"},
        "change": {"type": "NUMERIC(20,6)"},
        "change_pct": {"type": "NUMERIC(20,6)"},
        "trading_volume": {"type": "NUMERIC(28,6)"},
        "trading_value": {"type": "NUMERIC(28,6)"},
        "constituent_count": {"type": "INTEGER"},
        "peg": {"type": "NUMERIC(20,6)"},
        "source_url": {"type": "TEXT", "constraints": "NOT NULL"},
    }

    indexes = [
        {
            "name": "idx_csindex_perf_code_date",
            "columns": "index_code, trade_date",
            "unique": True,
        },
        {"name": "idx_csindex_perf_trade_date", "columns": "trade_date"},
        {"name": "idx_csindex_perf_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["index_code"].notna(), "指数代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["close"].notna() & (df["close"] > 0), "收盘点位必须为正"),
    ]
    validation_mode = "filter"

    def __init__(self, db_connection, api: Optional[CsindexAPI] = None, **kwargs):
        super().__init__(db_connection, **kwargs)
        self.api = api or CsindexAPI(
            logger=self.logger,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay,
        )

    def _apply_config(self, task_config: Dict):
        super()._apply_config(task_config)
        raw_codes = task_config.get("index_codes", self.default_index_codes)
        if isinstance(raw_codes, str):
            raw_codes = [
                part.strip() for part in raw_codes.replace(";", ",").split(",")
            ]
        self.index_codes = [
            str(code).strip().upper() for code in raw_codes if str(code).strip()
        ]
        if not self.index_codes:
            raise ValueError("csindex_index_performance.index_codes 不能为空")
        self.calendar_exchange = (
            str(task_config.get("calendar_exchange", self.default_calendar_exchange))
            .strip()
            .upper()
        )
        if not self.calendar_exchange:
            raise ValueError("csindex_index_performance.calendar_exchange 不能为空")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, str]]:
        start_date = kwargs.get("start_date") or getattr(
            self, "_effective_start_date", None
        )
        end_date = kwargs.get("end_date") or getattr(self, "_effective_end_date", None)
        if not start_date or not end_date:
            raise ValueError("中证指数任务需要 start_date 和 end_date")
        return [
            {"index_code": code, "start_date": start_date, "end_date": end_date}
            for code in self.index_codes
        ]

    async def prepare_params(self, batch: Dict[str, str]) -> Dict[str, str]:
        return dict(batch)

    async def fetch_batch(
        self, params: Dict[str, str], stop_event=None
    ) -> Optional[pd.DataFrame]:
        raw = await self.api.fetch_performance(
            params["index_code"],
            params["start_date"],
            params["end_date"],
            stop_event=stop_event,
        )
        if raw is None or raw.empty:
            return None

        rename_map = {
            "tradeDate": "trade_date",
            "indexCode": "index_code",
            "indexNameEn": "index_name_en",
            "indexNameEnAll": "index_name_en_all",
            "changePct": "change_pct",
            "tradingVol": "trading_volume",
            "tradingValue": "trading_value",
            "consNumber": "constituent_count",
        }
        frame = raw.rename(columns=rename_map).copy()
        observed = set(
            frame.get("index_code", pd.Series(dtype=str))
            .dropna()
            .astype(str)
            .str.upper()
        )
        expected = params["index_code"].upper()
        if observed != {expected}:
            raise ValueError(
                f"中证指数代码不一致: requested={expected}, observed={sorted(observed)}"
            )
        frame["index_code"] = expected
        frame["source_url"] = CSINDEX_PERFORMANCE_URL
        return await self._filter_to_open_trade_dates(frame, params)

    async def _filter_to_open_trade_dates(
        self, frame: pd.DataFrame, params: Dict[str, str]
    ) -> pd.DataFrame:
        """Fail closed against the configured exchange calendar.

        The official chart endpoint can emit one or more synthetic rows for
        weekends and holidays.  Comparing adjacent observations is not enough:
        a multi-day closure may contain several different anchor shapes.
        """

        start_date = pd.Timestamp(params["start_date"]).date()
        end_date = pd.Timestamp(params["end_date"]).date()
        rows = await self.db.fetch(
            """
            SELECT cal_date, is_open
            FROM rawdata.others_calendar
            WHERE exchange = $1
              AND cal_date BETWEEN $2 AND $3
            """,
            self.calendar_exchange,
            start_date,
            end_date,
        )
        calendar = {}
        for row in rows or []:
            cal_date = pd.to_datetime(row["cal_date"], errors="coerce")
            if pd.isna(cal_date):
                continue
            calendar[pd.Timestamp(cal_date).date()] = int(row["is_open"] or 0)

        observed_dates = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        observed = {value for value in observed_dates if not pd.isna(value)}
        missing = sorted(observed - set(calendar))
        if missing:
            preview = ", ".join(value.isoformat() for value in missing[:5])
            raise ValueError(
                f"交易日历缺少中证返回日期: exchange={self.calendar_exchange}, "
                f"dates={preview}"
            )

        open_dates = {value for value, is_open in calendar.items() if is_open == 1}
        filtered = frame.loc[observed_dates.isin(open_dates)].copy()
        removed = len(frame) - len(filtered)
        if removed:
            self.logger.info(
                "依据 %s 交易日历剔除 %s 条中证非交易日锚点",
                self.calendar_exchange,
                removed,
            )
        return filtered

    def process_data(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        frame = data.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        numeric_columns = [
            "close",
            "change",
            "change_pct",
            "trading_volume",
            "trading_value",
            "constituent_count",
            "peg",
        ]
        for column in numeric_columns:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame = frame.dropna(subset=["index_code", "trade_date", "close"])
        if frame.empty:
            return pd.DataFrame(columns=list(self.schema_def))
        frame = frame.sort_values(["index_code", "trade_date"]).reset_index(drop=True)

        start = getattr(self, "_effective_start_date", None) or getattr(
            self, "start_date", None
        )
        end = getattr(self, "_effective_end_date", None) or getattr(
            self, "end_date", None
        )
        if start:
            frame = frame[frame["trade_date"] >= pd.Timestamp(start)]
        if end:
            frame = frame[frame["trade_date"] <= pd.Timestamp(end)]

        frame["trade_date"] = frame["trade_date"].dt.date
        if "constituent_count" in frame.columns:
            frame["constituent_count"] = (
                frame["constituent_count"].round().astype("Int64")
            )
        frame = frame.drop_duplicates(subset=self.primary_keys, keep="last")
        keep = [column for column in self.schema_def if column in frame.columns]
        return frame[keep].reset_index(drop=True)

    def supports_incremental_update(self) -> bool:
        return True


__all__ = ["CsindexIndexPerformanceTask"]
