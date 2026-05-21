#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基于 Tinysoft pyTSL 的数据任务基类

核心设计：
1. 继承 FetcherTask，复用通用批处理、重试和保存能力
2. 统一封装 Tinysoft 查询参数与错误处理
3. 将 get_batch_list 保持为抽象方法，由具体任务定义分批策略
"""

import abc
import asyncio
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from alphahome.common.config_manager import get_tinysoft_config
from alphahome.fetchers.base.fetcher_task import FetcherTask
from .tinysoft_api import TinySoftAPI
from .tinysoft_opi_api import TinySoftOPIAPI


class TinySoftTask(FetcherTask, abc.ABC):
    """Tinysoft 数据任务基类。"""

    data_source = "tinysoft"

    # Tinysoft 特有配置默认值
    default_query_timeout_ms = 30_000
    default_request_interval = 0.2
    default_cycle = "1分钟线"
    default_service = ""
    default_stream_batches = True

    # 可选属性（由子类定义）
    fields: Optional[List[Any]] = None

    def __init__(
        self,
        db_connection,
        tinysoft_config: Optional[Dict[str, Any]] = None,
        api: Optional[TinySoftAPI] = None,
        **kwargs,
    ):
        task_config = kwargs.get("task_config", {})
        if "query_timeout_ms" not in task_config:
            task_config["query_timeout_ms"] = self.default_query_timeout_ms
        if "request_interval" not in task_config:
            task_config["request_interval"] = self.default_request_interval
        if "cycle" not in task_config:
            task_config["cycle"] = self.default_cycle
        if "service" not in task_config:
            task_config["service"] = self.default_service

        kwargs["task_config"] = task_config
        super().__init__(db_connection, **kwargs)

        self._failed_symbols: List[Dict[str, str]] = []

        self.tinysoft_config = (tinysoft_config or get_tinysoft_config() or {}).copy()
        self.api = api or self._build_api_from_config()

    def _build_api_from_config(self):
        mode = str(
            self.tinysoft_config.get(
                "mode",
                self.tinysoft_config.get(
                    "backend",
                    self.tinysoft_config.get("api_mode", "pytsl"),
                ),
            )
        ).strip().lower()

        timeout_ms = self._coerce_int(
            self.tinysoft_config.get("timeout_ms"),
            self.query_timeout_ms,
        )
        request_interval = self._coerce_float(
            self.tinysoft_config.get("request_interval"),
            self.request_interval,
        )
        service = str(self.tinysoft_config.get("service") or self.service or "")

        if mode in {"opi", "ts-opi", "http", "https"}:
            return TinySoftOPIAPI(
                user=self.tinysoft_config.get("user"),
                password=self.tinysoft_config.get("password"),
                opi_url=self.tinysoft_config.get("opi_url")
                or self.tinysoft_config.get("base_url"),
                auth_mode=str(self.tinysoft_config.get("opi_auth_mode") or "basic"),
                session_key=self.tinysoft_config.get("session_key")
                or self.tinysoft_config.get("opi_session_key")
                or self.tinysoft_config.get("api_key"),
                session_password=self.tinysoft_config.get("session_password")
                or self.tinysoft_config.get("opi_session_password"),
                service=service,
                event_name=self.tinysoft_config.get("event_name"),
                json_encode=str(self.tinysoft_config.get("json_encode") or "utf8"),
                run_func_name=self.tinysoft_config.get("run_func_name"),
                query_func_name=self.tinysoft_config.get("query_func_name"),
                timeout_ms=timeout_ms,
                request_interval=request_interval,
                logger=self.logger,
            )

        return TinySoftAPI(
            user=self.tinysoft_config.get("user"),
            password=self.tinysoft_config.get("password"),
            host=self.tinysoft_config.get("host", TinySoftAPI.DEFAULT_HOST),
            port=self._coerce_int(self.tinysoft_config.get("port"), TinySoftAPI.DEFAULT_PORT),
            ini_path=self.tinysoft_config.get("ini_path"),
            service=service,
            timeout_ms=timeout_ms,
            request_interval=request_interval,
            logger=self.logger,
        )

    @staticmethod
    def _coerce_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _coerce_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

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
    def _parse_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return pd.Timestamp(ts).date()

    def _apply_config(self, task_config: Dict):
        super()._apply_config(task_config)

        cls = type(self)
        self.query_timeout_ms = self._coerce_int(
            task_config.get("query_timeout_ms", cls.default_query_timeout_ms),
            cls.default_query_timeout_ms,
        )
        self.request_interval = self._coerce_float(
            task_config.get("request_interval", cls.default_request_interval),
            cls.default_request_interval,
        )
        self.cycle = str(task_config.get("cycle", cls.default_cycle))
        self.service = str(task_config.get("service", cls.default_service))

    async def _pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        await super()._pre_execute(stop_event=stop_event, **kwargs)
        self._failed_symbols = []

    def _record_skipped_symbol(self, ts_code: str, error: Exception) -> None:
        self._failed_symbols.append(
            {
                "ts_code": str(ts_code),
                "error": str(error),
            }
        )

    @staticmethod
    def _has_symbol_identifier(df: pd.DataFrame) -> bool:
        columns = {str(col).lower() for col in df.columns}
        return bool({"stockid", "ts_code", "tsl_code"} & columns) or "证券代码" in set(df.columns)

    async def fetch_infotable_for_symbol_pairs(
        self,
        *,
        table_id: int,
        symbol_pairs: List[Dict[str, Any]],
        where_clause: Optional[str] = None,
        skip_failed_symbols: bool = True,
        service: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        stop_event: Optional[asyncio.Event] = None,
        enable_batch: bool = True,
        error_label: str = "Tinysoft",
    ) -> Optional[pd.DataFrame]:
        """Fetch InfoTable rows for a symbol batch, preferring one multi-stock call."""
        valid_pairs: List[Dict[str, str]] = []
        for pair in symbol_pairs:
            if not isinstance(pair, dict):
                continue
            ts_code = str(pair.get("ts_code") or "").strip()
            stock = str(pair.get("stock") or "").strip()
            if not ts_code or not stock:
                continue
            valid_pairs.append({"ts_code": ts_code, "stock": stock})

        if not valid_pairs:
            return None

        stocks = [pair["stock"] for pair in valid_pairs]
        batch_method = getattr(self.api, "call_dataframe_for_stocks", None)
        if enable_batch and len(stocks) > 1 and callable(batch_method):
            try:
                batch_df = await batch_method(
                    "infoarray",
                    table_id,
                    stocks=stocks,
                    where_clause=where_clause,
                    service=service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
                if batch_df is None or batch_df.empty:
                    return None
                if self._has_symbol_identifier(batch_df):
                    return batch_df.copy()
                self.logger.warning(
                    "%s 批量拉取返回缺少 StockID/证券代码/ts_code，回退逐标的查询。",
                    error_label,
                )
            except Exception as e:
                self.logger.warning("%s 批量拉取失败，将回退逐标的查询: %s", error_label, e)

        merged_frames: List[pd.DataFrame] = []
        for pair in valid_pairs:
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError(f"{error_label} 批次拉取被取消")

            ts_code = pair["ts_code"]
            stock = pair["stock"]

            try:
                df = await self.api.call_dataframe(
                    "infoarray",
                    table_id,
                    stock=stock,
                    where_clause=where_clause,
                    service=service,
                    timeout_ms=timeout_ms,
                    stop_event=stop_event,
                )
            except Exception as e:
                if not skip_failed_symbols:
                    raise
                self._record_skipped_symbol(ts_code, e)
                self.logger.warning("%s 拉取失败（跳过）: %s, 错误: %s", error_label, ts_code, e)
                continue

            if df is None or df.empty:
                continue

            one = df.copy()
            if "StockID" not in one.columns and "stockid" not in {str(c).lower() for c in one.columns}:
                one["StockID"] = stock
            merged_frames.append(one)

        if not merged_frames:
            return None

        return pd.concat(merged_frames, ignore_index=True)

    async def _post_execute(self, result, stop_event: Optional[asyncio.Event] = None, **kwargs):
        await super()._post_execute(result, stop_event=stop_event, **kwargs)
        if not self._failed_symbols:
            return

        failed_count = len(self._failed_symbols)
        failed_sample = self._failed_symbols[:10]
        result["skipped_symbol_count"] = failed_count
        result["skipped_symbols"] = failed_sample
        result["skipped_symbol_warning"] = f"Tinysoft 拉取过程中跳过了 {failed_count} 个失败标的"

        if result.get("status") == "success":
            result["status"] = "partial_success"

    @staticmethod
    def _normalize_cycle(cycle: Optional[str]) -> str:
        if not cycle:
            return "1分钟线"

        raw = str(cycle).strip()
        key = raw.lower()

        cycle_map = {
            "1m": "1分钟线",
            "1min": "1分钟线",
            "1分钟": "1分钟线",
            "5m": "5分钟线",
            "5min": "5分钟线",
            "5分钟": "5分钟线",
            "15m": "15分钟线",
            "15min": "15分钟线",
            "15分钟": "15分钟线",
            "30m": "30分钟线",
            "30min": "30分钟线",
            "30分钟": "30分钟线",
            "60m": "60分钟线",
            "60min": "60分钟线",
            "60分钟": "60分钟线",
            "d": "日线",
            "day": "日线",
            "daily": "日线",
            "日线": "日线",
        }
        return cycle_map.get(key, raw)

    async def prepare_params(self, batch_params: Dict) -> Dict:
        return batch_params.copy()

    async def fetch_batch(
        self,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[pd.DataFrame]:
        stock = params.get("stock")
        begin_time = params.get("begin_time")
        end_time = params.get("end_time")
        if not stock or not begin_time or not end_time:
            raise ValueError(
                f"Tinysoft 批次参数缺失，必须包含 stock/begin_time/end_time，当前参数: {params}"
            )

        cycle = self._normalize_cycle(params.get("cycle", self.cycle))
        fields: Optional[Iterable[Any]] = params.get("fields", self.fields)
        service = params.get("service", self.service)
        timeout_ms = self._coerce_int(params.get("timeout_ms"), self.query_timeout_ms)
        rate = self._coerce_int(params.get("rate"), 0)
        rateday = params.get("rateday")
        precision = params.get("precision")
        viewpoint = params.get("viewpoint")
        cyclefilter = params.get("cyclefilter")

        data = await self.api.query(
            stock=str(stock),
            cycle=cycle,
            begin_time=begin_time,
            end_time=end_time,
            fields=fields,
            rate=rate,
            rateday=rateday,
            precision=precision,
            viewpoint=viewpoint,
            cyclefilter=cyclefilter,
            service=service,
            timeout_ms=timeout_ms,
            stop_event=stop_event,
        )

        if data is None or data.empty:
            self.logger.debug("Tinysoft 批次无数据: %s", params)
            return None

        return data

    @abc.abstractmethod
    async def get_batch_list(self, **kwargs) -> List[Dict]:
        raise NotImplementedError("每个 Tinysoft 任务子类必须实现 get_batch_list 方法")

    def supports_incremental_update(self) -> bool:
        return True

    def get_incremental_skip_reason(self) -> str:
        return ""

