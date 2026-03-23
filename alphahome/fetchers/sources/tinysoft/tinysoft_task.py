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
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from alphahome.common.config_manager import get_tinysoft_config
from alphahome.fetchers.base.fetcher_task import FetcherTask
from .tinysoft_api import TinySoftAPI


class TinySoftTask(FetcherTask, abc.ABC):
    """Tinysoft 数据任务基类。"""

    data_source = "tinysoft"

    # Tinysoft 特有配置默认值
    default_query_timeout_ms = 30_000
    default_request_interval = 0.2
    default_cycle = "1分钟线"
    default_service = ""

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

        self.tinysoft_config = (tinysoft_config or get_tinysoft_config() or {}).copy()
        self.api = api or TinySoftAPI(
            user=self.tinysoft_config.get("user"),
            password=self.tinysoft_config.get("password"),
            host=self.tinysoft_config.get("host", TinySoftAPI.DEFAULT_HOST),
            port=self._coerce_int(self.tinysoft_config.get("port"), TinySoftAPI.DEFAULT_PORT),
            ini_path=self.tinysoft_config.get("ini_path"),
            service=str(self.tinysoft_config.get("service") or self.service or ""),
            timeout_ms=self._coerce_int(
                self.tinysoft_config.get("timeout_ms"),
                self.query_timeout_ms,
            ),
            request_interval=self._coerce_float(
                self.tinysoft_config.get("request_interval"),
                self.request_interval,
            ),
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

