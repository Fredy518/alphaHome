#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
FRED 数据任务基类

继承 FetcherTask，实现 FRED 特有的逻辑：
- 在 `fetch_batch` 中依次拉取声明的 FRED 序列，按 observation_date 合并为宽表
- 内置轻量数据转换（列名重命名 + 类型转换 + 日期转换）
- 支持 SMART/MANUAL 模式下的生效日期窗口过滤

子类必须定义：
- series_ids: List[str] —— FRED 序列 ID 列表（如 ["DFEDTARU", "DFEDTARL", "DFF"]）
- column_mapping: Dict[str, str] —— 序列 ID 到目标列名的映射
- schema_def / primary_keys / date_column 等 FetcherTask 标准属性

设计参照 AkShareTask / AkShareNoDateSingleBatchTask。
"""

import abc
import asyncio
from typing import Any, Dict, List, Optional

import pandas as pd

from alphahome.common.constants import UpdateTypes
from alphahome.fetchers.base.fetcher_task import FetcherTask
from .fred_api import FredAPI
from ..yahoo.yahoo_api import YahooAPI


class FredTask(FetcherTask, abc.ABC):
    """FRED 经济数据任务基类。

    子类以声明式方式定义：series_ids（FRED 序列）+ column_mapping（序列→目标列）
    + schema_def / primary_keys / date_column。基类负责拉取、合并、转换与窗口过滤。
    """

    data_source = "fred"

    # FRED 特有配置（keyless fredgraph.csv 端点）
    default_request_interval = 1.0
    default_max_retries = 3
    default_retry_delay = 5
    default_stream_batches = False
    smart_refresh_interval_days = 1

    # 必须由具体任务定义
    series_ids: Optional[List[str]] = None

    # 可选属性
    column_mapping: Optional[Dict[str, str]] = None  # 序列 ID → 目标列名

    # 可选：Yahoo v8 fallback。当某 FRED 序列不可达/无数据时，用对应 Yahoo 代码取收盘。
    # 形如 {"DTWEXBGS": "DX-Y.NYB"}。仅对单值序列有效（收盘代理）。
    yahoo_fallback: Optional[Dict[str, str]] = None

    def __init__(self, db_connection, api: Optional[FredAPI] = None, **kwargs):
        task_config = kwargs.get("task_config", {})
        if "request_interval" not in task_config:
            task_config["request_interval"] = self.default_request_interval
        if "max_retries" not in task_config:
            task_config["max_retries"] = self.default_max_retries
        if "retry_delay" not in task_config:
            task_config["retry_delay"] = self.default_retry_delay
        kwargs["task_config"] = task_config

        super().__init__(db_connection, **kwargs)

        if not self.series_ids:
            raise ValueError("FredTask 子类必须定义非空 series_ids 属性")

        self.api = api or FredAPI(
            logger=self.logger,
            request_interval=self.request_interval,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay,
        )
        # Yahoo fallback 客户端（仅当子类声明 yahoo_fallback 时实际使用）
        self.yahoo_api = YahooAPI(logger=self.logger)

    def _apply_config(self, task_config: Dict):
        """合并代码默认值和配置文件设置。"""
        super()._apply_config(task_config)

        cls = type(self)
        self.request_interval = float(
            task_config.get("request_interval", cls.default_request_interval)
        )
        self.max_retries = int(
            task_config.get("max_retries", cls.default_max_retries)
        )
        self.retry_delay = float(
            task_config.get("retry_delay", cls.default_retry_delay)
        )

    async def prepare_params(self, batch_params: Dict) -> Dict:
        """透传批处理参数（已含 start_date/end_date）。"""
        return dict(batch_params)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """单批次：服务端日期过滤优于客户端，直接透传生效窗口。"""
        batch_params: Dict[str, Any] = {}
        start = kwargs.get("start_date") or getattr(self, "_effective_start_date", None)
        end = kwargs.get("end_date") or getattr(self, "_effective_end_date", None)
        if start:
            batch_params["start_date"] = start
        if end:
            batch_params["end_date"] = end
        self.logger.info(f"任务 {self.name}: 生成 FRED 单批次参数: {batch_params}")
        return [batch_params]

    async def _fetch_one_series(
        self,
        sid: str,
        start_date: Optional[str],
        end_date: Optional[str],
        stop_event: Optional[asyncio.Event],
    ) -> Optional[pd.DataFrame]:
        """拉取单个序列：先试 FRED，失败或无数据时尝试 Yahoo fallback。

        Yahoo fallback 返回的 close 列会被重命名为 <sid>，以保持与 FRED 输出形态一致，
        便于后续合并与 column_mapping 复用。
        """
        # 1. 先试 FRED
        fred_error: Optional[Exception] = None
        try:
            df = await self.api.fetch_series(
                series_id=sid,
                start_date=start_date,
                end_date=end_date,
                stop_event=stop_event,
            )
            if df is not None and not df.empty:
                return df
            self.logger.debug(f"FRED 序列 {sid} 未返回数据")
        except Exception as e:
            fred_error = e
            self.logger.warning(
                f"FRED 序列 {sid} 获取失败（{type(e).__name__}: {e}），尝试 Yahoo fallback"
            )

        # 2. Yahoo fallback（仅当子类声明了对应映射）
        yahoo_symbol = (self.yahoo_fallback or {}).get(sid)
        if not yahoo_symbol:
            if fred_error is not None:
                raise fred_error
            return None

        self.logger.info(f"任务 {self.name}: 使用 Yahoo fallback {yahoo_symbol} 替代 FRED {sid}")
        ydf = await self.yahoo_api.fetch_close(
            symbol=yahoo_symbol,
            start_date=start_date,
            end_date=end_date,
            stop_event=stop_event,
        )
        if ydf is None or ydf.empty:
            self.logger.warning(f"Yahoo fallback {yahoo_symbol} 也未返回数据")
            return None
        # close → <sid>，对齐 FRED 形态
        return ydf.rename(columns={"close": sid})

    async def fetch_batch(
        self,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[pd.DataFrame]:
        """依次拉取声明的 FRED 序列，按 observation_date 合并为宽表。"""
        try:
            assert self.series_ids, "series_ids 必须在任务子类中定义"

            start_date = params.get("start_date")
            end_date = params.get("end_date")

            merged: Optional[pd.DataFrame] = None
            for sid in self.series_ids:
                self.logger.debug(
                    f"调用 FRED 序列 {sid}，cosd={start_date}, coed={end_date}"
                )
                df = await self._fetch_one_series(sid, start_date, end_date, stop_event)
                if df is None or df.empty:
                    continue

                # 标准列：observation_date + <series_id>
                df = df[["observation_date", sid]].copy()
                if merged is None:
                    merged = df
                else:
                    merged = merged.merge(df, on="observation_date", how="outer")

            if merged is None or merged.empty:
                return None

            # 对声明了 column_mapping 但本轮未返回数据的序列，补 NaN 列，
            # 保证输出 schema 与 schema_def 一致（避免缺列导致 COPY 失败）。
            if self.column_mapping:
                for sid, target in self.column_mapping.items():
                    if sid not in merged.columns:
                        merged[target] = pd.NA

            # 列重命名：序列 ID → 目标列名；observation_date → date_column
            rename_map: Dict[str, str] = {}
            if self.column_mapping:
                for sid, target in self.column_mapping.items():
                    if sid in merged.columns:
                        rename_map[sid] = target
            date_col = self.date_column or "date"
            rename_map["observation_date"] = date_col
            merged = merged.rename(columns=rename_map)

            return merged

        except Exception as e:
            self.logger.error(
                f"获取批次数据失败，FRED 序列: {self.series_ids}，参数: {params}，错误: {e}",
                exc_info=True,
            )
            raise

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """应用 transformations + 日期转换 + 生效窗口过滤。"""
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        date_col = getattr(self, "date_column", None) or "date"
        if date_col in data.columns:
            col_series = pd.to_datetime(data[date_col], errors="coerce")
            data = data.assign(**{date_col: col_series.dt.date})
            data = data.dropna(subset=[date_col])

        # SMART/MANUAL 模式下按生效窗口过滤，实现“只回写所需时间窗”的增量效果
        if self.update_type in (UpdateTypes.SMART, UpdateTypes.MANUAL) and date_col in data.columns:
            start = getattr(self, "_effective_start_date", None) or getattr(self, "start_date", None)
            end = getattr(self, "_effective_end_date", None) or getattr(self, "end_date", None)
            if start and end:
                start_d = pd.to_datetime(start, errors="coerce")
                end_d = pd.to_datetime(end, errors="coerce")
                if not pd.isna(start_d) and not pd.isna(end_d):
                    mask = (col_series >= start_d) & (col_series <= end_d)
                    data = data[mask].copy()

        # 去重（按 date 保留最后一条，对齐 akshare 任务行为）
        if date_col in data.columns:
            data = data.drop_duplicates(subset=[date_col], keep="last").reset_index(drop=True)

        return data

    def supports_incremental_update(self) -> bool:
        return True
