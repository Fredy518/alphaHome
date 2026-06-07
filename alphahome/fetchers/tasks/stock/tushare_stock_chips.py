#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
每日筹码及胜率 (cyq_perf) 更新任务
获取A股每日筹码平均成本和胜率情况。
数据从2018年开始。
参考文档: https://tushare.pro/document/2?doc_id=293
"""

import asyncio  # 添加 asyncio 导入
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask




class TushareStockChipsTask(TushareTask):
    """Archived Tushare stock chips implementation for historical reference.

    This task is intentionally not registered. The cyq_perf endpoint is
    per-code, costly for routine SMART updates, and has shown unstable tail
    latency in this project.
    """

    # 1. 核心属性
    archived = True
    archived_reason = "Tushare cyq_perf is too costly/unstable for daily SMART updates."
    domain = "stock"  # 业务域标识
    name = "tushare_stock_chips"
    description = "获取A股每日筹码平均成本和胜率情况"
    table_name = "stock_chips"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20180101"  # 数据从2018年开始
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 5000  # cyq_perf 文档限制单次最大5000条
    default_code_batch_size = 300
    default_max_estimated_rows_per_code_batch = 4500
    default_multi_code_fallback = True

    # 2. 自定义索引
    indexes = [
        # 主键 ("ts_code", "trade_date") 索引由基类自动处理
        {"name": "idx_stock_chips_update_time", "columns": "update_time"}
    ]

    # 3. Tushare特有属性
    api_name = "cyq_perf"
    fields = [
        "ts_code",
        "trade_date",
        "his_low",
        "his_high",
        "cost_5pct",
        "cost_15pct",
        "cost_50pct",
        "cost_85pct",
        "cost_95pct",
        "weight_avg",
        "winner_rate",
    ]

    # 4. 数据类型转换
    transformations = {
        "his_low": float,
        "his_high": float,
        "cost_5pct": float,
        "cost_15pct": float,
        "cost_50pct": float,
        "cost_85pct": float,
        "cost_95pct": float,
        "weight_avg": float,
        "winner_rate": float,
    }

    # 5. 列名映射
    column_mapping = {}

    # 6. 数据库表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "his_low": {"type": "NUMERIC(15,4)"},
        "his_high": {"type": "NUMERIC(15,4)"},
        "cost_5pct": {"type": "NUMERIC(15,4)"},
        "cost_15pct": {"type": "NUMERIC(15,4)"},
        "cost_50pct": {"type": "NUMERIC(15,4)"},
        "cost_85pct": {"type": "NUMERIC(15,4)"},
        "cost_95pct": {"type": "NUMERIC(15,4)"},
        "weight_avg": {"type": "NUMERIC(15,4)"},
        "winner_rate": {"type": "NUMERIC(10,4)"},
        # update_time 列会自动添加
    }

    # 7. 数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['trade_date'].notna(),
        lambda df: df['his_high'] >= df['his_low'],
        lambda df: df['cost_15pct'] >= df['cost_5pct'],
        lambda df: df['cost_50pct'] >= df['cost_15pct'],
        lambda df: df['cost_85pct'] >= df['cost_50pct'],
        lambda df: df['cost_95pct'] >= df['cost_85pct'],
        lambda df: df['weight_avg'] > 0,
        lambda df: df['winner_rate'].between(0, 100),
    ]

    def _apply_config(self, task_config: Dict):
        super()._apply_config(task_config)
        cls = type(self)
        self.code_batch_size = max(
            1,
            int(
                task_config.get(
                    "code_batch_size",
                    task_config.get(
                        "multi_code_batch_size",
                        cls.default_code_batch_size,
                    ),
                )
            ),
        )
        self._code_batch_size_configured = any(
            key in task_config for key in ("code_batch_size", "multi_code_batch_size")
        )
        self.max_estimated_rows_per_code_batch = max(
            1,
            int(
                task_config.get(
                    "max_estimated_rows_per_code_batch",
                    cls.default_max_estimated_rows_per_code_batch,
                )
            ),
        )
        self.multi_code_fallback = self._parse_bool(
            task_config.get("multi_code_fallback", cls.default_multi_code_fallback),
            cls.default_multi_code_fallback,
        )

    @staticmethod
    def _get_row_value(row: Any, column: str) -> Any:
        try:
            return row[column]
        except (KeyError, TypeError):
            return row[0]

    @staticmethod
    def _dedupe_codes(codes: List[str]) -> List[str]:
        seen = set()
        result = []
        for code in codes:
            normalized = str(code).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    async def _load_listed_stock_codes(self) -> List[str]:
        """Load active A-share codes for cyq_perf batching."""
        if self.db:
            try:
                query = (
                    "SELECT ts_code FROM tushare.stock_basic "
                    "WHERE list_status = 'L' "
                    "ORDER BY ts_code"
                )
                self.logger.info(f"从数据库获取上市股票代码: {query}")
                rows = await self.db.fetch(query)
                codes = self._dedupe_codes(
                    [self._get_row_value(row, "ts_code") for row in rows or []]
                )
                if codes:
                    self.logger.info(f"从数据库获取到 {len(codes)} 个上市股票代码")
                    return codes
            except Exception as exc:
                self.logger.warning(f"从数据库获取上市股票代码失败，将尝试API兜底: {exc}")

        if self.api:
            self.logger.info("尝试通过 Tushare stock_basic 获取上市股票代码列表")
            df = await self.api.query(
                api_name="stock_basic",
                fields=["ts_code"],
                list_status="L",
            )
            if df is not None and not df.empty and "ts_code" in df.columns:
                codes = self._dedupe_codes(df["ts_code"].tolist())
                self.logger.info(f"通过API获取到 {len(codes)} 个上市股票代码")
                return codes

        self.logger.warning("无法获取上市股票代码列表，任务将不生成批次")
        return []

    def _build_code_batches(
        self,
        codes: List[str],
        additional_params: Dict[str, Any],
        batch_size: int,
    ) -> List[Dict[str, Any]]:
        batches: List[Dict[str, Any]] = []
        for start in range(0, len(codes), batch_size):
            code_batch = codes[start : start + batch_size]
            batches.append(
                {
                    **additional_params,
                    "ts_code": ",".join(code_batch),
                }
            )
        return batches

    def _resolve_code_batch_size(self, start_date: str, end_date: str) -> int:
        if self._code_batch_size_configured:
            return self.code_batch_size

        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d").date()
            end_dt = datetime.strptime(end_date, "%Y%m%d").date()
            calendar_days = max(1, (end_dt - start_dt).days + 1)
        except (TypeError, ValueError):
            calendar_days = 1

        estimated_trading_days = max(1, (calendar_days * 5 + 6) // 7)
        max_by_rows = max(
            1,
            self.max_estimated_rows_per_code_batch // estimated_trading_days,
        )
        return max(1, min(self.code_batch_size, max_by_rows))

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """使用 BatchPlanner 生成批处理参数列表

        cyq_perf 接口要求传入 ts_code，因此按股票代码生成批次，并为每个
        批次附带统一的 start_date/end_date 日期窗口。全市场场景默认按多个
        ts_code 合并成一个批次；如果 Tushare 返回参数错误，fetch_batch 会
        对该批次降级为逐代码查询。

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date_overall = kwargs.get("start_date")
        end_date_overall = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的股票代码

        # 确定总体起止日期
        if not start_date_overall:
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date_overall = latest_db_date + pd.Timedelta(days=1)
                start_date_overall = start_date_overall.strftime("%Y%m%d")
            else:
                start_date_overall = self.default_start_date
            self.logger.info(
                f"任务 {self.name}: 未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date_overall}"
            )

        if not end_date_overall:
            end_date_overall = datetime.now().strftime("%Y%m%d")
            self.logger.info(
                f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date_overall}"
            )

        if pd.to_datetime(start_date_overall) > pd.to_datetime(end_date_overall):
            self.logger.info(
                f"任务 {self.name}: 起始日期 ({start_date_overall}) 晚于结束日期 ({end_date_overall})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 使用 BatchPlanner 生成批处理列表，范围: {start_date_overall} 到 {end_date_overall}, 股票代码: {ts_code if ts_code else '所有'}"
        )

        try:
            additional_params = {
                "fields": ",".join(self.fields or []),
                "start_date": start_date_overall,
                "end_date": end_date_overall,
            }

            if ts_code:
                codes = self._dedupe_codes(str(ts_code).split(","))
                batch_size = self._resolve_code_batch_size(
                    start_date_overall,
                    end_date_overall,
                )
                batch_list = self._build_code_batches(codes, additional_params, batch_size)
            else:
                codes = await self._load_listed_stock_codes()
                batch_size = self._resolve_code_batch_size(
                    start_date_overall,
                    end_date_overall,
                )
                batch_list = self._build_code_batches(codes, additional_params, batch_size)
                self.logger.info(
                    "任务 %s: 生成 %s 个代码批次（%s 个上市股票代码, 每批最多 %s 个）",
                    self.name,
                    len(batch_list),
                    len(codes),
                    batch_size,
                )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            return []

    def _should_fallback_multi_code_query(
        self,
        params: Dict[str, Any],
        exc: Exception,
    ) -> bool:
        if not self.multi_code_fallback:
            return False
        ts_code = str(params.get("ts_code") or "")
        if "," not in ts_code:
            return False
        message = str(exc)
        return "50101" in message and ("查询数据失败" in message or "确认参数" in message)

    async def fetch_batch(
        self,
        params: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None,
    ) -> Optional[pd.DataFrame]:
        try:
            return await super().fetch_batch(params, stop_event=stop_event)
        except ValueError as exc:
            if not self._should_fallback_multi_code_query(params, exc):
                raise

            codes = self._dedupe_codes(str(params.get("ts_code") or "").split(","))
            self.logger.warning(
                "%s 多代码批次被 Tushare 拒绝，将降级为逐代码查询: %s 个代码",
                self.name,
                len(codes),
            )

            frames: List[pd.DataFrame] = []
            for code in codes:
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError
                single_params = params.copy()
                single_params["ts_code"] = code
                frame = await super().fetch_batch(single_params, stop_event=stop_event)
                if frame is not None and not frame.empty:
                    frames.append(frame)

            if not frames:
                return None
            return pd.concat(frames, ignore_index=True, sort=False)

    async def pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行前的准备工作"""
        await super().pre_execute(stop_event=stop_event, **kwargs)
        # 可以在这里添加特定于此任务的预处理逻辑

    async def post_execute(
        self,
        result: Dict[str, Any],
        stop_event: Optional[asyncio.Event] = None,
        **kwargs,
    ):
        """任务执行后的清理工作"""
        await super().post_execute(result, stop_event=stop_event, **kwargs)
        # 可以在这里添加特定于此任务的后处理逻辑


# 导出任务类
__all__ = ["TushareStockChipsTask"]
