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
from ....common.task_system.task_decorator import task_register




@task_register()
class TushareStockChipsTask(TushareTask):
    """获取A股每日筹码平均成本和胜率情况"""

    # 1. 核心属性
    name = "tushare_stock_chips"
    description = "获取A股每日筹码平均成本和胜率情况"
    table_name = "stock_chips"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20180101"  # 数据从2018年开始
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 5000  # API限制单次最大5000条

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
        "his_low": {"type": "FLOAT"},
        "his_high": {"type": "FLOAT"},
        "cost_5pct": {"type": "FLOAT"},
        "cost_15pct": {"type": "FLOAT"},
        "cost_50pct": {"type": "FLOAT"},
        "cost_85pct": {"type": "FLOAT"},
        "cost_95pct": {"type": "FLOAT"},
        "weight_avg": {"type": "FLOAT"},
        "winner_rate": {"type": "FLOAT"},
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

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """使用 BatchPlanner 生成批处理参数列表

        为每个交易日生成单独的批次，使用 trade_date 参数。

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date_overall = kwargs.get("start_date")
        end_date_overall = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的股票代码
        exchange = kwargs.get("exchange", "SSE")  # 传递 exchange 给日历工具

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
            # 使用标准的单日期批次生成工具
            from ...sources.tushare.batch_utils import generate_single_date_batches

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_single_date_batches(
                start_date=start_date_overall,
                end_date=end_date_overall,
                date_field="trade_date",  # 指定日期字段名
                ts_code=ts_code,
                exchange=exchange,
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            return []

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
