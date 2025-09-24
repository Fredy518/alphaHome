#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 融资融券交易明细数据任务

接口文档: https://tushare.pro/document/2?doc_id=59
数据说明:
- 获取融资融券每日交易明细数据
- 包含各股票的融资融券交易详情
- 支持多种批处理策略:
  1. 历史全量模式: 按月份范围分批获取
  2. 手动增量模式: 按月份范围分批获取
  3. 智能增量模式: 按交易日分批获取

权限要求: 需要至少2000积分，单次请求最大返回6000行数据
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import (
    generate_month_range_batches,
    generate_single_date_batches
)
from ....common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class TushareStockMarginDetailTask(TushareTask):
    """获取融资融券交易明细数据 (margin_detail)

    实现要求:
    - 历史全量模式: 按月份范围分批获取
    - 手动增量模式: 按月份范围分批获取
    - 智能增量模式: 按交易日分批获取
    """

    # 1. 核心属性
    name = "tushare_stock_margindetail"
    description = "获取融资融券每日交易明细数据"
    table_name = "stock_margindetail"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 交易日期
    default_start_date = "20100301"  # 默认开始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 降低并发，避免频率限制
    default_page_size = 6000  # API限制单次最大6000条

    # 2. TushareTask 特有属性
    api_name = "margin_detail"
    # Tushare margin_detail 接口返回的字段
    fields = [
        "trade_date",     # 交易日期
        "ts_code",        # TS股票代码
        "rzye",           # 融资余额(元)
        "rqye",           # 融券余额(元)
        "rzmre",          # 融资买入额(元)
        "rqyl",           # 融券余量(股)
        "rzche",          # 融资偿还额(元)
        "rqchl",          # 融券偿还量(股)
        "rqmcl",          # 融券卖出量(股)
        "rzrqye",         # 融资融券余额(元)
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "rzye": float,
        "rqye": float,
        "rzmre": float,
        "rqyl": float,
        "rzche": float,
        "rqchl": float,
        "rqmcl": float,
        "rzrqye": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "rzye": {"type": "NUMERIC(20,2)"},        # 融资余额(元)
        "rqye": {"type": "NUMERIC(20,2)"},        # 融券余额(元)
        "rzmre": {"type": "NUMERIC(20,2)"},       # 融资买入额(元)
        "rqyl": {"type": "NUMERIC(20,2)"},        # 融券余量(股)
        "rzche": {"type": "NUMERIC(20,2)"},       # 融资偿还额(元)
        "rqchl": {"type": "NUMERIC(20,2)"},       # 融券偿还量(股)
        "rqmcl": {"type": "NUMERIC(20,2)"},       # 融券卖出量(股)
        "rzrqye": {"type": "NUMERIC(20,2)"},      # 融资融券余额(元)
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_margindetail_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_margindetail_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_margindetail_rzrqye", "columns": "rzrqye"},  # 融资融券余额索引
        {"name": "idx_stock_margindetail_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['rzye'] >= 0, "融资余额必须非负"),
        (lambda df: df['rqye'] >= 0, "融券余额必须非负"),
        (lambda df: df['rzrqye'] >= 0, "融资融券余额必须非负"),
        (lambda df: df['rqyl'] >= 0, "融券余量必须非负"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        1. 历史全量模式: 按月份范围分批获取
        2. 手动增量模式: 按月份范围分批获取
        3. 智能增量模式: 按交易日分批获取

        Args:
            **kwargs: 包含start_date, end_date, update_type等参数

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        update_type = kwargs.get("update_type", "manual")  # 默认为手动模式

        # 判断是否为全量模式（基于日期范围是否覆盖默认起始日期到当前日期）
        current_date = datetime.now().strftime("%Y%m%d")
        is_full_mode = (start_date == self.default_start_date and end_date == current_date)

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 - is_full_mode: {is_full_mode}, update_type: {update_type}, start_date: {start_date}, end_date: {end_date}"
        )

        try:
            if is_full_mode:
                # 策略1: 历史全量模式 - 按月份范围分批获取
                self.logger.info(f"历史全量模式：按月份范围分批获取融资融券明细数据")
                return await generate_month_range_batches(
                    start_date=start_date,
                    end_date=end_date,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])}
                )
            else:
                # 增量模式处理
                if update_type == "smart":
                    # 智能增量模式：按交易日分批获取
                    latest_db_date = await self.get_latest_date()
                    if latest_db_date:
                        # 计算回看日期：最新日期 - 回看天数
                        lookback_date = latest_db_date - timedelta(days=self.smart_lookback_days)
                        start_date = lookback_date.strftime("%Y%m%d")
                        end_date = latest_db_date.strftime("%Y%m%d")
                        self.logger.info(
                            f"智能增量模式：使用数据库最新日期({latest_db_date.strftime('%Y%m%d')}) - 回看{self.smart_lookback_days}天的范围: {start_date} 到 {end_date}"
                        )
                    else:
                        # 如果没有最新日期，使用默认起始日期到当前日期
                        start_date = self.default_start_date
                        end_date = current_date
                        self.logger.info(
                            f"智能增量模式：未找到数据库最新日期，使用默认范围: {start_date} 到 {end_date}"
                        )

                    self.logger.info(f"智能增量模式：按交易日分批获取融资融券明细数据")
                    return await generate_single_date_batches(
                        start_date=start_date,
                        end_date=end_date,
                        date_field="trade_date",
                        logger=self.logger,
                        exchange=kwargs.get("exchange", "SSE"),
                        additional_params={"fields": ",".join(self.fields or [])}
                    )
                else:
                    # 手动增量模式：按月份范围分批获取
                    if not start_date or not end_date:
                        self.logger.error("手动增量模式需要提供 start_date 和 end_date")
                        return []

                    self.logger.info(
                        f"手动增量模式：使用指定的日期范围 {start_date} 到 {end_date}，按月份分批"
                    )
                    return await generate_month_range_batches(
                        start_date=start_date,
                        end_date=end_date,
                        logger=self.logger,
                        additional_params={"fields": ",".join(self.fields or [])}
                    )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: BatchPlanner 生成批次时出错: {e}", exc_info=True)
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
__all__ = ["TushareStockMarginDetailTask"]
