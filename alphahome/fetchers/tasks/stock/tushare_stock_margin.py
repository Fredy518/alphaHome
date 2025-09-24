#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 融资融券交易汇总数据任务

接口文档: https://tushare.pro/document/2?doc_id=58
数据说明:
- 获取融资融券每日交易汇总数据
- 包含融资余额、融券余额、融资融券余额等信息
- 支持三种批处理策略:
  1. 全量模式: 不分批，直接获取所有历史数据
  2. 手动增量模式: 直接按照给定的start_date和end_date返回一个批次
  3. 智能增量模式: 使用数据库最新日期-回看日期的范围返回一个批次

权限要求: 需要至少2000积分，单次请求最大返回4000行数据
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio

from ...sources.tushare.tushare_task import TushareTask
# 不使用batch_utils中的分批函数，全量和增量都直接返回单个批次
from ....common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class TushareStockMarginTask(TushareTask):
    """获取融资融券交易汇总数据 (margin)

    实现要求:
    - 全量更新: 不分批，直接获取所有历史数据
    - 手动增量模式: 直接按照给定的start_date和end_date返回一个批次
    - 智能增量模式: 使用数据库最新日期-回看日期的范围返回一个批次
    """

    # 1. 核心属性
    name = "tushare_stock_margin"
    description = "获取融资融券每日交易汇总数据"
    table_name = "stock_margin"
    primary_keys = ["trade_date", "exchange_id"]
    date_column = "trade_date"  # 交易日期
    default_start_date = "20050101"  # 默认开始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 3  # 降低并发，避免频率限制
    default_page_size = 4000  # API限制单次最大4000条

    # 2. TushareTask 特有属性
    api_name = "margin"
    # Tushare margin 接口返回的字段
    fields = [
        "trade_date",     # 交易日期
        "exchange_id",    # 交易所代码
        "rzye",           # 融资余额(元)
        "rzmre",          # 融资买入额(元)
        "rzche",          # 融资偿还额(元)
        "rqye",           # 融券余额(元)
        "rqmcl",          # 融券卖出量(股,份,手)
        "rzrqye",         # 融资融券余额(元)
        "rqyl",           # 融券余量(股,份,手)
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "rzye": float,
        "rzmre": float,
        "rzche": float,
        "rqye": float,
        "rqmcl": float,
        "rzrqye": float,
        "rqyl": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "exchange_id": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "rzye": {"type": "NUMERIC(20,2)"},        # 融资余额(元)
        "rzmre": {"type": "NUMERIC(20,2)"},      # 融资买入额(元)
        "rzche": {"type": "NUMERIC(20,2)"},      # 融资偿还额(元)
        "rqye": {"type": "NUMERIC(20,2)"},       # 融券余额(元)
        "rqmcl": {"type": "NUMERIC(20,2)"},      # 融券卖出量(股,份,手)
        "rzrqye": {"type": "NUMERIC(20,2)"},     # 融资融券余额(元)
        "rqyl": {"type": "NUMERIC(20,2)"},       # 融券余量(股,份,手)
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_margin_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_margin_exchange_id", "columns": "exchange_id"},
        {"name": "idx_stock_margin_rzrqye", "columns": "rzrqye"},  # 融资融券余额索引
        {"name": "idx_stock_margin_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['exchange_id'].notna(), "交易所代码不能为空"),
        (lambda df: df['exchange_id'].isin(['SSE', 'SZSE', 'BSE']), "交易所代码必须为SSE/SZSE/BSE"),
        (lambda df: df['rzye'] >= 0, "融资余额必须非负"),
        (lambda df: df['rqye'] >= 0, "融券余额必须非负"),
        (lambda df: df['rzrqye'] >= 0, "融资融券余额必须非负"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        1. 全量模式: 不分批，直接获取所有历史数据
        2. 手动增量模式: 直接按照给定的start_date和end_date返回一个批次
        3. 智能增量模式: 使用数据库最新日期-回看日期的范围返回一个批次

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
                # 策略1: 全量模式 - 不分批，直接获取所有历史数据
                self.logger.info(f"全量模式：直接获取所有融资融券历史数据")
                return [{
                    "fields": ",".join(self.fields or []),
                    # 全量模式下不传递任何过滤参数，获取所有历史数据
                }]
            else:
                # 增量模式处理
                if update_type == "smart":
                    # 智能增量模式：使用数据库最新日期 - 回看日期的范围
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
                else:
                    # 手动增量模式：直接使用给定的日期范围
                    if not start_date or not end_date:
                        self.logger.error("手动增量模式需要提供 start_date 和 end_date")
                        return []

                    self.logger.info(
                        f"手动增量模式：使用指定的日期范围: {start_date} 到 {end_date}"
                    )

                # 验证日期范围
                if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
                    self.logger.info(
                        f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
                    )
                    return []

                # 返回单个批次（不分批）
                return [{
                    "start_date": start_date,
                    "end_date": end_date,
                    "fields": ",".join(self.fields or [])
                }]

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
__all__ = ["TushareStockMarginTask"]
