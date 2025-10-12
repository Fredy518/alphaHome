#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 期货主力与连续合约映射数据任务

接口文档: https://tushare.pro/document/2?doc_id=189
数据说明:
- 获取期货主力（或连续）合约与月合约映射数据
- 支持按日期范围查询合约映射关系
- 支持多种批处理策略:
  1. 历史全量模式: 按月份范围分批获取
  2. 手动增量模式: 按月份范围分批获取
  3. 智能增量模式: 直接单一批次获取指定日期范围

权限要求: 需要至少2000积分，单次请求最大返回2000行数据
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_month_range_batches
from ....common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class TushareFutureMappingTask(TushareTask):
    """获取期货主力与连续合约映射数据 (fut_mapping)

    实现要求:
    - 历史全量模式: 按月份范围分批获取
    - 手动增量模式: 按月份范围分批获取
    - 智能增量模式: 直接单一批次使用start_date和end_date更新
    """

    # 1. 核心属性
    name = "tushare_future_mapping"
    description = "获取期货主力与连续合约映射数据"
    table_name = "future_mapping"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 起始日期
    default_start_date = "20010101"  # 默认开始日期
    data_source = "tushare"
    domain = "future"  # 业务域标识
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 3  # 降低并发，避免频率限制
    default_page_size = 2000  # API限制单次最大2000条

    # 2. TushareTask 特有属性
    api_name = "fut_mapping"
    # Tushare fut_mapping 接口返回的字段
    fields = [
        "ts_code",          # 连续合约代码
        "trade_date",       # 起始日期
        "mapping_ts_code",  # 期货合约代码
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {}

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "mapping_ts_code": {"type": "VARCHAR(20)"},  # 映射的期货合约代码
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_future_mapping_ts_code", "columns": "ts_code"},
        {"name": "idx_future_mapping_trade_date", "columns": "trade_date"},
        {"name": "idx_future_mapping_mapping_ts_code", "columns": "mapping_ts_code"},
        {"name": "idx_future_mapping_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "连续合约代码不能为空"),
        (lambda df: df['trade_date'].notna(), "起始日期不能为空"),
        (lambda df: df['mapping_ts_code'].notna(), "映射合约代码不能为空"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        1. 历史全量模式: 按月份范围分批获取
        2. 手动增量模式: 按月份范围分批获取
        3. 智能增量模式: 直接单一批次使用start_date和end_date更新

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
                self.logger.info(f"历史全量模式：按月份范围分批获取期货映射数据")
                return await generate_month_range_batches(
                    start_date=start_date,
                    end_date=end_date,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])}
                )
            else:
                # 增量模式处理
                if update_type == "smart":
                    # 智能增量模式：从上次更新日期前3天开始获取，确保覆盖可能遗漏的数据
                    latest_db_date = await self.get_latest_date()
                    if latest_db_date:
                        # 从数据库最新日期前3天开始获取，确保覆盖可能遗漏的数据
                        start_date = (latest_db_date - pd.Timedelta(days=self.smart_lookback_days)).strftime("%Y%m%d")
                        end_date = current_date
                        self.logger.info(
                            f"智能增量模式：从数据库最新日期({latest_db_date.strftime('%Y%m%d')})前{self.smart_lookback_days}天开始获取: {start_date} 到 {end_date}"
                        )
                    else:
                        # 如果没有最新日期，使用默认起始日期到当前日期
                        start_date = self.default_start_date
                        end_date = current_date
                        self.logger.info(
                            f"智能增量模式：未找到数据库最新日期，使用默认范围: {start_date} 到 {end_date}"
                        )

                    self.logger.info(f"智能增量模式：直接单一批次获取期货映射数据")
                    return [{
                        "start_date": start_date,
                        "end_date": end_date,
                        "fields": ",".join(self.fields or [])
                    }]
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
__all__ = ["TushareFutureMappingTask"]
