#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 股票分红送股数据任务

接口文档: https://tushare.pro/document/2?doc_id=103
数据说明:
- 获取股票分红送股数据
- 支持两种批处理策略:
  1. 全量模式: 按股票代码分批获取，过滤 div_proc='实施'
  2. 增量模式: 按实施公告日期(imp_ann_date)分批获取

权限要求: 需要至少2000积分
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio  # 添加 asyncio 导入

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import (
    generate_stock_code_batches,
    generate_trade_day_batches
)
from ....common.task_system.task_decorator import task_register

# BatchPlanner 导入
from ....common.planning.batch_planner import BatchPlanner, Source, Partition, Map

logger = logging.getLogger(__name__)

@task_register()
class TushareStockDividendTask(TushareTask):
    """获取股票分红送股数据 (dividend)

    实现要求:
    - 全量更新: 使用ts_code作为batch单位，批量获取全部数据（div_proc='实施'）
    - 增量模式: 使用ex_date字段进行更新，使用交易日历作为数据源
    """

    # 1. 核心属性
    name = "tushare_stock_dividend"
    description = "获取股票分红送股数据"
    table_name = "tushare_stock_dividend"
    primary_keys = ["ts_code", "ex_date"]
    date_column = "ex_date"  # 除权除息日
    default_start_date = "20050101"  # 默认开始日期
    data_source = "tushare"
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 降低并发，避免频率限制
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "dividend"
    # Tushare dividend 接口返回的字段
    fields = [
        "ts_code",  # TS代码
        "end_date",  # 分红年度
        "ann_date",  # 预案公告日
        "div_proc",  # 实施进度
        "stk_div",  # 每股送转
        "stk_bo_rate",  # 每股送股比例
        "stk_co_rate",  # 每股转增比例
        "cash_div",  # 每股分红（税后）
        "cash_div_tax",  # 每股分红（税前）
        "record_date",  # 股权登记日
        "ex_date",  # 除权除息日
        "pay_date",  # 派息日
        "div_listdate",  # 红股上市日
        "imp_ann_date",  # 实施公告日
        "base_date",  # 基准日
        "base_share",  # 基准股本（万）
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "stk_div": float,
        "stk_bo_rate": float,
        "stk_co_rate": float,
        "cash_div": float,
        "cash_div_tax": float,
        "base_share": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "div_proc": {"type": "VARCHAR(10)"},
        "stk_div": {"type": "NUMERIC(10,4)"},
        "stk_bo_rate": {"type": "NUMERIC(10,4)"},
        "stk_co_rate": {"type": "NUMERIC(10,4)"},
        "cash_div": {"type": "NUMERIC(10,4)"},
        "cash_div_tax": {"type": "NUMERIC(10,4)"},
        "record_date": {"type": "DATE"},
        "ex_date": {"type": "DATE", "constraints": "NOT NULL"},
        "pay_date": {"type": "DATE"},
        "div_listdate": {"type": "DATE"},
        "imp_ann_date": {"type": "DATE"},
        "base_date": {"type": "DATE"},
        "base_share": {"type": "NUMERIC(15,2)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_dividend_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_dividend_imp_ann_date", "columns": "imp_ann_date"},
        {"name": "idx_stock_dividend_div_proc", "columns": "div_proc"},
        {"name": "idx_stock_dividend_ex_date", "columns": "ex_date"},
        {"name": "idx_stock_dividend_update_time", "columns": "update_time"},
    ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        1. 全量模式(force_full=True): 按股票代码分批，使用股票基本信息作为数据源
        2. 增量模式: 按除权除息日期分批，使用交易日历作为数据源

        Args:
            **kwargs: 包含start_date, end_date, force_full等参数

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 判断是否为全量模式（基于日期范围是否覆盖默认起始日期到当前日期）
        current_date = datetime.now().strftime("%Y%m%d")
        is_full_mode = (start_date == self.default_start_date and end_date == current_date)

        self.logger.info(
            f"任务 {self.name}: 使用 BatchPlanner 生成批处理列表 - is_full_mode: {is_full_mode}, start_date: {start_date}, end_date: {end_date}"
        )

        try:
            if is_full_mode:
                # 策略1: 全量模式 - 按股票代码分批
                return await generate_stock_code_batches(
                    db_connection=self.db,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])},
                    filter_condition="list_status = 'L'", # 仅获取上市状态的股票
                )
            else:
                # 策略2: 增量模式 - 按日期分批
                # 确定总体起止日期
                if not start_date:
                    latest_db_date = await self.get_latest_date()
                    if latest_db_date:
                        next_day_obj = latest_db_date + timedelta(days=1)
                        start_date = next_day_obj.strftime("%Y%m%d") # type: ignore
                    else:
                        start_date = self.default_start_date
                    self.logger.info(
                        f"任务 {self.name}: 未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date}"
                    )

                if not end_date:
                    end_date = datetime.now().strftime("%Y%m%d")
                    self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

                if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"): # type: ignore
                    self.logger.info(
                        f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
                    )
                    return []

                return await generate_trade_day_batches(
                    logger=self.logger,
                    start_date=start_date,
                    end_date=end_date,
                    exchange=kwargs.get("exchange", "SSE"),
                    additional_params={"fields": ",".join(self.fields or [])},
                    **kwargs # 传递其他可能的参数
                )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: BatchPlanner 生成批次时出错: {e}", exc_info=True)
            return []

    async def fetch_batch(self, batch_params: Dict, stop_event: Optional[Any] = None) -> Optional[pd.DataFrame]:
        """重写批次获取方法，添加div_proc过滤

        获取数据后过滤div_proc='实施'的记录

        Args:
            batch_params: 批次参数
            stop_event: 停止事件（可选）

        Returns:
            Optional[pd.DataFrame]: 过滤后的数据
        """
        # 调用父类方法获取数据
        data = await super().fetch_batch(batch_params, stop_event)

        # 对数据进行div_proc过滤
        if data is not None and not data.empty and "div_proc" in data.columns:
            original_count = len(data)
            filtered_data = data[data["div_proc"] == "实施"].copy()
            filtered_count = len(filtered_data)

            ts_code = batch_params.get("ts_code", "未知")
            self.logger.debug(
                f"股票 {ts_code}: 获取到 {original_count} 条数据，过滤后 {filtered_count} 条已实施分红"
            )

            return filtered_data # type:ignore

        return data


# 导出任务类
__all__ = ["TushareStockDividendTask"]
