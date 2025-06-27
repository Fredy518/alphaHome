#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金日线行情 (fund_daily) 更新任务
获取场内基金（ETF、LOF等）的日线行情数据。
继承自 TushareTask，按 trade_date 增量更新。
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import (
    generate_single_date_batches,
    generate_trade_day_batches,
)


@task_register()
class TushareFundDailyTask(TushareTask):
    """获取基金日线行情数据"""

    # 1. 核心属性
    name = "tushare_fund_daily"
    description = "获取场内基金日线行情"
    table_name = "fund_daily"
    data_source = "tushare"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20000101"  # 调整为合理的起始日期
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 2
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "fund_daily"  # Tushare API 名称
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    # 2. TushareTask 特有属性
    api_name = "fund_daily"
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]

    # 3. 列名映射
    column_mapping = {"vol": "volume", "amount": "amount"}

    # 4. 数据类型转换
    transformations = {
        "open": lambda x: pd.to_numeric(x, errors="coerce"),
        "high": lambda x: pd.to_numeric(x, errors="coerce"),
        "low": lambda x: pd.to_numeric(x, errors="coerce"),
        "close": lambda x: pd.to_numeric(x, errors="coerce"),
        "pre_close": lambda x: pd.to_numeric(x, errors="coerce"),
        "change": lambda x: pd.to_numeric(x, errors="coerce"),
        "pct_chg": lambda x: pd.to_numeric(x, errors="coerce"),
        "vol": lambda x: pd.to_numeric(x, errors="coerce"),  # 原始列
        "amount": lambda x: pd.to_numeric(x, errors="coerce"),  # 原始列
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "FLOAT"},
        "high": {"type": "FLOAT"},
        "low": {"type": "FLOAT"},
        "close": {"type": "FLOAT"},
        "pre_close": {"type": "FLOAT"},
        "change": {"type": "FLOAT"},
        "pct_chg": {"type": "FLOAT"},
        "volume": {"type": "FLOAT"},  # 映射后的列名
        "amount": {"type": "FLOAT"},  # 修正：保持amount列名不变
        # update_time 会自动添加
    }

    # 6. 自定义索引 (主键已包含)
    indexes = [
        {
            "name": "idx_tushare_fund_daily_update_time",
            "columns": "update_time",
        }  # 新增 update_time 索引
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "基金代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['close'] > 0, "收盘价必须为正数"),
        (lambda df: df['high'] >= df['low'], "最高价不能低于最低价"),
        (lambda df: df['volume'] >= 0, "成交量不能为负数"),
        (lambda df: df['amount'] >= 0, "成交额不能为负数"),
    ]


    def __init__(self, db_connection, api_token=None, api=None, **kwargs):
        """初始化任务，并设置特定的API调用限制"""
        super().__init__(db_connection, api_token, api, **kwargs)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用单日期批次工具)。
        为每个交易日生成单独的批次，使用trade_date参数。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")

        if not start_date:
            latest_db_date = await self.get_latest_date()
            start_date = (
                (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
                if latest_db_date
                else self.default_start_date
            )
            self.logger.info(f"未提供 start_date，使用: {start_date}")
        if not end_date:
            end_date = pd.Timestamp.now().strftime("%Y%m%d")
            self.logger.info(f"未提供 end_date，使用: {end_date}")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}"
        )

        try:
            # 使用专用的单日期批次生成函数
            batch_list = await generate_single_date_batches(
                start_date=start_date,
                end_date=end_date,
                date_field="trade_date",
                ts_code=ts_code,
                logger=self.logger,
            )

            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []

    # validate_data 可以使用基类或自定义