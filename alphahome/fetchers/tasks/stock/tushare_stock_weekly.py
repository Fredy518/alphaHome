#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 股票周线行情数据任务

接口文档: https://tushare.pro/document/2?doc_id=336
数据说明:
- 获取股票周线行情数据(每日更新)
- 支持两种批处理策略:
  1. 全量模式: 按股票代码分批获取
  2. 增量模式: 按交易日期分批获取，每批30个交易日

权限要求: 需要至少2000积分
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import (
    generate_natural_day_batches,
    normalize_date_range
)
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes

# BatchPlanner 导入
from ....common.planning.batch_planner import BatchPlanner, Source, Partition, Map

logger = logging.getLogger(__name__)

@task_register()
class TushareStockWeeklyTask(TushareTask):
    """获取股票周线行情数据 (stk_weekly_monthly)
    """

    # 1. 核心属性
    name = "tushare_stock_weekly"
    description = "获取股票周线行情数据"
    table_name = "stock_weekly"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 交易日期
    default_start_date = "20050101"  # 默认开始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识
    smart_lookback_days = 30  # 智能增量模式下，回看30天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 降低并发，避免频率限制
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "stk_weekly_monthly"
    # Tushare stk_weekly_monthly 接口返回的字段
    fields = [
        "ts_code",      # TS代码
        "trade_date",   # 交易日期（每周五或者月末日期）
        "end_date",     # 计算截至日期
        "open",         # 周开盘价
        "high",         # 周最高价
        "low",          # 周最低价
        "close",        # 周收盘价
        "pre_close",    # 上一周收盘价
        "vol",          # 周成交量
        "amount",       # 周成交额
        "change",       # 周涨跌额
        "pct_chg",      # 周涨跌幅(未复权)
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {
        "vol": "volume"  # 将vol字段映射为volume
    }

    # 4. 数据类型转换
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "pre_close": float,
        "vol": float,
        "amount": float,
        "change": float,
        "pct_chg": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE"},
        "open": {"type": "NUMERIC(10,3)"},
        "high": {"type": "NUMERIC(10,3)"},
        "low": {"type": "NUMERIC(10,3)"},
        "close": {"type": "NUMERIC(10,3)"},
        "pre_close": {"type": "NUMERIC(10,3)"},
        "volume": {"type": "NUMERIC(20,2)"},
        "amount": {"type": "NUMERIC(20,2)"},
        "change": {"type": "NUMERIC(10,3)"},
        "pct_chg": {"type": "NUMERIC(8,4)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_weekly_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_weekly_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_weekly_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['trade_date'].notna(),
        lambda df: df['open'] >= 0,
        lambda df: df['high'] >= 0,
        lambda df: df['low'] >= 0,
        lambda df: df['close'] >= 0,
        lambda df: df['volume'] >= 0,
        lambda df: df['amount'] >= 0,
        # 价格逻辑检查
        lambda df: df['high'] >= df['low'],
        lambda df: df['high'] >= df['open'],
        lambda df: df['high'] >= df['close'],
        lambda df: df['low'] <= df['open'],
        lambda df: df['low'] <= df['close'],
    ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 generate_natural_day_batches 生成批处理参数列表

        策略说明:
        - 统一使用自然日分批策略，每批60个自然日
        - 全量更新：使用default_start_date作为开始时间
        - 增量更新：使用数据库最新日期+1天作为开始时间
        - 全量和智能增量模式：end_date对齐为当前周周日
        - 手动模式：end_date保持不变

        Args:
            **kwargs: 包含start_date, end_date等参数

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        self.logger.info(
            f"任务 {self.name}: 使用 generate_natural_day_batches 生成批处理列表 - start_date: {start_date}, end_date: {end_date}"
        )

        try:
            # 标准化日期范围处理
            normalized_start, normalized_end = normalize_date_range(
                start_date=start_date,
                end_date=end_date,
                default_start_date=self.default_start_date,
                logger=self.logger,
                task_name=self.name
            )

            # 根据更新模式决定是否对齐end_date
            update_type = kwargs.get("update_type")
            if update_type in (UpdateTypes.FULL, UpdateTypes.SMART):
                # 全量和智能增量模式：end_date对齐为当前周周日
                current_date = datetime.now()
                # 计算当前周的最后一天（周日）
                days_since_monday = current_date.weekday()
                days_to_sunday = 6 - days_since_monday
                week_end = current_date + timedelta(days=days_to_sunday)
                normalized_end = week_end.strftime("%Y%m%d")
                self.logger.info(f"任务 {self.name}: {update_type}模式下end_date对齐为当前周周日: {normalized_end}")
            else:
                # 手动模式：保持normalized_end不变
                self.logger.info(f"任务 {self.name}: {update_type}模式下保持end_date不变: {normalized_end}")

            # 检查日期范围有效性
            if datetime.strptime(normalized_start, "%Y%m%d") > datetime.strptime(normalized_end, "%Y%m%d"):
                self.logger.info(
                    f"起始日期 ({normalized_start}) 晚于结束日期 ({normalized_end})，无需执行任务。"
                )
                return []

            # 使用 generate_natural_day_batches 生成批次，每批30个自然日
            return await generate_natural_day_batches(
                start_date=normalized_start,
                end_date=normalized_end,
                batch_size=30,  # 每批30个自然日
                additional_params={
                    "fields": ",".join(self.fields or []),
                    "freq": "week"
                },
                logger=self.logger
            )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []

    async def fetch_batch(self, batch_params: Dict, stop_event: Optional[Any] = None) -> Optional[pd.DataFrame]:
        """重写批次获取方法，添加数据过滤

        获取数据后可以进行必要的过滤处理

        Args:
            batch_params: 批次参数
            stop_event: 停止事件（可选）

        Returns:
            Optional[pd.DataFrame]: 处理后的数据
        """
        # 调用父类方法获取数据
        data = await super().fetch_batch(batch_params, stop_event)

        # 对数据进行必要的过滤和处理
        if data is not None and not data.empty:
            original_count = len(data)
            
            # 过滤掉无效的股票代码
            if "ts_code" in data.columns:
                data = data[data["ts_code"].str.contains(r'\.(?:SZ|SH)$', na=False)].copy()
            
            filtered_count = len(data)
            
            ts_code = batch_params.get("ts_code", "未知")
            self.logger.debug(
                f"股票 {ts_code}: 获取到 {original_count} 条数据，过滤后 {filtered_count} 条有效数据"
            )

            return data

        return data


# 导出任务类
__all__ = ["TushareStockWeeklyTask"]
