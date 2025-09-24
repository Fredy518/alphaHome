#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 涨跌停列表数据任务

接口文档: https://tushare.pro/document/2?doc_id=298
数据说明:
- 获取A股每日涨跌停、炸板数据情况，数据从2020年开始
- 不提供ST股票的统计
- 支持两种批处理策略:
  1. 全量模式: 按季度范围分批获取历史数据
  2. 增量模式: 按交易日分批获取增量数据

权限要求: 需要至少5000积分，单次最大2500条
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import (
    generate_quarter_range_batches,
    generate_single_date_batches
)
from ....common.task_system.task_decorator import task_register

logger = logging.getLogger(__name__)

@task_register()
class TushareStockLimitListTask(TushareTask):
    """获取涨跌停列表数据 (limit_list_d)

    实现要求:
    - 全量更新: 按季度范围分批获取全部历史数据
    - 增量模式: 按交易日分批获取增量数据
    """

    # 1. 核心属性
    name = "tushare_stock_limitlist"
    description = "获取A股每日涨跌停、炸板数据"
    table_name = "stock_limitlist"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 交易日期
    default_start_date = "20200101"  # 数据从2020年开始
    data_source = "tushare"
    domain = "stock"  # 业务域标识
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 降低并发，避免频率限制
    default_page_size = 2500  # API限制单次最大2500条

    # 2. TushareTask 特有属性
    api_name = "limit_list_d"
    # Tushare limit_list_d 接口返回的字段
    fields = [
        "trade_date",     # 交易日期
        "ts_code",        # 股票代码
        "industry",       # 所属行业
        "name",           # 股票名称
        "close",          # 收盘价
        "pct_chg",        # 涨跌幅
        "amount",         # 成交额
        "limit_amount",   # 板上成交金额
        "float_mv",       # 流通市值
        "total_mv",       # 总市值
        "turnover_ratio", # 换手率
        "fd_amount",      # 封单金额
        "first_time",     # 首次封板时间
        "last_time",      # 最后封板时间
        "open_times",     # 炸板次数
        "up_stat",        # 涨停统计
        "limit_times",    # 连板数
        "limit",          # 涨跌停类型
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "close": float,
        "pct_chg": float,
        "amount": float,
        "limit_amount": float,
        "float_mv": float,
        "total_mv": float,
        "turnover_ratio": float,
        "fd_amount": float,
        "open_times": int,
        "limit_times": int,
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "industry": {"type": "VARCHAR(50)"},
        "name": {"type": "VARCHAR(50)"},
        "close": {"type": "NUMERIC(15,4)"},
        "pct_chg": {"type": "NUMERIC(10,4)"},
        "amount": {"type": "NUMERIC(20,2)"},
        "limit_amount": {"type": "NUMERIC(20,2)"},
        "float_mv": {"type": "NUMERIC(20,2)"},
        "total_mv": {"type": "NUMERIC(20,2)"},
        "turnover_ratio": {"type": "NUMERIC(10,4)"},
        "fd_amount": {"type": "NUMERIC(20,2)"},
        "first_time": {"type": "VARCHAR(10)"},  # 时间格式如 "09:30:00"
        "last_time": {"type": "VARCHAR(10)"},
        "open_times": {"type": "INTEGER"},
        "up_stat": {"type": "VARCHAR(20)"},  # 如 "N/T" 格式
        "limit_times": {"type": "INTEGER"},
        "limit": {"type": "VARCHAR(1)"},  # D跌停U涨停Z炸板
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_limitlist_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_limitlist_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_limitlist_limit", "columns": "limit"},  # 涨跌停类型索引
        {"name": "idx_stock_limitlist_limit_times", "columns": "limit_times"},  # 连板数索引
        {"name": "idx_stock_limitlist_industry", "columns": "industry"},  # 行业索引
        {"name": "idx_stock_limitlist_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['limit'].isin(['D', 'U', 'Z']), "涨跌停类型必须为D/U/Z"),
        (lambda df: df['close'] > 0, "收盘价必须为正数"),
        (lambda df: df['amount'] >= 0, "成交额必须非负"),
        # 修复：处理空值的验证规则
        (lambda df: (df['open_times'] >= 0) | df['open_times'].isna(), "炸板次数必须非负或为空"),
        (lambda df: (df['limit_times'] >= 0) | df['limit_times'].isna(), "连板数必须非负或为空"),
        # 涨跌幅合理性检查（考虑到涨跌停限制和空值）
        (lambda df: (df['pct_chg'].abs() <= 25) | df['pct_chg'].isna(), "涨跌幅应在合理范围内（±25%）或为空"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        1. 全量模式: 按季度范围分批获取历史数据
        2. 增量模式: 按交易日分批获取增量数据

        Args:
            **kwargs: 包含start_date, end_date等参数

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 判断是否为全量模式（基于日期范围是否覆盖默认起始日期到当前日期）
        current_date = datetime.now().strftime("%Y%m%d")
        is_full_mode = (start_date == self.default_start_date and end_date == current_date)

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 - is_full_mode: {is_full_mode}, start_date: {start_date}, end_date: {end_date}"
        )

        try:
            if is_full_mode:
                # 策略1: 全量模式 - 按季度范围分批获取数据
                self.logger.info(f"全量模式：按季度范围分批获取涨跌停数据")
                return await generate_quarter_range_batches(
                    start_date=start_date,
                    end_date=end_date,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])}
                )
            else:
                # 策略2: 增量模式 - 按交易日分批
                # 确定总体起止日期
                if not start_date:
                    latest_db_date = await self.get_latest_date()
                    if latest_db_date:
                        next_day_obj = latest_db_date + timedelta(days=1)
                        start_date = next_day_obj.strftime("%Y%m%d")
                    else:
                        start_date = self.default_start_date
                    self.logger.info(
                        f"任务 {self.name}: 未提供 start_date，使用数据库最新日期+1天或默认起始日期: {start_date}"
                    )

                if not end_date:
                    end_date = datetime.now().strftime("%Y%m%d")
                    self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

                if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
                    self.logger.info(
                        f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
                    )
                    return []

                self.logger.info(f"增量模式：按交易日分批获取涨跌停数据")
                return await generate_single_date_batches(
                    start_date=start_date,
                    end_date=end_date,
                    date_field="trade_date",
                    logger=self.logger,
                    exchange=kwargs.get("exchange", "SSE"),
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
__all__ = ["TushareStockLimitListTask"]
