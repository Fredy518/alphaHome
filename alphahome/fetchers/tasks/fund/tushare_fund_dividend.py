#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 公募基金分红数据任务

接口文档: https://tushare.pro/document/2?doc_id=120
数据说明:
- 获取公募基金分红数据
- 支持两种批处理策略:
  1. 全量模式: 按基金代码分批获取
  2. 增量模式: 按派息日(pay_date)分批获取

权限要求: 需要至少400积分
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import asyncio  # 添加 asyncio 导入

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import (
    generate_fund_code_batches,
    generate_single_date_batches
)
from ....common.task_system.task_decorator import task_register

# BatchPlanner 导入
from ....common.planning.batch_planner import BatchPlanner, Source, Partition, Map

logger = logging.getLogger(__name__)

@task_register()
class TushareFundDividendTask(TushareTask):
    """获取公募基金分红数据 (fund_div)

    实现要求:
    - 全量更新: 使用ts_code作为batch单位，批量获取全部数据
    - 增量模式: 使用pay_date字段进行更新，使用交易日历作为数据源
    """

    # 1. 核心属性
    name = "tushare_fund_dividend"
    description = "获取公募基金分红数据"
    table_name = "fund_dividend"
    primary_keys = ["ts_code", "pay_date"]
    date_column = "pay_date"  # 派息日
    default_start_date = "20050101"  # 默认开始日期
    data_source = "tushare"
    domain = "fund"  # 业务域标识
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 降低并发，避免频率限制
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "fund_div"
    # Tushare fund_div 接口返回的字段
    fields = [
        "ts_code",  # TS代码
        "ann_date",  # 公告日期
        "imp_anndate",  # 分红实施公告日
        "base_date",  # 分配收益基准日
        "div_proc",  # 方案进度
        "record_date",  # 权益登记日
        "ex_date",  # 除息日
        "pay_date",  # 派息日
        "earpay_date",  # 收益支付日
        "net_ex_date",  # 净值除权日
        "div_cash",  # 每股派息(元)
        "base_unit",  # 基准基金份额(万份)
        "ear_distr",  # 可分配收益(元)
        "ear_amount",  # 收益分配金额(元)
        "account_date",  # 红利再投资到账日
        "base_year",  # 份额基准年度
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "div_cash": float,
        "base_unit": float,
        "ear_distr": float,
        "ear_amount": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "imp_anndate": {"type": "DATE"},
        "base_date": {"type": "DATE"},
        "div_proc": {"type": "VARCHAR(10)"},
        "record_date": {"type": "DATE"},
        "ex_date": {"type": "DATE"},
        "pay_date": {"type": "DATE", "constraints": "NOT NULL"},
        "earpay_date": {"type": "DATE"},
        "net_ex_date": {"type": "DATE"},
        "div_cash": {"type": "NUMERIC(10,4)"},
        "base_unit": {"type": "NUMERIC(15,2)"},
        "ear_distr": {"type": "NUMERIC(20,2)"},
        "ear_amount": {"type": "NUMERIC(20,2)"},
        "account_date": {"type": "DATE"},
        "base_year": {"type": "VARCHAR(10)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_dividend_ts_code", "columns": "ts_code"},
        {"name": "idx_fund_dividend_ann_date", "columns": "ann_date"},
        {"name": "idx_fund_dividend_pay_date", "columns": "pay_date"},
        {"name": "idx_fund_dividend_div_proc", "columns": "div_proc"},
        {"name": "idx_fund_dividend_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['pay_date'].notna(),
        lambda df: df['div_cash'] >= 0,
        lambda df: df['base_unit'] >= 0,
        lambda df: df['ear_distr'] >= 0,
        lambda df: df['ear_amount'] >= 0,
        lambda df: df['div_proc'].isin(['预案', '股东大会通过', '实施']),
        # 逻辑日期检查 (允许某些日期为空)
        lambda df: (df['pay_date'] >= df['ex_date']) | df['ex_date'].isnull(),
        lambda df: (df['ex_date'] >= df['record_date']) | df['record_date'].isnull(),
        lambda df: (df['ear_amount'] <= df['ear_distr']) | df['ear_distr'].isnull(), # 分配金额不应超过可分配收益
    ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        策略说明:
        1. 全量模式(force_full=True): 按基金代码分批，使用基金基本信息作为数据源
        2. 增量模式: 按派息日期分批，使用交易日历作为数据源

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
                # 策略1: 全量模式 - 按基金代码分批
                return await generate_fund_code_batches(
                    db_connection=self.db,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields or [])},
                    filter_condition="market = 'E'", # 只获取沪深交易所上市的基金
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

                return await generate_single_date_batches(
                    start_date=start_date,
                    end_date=end_date,
                    date_field="pay_date",
                    logger=self.logger,
                    exchange=kwargs.get("exchange", "SSE"),
                    additional_params={"fields": ",".join(self.fields or [])}
                )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: BatchPlanner 生成批次时出错: {e}", exc_info=True)
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
            
            # 过滤掉无效的基金代码
            if "ts_code" in data.columns:
                data = data[data["ts_code"].str.contains(r'\.(?:OF|SZ|SH)$', na=False)].copy()
            
            filtered_count = len(data)
            
            ts_code = batch_params.get("ts_code", "未知")
            self.logger.debug(
                f"基金 {ts_code}: 获取到 {original_count} 条数据，过滤后 {filtered_count} 条有效数据"
            )

            return data

        return data


# 导出任务类
__all__ = ["TushareFundDividendTask"]
