#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金持仓数据 (fund_portfolio) 更新任务
获取公募基金季度末持仓明细数据。
继承自 TushareTask，按 ann_date (公告日期) 增量更新。
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_natural_day_batches


@task_register()
class TushareFundPortfolioTask(TushareTask):
    """获取公募基金持仓数据"""

    # 1. 核心属性
    name = "tushare_fund_portfolio"
    description = "获取公募基金持仓明细"
    table_name = "fund_portfolio"
    data_source = "tushare"
    domain = "fund"  # 业务域标识
    # 主键：基金代码 + 公告日期 + 股票代码 + 报告期 能够唯一确定一条持仓记录
    primary_keys = ["ts_code", "ann_date", "symbol", "end_date"]
    date_column = "ann_date"  # 使用公告日期进行增量更新
    default_start_date = "19980101"  # 基金持仓数据大致起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 4000

    # 2. TushareTask 特有属性
    api_name = "fund_portfolio"  # Tushare API 名称
    # 根据 https://tushare.pro/document/2?doc_id=121 列出字段
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "symbol",
        "mkv",
        "amount",
        "stk_mkv_ratio",
        "stk_float_ratio",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "mkv": lambda x: pd.to_numeric(x, errors="coerce"),  # 持有股票市值(元)
        "amount": lambda x: pd.to_numeric(x, errors="coerce"),  # 持有股票数量（股）
        "stk_mkv_ratio": lambda x: pd.to_numeric(x, errors="coerce"),  # 占股票市值比
        "stk_float_ratio": lambda x: pd.to_numeric(
            x, errors="coerce"
        ),  # 占流通股本比例
        # ann_date 和 end_date 由基类 process_data 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},  # 报告期也加入非空
        "symbol": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},  # 股票代码
        "mkv": {"type": "NUMERIC(20,3)"},
        "amount": {"type": "NUMERIC(20,2)"},
        "stk_mkv_ratio": {"type": "NUMERIC(10,4)"},
        "stk_float_ratio": {"type": "NUMERIC(10,4)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_portfolio_ts_code", "columns": "ts_code"},
        {"name": "idx_fund_portfolio_ann_date", "columns": "ann_date"},
        {"name": "idx_fund_portfolio_end_date", "columns": "end_date"},
        {"name": "idx_fund_portfolio_symbol", "columns": "symbol"},
        {
            "name": "idx_fund_portfolio_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 7. 分批配置 (按自然日，约3个月一批)
    batch_size_days = 90

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。

        fund_portfolio API 需要至少提供 ts_code、ann_date 或 period 中的一个参数。
        这里我们使用 ann_date (公告日期) 作为主要参数，按时间范围分批处理。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选参数

        if not start_date:
            latest_db_date = await self.get_latest_date()
            start_date = (
                (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
                if latest_db_date
                else self.default_start_date
            )
            self.logger.info(
                f"未提供 start_date，使用数据库最新公告日期+1天或默认起始日期: {start_date}"
            )
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"未提供 end_date，使用当前日期: {end_date}")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 (基于 ann_date)，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}"
        )

        try:
            # 生成季度批次，因为基金持仓数据通常按季度披露
            batch_list = await self._generate_quarterly_batches(start_date, end_date, ts_code)

            self.logger.info(f"生成了 {len(batch_list)} 个季度批次用于 fund_portfolio API")
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []

    async def _generate_quarterly_batches(self, start_date: str, end_date: str, ts_code: Optional[str] = None) -> List[Dict]:
        """
        生成按季度的批次参数，每个批次使用 ann_date 参数指定公告日期范围。

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            ts_code: 可选的基金代码

        Returns:
            批次参数列表，每个批次包含 ann_date 参数
        """
        from dateutil.relativedelta import relativedelta

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        batches = []

        # 从开始日期的季度开始
        current_start = start_dt.replace(month=((start_dt.month - 1) // 3) * 3 + 1, day=1)
        current_end = min(current_start + relativedelta(months=3, days=-1), end_dt)

        while current_start <= end_dt:
            # 为每个季度生成一个批次，使用 ann_date 参数
            batch = {
                "ann_date": f"{current_start.strftime('%Y%m%d')},{current_end.strftime('%Y%m%d')}"
            }

            if ts_code:
                batch["ts_code"] = ts_code

            batches.append(batch)

            # 移动到下一个季度
            current_start = current_start + relativedelta(months=3)
            current_end = min(current_start + relativedelta(months=3, days=-1), end_dt)

        return batches

    # prepare_params 可以使用基类默认实现，它会传递 batch_list 中的所有参数
