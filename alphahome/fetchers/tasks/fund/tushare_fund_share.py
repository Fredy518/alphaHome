#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
基金规模数据 (fund_share) 更新任务
获取基金规模数据，包含上海和深圳ETF基金。
继承自 TushareTask，按日期增量更新。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_trade_day_batches


@task_register()
class TushareFundShareTask(TushareTask):
    """获取基金规模数据 (含ETF)"""

    # 1. 核心属性
    domain = "fund"  # 业务域标识
    name = "tushare_fund_share"
    description = "获取基金规模数据 (含ETF)"
    table_name = "fund_share"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20000101"  # 根据实际情况调整

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "fund_share"
    fields = ["ts_code", "trade_date", "fd_share"]

    # 3. 列名映射 (无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "fd_share": lambda x: pd.to_numeric(x, errors="coerce")
        # trade_date 由基类 process_data 中的 _process_date_column 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "fd_share": {"type": "NUMERIC(20,2)"},  # 单位：万份
        # update_time 会自动添加
        # 主键 ("ts_code", "trade_date") 索引由基类根据 primary_keys 自动处理
    }

    # 6. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "基金代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['fd_share'] >= 0, "基金份额不能为负数"),
    ]

    # 7. 自定义索引 (主键已包含，无需额外添加)
    indexes = [
        {
            "name": "idx_tushare_fund_share_update_time",
            "columns": "update_time",
        }  # 新增 update_time 索引
    ]

    # 7. 分批配置 (根据接口特性和数据量调整)
    batch_trade_days_single_code = 360  # 单基金查询时，每个批次的交易日数量 (约1.5年)
    batch_trade_days_all_codes = 5  # 全市场查询时，每个批次的交易日数量 (1周)

    # 8. 初始化 (如果需要特殊逻辑)
    # def __init__(self, db_connection, api_token=None, api=None, **kwargs):
    #     super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
    #     # 可在此处添加特定初始化逻辑

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用交易日批次工具)。
        支持按日期范围和可选的 ts_code 进行批处理。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")  # 可选的基金代码

        # 检查必要的日期参数
        if not start_date:
            # 如果未提供 start_date，尝试从数据库获取最新日期 + 1天作为开始日期
            latest_db_date = await self.get_latest_date()
            if latest_db_date:
                start_date = (latest_db_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
            else:
                start_date = self.default_start_date
            self.logger.info(f"未提供 start_date，使用: {start_date}")

        if not end_date:
            end_date = pd.Timestamp.now().strftime("%Y%m%d")  # 默认到今天
            self.logger.info(f"未提供 end_date，使用: {end_date}")

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 代码: {ts_code if ts_code else '所有'}"
        )

        try:
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                # 根据是否提供 ts_code 选择不同的批次大小
                batch_size=(
                    self.batch_trade_days_single_code
                    if ts_code
                    else self.batch_trade_days_all_codes
                ),
                # 将 ts_code 传递给批处理函数，以便在参数中包含它
                ts_code=ts_code,
                logger=self.logger,
                # 注意：fund_share API 可能需要 market 参数，但 generate_trade_day_batches 目前不直接支持
                # 如果需要按 market 分批，需要自定义 get_batch_list 逻辑或扩展工具函数
            )
            # 批处理函数返回的字典已包含 start_date 和 end_date
            # 如果提供了 ts_code，它也会包含在字典中，可以直接用于 API 调用
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True
            )
            return []

