#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金复权因子 (adj_factor) 更新任务
获取公募基金复权因子，用于计算基金日线行情的后复权价格。
继承自 TushareTask，按 trade_date 增量更新。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入批处理工具
from ...sources.tushare.batch_utils import generate_trade_day_batches


@task_register()
class TushareFundAdjFactorTask(TushareTask):
    """获取基金复权因子数据"""

    # 1. 核心属性
    name = "tushare_fund_adjfactor"
    description = "获取公募基金复权因子"
    table_name = "fund_adjfactor"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "20000101"  # 与基金净值/日线保持一致
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 2000

    # 2. TushareTask 特有属性
    api_name = "fund_adj"  # Tushare API 名称
    fields = ["ts_code", "trade_date", "adj_factor"]

    # 3. 列名映射 (无需映射)
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "adj_factor": lambda x: pd.to_numeric(x, errors="coerce")
        # trade_date 由基类 process_data 处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "adj_factor": {"type": "FLOAT"},
        # update_time 会自动添加
        # 主键 ("ts_code", "trade_date") 索引由基类自动处理
    }

    # 6. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "基金代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['adj_factor'] > 0, "复权因子必须为正数"),
        (lambda df: df['adj_factor'] <= 100, "复权因子应在合理范围内（≤100）"),
    ]

    # 7. 自定义索引 (主键已包含)
    indexes = [
        {
            "name": "idx_tushare_fund_adjfactor_update_time",
            "columns": "update_time",
        }  # 新增 update_time 索引
    ]

    # 7. 分批配置
    batch_trade_days_single_code = 720  # 假设单基金复权因子查询量不大，可以一次多查点
    batch_trade_days_all_codes = 10  # 全市场查询时，批次可以大一些

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表 (使用交易日批次工具)。
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

        # 注意：adj_factor 接口本身不支持按 ts_code 列表查询，
        # 如果需要更新所有基金的复权因子，必须多次调用或不传递 ts_code (如果接口支持)。
        # Tushare 的 adj_factor 接口文档说明它支持单次查询单个标的。
        # 因此，全量更新需要先获取所有基金代码列表，然后为每个代码生成批次。
        # 这里简化处理：假设调用者会传入 ts_code，或者接口能处理不传 ts_code 的情况。
        # 实际生产中，全量更新所有基金的复权因子需要更复杂的 get_batch_list 逻辑。
        # 这里我们假设 generate_trade_day_batches 的 ts_code 参数能被接口正确处理或忽略。

        try:
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=(
                    self.batch_trade_days_single_code
                    if ts_code
                    else self.batch_trade_days_all_codes
                ),
                ts_code=ts_code,
                logger=self.logger,
            )
            # Note: The 'fund_adj' API supports multiple ts_codes. This batching strategy primarily splits by date.
            # For multi-code requests within a date range, the API handles it directly if no ts_code is passed in the batch params,
            # or if a single ts_code is passed.
            # Handling a list of specific ts_codes would require adjusting this batch generation logic.
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True
            )
            return []

    # adj_factor 接口非常简单，可能不需要复杂的验证
