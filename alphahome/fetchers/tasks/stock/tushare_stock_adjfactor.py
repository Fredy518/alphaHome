from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockAdjFactorTask(TushareTask):
    """股票复权因子任务

    获取股票的复权因子数据。复权因子是用来计算复权价格的基础数据，
    当股票由于分红、送股等原因发生除权除息时，会产生新的复权因子。
    """

    # 1.核心属性
    domain = "stock"  # 业务域标识
    name = "tushare_stock_adjfactor"
    description = "获取股票复权因子"
    table_name = "stock_adjfactor"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19901219"  # Tushare最早的日期
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 6000

    # 2.自定义索引
    indexes = [
        {"name": "idx_stock_adjfactor_code", "columns": "ts_code"},
        {"name": "idx_stock_adjfactor_date", "columns": "trade_date"},
        {"name": "idx_stock_adjfactor_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "adj_factor"
    fields = ["ts_code", "trade_date", "adj_factor"]

    # 4.数据类型转换
    transformations = {"adj_factor": float}

    # 5.列名映射
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "adj_factor": {"type": "FLOAT"},
    }

    # 7.数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["adj_factor"] > 0, "复权因子必须为正数"),
    ]

    # 8. 分批配置
    batch_trade_days_single_code = 240
    batch_trade_days_all_codes = 5  # 全市场查询时，每个批次的交易日数量 (1周)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        # 获取查询参数
        start_date = kwargs.get("start_date", self.default_start_date)
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))
        ts_code = kwargs.get("ts_code")  # 获取可能的ts_code
        exchange = kwargs.get("exchange", "SSE")  # 获取可能的交易所参数

        self.logger.info(
            f"任务 {self.name}: 使用 BatchPlanner 生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用标准的交易日批次生成工具
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            # 根据是否有指定股票代码选择不同的批次大小
            batch_size = (
                self.batch_trade_days_single_code
                if ts_code
                else self.batch_trade_days_all_codes
            )

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                ts_code=ts_code,
                exchange=exchange,
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            return []


# 导出任务类
__all__ = ["TushareStockAdjFactorTask"]
