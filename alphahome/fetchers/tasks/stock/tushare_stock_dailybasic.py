from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockDailyBasicTask(TushareTask):
    """股票每日基本面指标任务

    获取股票的每日基本面指标，包括市盈率、市净率、换手率、总市值等数据。
    该任务使用Tushare的daily_basic接口获取数据，并依赖于股票日线数据任务。
    """

    # 1.核心属性
    name = "tushare_stock_dailybasic"
    description = "获取股票每日基本面指标"
    table_name = "stock_dailybasic"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    default_start_date = "19910101"  # Tushare 股票日基本指标大致起始日期
    data_source = "tushare"
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 6000

    # 2.自定义索引
    indexes = [
        {"name": "idx_tushare_daily_basic_code", "columns": "ts_code"},
        {"name": "idx_tushare_daily_basic_date", "columns": "trade_date"},
        {"name": "idx_tushare_daily_basic_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "daily_basic"
    fields = [
        "ts_code",
        "trade_date",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    ]

    # 4.数据类型转换
    transformations = {
        "close": float,
        "turnover_rate": float,
        "turnover_rate_f": float,
        "volume_ratio": float,
        "pe": float,
        "pe_ttm": float,
        "pb": float,
        "ps": float,
        "ps_ttm": float,
        "dv_ratio": float,
        "dv_ttm": float,
        "total_share": float,
        "float_share": float,
        "free_share": float,
        "total_mv": float,
        "circ_mv": float,
    }

    # 5.列名映射
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "close": {"type": "NUMERIC(18,4)"},
        "turnover_rate": {"type": "FLOAT"},
        "turnover_rate_f": {"type": "FLOAT"},
        "volume_ratio": {"type": "FLOAT"},
        "pe": {"type": "FLOAT"},
        "pe_ttm": {"type": "FLOAT"},
        "pb": {"type": "FLOAT"},
        "ps": {"type": "FLOAT"},
        "ps_ttm": {"type": "FLOAT"},
        "dv_ratio": {"type": "FLOAT"},
        "dv_ttm": {"type": "FLOAT"},
        "total_share": {"type": "FLOAT"},
        "float_share": {"type": "FLOAT"},
        "free_share": {"type": "FLOAT"},
        "total_mv": {"type": "FLOAT"},
        "circ_mv": {"type": "FLOAT"},
    }

    # 7.数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["total_mv"].fillna(0) >= 0, "总市值不能为负数"),
        (lambda df: df["circ_mv"].fillna(0) >= 0, "流通市值不能为负数"),
        (lambda df: df["turnover_rate"].fillna(0) >= 0, "换手率不能为负数"),
        (lambda df: df["total_share"].fillna(0) >= df["float_share"].fillna(0), "总股本应大于等于流通股本"),
    ]

    # 8. 分批配置 (与 TushareStockDailyTask 保持一致或根据需要调整)
    batch_trade_days_single_code = 240  # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 15  # 全市场查询时，每个批次的交易日数量 (3周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        # 获取查询参数
        start_date = kwargs.get("start_date", "19910101")  # 保留原始默认值
        end_date = kwargs.get(
            "end_date", datetime.now().strftime("%Y%m%d")
        )  # 保留原始默认值
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
