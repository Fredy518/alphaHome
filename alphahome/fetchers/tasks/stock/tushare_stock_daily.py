from datetime import datetime
from typing import Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockDailyTask(TushareTask):
    """股票日线数据任务

    获取股票的日线交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额等信息。
    该任务使用Tushare的daily接口获取数据。
    """

    # 1.核心属性
    name = "tushare_stock_daily"
    description = "获取A股股票日线行情数据"
    table_name = "stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "19901219"  # A股最早交易日
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 6000

    # 2.自定义索引
    indexes = [
        {"name": "idx_stock_daily_code", "columns": "ts_code"},
        {"name": "idx_stock_daily_date", "columns": "trade_date"},
        {"name": "idx_stock_daily_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "daily"
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

    # 4.数据类型转换
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "pre_close": float,
        "change": float,
        "pct_chg": float,
        "vol": float,  # 原始字段名
        "amount": float,
    }

    # 5.列名映射
    column_mapping = {"vol": "volume"}  # 将vol映射为volume

    # 6.表结构定义
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
        "volume": {"type": "FLOAT"},  # 目标字段名
        "amount": {"type": "FLOAT"},
    }

    # 7.数据验证规则 (使用目标字段名 volume) - 真正生效的验证机制
    validations = [
        (lambda df: df["close"] > 0, "收盘价必须为正数"),
        (lambda df: df["open"] > 0, "开盘价必须为正数"),
        (lambda df: df["high"] > 0, "最高价必须为正数"),
        (lambda df: df["low"] > 0, "最低价必须为正数"),
        (lambda df: df["volume"] >= 0, "成交量不能为负数"),
        (lambda df: df["amount"] >= 0, "成交额不能为负数"),
        (lambda df: df["high"] >= df["low"], "最高价不能低于最低价"),
        (lambda df: df["high"] >= df["open"], "最高价不能低于开盘价"),
        (lambda df: df["high"] >= df["close"], "最高价不能低于收盘价"),
        (lambda df: df["low"] <= df["open"], "最低价不能高于开盘价"),
        (lambda df: df["low"] <= df["close"], "最低价不能高于收盘价"),
    ]

    # 8.验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    # 8. 分批配置
    batch_trade_days_single_code = 240  # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 5  # 全市场查询时，每个批次的交易日数量 (1周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表

        BatchPlanner 提供了声明式、可组合的批处理规划方式，
        将数据源、分区策略和参数映射分离，提高了代码的可读性和可维护性。

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")
        exchange = kwargs.get("exchange", "SSE")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

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

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                ts_code=ts_code,
                exchange=exchange,
                logger=self.logger,
            )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            return []


