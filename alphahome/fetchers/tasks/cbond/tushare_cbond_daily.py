from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareCBondDailyTask(TushareTask):
    """可转债日线数据任务

    获取可转债的日线交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额、
    纯债价值、纯债溢价率、转股价值、转股溢价率等信息。
    该任务使用Tushare的cb_daily接口获取数据。
    """

    # 1.核心属性
    domain = "cbond"  # 业务域标识
    name = "tushare_cbond_daily"
    description = "获取可转债日线行情数据"
    table_name = "cbond_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20050101"  # 可转债数据起始日期
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 2  # 默认并发限制
    default_page_size = 2000

    # 2.自定义索引
    indexes = [
        {"name": "idx_cbond_daily_code", "columns": "ts_code"},
        {"name": "idx_cbond_daily_date", "columns": "trade_date"},
        {"name": "idx_cbond_daily_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "cb_daily"
    fields = [
        "ts_code",
        "trade_date",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "change",
        "pct_chg",
        "vol",
        "amount",
        "bond_value",
        "bond_over_rate",
        "cb_value",
        "cb_over_rate",
    ]

    # 4.数据类型转换
    transformations = {
        "pre_close": float,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "change": float,
        "pct_chg": float,
        "vol": float,  # 原始字段名
        "amount": float,
        "bond_value": float,
        "bond_over_rate": float,
        "cb_value": float,
        "cb_over_rate": float,
    }

    # 5.列名映射
    column_mapping = {"vol": "volume"}  # 将vol映射为volume

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "pre_close": {"type": "NUMERIC(15,4)"},
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "change": {"type": "NUMERIC(15,4)"},
        "pct_chg": {"type": "NUMERIC(10,4)"},
        "volume": {"type": "NUMERIC(20,2)"},  # 目标字段名
        "amount": {"type": "NUMERIC(20,3)"},
        "bond_value": {"type": "NUMERIC(15,4)"},
        "bond_over_rate": {"type": "NUMERIC(10,4)"},
        "cb_value": {"type": "NUMERIC(15,4)"},
        "cb_over_rate": {"type": "NUMERIC(10,4)"},
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

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成基于交易日的批处理参数列表。

        使用 BatchPlanner 的 generate_trade_day_batches 方案，
        根据是否指定可转债代码选择不同的批次大小。
        """
        # 参数提取和验证
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

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 缺少必要的日期参数")
            return []

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 可转债代码: {ts_code if ts_code else '所有'}"
        )

        try:
            # 使用标准的交易日批次生成工具
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            # 根据是否有指定可转债代码选择不同的批次大小
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
