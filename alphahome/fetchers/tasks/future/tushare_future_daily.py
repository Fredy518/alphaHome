#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货及期权日线行情 (fut_daily) 更新任务
获取期货及期权的日线交易数据。
继承自 TushareTask。
使用自然日按月生成批次。
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入自然日批次生成工具函数
from ...sources.tushare.batch_utils import generate_natural_day_batches

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareFutureDailyTask(TushareTask):
    """获取期货及期权日线行情数据"""

    # 1. 核心属性
    name = "tushare_future_daily"
    description = "获取期货及期权日线行情数据"
    table_name = "future_daily"
    primary_keys = ["ts_code", "trade_date"]  # 合约代码和交易日期组合是主键
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "19950401"  # 中国期货市场较早的交易日期，可根据实际情况调整

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 2000  # 单次最大2000行

    # 2. TushareTask 特有属性
    api_name = "fut_daily"
    # Tushare fut_daily 接口实际返回的字段 (根据文档 [https://tushare.pro/document/2?doc_id=138])
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "settle",
        "pre_settle",
        "zd_1d",
        "zd_dt",
        "vol",
        "amount",
        "oi",
        "oi_chg",
    ]

    # 3. 列名映射 (API字段名与数据库列名不完全一致，进行映射)
    column_mapping = {"vol": "volume"}  # 将vol映射为volume

    # 4. 数据类型转换 (日期列在基类处理，数值列转换为 float)
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "pre_close": float,
        "settle": float,
        "pre_settle": float,
        "zd_1d": float,
        "zd_dt": float,
        "vol": float,  # 原始字段名
        "amount": float,
        "oi": float,
        "oi_chg": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {
            "type": "VARCHAR(20)",
            "constraints": "NOT NULL",
        },  # 期货代码可能比股票长
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "pre_close": {"type": "NUMERIC(15,4)"},
        "settle": {"type": "NUMERIC(15,4)"},  # 结算价
        "pre_settle": {"type": "NUMERIC(15,4)"},  # 昨日结算价
        "zd_1d": {"type": "NUMERIC(15,4)"},  # 涨跌额
        "zd_dt": {"type": "NUMERIC(15,4)"},  # 涨跌幅
        "volume": {"type": "NUMERIC(20,4)"},  # 成交量 (映射后的名称)
        "amount": {"type": "NUMERIC(20,4)"},  # 成交额
        "oi": {"type": "NUMERIC(20,4)"},  # 持仓量
        "oi_chg": {"type": "NUMERIC(20,4)"},  # 持仓量变化
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_future_daily_code_date", "columns": ["ts_code", "trade_date"]},
        {"name": "idx_future_daily_code", "columns": "ts_code"},
        {"name": "idx_future_daily_date", "columns": "trade_date"},
        {"name": "idx_future_daily_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['trade_date'].notna(),
        lambda df: df['close'] > 0,
        lambda df: df['high'] >= df['low'],
        lambda df: df['volume'] >= 0,
        lambda df: df['amount'] >= 0,
        lambda df: df['oi'] >= 0, # 持仓量不能为负
    ]

    # 8. 分批配置
    # 自然日按月分批，一个月大约30天
    batch_natural_days_month = 30

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表，使用自然日按月分批。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            from datetime import datetime
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        self.logger.info(
            f"任务 {self.name}: 使用自然日按月生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用自然日批次工具函数，批次大小设置为一个月（约30天自然日）
            # 不传入 ts_code，获取全市场的日线数据
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_natural_days_month,
                ts_code=None,  # 不按代码分批
                logger=self.logger,
            )
            self.logger.info(
                f"任务 {self.name}: 成功生成 {len(batch_list)} 个自然日批次。"
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True
            )
            # 抛出异常以便上层调用者感知
            raise RuntimeError(f"任务 {self.name}: 生成自然日批次失败") from e

