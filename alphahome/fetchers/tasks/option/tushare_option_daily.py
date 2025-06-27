#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货及股票期权日线行情 (opt_daily) 更新任务
获取期货及期权的日线交易数据。
继承自 TushareTask。
使用自然日按生成批次。
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
class TushareOptionDailyTask(TushareTask):
    """获取期货及股票期权日线行情数据"""

    # 1. 核心属性
    name = "tushare_option_daily"
    description = "获取期货及股票期权日线行情数据"
    table_name = "option_daily"
    primary_keys = ["ts_code", "trade_date"]  # 合约代码和交易日期组合是主键
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20150209"  # 上海50ETF期权上市日期
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 15000  # 与API单次最大返回条数一致

    # 指定获取数据的交易所列表
    option_exchanges = ["SSE", "SZSE", "CFFEX"]
    # 自然日分批
    batch_natural_days_week = 30

    # 2. TushareTask 特有属性
    api_name = "opt_daily"
    # Tushare opt_daily 接口实际返回的字段 (根据文档 https://tushare.pro/document/2?doc_id=159)
    fields = [
        "ts_code",
        "trade_date",
        "exchange",
        "pre_settle",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "settle",
        "vol",
        "amount",
        "oi",
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
        "vol": float,  # 原始字段名
        "amount": float,
        "oi": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "exchange": {"type": "VARCHAR(10)"},
        "pre_settle": {"type": "FLOAT"},
        "pre_close": {"type": "FLOAT"},
        "open": {"type": "FLOAT"},
        "high": {"type": "FLOAT"},
        "low": {"type": "FLOAT"},
        "close": {"type": "FLOAT"},
        "settle": {"type": "FLOAT"},  # 结算价
        "pre_settle": {"type": "FLOAT"},  # 昨日结算价
        "volume": {"type": "FLOAT"},  # 成交量 (映射后的名称)
        "amount": {"type": "FLOAT"},  # 成交额
        "oi": {"type": "FLOAT"},  # 持仓量
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_option_daily_code_date", "columns": ["ts_code", "trade_date"]},
        {"name": "idx_option_daily_code", "columns": "ts_code"},
        {"name": "idx_option_daily_date", "columns": "trade_date"},
        {"name": "idx_option_daily_exchange", "columns": "exchange"},
        {"name": "idx_option_daily_update_time", "columns": "update_time"},
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表，按指定交易所和自然日分批。
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

        all_batches = []
        self.logger.info(
            f"任务 {self.name}: 按交易所和自然日生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            for exchange in self.option_exchanges:
                self.logger.info(
                    f"任务 {self.name}: 为交易所 {exchange} 生成自然日批次..."
                )
                # 使用自然日批次工具函数
                # 不传入 ts_code，获取该交易所全市场的日线数据
                exchange_batches = await generate_natural_day_batches(
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=self.batch_natural_days_week,
                    ts_code=None,  # 不按代码分批
                    logger=self.logger,
                )

                # 为该交易所的每个批次添加 exchange 参数
                for batch in exchange_batches:
                    batch["exchange"] = exchange

                all_batches.extend(exchange_batches)
                self.logger.info(
                    f"任务 {self.name}: 交易所 {exchange} 生成了 {len(exchange_batches)} 个批次。"
                )

            self.logger.info(
                f"任务 {self.name}: 成功生成总计 {len(all_batches)} 个批次。"
            )
            return all_batches

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            # 抛出异常以便上层调用者感知
            raise RuntimeError(f"任务 {self.name}: 生成批次失败") from e

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        调用基类方法完成通用处理（日期转换、transformations 应用）。
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            exchange = kwargs.get("exchange", "未知交易所")
            self.logger.info(
                f"任务 {self.name}: process_data 接收到 {exchange} 的空 DataFrame，跳过处理。"
            )
            return df

        # 调用基类方法完成 transformations 应用、日期转换等
        # df = super().process_data(df, **kwargs)
        # 基类已经处理了 transformations 和日期转换。
        # 此处可以添加 opt_daily 特有的额外处理，如果需要的话。

        self.logger.info(
            f"任务 {self.name}: process_data 被调用，返回 DataFrame (行数: {len(df)}). 基类处理已完成。"
        )
        return df

    # 验证规则：使用 validations 列表（真正生效的验证机制）
    validations = [
        lambda df: df['ts_code'].notna(),       # 期权代码不能为空
        lambda df: df['trade_date'].notna(),    # 交易日期不能为空
        lambda df: (df['close'] >= 0),          # 收盘价不能为负
        lambda df: (df['vol'] >= 0),            # 成交量不能为负
    ]
    

