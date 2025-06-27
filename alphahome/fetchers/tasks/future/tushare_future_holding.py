#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货每日成交及持仓排名 (fut_holding) 更新任务
获取期货每日成交及持仓排名数据。
继承自 TushareTask。
使用单交易日按交易所分批。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入单交易日批次生成工具函数
from ...sources.tushare.batch_utils import generate_single_date_batches

# logger 由 Task 基类提供
# import logging
# logger = logging.getLogger(__name__)


@task_register()
class TushareFutureHoldingTask(TushareTask):
    """获取期货每日成交及持仓排名"""

    # 1. 核心属性
    name = "tushare_future_holding"
    description = "获取期货持仓数据"
    table_name = "future_holding"
    primary_keys = ["trade_date", "symbol", "broker", "exchange"]
    date_column = "trade_date"
    default_start_date = (
        "20100416"  # 可根据实际期货市场数据最早日期调整【中金所最早数据】
    )

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 4000  # 用户指定

    # 2. TushareTask 特有属性
    api_name = "fut_holding"
    fields = [
        "trade_date",
        "symbol",
        "broker",
        "vol",
        "vol_chg",
        "long_hld",
        "long_chg",
        "short_hld",
        "short_chg",
        "exchange",
    ]

    SUPPORTED_EXCHANGES = [
        "CFFEX"
    ]  # 暂时仅保存中金所数据; 其他交易所代码:'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX'

    # 3. 列名映射
    column_mapping = {"vol": "volume"}

    # 4. 数据类型转换
    transformations = {
        "vol": int,  # 原始字段名
        "vol_chg": int,
        "long_hld": int,
        "long_chg": int,
        "short_hld": int,
        "short_chg": int,
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "symbol": {
            "type": "VARCHAR(30)",
            "constraints": "NOT NULL",
        },  # 合约代码，可能包含交易所后缀或特定产品代码
        "broker": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},  # 期货公司名称
        "exchange": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},  # 交易所代码
        "volume": {"type": "BIGINT"},  # 成交量 (映射后)
        "vol_chg": {"type": "BIGINT"},  # 成交量变化
        "long_hld": {"type": "BIGINT"},  # 持买仓量
        "long_chg": {"type": "BIGINT"},  # 持买仓量变化
        "short_hld": {"type": "BIGINT"},  # 持卖仓量
        "short_chg": {"type": "BIGINT"},  # 持卖仓量变化
        # update_time 会自动添加
    }

    # 6. 自定义索引 (主键索引会自动创建)
    indexes = [
        {"name": "idx_fut_hold_sym", "columns": "symbol"},
        {"name": "idx_fut_hold_broker", "columns": "broker"},
        # exchange 已经是主键一部分，但单独查询也可能需要
        {"name": "idx_fut_hold_exch", "columns": "exchange"},
        {"name": "idx_fut_hold_upd", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        lambda df: df['trade_date'].notna(),
        lambda df: df['symbol'].notna(),
        lambda df: df['broker'].notna(),
        lambda df: df['exchange'].notna(),
        lambda df: df['volume'] >= 0,
        lambda df: df['long_hld'] >= 0,
        lambda df: df['short_hld'] >= 0,
    ]

    # def __init__(
    #     self, db_connection, api_token: Optional[str] = None, api: Optional[Any] = None
    # ):
    #     """初始化任务"""
    #     super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
    #     self.logger.info(f"任务 {self.name} 已配置初始化。")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。按交易所分批，每个交易所内按单交易日生成批次。
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

        all_batches: List[Dict] = []
        self.logger.info(
            f"任务 {self.name}: 为交易所列表 {self.SUPPORTED_EXCHANGES} 生成单交易日批次，范围: {start_date} 到 {end_date}"
        )

        for ex_code in self.SUPPORTED_EXCHANGES:
            try:
                # 注意：generate_single_date_batches 的 exchange 参数用于获取该交易所的交易日历
                # additional_params 中的 exchange 用于 Tushare API 调用
                batches_for_exchange = await generate_single_date_batches(
                    start_date=start_date,
                    end_date=end_date,
                    ts_code=None,  # 获取该交易所下所有合约的排名，不指定特定 symbol
                    exchange=ex_code,  # 用于 get_trade_days_between 内部获取交易日
                    additional_params={
                        "exchange": ex_code
                    },  # 此 exchange 参数会传递给 Tushare API
                    logger=self.logger,
                )
                if batches_for_exchange:
                    all_batches.extend(batches_for_exchange)
                    self.logger.info(
                        f"任务 {self.name}: 为交易所 {ex_code} 生成了 {len(batches_for_exchange)} 个批次。"
                    )
                else:
                    self.logger.info(
                        f"任务 {self.name}: 交易所 {ex_code} 在指定日期范围 {start_date}-{end_date} 内无批次生成（可能无交易日）。"
                    )
            except Exception as e:
                self.logger.error(
                    f"任务 {self.name}: 为交易所 {ex_code} 生成批次时出错: {e}",
                    exc_info=True,
                )
                # 选择继续为其他交易所生成批次，或在此处抛出异常停止整个任务
                # 此处选择继续

        self.logger.info(f"任务 {self.name}: 总共生成 {len(all_batches)} 个批次。")
        return all_batches

