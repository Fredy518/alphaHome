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
from alphahome.common.task_system.task_decorator import task_register

# 导入单交易日批次生成工具函数
from ...tools.batch_utils import generate_single_date_batches

# logger 由 Task 基类提供
# import logging
# logger = logging.getLogger(__name__)


@task_register()
class TushareFutureHoldingTask(TushareTask):
    """获取期货每日成交及持仓排名"""

    # 1. 核心属性
    name = "tushare_future_holding"
    description = "获取期货每日成交及持仓排名"
    table_name = "tushare_future_holding"
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

    def __init__(
        self, db_connection, api_token: Optional[str] = None, api: Optional[Any] = None
    ):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api)
        self.logger.info(f"任务 {self.name} 已配置初始化。")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。按交易所分批，每个交易所内按单交易日生成批次。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

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
                    date_field="trade_date",  # API 使用的日期字段名
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

    async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            self.logger.info(
                f"任务 {self.name}: process_data 接收到空 DataFrame，跳过处理。"
            )
            return df

        # 确保 API 返回的 'exchange' 字段（如果存在且应该填充）被正确处理
        # 如果 API 返回的 DataFrame 中可能没有 'exchange' 列，但我们批处理时指定了，
        # 我们需要确保它被添加到 DataFrame 中，因为它是主键一部分。
        # Tushare fut_holding 的 fields 包含 'exchange'，所以API应该会返回它。
        # 如果 fut_holding 在某些情况下（如未指定 symbol）不返回 exchange，则需要在这里填充。
        # 但根据接口文档，exchange 是输出字段，应该总是存在。

        # kwargs 包含API调用时的参数，包括我们通过 additional_params 传递的 'exchange'
        # api_call_exchange = kwargs.get('exchange')
        # if api_call_exchange and 'exchange' not in df.columns:
        #     df['exchange'] = api_call_exchange
        # elif api_call_exchange and df['exchange'].isnull().any():
        #     # 如果列存在但有空值，用API调用时的 exchange 填充
        #     df['exchange'] = df['exchange'].fillna(api_call_exchange)

        # 调用基类方法完成通用处理（列名映射、transformations 应用、日期转换等）
        # df = super().process_data(df, **kwargs) # 基类会处理 column_mapping 和 transformations

        self.logger.info(
            f"任务 {self.name}: process_data 处理 DataFrame (行数: {len(df)})."
        )
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        """
        if df.empty:
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。"
            )
            return df

        # 使用映射后的列名 'volume'
        critical_cols = ["trade_date", "symbol", "broker", "exchange", "volume"]
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 检查关键字段是否全部为空行
        # 替换空字符串为 NA 以便 isnull() 检测，对数值列不适用，但对字符主键列有用
        df_check = df.copy()
        for col in ["symbol", "broker", "exchange"]:  # 字符串列
            if col in df_check.columns:
                df_check[col] = df_check[col].replace("", pd.NA)

        # 检查主键列（包括字符串和日期）是否有空值
        # trade_date 由 TushareDataTransformer 转换为日期对象，不太可能为 pd.NA 除非原始数据就有问题
        # symbol, broker, exchange 是字符串，现在已处理空字符串
        # 主键列不应有空值
        for pk_col in self.primary_keys:
            if df_check[pk_col].isnull().any():
                error_msg = (
                    f"任务 {self.name}: 数据验证失败 - 主键字段 '{pk_col}' 包含空值。"
                )
                self.logger.error(error_msg)
                # 可以选择记录具体哪一行，但为了简化，先抛出错误
                # self.logger.error(f"空值所在行:\n{{df_check[df_check[pk_col].isnull()]}}")
                raise ValueError(error_msg)

        # 检查数值型的 volume 是否有不合理的值 (例如负数)，但成交量为0是可能的
        # if 'volume' in df.columns and (df['volume'] < 0).any():
        #     error_msg = f"任务 {self.name}: 数据验证失败 - 'volume' 字段包含负值。"
        #     self.logger.error(error_msg)
        #     raise ValueError(error_msg)

        self.logger.info(
            f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。"
        )
        return df
