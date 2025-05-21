#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货及期权合约基础信息 (fut_basic) 更新任务
每次执行时，获取所有交易所的期货及期权合约基础信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import pandas as pd
import logging
from typing import Dict, List, Any

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)

@task_register()
class TushareFutureBasicTask(TushareTask):
    """获取期货及期权合约基础信息 (全量更新，按交易所分批)"""

    # 1. 核心属性
    name = "tushare_future_basic"
    description = "获取期货及期权合约基础信息"
    table_name = "tushare_future_basic"
    primary_keys = ["ts_code"] # 合约代码是唯一主键
    date_column = None # 该任务不以日期为主，全量更新
    default_start_date = None # 全量任务不需要起始日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    # 考虑到需要按交易所分批，可以适当增加并发限制
    default_concurrent_limit = 3
    default_page_size = 8000 # Tushare 文档未明确给出 fut_basic 的分页限制，沿用 stock_basic 设置

    # 2. TushareTask 特有属性
    api_name = "fut_basic"
    # Tushare fut_basic 接口实际返回的字段 (根据文档 [https://tushare.pro/document/2?doc_id=135])
    fields = [
        'ts_code', 'symbol', 'exchange', 'name', 'fut_code', 'multiplier',
        'trade_unit', 'per_unit', 'quote_unit', 'quote_unit_desc',
        'd_mode_desc', 'list_date', 'delist_date', 'd_month', 'last_ddate',
        'trade_time_desc'
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理，其他需要转换的列可以在这里添加)
    # multiplier, per_unit 可能需要转换为 numeric
    transformations = {
        "multiplier": lambda x: pd.to_numeric(x, errors='coerce'),
        "per_unit": lambda x: pd.to_numeric(x, errors='coerce'),
    }


    # 5. 数据库表结构 (根据 fields 和文档类型说明定义)
    schema = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"}, # 期货代码可能比股票长
        "symbol": {"type": "VARCHAR(10)"},
        "exchange": {"type": "VARCHAR(10)"},
        "name": {"type": "VARCHAR(50)"},
        "fut_code": {"type": "VARCHAR(10)"},
        "multiplier": {"type": "FLOAT"}, # 合约乘数
        "trade_unit": {"type": "VARCHAR(10)"},
        "per_unit": {"type": "FLOAT"}, # 交易单位
        "quote_unit": {"type": "VARCHAR(10)"},
        "quote_unit_desc": {"type": "VARCHAR(50)"},
        "d_mode_desc": {"type": "VARCHAR(50)"},
        "list_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "d_month": {"type": "VARCHAR(10)"}, # 交割月份可能是字符串 'YYYYMM'
        "last_ddate": {"type": "DATE"}, # 最后交割日
        "trade_time_desc": {"type": "VARCHAR(255)"} # 交易时间说明可能较长
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_future_basic_ts_code", "columns": "ts_code"},
        {"name": "idx_future_basic_exchange", "columns": "exchange"},
        {"name": "idx_future_basic_fut_code", "columns": "fut_code"},
        {"name": "idx_future_basic_update_time", "columns": "update_time"}
    ]

    def __init__(self, db_connection, api_token=None, api=None):
        """初始化任务"""
        super().__init__(db_connection, api_token=api_token, api=api)
        self.logger.info(f"任务 {self.name} 已配置初始化。")

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 fut_basic，按交易所分批获取。
        """
        # Tushare 文档中列出的交易所代码
        exchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
        batch_list = [{'exchange': exc} for exc in exchanges]
        self.logger.info(f"任务 {self.name}: 按交易所分批获取模式，生成 {len(batch_list)} 个批次。")
        return batch_list

    async def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        此方法可以被子类覆盖以实现特定的数据转换逻辑。
        TushareDataTransformer 已处理通用日期和 transformations 中的转换。
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            # 注意：当某个交易所没有数据时，Tushare可能返回空DataFrame，这是正常情况
            exchange = kwargs.get('exchange', '未知交易所')
            self.logger.info(f"任务 {self.name}: process_data 接收到 {exchange} 的空 DataFrame，跳过处理。")
            return df

        # 调用基类方法完成其他处理 (应用 transformations, 排序等)
        # df = super().process_data(df, **kwargs)
        # 基类已经处理了 transformations 和日期转换。
        # 此处可以添加 fut_basic 特有的额外处理，如果需要的话。
        # 例如，处理 d_month 列，如果需要转换为特定格式。

        # 目前，没有 fut_basic 特有的额外处理，直接返回。
        self.logger.info(f"任务 {self.name}: process_data 被调用，返回 DataFrame (行数: {len(df)}). TushareDataTransformer 已完成主要处理。")
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空 (注意空是允许的，如果某个交易所没有合约)。
        - 检查关键业务字段 ('ts_code', 'name', 'exchange') 是否全部为空。
        """
        if df.empty:
            exchange = kwargs.get('exchange', '未知交易所')
            self.logger.warning(f"任务 {self.name}: 从 API 获取 {exchange} 的 DataFrame 为空，无需验证。")
            return df

        critical_cols = ['ts_code', 'name', 'exchange']
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 替换空字符串为 NA 以便 isnull() 检测
        df_check = df[critical_cols].replace('', pd.NA)
        if df_check.isnull().all(axis=1).all():
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。"
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        self.logger.info(f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。")
        return df
